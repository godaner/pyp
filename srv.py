import logging
import socket
import sys
import threading
import traceback
import uuid

import yaml

import protocol


class Srv:
    def __init__(self, config_file: str):
        with open(config_file, 'r') as f:
            self.conf = yaml.safe_load(f)
        self.logger = logging.getLogger()
        self.user_conn_create_resp_event = {}
        self.user_conn_create_resp_pkg = {}
        self.conn_id_mapping_user_conn = {}
        self.client_id_mapping_client_conn = {}
        self.client_id_mapping_user_conn = {}
        self.client_id_mapping_listen_port = {}

    def __str__(self):
        return str(self.conf)

    def start(self):
        self.logger.info("start client!")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            host = self.conf["server"]["host"]
        except Exception as e:
            self.logger.info("get host from config fail: {0}".format(e))
            raise e
        try:
            port = self.conf["server"]["port"]
        except Exception as e:
            self.logger.info("get port from config fail: {0}".format(e))
            raise e
        s.bind((host, port))
        s.listen()
        while 1:
            try:
                client_conn, addr = s.accept()
                self.logger.info("accept client conn addr: {0}".format(addr))
                t = threading.Thread(target=self.__handle_client_conn__, args=(client_conn,))
                t.start()
            except BaseException as e:
                self.logger.error("exit handle client conn: {0}!".format(e))
                try:
                    s.shutdown(socket.SHUT_RDWR)
                    s.close()
                except BaseException as e:
                    ...
                self.__when_listen_conn_close__()
                raise e

    def __when_listen_conn_close__(self):
        client_ids = []
        for client_id in self.client_id_mapping_client_conn:
            client_ids.append(client_id)
        for client_id in client_ids:
            client_conn = self.client_id_mapping_client_conn[client_id]
            self.logger.info("closing client conn: {0}".format(str(client_conn)))
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
            except BaseException as e:
                ...

    def __when_client_conn_close__(self, client_id: str):

        listens = self.client_id_mapping_listen_port.pop(client_id)
        for listen in listens:
            self.logger.info("closing listen_port: {0}".format(str(listens[listen])))
            try:
                listens[listen].shutdown(socket.SHUT_RDWR)
                listens[listen].close()
            except BaseException as e:
                ...
        user_conns = self.client_id_mapping_user_conn.pop(client_id)
        for user_conn in user_conns:
            self.logger.info("closing listen_port: {0}".format(str(user_conns[user_conn])))
            try:
                user_conns[user_conn].shutdown(socket.SHUT_RDWR)
                user_conns[user_conn].close()
            except BaseException as e:
                ...

    def __handle_client_conn__(self, client_conn: socket.socket):
        client_id = str(uuid.uuid4())
        self.client_id_mapping_listen_port[client_id] = {}
        self.client_id_mapping_user_conn[client_id] = {}
        self.client_id_mapping_client_conn[client_id] = client_conn
        try:
            while 1:
                len_bs = client_conn.recv(32)
                if len(len_bs) == 0:
                    raise Exception("EOF")
                len_int = int.from_bytes(len_bs, 'big')
                bs = client_conn.recv(len_int)
                pkg = protocol.un_serialize(bs)
                if pkg.ty == protocol.TYPE_CLIENT_HELLO_REQ:
                    self.logger.info("recv type: {0}!".format(pkg.ty))
                    threading.Thread(target=self.__handle_client_hello_req__,
                                     args=(client_conn, client_id, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_USER_CREATE_CONN_RESP:
                    self.logger.info("recv type: {0}!".format(pkg.ty))
                    threading.Thread(target=self.__handle_user_create_conn_resp__,
                                     args=(client_conn, client_id, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_PAYLOAD:
                    # threading.Thread(target=self.__handle_payload__, args=(client_conn, client_id, pkg)).start()
                    self.__handle_payload__(client_conn, client_id, pkg)
                    continue
                self.logger.error("recv client pkg type error!")
        except BaseException as e:
            self.logger.error("client conn recv err: {0}!".format(e))
            # self.logger.error("client conn recv err: {0}!".format(traceback.format_exc()))
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
            except BaseException as e:
                ...
            try:
                self.client_id_mapping_client_conn.pop(client_id)
            except BaseException as e:
                ...
            self.__when_client_conn_close__(client_id)

    def __handle_user_create_conn_resp__(self, client_conn: socket.socket, client_id: str, pkg: protocol.package):
        try:
            self.user_conn_create_resp_pkg[pkg.conn_id] = pkg
            wait = self.user_conn_create_resp_event[pkg.conn_id]
            wait.set()
        except BaseException as e:
            self.logger.error("can not find create user conn resp event: {0}!".format(e))

    def __handle_client_hello_req__(self, client_conn: socket.socket, client_id: str, pkg: protocol.package):
        if not len(pkg.listen_ports):
            self.logger.error("listen ports is empty!")
        error = ""
        listen_ports = {}
        try:
            for listen_port in pkg.listen_ports:
                listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listen.bind(("0.0.0.0", listen_port))
                listen.listen()
                listen_ports[listen_port] = listen
            for listen_port in listen_ports:
                listen = listen_ports[listen_port]
                self.client_id_mapping_listen_port[client_id][id(listen)] = listen
                t = threading.Thread(target=self.__listen_port__,
                                     args=(client_conn, client_id, listen_ports[listen_port], listen_port))
                t.start()
        except BaseException as e:
            for listen_port in listen_ports:
                listen_ports[listen_port].shutdown(socket.SHUT_RDWR)
                listen_ports[listen_port].close()
            self.logger.error("listen ports err: {0}!".format(e))
            error = str(e)
        finally:
            bs = protocol.serialize(protocol.package(ty=protocol.TYPE_CLIENT_HELLO_RESP, error=error))
            client_conn.send(len(bs).to_bytes(32, 'big') + bs)

    def __listen_port__(self, client_conn, client_id: str, listen: socket.socket, listen_port):
        try:
            while 1:
                user_conn, addr = listen.accept()
                self.logger.info("accept user conn addr: {0}".format(addr))
                t = threading.Thread(target=self.__handle_user_conn__,
                                     args=(client_conn, client_id, user_conn, listen_port))
                t.start()
        except BaseException as e:
            self.logger.info("accept user conn err, listen_port is: {0}, err is: {1}".format(listen_port, e))
            try:
                listen.shutdown(socket.SHUT_RDWR)
                listen.close()
            except BaseException as e:
                ...
            # self.client_id_mapping_listen_port[client_id].pop(id(listen))

    def __handle_user_conn__(self, client_conn: socket.socket, client_id: str, user_conn: socket.socket, listen_port):
        conn_id = str(uuid.uuid4())
        self.client_id_mapping_user_conn[client_id][id(user_conn)] = user_conn
        event = threading.Event()
        self.user_conn_create_resp_event[conn_id] = event
        bs = protocol.serialize(
            protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_REQ, conn_id=conn_id, listen_ports=[listen_port],
                             error=""))
        client_conn.send(len(bs).to_bytes(32, 'big') + bs)
        # wait user conn create resp
        try:
            event.wait(100)
        except BaseException as e:
            try:
                user_conn.shutdown(socket.SHUT_RDWR)
                user_conn.close()
            except BaseException as e:
                ...
            self.logger.error("wait user conn create resp err: {0}".format(e))
            raise e
        finally:
            self.user_conn_create_resp_event.pop(conn_id)
            try:
                pkg = self.user_conn_create_resp_pkg.pop(conn_id)
                if pkg.error != "":
                    try:
                        user_conn.shutdown(socket.SHUT_RDWR)
                        user_conn.close()
                    except BaseException as e:
                        ...
                    self.logger.error("wait user conn create resp pkg err: {0}".format(pkg.error))
                    raise Exception("user conn create resp error: {0}".format(pkg.error))
            except BaseException as e:
                try:
                    user_conn.shutdown(socket.SHUT_RDWR)
                    user_conn.close()
                except BaseException as e:
                    ...
                self.logger.error("can not find user conn create resp pkg: {0}".format(e))
                raise Exception("can not find user conn create resp pkg: {0}".format(e))
        self.conn_id_mapping_user_conn[conn_id] = user_conn
        try:
            while 1:
                bs = user_conn.recv(1024)
                if len(bs) == 0:
                    raise Exception("EOF")
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_PAYLOAD, payload=bs, conn_id=conn_id, error=""))
                client_conn.send(len(bs).to_bytes(32, 'big') + bs)
        except BaseException as e:
            self.logger.error("user conn recv err: {0}!".format(e))
            try:
                user_conn.shutdown(socket.SHUT_RDWR)
                user_conn.close()
            except BaseException as e:
                ...
            try:
                self.conn_id_mapping_user_conn.pop(conn_id)
            except BaseException as e:
                ...

    def __handle_payload__(self, client_conn: socket.socket, client_id: str, pkg: protocol.package):
        conn_id = pkg.conn_id
        try:
            user_conn = self.conn_id_mapping_user_conn[conn_id]
            user_conn.send(pkg.payload)
        except BaseException as e:
            self.logger.error("forward payload to user conn err: {0}!".format(e))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        config_file = "./srv.yaml"
    else:
        config_file = sys.argv[1]
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)d %(thread)s %(message)s')
    logger = logging.getLogger()
    logger.info("use config file: {0}!".format(config_file))
    cli = Srv(config_file)
    logger.info("server info: {0}!".format(cli))
    try:
        cli.start()
    except BaseException as e:
        logger.info("server err:{0} {1}!".format(e, traceback.format_exc()))

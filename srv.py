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
        self.client_id_mapping_user_conn = {}
        self.client_id_mapping_listen_port = {}
        self.app_close_conn = {}

    def __str__(self):
        return str(self.conf)

    def start(self):
        self.logger.info("start client!")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            host = self.conf["server"]["host"]
        except Exception as e:
            self.logger.info("get host from config fail: {0}".format(e))
            return
        try:
            port = self.conf["server"]["port"]
        except Exception as e:
            self.logger.info("get port from config fail: {0}".format(e))
            return
        s.bind((host, port))
        s.listen()
        while 1:
            client_id = str(uuid.uuid4())
            self.client_id_mapping_listen_port[client_id] = {}
            self.client_id_mapping_user_conn[client_id] = {}
            try:
                client_conn, addr = s.accept()
                self.logger.info("accept client conn addr: {0}".format(addr))
                t = threading.Thread(target=self.__handle_client_conn__, args=(client_conn, client_id))
                t.start()
            except (SystemExit, KeyboardInterrupt, GeneratorExit) as e:
                self.logger.error("exit handle client conn: {0}!".format(e))
                self.__when_client_conn_close__(client_id)
                return
            except Exception as e:
                self.logger.error("accept client conn fail: {0}!".format(e))
                self.__when_client_conn_close__(client_id)

    def __when_client_conn_close__(self, client_id: str):
        listens = self.client_id_mapping_listen_port.pop(client_id)
        for listen in listens:
            listens[listen].close()
        user_conns = self.client_id_mapping_user_conn.pop(client_id)
        for user_conn in user_conns:
            user_conns[user_conn].close()

    def __handle_client_conn__(self, client_conn: socket.socket, client_id: str):
        while 1:
            len_bs = client_conn.recv(32)
            len_int = int.from_bytes(len_bs, 'big')
            bs = client_conn.recv(len_int)
            pkg = protocol.un_serialize(bs)
            self.logger.info("recv type: {0}!".format(pkg.ty))
            if pkg.ty == protocol.TYPE_CLIENT_HELLO_REQ:
                self.__handle_client_hello_req__(client_conn, client_id, pkg)
                continue
            if pkg.ty == protocol.TYPE_USER_CREATE_CONN_RESP:
                self.__handle_user_create_conn_resp__(client_conn, client_id, pkg)
                continue
            if pkg.ty == protocol.TYPE_CLOSE_USER_CONN_REQ:
                self.__handle_close_user_conn__(client_conn, client_id, pkg)
                continue
            if pkg.ty == protocol.TYPE_PAYLOAD:
                self.__handle_payload__(client_conn, client_id, pkg)
                continue
            self.logger.error("recv client pkg type error!")

    def __handle_user_create_conn_resp__(self, client_conn: socket.socket, client_id: str, pkg: protocol.package):
        self.user_conn_create_resp_pkg[pkg.conn_id] = pkg
        wait = self.user_conn_create_resp_event[pkg.conn_id]
        wait.set()

    def __handle_client_hello_req__(self, client_conn: socket.socket, client_id: str, pkg: protocol.package):
        if not len(pkg.listen_ports):
            self.logger.error("listen ports is empty!")
        error = ""
        listen_ports = {}
        try:
            for listen_port in pkg.listen_ports:
                listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
                listen_ports[listen_port].close()
            self.logger.error("listen ports err: {0}!".format(e))
            error = str(e)
        finally:
            bs = protocol.serialize(protocol.package(ty=protocol.TYPE_CLIENT_HELLO_RESP, error=error))
            client_conn.send(len(bs).to_bytes(32, 'big'))
            client_conn.send(bs)

    def __listen_port__(self, client_conn, client_id: str, listen, listen_port):
        while 1:
            try:
                user_conn, addr = listen.accept()
                self.logger.info("accept user conn addr: {0}".format(addr))
                t = threading.Thread(target=self.__handle_user_conn__,
                                     args=(client_conn, client_id, user_conn, listen_port))
                t.start()
            except BaseException as e:
                self.logger.info("accept user conn err: {0}".format(e))
                self.client_id_mapping_listen_port[client_id].pop(id(listen))

    def __handle_user_conn__(self, client_conn: socket.socket, client_id: str, user_conn: socket.socket, listen_port):
        conn_id = str(uuid.uuid4())
        self.client_id_mapping_user_conn[client_id][id(user_conn)] = user_conn
        event = threading.Event()
        self.user_conn_create_resp_event[conn_id] = event
        bs = protocol.serialize(
            protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_REQ, conn_id=conn_id, listen_ports=[listen_port],
                             error=""))
        client_conn.send(len(bs).to_bytes(32, 'big'))
        client_conn.send(bs)
        # wait user conn create resp
        try:
            event.wait(1)
        except BaseException as e:
            self.logger.error("wait user conn create resp err: {0}".format(e))
            raise e
        finally:
            self.user_conn_create_resp_event.pop(conn_id)
            pkg = self.user_conn_create_resp_pkg.pop(conn_id)
            if pkg.error != "":
                raise Exception("user conn create resp error: {0}".format(pkg.error))
        self.conn_id_mapping_user_conn[conn_id] = user_conn
        try:
            while 1:
                bs = user_conn.recv(1024)
                if len(bs) == 0:
                    raise Exception("EOF")
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_PAYLOAD, payload=bs, conn_id=conn_id, error=""))
                client_conn.send(len(bs).to_bytes(32, 'big'))
                client_conn.send(bs)
        except BaseException as e:
            # raise e
            self.logger.error("user conn recv err: {0}!".format(e))
        finally:
            self.client_id_mapping_user_conn[client_id].pop(id(user_conn))
            try:
                self.app_close_conn.pop(conn_id)
            except BaseException as e:
                try:
                    user_conn.close()
                    bs = protocol.serialize(
                        protocol.package(ty=protocol.TYPE_CLOSE_APP_CONN_REQ, conn_id=conn_id, error=""))
                    client_conn.send(len(bs).to_bytes(32, 'big'))
                    client_conn.send(bs)
                except BaseException as e:
                    self.logger.error("close user conn err: {0}!".format(e))

    def __handle_close_user_conn__(self, client_conn: socket.socket, client_id: str, pkg: protocol.package):
        self.app_close_conn[pkg.conn_id] = True
        self.conn_id_mapping_user_conn[pkg.conn_id].close()

    def __handle_payload__(self, client_conn: socket.socket, client_id: str, pkg: protocol.package):
        conn_id = pkg.conn_id
        user_conn = self.conn_id_mapping_user_conn[conn_id]
        user_conn.send(pkg.payload)


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
    except Exception as e:
        logger.info("server err: {0}!".format(e))
        logger.info("server err: {0}!".format(traceback.format_exc()))

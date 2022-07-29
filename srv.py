import logging
import socket
import sys
import threading
import time
import traceback

import yaml

import protocol
import sock


class Srv:
    def __init__(self, conf):
        self.id = 0
        self.secret = ""
        self.conf = conf
        self.logger = logging.getLogger()
        self.user_conn_create_resp_event = {}
        self.user_conn_create_resp_pkg = {}
        self.conn_id_mapping_client_app_conn = {}
        self.conn_id_mapping_user_conn = {}
        self.client_id_mapping_client_conn = {}
        self.client_id_mapping_client_app_conn = {}
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
        try:
            self.secret = self.conf["server"]["secret"]
        except Exception as e:
            self.logger.info("get secret from config fail: {0}".format(e))
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
                except BaseException as ee:
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

    def __when_client_conn_close__(self, client_id: int):
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
            self.logger.info("closing user conn: {0}".format(str(user_conns[user_conn])))
            try:
                user_conns[user_conn].shutdown(socket.SHUT_RDWR)
                user_conns[user_conn].close()
            except BaseException as e:
                ...
        client_app_conns = self.client_id_mapping_client_app_conn.pop(client_id)
        for client_app_conn in client_app_conns:
            self.logger.info("closing client app conn: {0}".format(str(client_app_conns[client_app_conn])))
            try:
                client_app_conns[client_app_conn].shutdown(socket.SHUT_RDWR)
                client_app_conns[client_app_conn].close()
            except BaseException as e:
                ...

    def __handle_client_conn__(self, client_conn: socket.socket):
        client_id = ""
        try:
            while 1:
                len_bs = sock.recv_full(client_conn, 4)
                if len(len_bs) == 0:
                    raise Exception("EOF")
                len_int = int.from_bytes(len_bs, 'big')
                if len_int == 0:
                    continue
                bs = sock.recv_full(client_conn, len_int)
                self.logger.debug("recv len: {0}, len(bs): {1}".format(len_int, len(bs)))
                pkg = protocol.un_serialize(bs)
                if pkg.ty == protocol.TYPE_CLIENT_HELLO_REQ:
                    self.logger.info("recv type: {0}!".format(pkg.ty))
                    client_id = self.__id__()
                    self.client_id_mapping_listen_port[client_id] = {}
                    self.client_id_mapping_user_conn[client_id] = {}
                    self.client_id_mapping_client_app_conn[client_id] = {}
                    self.client_id_mapping_client_conn[client_id] = client_conn
                    threading.Thread(target=self.__handle_client_hello_req__,
                                     args=(client_conn, client_id, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_HEARTBEAT_REQ:
                    self.logger.debug("recv type: {0}!".format(pkg.ty))
                    threading.Thread(target=self.__handle_heartbeat_req__,
                                     args=(client_conn, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_USER_CREATE_CONN_RESP:
                    self.logger.info("recv type: {0}!".format(pkg.ty))
                    threading.Thread(target=self.__handle_user_create_conn_resp__,
                                     args=(client_conn, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_PAYLOAD:
                    # threading.Thread(target=self.__handle_payload__, args=(client_conn, client_id, pkg)).start()
                    self.__handle_payload__(client_conn, pkg)
                    continue
                self.logger.error("recv client pkg type error!")
        except BaseException as e:
            self.logger.error("client conn recv err: {0}!".format(e))
            self.logger.debug("client conn recv err: {0}!".format(traceback.format_exc()))
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
            except BaseException as e:
                ...
            try:
                self.client_id_mapping_client_app_conn[client_id].pop(client_id)
            except BaseException as e:
                ...
            try:
                self.client_id_mapping_client_conn.pop(client_id)
            except BaseException as e:
                ...
            if client_id != "":
                self.__when_client_conn_close__(client_id)

    def __handle_heartbeat_req__(self, client_conn: socket.socket, pkg: protocol.package):
        try:
            bs = protocol.serialize(
                protocol.package(ty=protocol.TYPE_HEARTBEAT_RESP, client_id=pkg.client_id, conn_id=pkg.conn_id,
                                 error=""))
            self.logger.debug("send heartbeat resp")
            client_conn.send(len(bs).to_bytes(4, 'big') + bs)
        except BaseException as e:
            self.logger.error("send heartbeat err: {0}".format(e))
            try:
                client_conn.close()
            except BaseException as e:
                ...

    def __handle_user_create_conn_resp__(self, client_app_conn: socket.socket, pkg: protocol.package):
        try:
            if pkg.error == "":
                self.client_id_mapping_client_app_conn[pkg.client_id][pkg.conn_id] = client_app_conn
                self.conn_id_mapping_client_app_conn[pkg.conn_id] = client_app_conn
            self.user_conn_create_resp_pkg[pkg.conn_id] = pkg
            wait = self.user_conn_create_resp_event[pkg.conn_id]
            wait.set()
        except BaseException as e:
            self.logger.error("can not find create user conn resp event: {0}!".format(e))

    def __handle_client_hello_req__(self, client_conn: socket.socket, client_id: int, pkg: protocol.package):
        error = ""
        listen_ports = {}
        try:
            if not len(pkg.listen_ports):
                raise Exception("listen ports is empty!")
            if self.secret != pkg.secret:
                raise Exception("secret error")
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
            client_conn.send(len(bs).to_bytes(4, 'big') + bs)

    def __listen_port__(self, client_conn, client_id: int, listen: socket.socket, listen_port):
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

    def __id__(self):
        exec_time = 0
        while 1:
            if exec_time >= 65535:
                raise Exception("can not find new id")
            if self.id >= 65535:
                self.id = 0
            self.id += 1
            exec_time += 1
            try:
                _ = self.conn_id_mapping_user_conn[self.id]
                continue
            except KeyError as e:
                try:
                    _ = self.client_id_mapping_user_conn[self.id]
                    continue
                except KeyError as e:
                    return self.id

    def __handle_user_conn__(self, client_conn: socket.socket, client_id: int, user_conn: socket.socket, listen_port):
        conn_id = self.__id__()
        self.client_id_mapping_user_conn[client_id][id(user_conn)] = user_conn
        event = threading.Event()
        self.user_conn_create_resp_event[conn_id] = event
        bs = protocol.serialize(
            protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_REQ, client_id=client_id, conn_id=conn_id,
                             listen_ports=[listen_port],
                             error=""))
        client_conn.send(len(bs).to_bytes(4, 'big') + bs)
        # wait user conn create resp
        try:
            event.wait(60)
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
                except BaseException as ee:
                    ...
                self.logger.error("can not find user conn create resp pkg: {0}".format(e))
                raise Exception("can not find user conn create resp pkg: {0}".format(e))
        # create user conn success
        self.conn_id_mapping_user_conn[conn_id] = user_conn
        client_app_conn = self.conn_id_mapping_client_app_conn[conn_id]
        try:
            while 1:
                bs = user_conn.recv(1024)
                if len(bs) == 0:
                    raise Exception("EOF")
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_PAYLOAD, payload=bs, conn_id=conn_id, error=""))
                self.logger.debug("send len: {0}".format(len(bs)))
                client_app_conn.send(len(bs).to_bytes(4, 'big') + bs)
        except BaseException as e:
            self.logger.error("user conn recv err: {0}!".format(e))
            try:
                client_app_conn.shutdown(socket.SHUT_RDWR)
                client_app_conn.close()
            except BaseException as e:
                ...
            try:
                self.conn_id_mapping_client_app_conn.pop(conn_id)
            except BaseException as e:
                ...
            try:
                user_conn.shutdown(socket.SHUT_RDWR)
                user_conn.close()
            except BaseException as e:
                ...
            try:
                self.conn_id_mapping_user_conn.pop(conn_id)
            except BaseException as e:
                ...

    def __handle_payload__(self, client_conn: socket.socket, pkg: protocol.package):
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
    with open(config_file, 'r') as f:
        conf = yaml.safe_load(f)
    lev = logging.INFO
    try:
        debug = conf['debug']
    except BaseException as e:
        debug = False
    if debug:
        lev = logging.DEBUG
    logging.basicConfig(level=lev,
                        format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)d %(thread)s %(message)s')
    logger = logging.getLogger()
    srv = Srv(conf)
    logger.info("server info: {0}!".format(srv))
    while 1:
        try:
            srv.start()
        except KeyboardInterrupt as e:
            raise e
        except BaseException as e:
            logger.info("server err:{0} {1}!".format(e, traceback.format_exc()))
            logger.info("server will start in 5s...")
            time.sleep(5)

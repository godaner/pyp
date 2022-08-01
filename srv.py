#!/usr/bin/env python3
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
        self._id = 0
        self._conf = conf
        self._logger = logging.getLogger()
        self._user_conn_create_resp_event = {}
        self._user_conn_create_resp_pkg = {}
        self._conn_id_mapping_client_app_conn = {}
        self._conn_id_mapping_user_conn = {}
        self._client_id_mapping_client_conn = {}
        self._client_id_mapping_client_app_conns = {}
        self._client_id_mapping_user_conns = {}
        self._client_id_mapping_listen_port = {}
        try:
            self._server_host = self._conf["server"]["host"]
        except BaseException as e:
            raise SystemExit("get server.host err: {0}".format(e))
        try:
            self._server_port = self._conf["server"]["port"]
        except BaseException as e:
            raise SystemExit("get server.port err: {0}".format(e))
        try:
            self._secret = self._conf["server"]["secret"]
        except BaseException as e:
            raise SystemExit("get server.secret err: {0}".format(e))

    def __str__(self):
        return str(self._conf)

    def start(self):
        self._logger.info("start server")
        listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen.bind((self._server_host, self._server_port))
        listen.listen()
        self._logger.info(
            "listen client conn {}:{}".format(self._server_host, self._server_port))
        while 1:
            try:
                client_conn, addr = listen.accept()
                t = threading.Thread(target=self._handle_client_conn, args=(client_conn,))
                t.start()
            except BaseException as e:
                self._logger.error(
                    "closing listen client conn {}:{}, {}".format(self._server_host, self._server_port, e))
                try:
                    listen.shutdown(socket.SHUT_RDWR)
                    listen.close()
                except BaseException as ee:
                    ...
                self._when_listen_conn_close()
                raise e

    def _when_listen_conn_close(self):
        client_ids = []
        for client_id in self._client_id_mapping_client_conn:
            client_ids.append(client_id)
        for client_id in client_ids:
            client_conn = self._client_id_mapping_client_conn[client_id]
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
            except BaseException as e:
                ...

    def _when_client_conn_close(self, client_id: int):
        listens = self._client_id_mapping_listen_port.pop(client_id)
        for listen in listens:
            try:
                listens[listen].shutdown(socket.SHUT_RDWR)
                listens[listen].close()
            except BaseException as e:
                ...
        user_conns = self._client_id_mapping_user_conns.pop(client_id)
        for user_conn in user_conns:
            try:
                user_conns[user_conn].shutdown(socket.SHUT_RDWR)
                user_conns[user_conn].close()
            except BaseException as e:
                ...
        client_app_conns = self._client_id_mapping_client_app_conns.pop(client_id)
        for client_app_conn in client_app_conns:
            try:
                client_app_conns[client_app_conn].shutdown(socket.SHUT_RDWR)
                client_app_conns[client_app_conn].close()
            except BaseException as e:
                ...

    def _handle_client_conn(self, client_conn: socket.socket):
        client_conn_addr = client_conn.getsockname()
        client_id = ""
        try:
            while 1:
                len_bs = sock.recv_full(client_conn, 4)
                if len(len_bs) == 0:
                    raise Exception("EOF")
                len_int = int.from_bytes(len_bs, 'big')
                bs = sock.recv_full(client_conn, len_int)
                self._logger.debug("recv len: {0}, len(bs): {1}".format(len_int, len(bs)))
                pkg = protocol.un_serialize(bs)
                if pkg.ty == protocol.TYPE_CLIENT_HELLO_REQ:
                    self._logger.info("recv type: {0}".format(pkg.ty))
                    client_id = self._gen_id()
                    self._client_id_mapping_listen_port[client_id] = {}
                    self._client_id_mapping_user_conns[client_id] = {}
                    self._client_id_mapping_client_app_conns[client_id] = {}
                    self._client_id_mapping_client_conn[client_id] = client_conn
                    threading.Thread(target=self._handle_client_hello_req,
                                     args=(client_conn, client_id, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_HEARTBEAT_REQ:
                    self._logger.debug("recv type: {0}".format(pkg.ty))
                    threading.Thread(target=self._handle_heartbeat_req,
                                     args=(client_conn, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_USER_CREATE_CONN_RESP:
                    self._logger.info("recv type: {0}".format(pkg.ty))
                    threading.Thread(target=self._handle_user_create_conn_resp,
                                     args=(client_conn, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_PAYLOAD:
                    self._handle_payload(client_conn, pkg)
                    continue
                self._logger.error("recv client pkg type error")
        except BaseException as e:
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
            except BaseException as ee:
                ...
            if client_id != "":
                self._logger.error(
                    "closing client conn {}:{} <-> {}:{}, {}".format(client_conn_addr[0], client_conn_addr[1],
                                                                     self._server_host,
                                                                     self._server_port, e))
                try:
                    self._client_id_mapping_client_conn.pop(client_id)
                except BaseException as ee:
                    ...
                self._when_client_conn_close(client_id)

    def _handle_heartbeat_req(self, client_conn: socket.socket, pkg: protocol.package):
        try:
            bs = protocol.serialize(
                protocol.package(ty=protocol.TYPE_HEARTBEAT_RESP, client_id=pkg.client_id, conn_id=pkg.conn_id,
                                 error=""))
            self._logger.debug("send heartbeat resp")
            client_conn.send(len(bs).to_bytes(4, 'big') + bs)
        except BaseException as e:
            self._logger.error("send heartbeat err: {0}".format(e))
            try:
                client_conn.close()
            except BaseException as e:
                ...

    def _handle_user_create_conn_resp(self, client_app_conn: socket.socket, pkg: protocol.package):
        try:
            if pkg.error == "":
                self._client_id_mapping_client_app_conns[pkg.client_id][pkg.conn_id] = client_app_conn
                self._conn_id_mapping_client_app_conn[pkg.conn_id] = client_app_conn
            self._user_conn_create_resp_pkg[pkg.conn_id] = pkg
            wait = self._user_conn_create_resp_event[pkg.conn_id]
            wait.set()
        except BaseException as e:
            self._logger.error("can not find create user conn resp event: {0}".format(e))
            try:
                self._client_id_mapping_client_app_conns[pkg.client_id].pop(pkg.conn_id)
            except BaseException as ee:
                ...
            try:
                self._conn_id_mapping_client_app_conn.pop(pkg.conn_id)
            except BaseException as ee:
                ...
            try:
                self._user_conn_create_resp_pkg.pop(pkg.conn_id)
            except BaseException as ee:
                ...
            try:
                self._user_conn_create_resp_event.pop(pkg.conn_id)
            except BaseException as ee:
                ...

    def _handle_client_hello_req(self, client_conn: socket.socket, client_id: int, pkg: protocol.package):
        error = ""
        listen_ports = {}
        client_conn_addr = client_conn.getpeername()
        try:
            if not len(pkg.listen_ports):
                raise Exception("listen ports is empty")
            if self._secret != pkg.secret:
                raise Exception("secret error")
            for listen_port in pkg.listen_ports:
                listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listen.bind(("0.0.0.0", listen_port))
                listen.listen()
                listen_ports[listen_port] = listen
            for listen_port in listen_ports:
                listen = listen_ports[listen_port]
                self._client_id_mapping_listen_port[client_id][id(listen)] = listen
                t = threading.Thread(target=self._listen_port,
                                     args=(client_conn, client_id, listen_ports[listen_port], listen_port))
                t.start()
        except BaseException as e:
            for listen_port in listen_ports:
                listen_ports[listen_port].shutdown(socket.SHUT_RDWR)
                listen_ports[listen_port].close()
            self._logger.error("listen port err: {}, {}".format(listen_ports, e))
            error = str(e)
            return
        finally:
            bs = protocol.serialize(protocol.package(ty=protocol.TYPE_CLIENT_HELLO_RESP, error=error))
            client_conn.send(len(bs).to_bytes(4, 'big') + bs)

        self._logger.info(
            "accept client conn {}:{} <-> {}:{}".format(client_conn_addr[0], client_conn_addr[1], self._server_host,
                                                        self._server_port))

    def _listen_port(self, client_conn, client_id: int, listen: socket.socket, listen_port):
        self._logger.info(
            "listen user conn :{}".format(listen_port))
        try:
            while 1:
                user_conn, addr = listen.accept()
                t = threading.Thread(target=self._handle_user_conn,
                                     args=(client_conn, client_id, user_conn, listen_port))
                t.start()
        except BaseException as e:
            self._logger.error("closing listen user conn :{}, {}".format(listen_port, e))
            try:
                listen.shutdown(socket.SHUT_RDWR)
                listen.close()
            except BaseException as ee:
                ...
            try:
                self._client_id_mapping_listen_port[client_id].pop(id(listen))
            except BaseException as ee:
                ...

    def _gen_id(self):
        exec_time = 0
        while 1:
            if exec_time >= 65535:
                raise Exception("can not find new id")
            if self._id >= 65535:
                self._id = 0
            self._id += 1
            exec_time += 1
            try:
                _ = self._conn_id_mapping_user_conn[self._id]
                continue
            except KeyError as e:
                try:
                    _ = self._client_id_mapping_client_conn[self._id]
                    continue
                except KeyError as e:
                    return self._id

    def _handle_user_conn(self, client_conn: socket.socket, client_id: int, user_conn: socket.socket, listen_port):
        user_conn_addr = user_conn.getpeername()
        self._logger.info(
            "accept user conn {}:{} <-> :{}".format(user_conn_addr[0], user_conn_addr[1], listen_port))
        conn_id = self._gen_id()
        event = threading.Event()
        self._user_conn_create_resp_event[conn_id] = event
        bs = protocol.serialize(
            protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_REQ, client_id=client_id, conn_id=conn_id,
                             listen_ports=[listen_port],
                             error=""))
        client_conn.send(len(bs).to_bytes(4, 'big') + bs)
        # wait user conn create resp
        try:
            event.wait(10)
            try:
                self._user_conn_create_resp_event.pop(conn_id)
            except BaseException as e:
                ...
            if not event.is_set():
                raise Exception("timeout")
            pkg = self._user_conn_create_resp_pkg.pop(conn_id)
            if pkg.error != "":
                raise Exception(pkg.error)
        except BaseException as e:
            try:
                self._logger.error(
                    "closing user conn {}:{} <-> :{}, {}".format(user_conn_addr[0], user_conn_addr[1], listen_port, e))
                user_conn.shutdown(socket.SHUT_RDWR)
                user_conn.close()
            except BaseException as ee:
                ...
            self._logger.error("wait user conn create resp err: {0}".format(e))
            return
        # create user conn success
        self._conn_id_mapping_user_conn[conn_id] = user_conn
        self._client_id_mapping_user_conns[client_id][id(user_conn)] = user_conn
        client_app_conn = self._conn_id_mapping_client_app_conn[conn_id]
        client_app_conn_addr = client_app_conn.getpeername()
        self._logger.info(
            "accept client app conn {}:{} <-> {}:{}".format(client_app_conn_addr[0], client_app_conn_addr[1],
                                                            self._server_host,
                                                            self._server_port))
        try:
            while 1:
                bs = user_conn.recv(40960)
                if len(bs) == 0:
                    raise Exception("EOF")
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_PAYLOAD, payload=bs, conn_id=conn_id, error=""))
                self._logger.debug("send len: {0}".format(len(bs)))
                client_app_conn.send(len(bs).to_bytes(4, 'big') + bs)
        except BaseException as e:
            self._logger.error(
                "closing user conn {}:{} <-> :{}, {}".format(user_conn_addr[0], user_conn_addr[1], listen_port, e))
            self._logger.error(
                "closing client app conn {}:{} <-> {}:{}, {}".format(client_app_conn_addr[0], client_app_conn_addr[1],
                                                                     self._server_host, self._server_port, e))
            try:
                client_app_conn.shutdown(socket.SHUT_RDWR)
                client_app_conn.close()
            except BaseException as ee:
                ...
            try:
                self._client_id_mapping_user_conns[client_id].pop(id(user_conn))
            except BaseException as ee:
                ...
            try:
                self._client_id_mapping_client_app_conns[client_id].pop(conn_id)
            except BaseException as ee:
                ...
            try:
                self._conn_id_mapping_client_app_conn.pop(conn_id)
            except BaseException as ee:
                ...
            try:
                user_conn.shutdown(socket.SHUT_RDWR)
                user_conn.close()
            except BaseException as ee:
                ...
            try:
                self._conn_id_mapping_user_conn.pop(conn_id)
            except BaseException as ee:
                ...

    def _handle_payload(self, client_conn: socket.socket, pkg: protocol.package):
        conn_id = pkg.conn_id
        try:
            user_conn = self._conn_id_mapping_user_conn[conn_id]
            user_conn.send(pkg.payload)
        except BaseException as e:
            self._logger.error("forward payload to user conn err: {0}".format(e))


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
    logger.info("server info: {0}".format(srv))
    while 1:
        try:
            srv.start()
        except (SystemExit, KeyboardInterrupt) as e:
            raise e
        except BaseException as e:
            logger.info("server err:{0} {1}".format(e, traceback.format_exc()))
            logger.info("server will start in 5s...")
            time.sleep(5)

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


class Cli:
    def __init__(self, conf):
        self.conf = conf
        self.client_ip = 0
        self.server_host = ""
        self.server_port = ""
        self.logger = logging.getLogger()
        self.outer_port_mapping_inner = {}
        self.conn_id_mapping_app_conn = {}
        self.conn_id_mapping_client_app_conn = {}
        self.heartbeat_event = threading.Event()
        self.exit_event = threading.Event()

    def __str__(self):
        return str(self.conf)

    def start(self):
        self.logger.info("start client!")
        client_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.server_host = self.conf["server"]["host"]
        except Exception as e:
            self.logger.info("get host from config fail: {0}".format(e))
            raise e
        try:
            self.server_port = self.conf["server"]["port"]
        except Exception as e:
            self.logger.info("get port from config fail: {0}".format(e))
            raise e
        try:
            secret = self.conf["server"]["secret"]
        except Exception as e:
            self.logger.info("get secret from config fail: {0}".format(e))
            raise e
        listen_ports = []
        for app in self.conf['app']:
            self.outer_port_mapping_inner[app['outer']['port']] = app['inner']
            listen_ports.append(app['outer']['port'])
        if not len(listen_ports):
            self.logger.error("listen_ports is empty!")
        client_conn.connect((self.server_host, self.server_port))
        # client hello req
        bs = protocol.serialize(
            protocol.package(ty=protocol.TYPE_CLIENT_HELLO_REQ, listen_ports=listen_ports, error="", secret=secret))
        client_conn.send(len(bs).to_bytes(4, 'big') + bs)
        # wait client hello resp
        len_bs = sock.recv_full(client_conn, 4)
        len_int = int.from_bytes(len_bs, 'big')
        bs = sock.recv_full(client_conn, len_int)
        pkg = protocol.un_serialize(bs)
        if pkg.ty != protocol.TYPE_CLIENT_HELLO_RESP:
            self.logger.error("recv resp is not client hello resp")
            raise Exception("recv resp is not client hello resp")
        if pkg.error != "":
            self.logger.error("recv client hello resp err: {0}!".format(pkg.error))
            raise Exception("recv client hello resp err: {0}!".format(pkg.error))
        self.logger.info("recv client hello resp!")
        self.client_ip = pkg.client_id
        self.exit_event = threading.Event()
        # send heartbeat
        threading.Thread(target=self.__heartbeat__, args=(client_conn,)).start()
        # recv user create conn req
        try:
            while 1:
                len_bs = sock.recv_full(client_conn, 4)
                if len(len_bs) == 0:
                    raise Exception("EOF")
                len_int = int.from_bytes(len_bs, 'big')
                bs = sock.recv_full(client_conn, len_int)
                self.logger.debug("recv len: {0}, len(bs): {1}".format(len_int, len(bs)))
                pkg = protocol.un_serialize(bs)
                if pkg.ty == protocol.TYPE_HEARTBEAT_RESP:
                    self.logger.debug("recv type: {0}!".format(pkg.ty))
                    threading.Thread(target=self.__handle_heartbeat_req__, args=()).start()
                    continue
                if pkg.ty == protocol.TYPE_USER_CREATE_CONN_REQ:
                    self.logger.info("recv type: {0}!".format(pkg.ty))
                    threading.Thread(target=self.__handle_user_create_conn_req__, args=(client_conn, pkg)).start()
                    continue
                self.logger.error("recv server pkg type error!")
        except BaseException as e:
            self.logger.error("client conn recv err: {0}!".format(e))
            self.logger.debug("client conn recv err: {0}!".format(traceback.format_exc()))
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
            except BaseException as ee:
                ...
            self.__when_client_conn_close__()
            raise e

    def __handle_heartbeat_req__(self):
        self.heartbeat_event.set()

    def __heartbeat__(self, client_conn: socket.socket):
        try:
            while 1:
                self.exit_event.wait(10)
                if self.exit_event.is_set():
                    return
                self.heartbeat_event = threading.Event()
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_HEARTBEAT_REQ, client_id=self.client_ip,
                                     conn_id=0,
                                     error=""))
                self.logger.debug("send heartbeat req")
                client_conn.send(len(bs).to_bytes(4, 'big') + bs)
                self.heartbeat_event.wait(10)
                if not self.heartbeat_event.is_set():
                    raise Exception("wait heartbeat resp timeout")
        except BaseException as e:
            self.logger.error("send heartbeat err: {0}".format(e))
            try:
                client_conn.close()
            except BaseException as e:
                ...

    def __when_client_conn_close__(self):
        self.exit_event.set()
        app_conns = []
        for conn_id in self.conn_id_mapping_app_conn:
            app_conns.append(self.conn_id_mapping_app_conn[conn_id])
        for app_conn in app_conns:
            self.logger.info("closing app conn: {0}".format(str(app_conn)))
            try:
                app_conn.shutdown(socket.SHUT_RDWR)
                app_conn.close()
            except BaseException as e:
                ...

        client_app_conns = []
        for conn_id in self.conn_id_mapping_client_app_conn:
            client_app_conns.append(self.conn_id_mapping_client_app_conn[conn_id])
        for client_app_conn in client_app_conns:
            self.logger.info("closing client app conn: {0}".format(str(client_app_conn)))
            try:
                client_app_conn.shutdown(socket.SHUT_RDWR)
                client_app_conn.close()
            except BaseException as e:
                ...

    def __handle_user_create_conn_req__(self, client_conn: socket.socket, pkg: protocol.package):
        try:
            # app conn
            inner = self.outer_port_mapping_inner[pkg.listen_ports[0]]
            app_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            app_conn.connect((inner['host'], inner['port']))

            # client app conn
            client_app_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                client_app_conn.connect((self.server_host, self.server_port))
            except BaseException as e:
                try:
                    app_conn.close()
                except BaseException as ee:
                    ...
                try:
                    client_app_conn.close()
                except BaseException as ee:
                    ...
                raise e
            bs = protocol.serialize(
                protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_RESP, client_id=pkg.client_id, conn_id=pkg.conn_id,
                                 error=""))
            client_app_conn.send(len(bs).to_bytes(4, 'big') + bs)

            # handle app conn
            threading.Thread(target=self.__handle_client_app_conn__,
                             args=(client_app_conn, pkg.conn_id, app_conn)).start()
            threading.Thread(target=self.__handle_app_conn__, args=(client_app_conn, pkg.conn_id, app_conn)).start()

        except BaseException as e:
            self.logger.error("connect to app err: {0}!".format(e))
            bs = protocol.serialize(
                protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_RESP, client_id=pkg.client_id, conn_id=pkg.conn_id,
                                 error=str(e)))
            client_conn.send(len(bs).to_bytes(4, 'big') + bs)

    def __handle_client_app_conn__(self, client_app_conn: socket.socket, conn_id, app_conn):
        self.conn_id_mapping_client_app_conn[conn_id] = client_app_conn
        try:
            while 1:
                len_bs = sock.recv_full(client_app_conn, 4)
                if len(len_bs) == 0:
                    raise Exception("EOF")
                len_int = int.from_bytes(len_bs, 'big')
                bs = sock.recv_full(client_app_conn, len_int)
                self.logger.debug("recv len: {0}, len(bs): {1}".format(len_int, len(bs)))
                pkg = protocol.un_serialize(bs)
                if pkg.ty == protocol.TYPE_PAYLOAD:
                    self.__handle_payload__(app_conn, pkg)
                    continue
                self.logger.error("recv server pkg type error!")
        except BaseException as e:
            self.logger.error("client app conn recv err: {0}!".format(e))
            self.logger.debug("client app conn recv err: {0}!".format(traceback.format_exc()))
            try:
                app_conn.shutdown(socket.SHUT_RDWR)
                app_conn.close()
            except BaseException as e:
                ...
            try:
                client_app_conn.shutdown(socket.SHUT_RDWR)
                client_app_conn.close()
            except BaseException as e:
                ...
            try:
                self.conn_id_mapping_app_conn.pop(conn_id)
            except BaseException as e:
                ...
            try:
                self.conn_id_mapping_client_app_conn.pop(conn_id)
            except BaseException as e:
                ...

    def __handle_app_conn__(self, client_app_conn: socket.socket, conn_id, app_conn):
        self.conn_id_mapping_app_conn[conn_id] = app_conn
        try:
            while 1:
                bs = app_conn.recv(1024)
                if len(bs) == 0:
                    raise Exception("EOF")
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_PAYLOAD, payload=bs, conn_id=conn_id, error=""))
                self.logger.debug("send len: {0}".format(len(bs)))
                client_app_conn.send(len(bs).to_bytes(4, 'big') + bs)
        except BaseException as e:
            self.logger.error("app conn recv err: {0}!".format(e))
            try:
                app_conn.shutdown(socket.SHUT_RDWR)
                app_conn.close()
            except BaseException as e:
                ...
            try:
                client_app_conn.shutdown(socket.SHUT_RDWR)
                client_app_conn.close()
            except BaseException as e:
                ...
            try:
                self.conn_id_mapping_app_conn.pop(conn_id)
            except BaseException as e:
                ...
            try:
                self.conn_id_mapping_client_app_conn.pop(conn_id)
            except BaseException as e:
                ...

    def __handle_payload__(self, app_conn: socket.socket, pkg: protocol.package):
        app_conn.send(pkg.payload)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        config_file = "./cli.yaml"
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
    cli = Cli(conf)
    logger.info("cli info: {0}!".format(cli))
    while 1:
        try:
            cli.start()
        except KeyboardInterrupt as e:
            raise e
        except BaseException as e:
            logger.info("cli err: {0} {1}!".format(e, traceback.format_exc()))
            logger.info("cli will start in 5s...")
            time.sleep(5)

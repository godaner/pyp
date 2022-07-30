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
        self._conf = conf
        self._client_ip = 0
        self._logger = logging.getLogger()
        self._outer_port_mapping_inner = {}
        self._conn_id_mapping_app_conn = {}
        self._conn_id_mapping_client_app_conn = {}
        self._heartbeat_event = threading.Event()
        self._exit_event = threading.Event()
        try:
            self._server_host = self._conf["server"]["host"]
        except Exception as e:
            raise SystemExit(e)
        try:
            self._server_port = self._conf["server"]["port"]
        except Exception as e:
            raise SystemExit(e)
        try:
            self._secret = self._conf["server"]["secret"]
        except Exception as e:
            raise SystemExit(e)
        self._listen_ports = []
        for app in self._conf['app']:
            self._outer_port_mapping_inner[app['outer']['port']] = app['inner']
            self._listen_ports.append(app['outer']['port'])
        if not len(self._listen_ports):
            raise SystemExit("listen_ports is empty")

    def __str__(self):
        return str(self._conf)

    def start(self):
        self._logger.info("start client")
        client_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        client_conn.connect((self._server_host, self._server_port))
        # client hello req
        bs = protocol.serialize(
            protocol.package(ty=protocol.TYPE_CLIENT_HELLO_REQ, listen_ports=self._listen_ports, error="",
                             secret=self._secret))
        client_conn.send(len(bs).to_bytes(4, 'big') + bs)
        # wait client hello resp
        len_bs = sock.recv_full(client_conn, 4)
        len_int = int.from_bytes(len_bs, 'big')
        bs = sock.recv_full(client_conn, len_int)
        pkg = protocol.un_serialize(bs)
        if pkg.ty != protocol.TYPE_CLIENT_HELLO_RESP:
            raise Exception("recv resp is not client hello resp")
        if pkg.error != "":
            raise Exception("recv client hello resp err: {0}".format(pkg.error))
        self._logger.info("recv type: {0}".format(pkg.ty))
        self._client_ip = pkg.client_id
        self._exit_event = threading.Event()
        # send heartbeat
        threading.Thread(target=self._heartbeat, args=(client_conn,)).start()
        # log
        client_conn_addr = client_conn.getsockname()
        self._logger.info(
            "connect client conn {}:{} <-> {}:{}".format(client_conn_addr[0], client_conn_addr[1], self._server_host,
                                                         self._server_port))
        # recv user create conn req
        try:
            while 1:
                len_bs = sock.recv_full(client_conn, 4)
                if len(len_bs) == 0:
                    raise Exception("EOF")
                len_int = int.from_bytes(len_bs, 'big')
                bs = sock.recv_full(client_conn, len_int)
                self._logger.debug("recv len: {0}, len(bs): {1}".format(len_int, len(bs)))
                pkg = protocol.un_serialize(bs)
                if pkg.ty == protocol.TYPE_HEARTBEAT_RESP:
                    self._logger.debug("recv type: {0}".format(pkg.ty))
                    threading.Thread(target=self._handle_heartbeat_req, args=()).start()
                    continue
                if pkg.ty == protocol.TYPE_USER_CREATE_CONN_REQ:
                    self._logger.info("recv type: {0}".format(pkg.ty))
                    threading.Thread(target=self._handle_user_create_conn_req, args=(client_conn, pkg)).start()
                    continue
                self._logger.error("recv server pkg type error")
        except BaseException as e:
            self._logger.info(
                "closing client conn {}:{} <-> {}:{}".format(client_conn_addr[0], client_conn_addr[1],
                                                             self._server_host,
                                                             self._server_port))
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
            except BaseException as ee:
                ...
            self._when_client_conn_close()
            raise e

    def _handle_heartbeat_req(self):
        self._heartbeat_event.set()

    def _heartbeat(self, client_conn: socket.socket):
        try:
            while 1:
                self._exit_event.wait(10)
                if self._exit_event.is_set():
                    return
                self._heartbeat_event = threading.Event()
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_HEARTBEAT_REQ, client_id=self._client_ip,
                                     conn_id=0,
                                     error=""))
                self._logger.debug("send heartbeat req")
                client_conn.send(len(bs).to_bytes(4, 'big') + bs)
                self._heartbeat_event.wait(10)
                if not self._heartbeat_event.is_set():
                    raise Exception("wait heartbeat resp timeout")
        except BaseException as e:
            self._logger.error("send heartbeat err: {0}".format(e))
            try:
                client_conn.close()
            except BaseException as ee:
                ...

    def _when_client_conn_close(self):
        self._heartbeat_event.set()
        self._exit_event.set()
        app_conns = []
        for conn_id in self._conn_id_mapping_app_conn:
            app_conns.append(self._conn_id_mapping_app_conn[conn_id])
        for app_conn in app_conns:
            self._logger.info("closing app conn: {0}".format(str(app_conn)))
            try:
                app_conn.shutdown(socket.SHUT_RDWR)
                app_conn.close()
            except BaseException as e:
                ...

        client_app_conns = []
        for conn_id in self._conn_id_mapping_client_app_conn:
            client_app_conns.append(self._conn_id_mapping_client_app_conn[conn_id])
        for client_app_conn in client_app_conns:
            self._logger.info("closing client app conn: {0}".format(str(client_app_conn)))
            try:
                client_app_conn.shutdown(socket.SHUT_RDWR)
                client_app_conn.close()
            except BaseException as e:
                ...

    def _handle_user_create_conn_req(self, client_conn: socket.socket, pkg: protocol.package):
        inner = self._outer_port_mapping_inner[pkg.listen_ports[0]]
        try:
            # app conn
            app_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            app_conn.connect((inner['host'], inner['port']))
            # client app conn
            client_app_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                client_app_conn.connect((self._server_host, self._server_port))
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
            threading.Thread(target=self._handle_client_app_conn,
                             args=(client_app_conn, pkg.conn_id, app_conn)).start()
            threading.Thread(target=self._handle_app_conn,
                             args=(client_app_conn, pkg.conn_id, app_conn, inner)).start()

        except BaseException as e:
            self._logger.error("connect to app err: {}:{}, {}".format(inner['host'], inner['port'], e))
            bs = protocol.serialize(
                protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_RESP, client_id=pkg.client_id, conn_id=pkg.conn_id,
                                 error=str(e)))
            client_conn.send(len(bs).to_bytes(4, 'big') + bs)

    def _handle_client_app_conn(self, client_app_conn: socket.socket, conn_id, app_conn):
        client_app_conn_addr = client_app_conn.getsockname()
        self._logger.info(
            "connect client app conn {}:{} <-> {}:{}".format(client_app_conn_addr[0], client_app_conn_addr[1],
                                                             self._server_host,
                                                             self._server_port))
        self._conn_id_mapping_client_app_conn[conn_id] = client_app_conn
        try:
            while 1:
                len_bs = sock.recv_full(client_app_conn, 4)
                if len(len_bs) == 0:
                    raise Exception("EOF")
                len_int = int.from_bytes(len_bs, 'big')
                bs = sock.recv_full(client_app_conn, len_int)
                self._logger.debug("recv len: {0}, len(bs): {1}".format(len_int, len(bs)))
                pkg = protocol.un_serialize(bs)
                if pkg.ty == protocol.TYPE_PAYLOAD:
                    self._handle_payload(app_conn, pkg)
                    continue
                self._logger.error("recv server pkg type error")
        except BaseException as e:
            self._logger.error(
                "closing client app conn {}:{} <-> {}:{}, {}".format(client_app_conn_addr[0], client_app_conn_addr[1],
                                                                     self._server_host,
                                                                     self._server_port, e))
            try:
                app_conn.shutdown(socket.SHUT_RDWR)
                app_conn.close()
            except BaseException as ee:
                ...
            try:
                client_app_conn.shutdown(socket.SHUT_RDWR)
                client_app_conn.close()
            except BaseException as ee:
                ...
            try:
                self._conn_id_mapping_app_conn.pop(conn_id)
            except BaseException as ee:
                ...
            try:
                self._conn_id_mapping_client_app_conn.pop(conn_id)
            except BaseException as ee:
                ...

    def _handle_app_conn(self, client_app_conn: socket.socket, conn_id, app_conn, inner):
        app_conn_addr = app_conn.getsockname()
        self._logger.info(
            "connect app conn {}:{} <-> {}:{}".format(app_conn_addr[0], app_conn_addr[1],
                                                      inner['host'], inner['port']))
        self._conn_id_mapping_app_conn[conn_id] = app_conn
        try:
            while 1:
                bs = app_conn.recv(1024)
                if len(bs) == 0:
                    raise Exception("EOF")
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_PAYLOAD, payload=bs, conn_id=conn_id, error=""))
                self._logger.debug("send len: {0}".format(len(bs)))
                client_app_conn.send(len(bs).to_bytes(4, 'big') + bs)
        except BaseException as e:
            self._logger.error(
                "closing app conn {}:{} <-> {}:{}, {}".format(app_conn_addr[0], app_conn_addr[1],
                                                              inner['host'], inner['port'], e))
            try:
                app_conn.shutdown(socket.SHUT_RDWR)
                app_conn.close()
            except BaseException as ee:
                ...
            try:
                client_app_conn.shutdown(socket.SHUT_RDWR)
                client_app_conn.close()
            except BaseException as ee:
                ...
            try:
                self._conn_id_mapping_app_conn.pop(conn_id)
            except BaseException as ee:
                ...
            try:
                self._conn_id_mapping_client_app_conn.pop(conn_id)
            except BaseException as ee:
                ...

    def _handle_payload(self, app_conn: socket.socket, pkg: protocol.package):
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
    logger.info("cli info: {0}".format(cli))
    while 1:
        try:
            cli.start()
        except (SystemExit, KeyboardInterrupt) as e:
            raise e
        except BaseException as e:
            logger.info("cli err: {0} {1}".format(e, traceback.format_exc()))
            logger.info("cli will start in 5s...")
            time.sleep(5)

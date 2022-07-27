import logging
import socket
import sys
import threading
import traceback

import yaml

import protocol


class Cli:
    def __init__(self, config_file: str):
        with open(config_file, 'r') as f:
            self.conf = yaml.safe_load(f)
        self.logger = logging.getLogger()
        self.outer_port_mapping_inner = {}
        self.conn_id_mapping_app_conn = {}

    def __str__(self):
        return str(self.conf)

    def start(self):
        self.logger.info("start client!")
        client_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
        listen_ports = []
        for app in self.conf['app']:
            self.outer_port_mapping_inner[app['outer']['port']] = app['inner']
            listen_ports.append(app['outer']['port'])
        if not len(listen_ports):
            self.logger.error("listen_ports is empty!")
        client_conn.connect((host, port))
        bs = protocol.serialize(
            protocol.package(ty=protocol.TYPE_CLIENT_HELLO_REQ, listen_ports=listen_ports, error=""))
        client_conn.send(len(bs).to_bytes(32, 'big') + bs)
        # wait client hello resp
        len_bs = client_conn.recv(32)
        len_int = int.from_bytes(len_bs, 'big')
        bs = client_conn.recv(len_int)
        pkg = protocol.un_serialize(bs)
        if pkg.ty != protocol.TYPE_CLIENT_HELLO_RESP:
            self.logger.error("recv resp is not client hello resp")
            raise Exception("recv resp is not client hello resp")
        if pkg.error != "":
            self.logger.error("recv client hello resp err: {0}!".format(pkg.error))
            raise Exception("recv client hello resp err: {0}!".format(pkg.error))
        self.logger.info("recv client hello resp!")
        try:
            while 1:
                len_bs = client_conn.recv(32)
                if len(len_bs) == 0:
                    raise Exception("EOF")
                len_int = int.from_bytes(len_bs, 'big')
                bs = client_conn.recv(len_int)
                pkg = protocol.un_serialize(bs)
                if pkg.ty == protocol.TYPE_USER_CREATE_CONN_REQ:
                    self.logger.info("recv type: {0}!".format(pkg.ty))
                    threading.Thread(target=self.__handle_user_create_conn_req__, args=(client_conn, pkg)).start()
                    continue
                if pkg.ty == protocol.TYPE_PAYLOAD:
                    # threading.Thread(target=self.__handle_payload__, args=(client_conn, pkg)).start()
                    self.__handle_payload__(client_conn, pkg)
                    continue
                self.logger.error("recv server pkg type error!")
        except BaseException as e:
            self.logger.error("client conn recv err: {0}!".format(e))
            try:
                client_conn.shutdown(socket.SHUT_RDWR)
                client_conn.close()
            except BaseException as ee:
                ...
            self.__when_client_conn_close__()
            raise e

    def __when_client_conn_close__(self):
        app_conns = []
        for conn_id in self.conn_id_mapping_app_conn:
            app_conns.append(self.conn_id_mapping_app_conn[conn_id])
        for app_conn in app_conns:
            self.logger.info("closing client conn: {0}".format(str(app_conn)))
            try:
                app_conn.shutdown(socket.SHUT_RDWR)
                app_conn.close()
            except BaseException as e:
                ...

    def __handle_user_create_conn_req__(self, client_conn: socket.socket, pkg: protocol.package):
        error = ""
        try:
            inner = self.outer_port_mapping_inner[pkg.listen_ports[0]]
            app_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            app_conn.connect((inner['host'], inner['port']))
            t = threading.Thread(target=self.__handle_app_conn__, args=(client_conn, pkg.conn_id, app_conn))
            t.start()
        except BaseException as e:
            self.logger.error("connect to app err: {0}!".format(e))
            error = str(e)
        finally:
            bs = protocol.serialize(
                protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_RESP, conn_id=pkg.conn_id, error=error))
            client_conn.send(len(bs).to_bytes(32, 'big') + bs)

    def __handle_app_conn__(self, client_conn: socket.socket, conn_id, app_conn):
        self.conn_id_mapping_app_conn[conn_id] = app_conn
        try:
            while 1:
                bs = app_conn.recv(1024)
                if len(bs) == 0:
                    raise Exception("EOF")
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_PAYLOAD, payload=bs, conn_id=conn_id, error=""))
                client_conn.send(len(bs).to_bytes(32, 'big') + bs)
        except BaseException as e:
            self.logger.error("app conn recv err: {0}!".format(e))
            try:
                app_conn.shutdown(socket.SHUT_RDWR)
                app_conn.close()
            except BaseException as e:
                ...
            try:
                self.conn_id_mapping_app_conn.pop(conn_id)
            except BaseException as e:
                ...

    def __handle_payload__(self, client_conn: socket.socket, pkg: protocol.package):
        conn_id = pkg.conn_id
        app_conn = self.conn_id_mapping_app_conn[conn_id]
        app_conn.send(pkg.payload)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        config_file = "./cli.yaml"
    else:
        config_file = sys.argv[1]
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)d %(thread)s %(message)s')
    logger = logging.getLogger()
    logger.info("use config file: {0}!".format(config_file))
    cli = Cli(config_file)
    logger.info("cli info: {0}!".format(cli))
    try:
        cli.start()
    except BaseException as e:
        logger.info("cli err: {0} {1}!".format(e, traceback.format_exc()))

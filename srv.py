import logging
import socket
import sys
import threading
import uuid

import yaml

import protocol
import sock


class Srv:
    def __init__(self, config_file: str):
        with open(config_file, 'r') as f:
            self.conf = yaml.safe_load(f)
        self.logger = logging.getLogger()
        self.user_conn_create_resp_event = {}
        self.user_conn_create_resp_pkg = {}
        self.conn_id_mapping_user_conn = {}
        self.app_close_conn = {}

    def __str__(self):
        return str(self.conf)

    def start(self):
        self.logger.info("start client!")
        s = sock.sock(socket.AF_INET, socket.SOCK_STREAM)
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
            try:
                client_conn, addr = s.accept()
                self.logger.info("accept client conn addr: {0}".format(addr))
                t = threading.Thread(target=self.__handle_client_conn__, args=(client_conn,))
                t.start()
            except (SystemExit, KeyboardInterrupt, GeneratorExit) as e:
                self.logger.warn("exit handle client conn: {0}!".format(e))
                return
            except Exception as e:
                self.logger.info("accept client conn fail: {0}!".format(e))

    def __handle_client_conn__(self, client_conn: sock.sock):
        while not client_conn.down_event.is_set():
            len_bs = client_conn.recv(32)
            len_int = int.from_bytes(len_bs)
            bs = client_conn.recv(len_int)
            pkg = protocol.un_serialize(bs)
            self.logger.debug("recv client pkg: {0}!".format(pkg))
            if pkg.ty == protocol.TYPE_CLIENT_HELLO_REQ:
                self.__handle_client_hello_req__(client_conn, pkg)
                continue
            if pkg.ty == protocol.TYPE_USER_CREATE_CONN_RESP:
                self.__handle_user_create_conn_resp__(client_conn, pkg)
                continue
            if pkg.ty == protocol.TYPE_CLOSE_USER_CONN_REQ:
                self.__handle_close_user_conn__(client_conn, pkg)
                continue
            if pkg.ty == protocol.TYPE_PAYLOAD:
                self.__handle_payload__(client_conn, pkg)
                continue
            self.logger.error("recv client pkg type error!")

    def __handle_user_create_conn_resp__(self, client_conn: sock.sock, pkg: protocol.package):
        self.user_conn_create_resp_pkg[pkg.conn_id] = pkg
        wait = self.user_conn_create_resp_event[pkg.conn_id]
        wait.set()

    def __handle_client_hello_req__(self, client_conn: sock.sock, pkg: protocol.package):
        self.logger.info("recv client hello req")
        s = sock.sock(socket.AF_INET, socket.SOCK_STREAM)
        port = pkg.listen_port
        error = ""
        try:
            s.bind(("0.0.0.0", port))
            s.listen()
        except BaseException as e:
            self.logger.error("listen client hello port err: {0}!".format(e))
            error = str(e)
        bs = protocol.serialize(protocol.package(ty=protocol.TYPE_CLIENT_HELLO_RESP, error=error))
        client_conn.send(len(bs).to_bytes(4))
        client_conn.send(bs)
        if error != "":
            return
        # wait user conn
        while not client_conn.down_event.is_set():
            try:
                user_conn, addr = s.accept()
                logger.info("accept user conn addr: {0}".format(addr))
                t = threading.Thread(target=self.__handle_user_conn__, args=(client_conn, user_conn))
                t.start()
            except (SystemExit, KeyboardInterrupt, GeneratorExit) as e:
                logger.warn("exit handle user conn: {0}".format(e))
                return
            except Exception as e:
                logger.info("accept user conn fail: {0}!".format(e))

    def __handle_user_conn__(self, client_conn: sock.sock, user_conn: sock.sock):
        conn_id = str(uuid.uuid4())
        event = threading.Event()
        self.user_conn_create_resp_event[conn_id] = event
        bs = protocol.serialize(
            protocol.package(ty=protocol.TYPE_USER_CREATE_CONN_REQ, conn_id=conn_id, error=""))
        client_conn.send(len(bs).to_bytes(4))
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
            while not client_conn.down_event.is_set() and not user_conn.close():
                bs = user_conn.recv()
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_PAYLOAD, payload=bs, conn_id=conn_id, error=""))
                client_conn.send(bs)
        except BaseException as e:
            raise e
        finally:
            app_close = self.app_close_conn.pop(conn_id)
            if not app_close:
                user_conn.close()
                bs = protocol.serialize(
                    protocol.package(ty=protocol.TYPE_CLOSE_APP_CONN_REQ, conn_id=conn_id, error=""))
                client_conn.send(bs)

    def __handle_close_user_conn__(self, client_conn: sock.sock, pkg: protocol.package):
        self.app_close_conn[pkg.conn_id] = True
        self.conn_id_mapping_user_conn[pkg.conn_id].close()

    def __handle_payload__(self, client_conn: sock.sock, pkg: protocol.package):
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

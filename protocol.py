import pickle

TYPE_CLIENT_HELLO_REQ = "client hello req"
TYPE_CLIENT_HELLO_RESP = "client hello resp"
TYPE_USER_CREATE_CONN_REQ = "user create conn req"
TYPE_USER_CREATE_CONN_RESP = "user create conn resp"
TYPE_PAYLOAD = "payload"


class package:
    ty = str
    client_id = str
    listen_ports = []
    conn_id = str
    error = str
    payload = bytes

    def __init__(self, ty, listen_ports=[], client_id=str, conn_id=str, error=str, payload=bytes):
        self.ty = ty
        self.client_id = client_id
        self.listen_ports = listen_ports
        self.conn_id = conn_id
        self.error = error
        self.payload = payload


def serialize(obj):
    return pickle.dumps(obj)


def un_serialize(bs):
    return pickle.loads(bs)

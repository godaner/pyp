import pickle

TYPE_CLIENT_HELLO_REQ = "client hello req"
TYPE_CLIENT_HELLO_RESP = "client hello resp"
TYPE_USER_CREATE_CONN_REQ = "user create conn req"
TYPE_USER_CREATE_CONN_RESP = "user create conn resp"
TYPE_CLOSE_APP_CONN_REQ = "close app conn req"
TYPE_CLOSE_USER_CONN_REQ = "close user conn req"
TYPE_PAYLOAD = "payload"


class package:
    ty = str
    listen_port = int
    conn_id = str
    error = str
    payload = bytes

    def __init__(self, ty, listen_port=int, conn_id=str, error=str, payload=bytes):
        self.ty = ty
        self.listen_port = listen_port
        self.conn_id = conn_id
        self.error = error
        self.payload = payload


def serialize(obj):
    return pickle.dumps(obj)


def un_serialize(bs):
    return pickle.loads(bs)

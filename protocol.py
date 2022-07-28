TYPE_CLIENT_HELLO_REQ = "client hello req"
TYPE_CLIENT_HELLO_RESP = "client hello resp"
TYPE_USER_CREATE_CONN_REQ = "user create conn req"
TYPE_USER_CREATE_CONN_RESP = "user create conn resp"
TYPE_PAYLOAD = "payload"
type_map_bs = {
    TYPE_CLIENT_HELLO_REQ: int(0x01).to_bytes(1, 'big'),
    TYPE_CLIENT_HELLO_RESP: int(0x02).to_bytes(1, 'big'),
    TYPE_USER_CREATE_CONN_REQ: int(0x03).to_bytes(1, 'big'),
    TYPE_USER_CREATE_CONN_RESP: int(0x04).to_bytes(1, 'big'),
    TYPE_PAYLOAD: int(0x05).to_bytes(1, 'big'),
}
bs_map_type = {int.from_bytes(val, 'big'): key for (key, val) in type_map_bs.items()}


class package:
    ty: str
    client_id: int
    conn_id: int
    secret: str
    error: str
    payload: bytes
    listen_ports: []

    def __init__(self, ty: str = "", client_id: int = 0, conn_id: int = 0, error: str = "", secret: str = "",
                 listen_ports=None, payload: bytes = bytes()):
        if listen_ports is None:
            listen_ports = []
        self.ty = ty
        self.client_id = client_id
        self.listen_ports = listen_ports
        self.conn_id = conn_id
        self.error = error
        self.payload = payload
        self.secret = secret


def serialize(obj: package) -> bytes:
    bs = bytes()
    bs += type_map_bs[obj.ty]
    bs += obj.client_id.to_bytes(4, 'big')
    bs += obj.conn_id.to_bytes(4, 'big')
    bs += len(obj.secret).to_bytes(2, 'big')
    bs += bytes(obj.secret, encoding='utf-8')
    bs += len(obj.error).to_bytes(2, 'big')
    bs += bytes(obj.error, encoding='utf-8')
    bs += len(obj.payload).to_bytes(4, 'big')
    bs += obj.payload
    bs += len(obj.listen_ports).to_bytes(1, 'big')
    for listen_port in obj.listen_ports:
        bs += listen_port.to_bytes(4, 'big')
    return bs


def un_serialize(bs) -> package:
    type_bs = bs[:1]
    bs = bs[1:]
    ty = bs_map_type[int.from_bytes(type_bs, 'big')]

    client_id_bs = bs[:4]
    bs = bs[4:]
    client_id = int.from_bytes(client_id_bs, 'big')

    conn_id_bs = bs[:4]
    bs = bs[4:]
    conn_id = int.from_bytes(conn_id_bs, 'big')

    secret_len_bs = bs[:2]
    bs = bs[2:]
    secret_len = int.from_bytes(secret_len_bs, 'big')

    secret_bs = bs[:secret_len]
    bs = bs[secret_len:]
    secret = str(secret_bs, encoding='utf-8')

    error_len_bs = bs[:2]
    bs = bs[2:]
    error_len = int.from_bytes(error_len_bs, 'big')

    error_bs = bs[:error_len]
    bs = bs[error_len:]
    error = str(error_bs, encoding='utf-8')

    payload_len_bs = bs[:4]
    bs = bs[4:]
    payload_len = int.from_bytes(payload_len_bs, 'big')

    payload_bs = bs[:payload_len]

    listen_ports_len_bs = bs[:1]
    bs = bs[1:]
    listen_ports_len = int.from_bytes(listen_ports_len_bs, 'big')
    
    listen_ports = []
    for i in range(listen_ports_len):
        listen_port_bs = bs[:4]
        bs = bs[4:]
        listen_ports.append(int.from_bytes(listen_port_bs, 'big'))

    return package(ty=ty, client_id=client_id, listen_ports=listen_ports, conn_id=conn_id, error=error,
                   payload=payload_bs, secret=secret)

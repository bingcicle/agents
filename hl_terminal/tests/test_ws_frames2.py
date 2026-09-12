import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Тести нового WS-шару server.py: фрейми, фрагментація, handshake."""
import socket, struct, threading, os, re, sys

src = open((_HL + "/server.py")).read()
m = re.search(r"(def ws_recv.*?)(?=\ndef _ws_conn)", src, re.S)
ns = {"socket": socket, "struct": struct, "os": os}
exec(m.group(1), ns)
ws_recv, ws_send_frame, ws_send = ns["ws_recv"], ns["ws_send_frame"], ns["ws_send"]

# ws_handshake окремо (перед ws_recv у файлі)
m2 = re.search(r"(def ws_handshake.*?)(?=\ndef ws_recv)", src, re.S)
ns2 = {"socket": socket, "os": os}
exec(m2.group(1), ns2)
ws_handshake = ns2["ws_handshake"]

def sf(opcode, payload=b"", fin=True):
    n = len(payload)
    b0 = (0x80 if fin else 0x00) | opcode
    if n < 126: hdr = bytes([b0, n])
    elif n < 65536: hdr = bytes([b0, 126]) + struct.pack(">H", n)
    else: hdr = bytes([b0, 127]) + struct.pack(">Q", n)
    return hdr + payload

def parse_client_frame(sock):
    h = sock.recv(2)
    opcode = h[0] & 0x0F
    length = h[1] & 0x7F
    if length == 126: length = struct.unpack(">H", sock.recv(2))[0]
    mask = sock.recv(4)
    p = b""
    while len(p) < length: p += sock.recv(length - len(p))
    return opcode, bytes(x ^ mask[i % 4] for i, x in enumerate(p))

fails = []
def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond: fails.append(name)

# 1. Фрагментоване повідомлення збирається
a, b = socket.socketpair()
b.sendall(sf(0x1, b'{"a":', fin=False) + sf(0x0, b'1}', fin=True))
check("fragmented message reassembled", ws_recv(a) == b'{"a":1}')

# 2. Ping МІЖ фрагментами: pong іде, повідомлення збирається
lock = threading.Lock()
b.sendall(sf(0x1, b'{"x":', fin=False) + sf(0x9, b'hb') + sf(0x0, b'2}', fin=True))
r = ws_recv(a, lock)
op, pl = parse_client_frame(b)
check("ping inside fragments -> pong", op == 0xA and pl == b'hb')
check("message survives interleaved ping", r == b'{"x":2}')

# 3. Одиночний ping поза повідомленням -> b"" + pong
b.sendall(sf(0x9, b'p1'))
r = ws_recv(a, lock)
op, pl = parse_client_frame(b)
check("standalone ping -> empty + pong", r == b"" and op == 0xA and pl == b'p1')

# 4. Pong-фрейм від сервера -> b""
b.sendall(sf(0xA, b'x'))
check("server pong frame -> empty", ws_recv(a) == b"")

# 5. rbuf: байти, що прийшли з handshake, читаються першими
rbuf = bytearray(sf(0x1, b'early'))
check("leftover buffer consumed first", ws_recv(a, None, rbuf) == b'early' and not rbuf)

# 6. rbuf з половиною фрейма + решта з сокета
whole = sf(0x1, b'split-frame-data')
rbuf = bytearray(whole[:5])
b.sendall(whole[5:])
check("split across rbuf and socket", ws_recv(a, None, rbuf) == b'split-frame-data')

# 7. Close/закритий сокет
b.sendall(sf(0x8))
check("close frame -> None", ws_recv(a) is None)
b.close(); check("closed socket -> None", ws_recv(a) is None); a.close()

# 8. Великий фрейм і великий send
a, b = socket.socketpair()
big = os.urandom(70000)
threading.Thread(target=lambda: b.sendall(sf(0x2, big)), daemon=True).start()
check("large 70KB frame", ws_recv(a) == big)
msg = "y" * 70000
threading.Thread(target=lambda: ws_send(a, msg), daemon=True).start()
h = b.recv(2); assert h[1] & 0x7F == 127
ln = struct.unpack(">Q", b.recv(8))[0]; mask = b.recv(4)
p = b""
while len(p) < ln: p += b.recv(ln - len(p))
check("ws_send 70KB", bytes(x ^ mask[i % 4] for i, x in enumerate(p)) == msg.encode())
a.close(); b.close()

# 9. Handshake: справжній 101 + leftover зберігається
def hs_server(sock, resp):
    sock.recv(65536); sock.sendall(resp)
a, b = socket.socketpair()
frame_after = sf(0x1, b'{"channel":"subscriptionResponse"}')
threading.Thread(target=hs_server, args=(b, b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n" + frame_after), daemon=True).start()
ok, leftover = ws_handshake(a, "h", "/ws")
check("handshake 101 accepted", ok)
check("leftover preserved", leftover == frame_after)
rbuf = bytearray(leftover)
check("leftover parses as frame", ws_recv(a, None, rbuf) == b'{"channel":"subscriptionResponse"}')
a.close(); b.close()

# 10. Handshake: 403 з "101" у тілі НЕ приймається
a, b = socket.socketpair()
threading.Thread(target=hs_server, args=(b, b"HTTP/1.1 403 Forbidden\r\nX-Info: error 101\r\n\r\nban 101"), daemon=True).start()
ok, _ = ws_handshake(a, "h", "/ws")
check("403 with '101' in body rejected", ok is False)
a.close(); b.close()

sys.exit(1 if fails else 0)

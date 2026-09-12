import os as _os_t
_HL = _os_t.path.dirname(_os_t.path.dirname(_os_t.path.abspath(__file__)))   # hl_terminal/
_SPT = _os_t.path.join(_HL, "tests", "_tmp"); _os_t.makedirs(_SPT, exist_ok=True)   # тимчасові файли
"""Тест ws_recv / ws_send_frame / ws_send із server.py без імпорту модуля
(модуль на імпорті запускає сервер). Витягуємо тільки потрібні функції."""
import socket, struct, threading, os, re, sys

src = open((_HL + "/server.py")).read()

# Вирізаємо блок від def ws_recv до def _ws_conn
m = re.search(r"(def ws_recv.*?)(?=\ndef _ws_conn)", src, re.S)
assert m, "ws function block not found"
ns = {"socket": socket, "struct": struct, "os": os}
exec(m.group(1), ns)
ws_recv, ws_send_frame, ws_send = ns["ws_recv"], ns["ws_send_frame"], ns["ws_send"]

def server_frame(opcode, payload=b""):
    """Немаскований фрейм, як шле сервер."""
    n = len(payload)
    if n < 126:
        hdr = bytes([0x80 | opcode, n])
    elif n < 65536:
        hdr = bytes([0x80 | opcode, 126]) + struct.pack(">H", n)
    else:
        hdr = bytes([0x80 | opcode, 127]) + struct.pack(">Q", n)
    return hdr + payload

def parse_client_frame(sock):
    """Читає масковані фрейми клієнта (як їх бачив би сервер)."""
    h = sock.recv(2)
    opcode = h[0] & 0x0F
    length = h[1] & 0x7F
    assert h[1] & 0x80, "client frame must be masked"
    if length == 126:
        length = struct.unpack(">H", sock.recv(2))[0]
    elif length == 127:
        length = struct.unpack(">Q", sock.recv(8))[0]
    mask = sock.recv(4)
    payload = b""
    while len(payload) < length:
        payload += sock.recv(min(65536, length - len(payload)))
    return opcode, bytes(b ^ mask[i % 4] for i, b in enumerate(payload))

fails = []
def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond: fails.append(name)

# 1. Текстовий фрейм від сервера
a, b = socket.socketpair()
b.sendall(server_frame(0x1, b'{"channel":"trades"}'))
check("text frame", ws_recv(a) == b'{"channel":"trades"}')

# 2. Ping від сервера -> ws_recv шле pong і повертає b""
lock = threading.Lock()
b.sendall(server_frame(0x9, b"hb-123"))
r = ws_recv(a, lock)
op, pl = parse_client_frame(b)
check("ping returns empty", r == b"")
check("pong sent with same payload", op == 0xA and pl == b"hb-123")

# 3. Close frame -> None
b.sendall(server_frame(0x8))
check("close frame -> None", ws_recv(a) is None)

# 4. Закритий сокет -> None (а не вічний цикл)
b.close()
check("closed socket -> None", ws_recv(a) is None)
a.close()

# 5. Великий фрейм від сервера (70000 байт, гілка 127)
a, b = socket.socketpair()
big = os.urandom(70000)
threading.Thread(target=lambda: b.sendall(server_frame(0x2, big)), daemon=True).start()
check("large 70KB frame", ws_recv(a) == big)

# 6. ws_send великого повідомлення (>65536) парситься сервером
msg = "x" * 70000
threading.Thread(target=lambda: ws_send(a, msg), daemon=True).start()
op, pl = parse_client_frame(b)
check("ws_send 70KB", op == 0x1 and pl == msg.encode())

# 7. Звичайний ws_send (маленький)
threading.Thread(target=lambda: ws_send(a, '{"method":"ping"}'), daemon=True).start()
op, pl = parse_client_frame(b)
check("ws_send small", op == 0x1 and pl == b'{"method":"ping"}')
a.close(); b.close()

sys.exit(1 if fails else 0)

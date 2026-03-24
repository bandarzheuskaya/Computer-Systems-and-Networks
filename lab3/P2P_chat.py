import socket
import struct
import threading
import time
from datetime import datetime
from enum import IntEnum


# =========================
# Константы
# =========================
UDP_PORT = 4242
TCP_PORT = 4243
BUFFER_SIZE = 4096

HEADER_FORMAT = "!BI"   # 1 byte type, 4 bytes payload length
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

HELLO_INTERVAL = 2.0
SOCKET_TIMEOUT = 5.0


# =========================
# Типы сообщений
# =========================
class MessageType(IntEnum):
    CHAT = 1
    NAME = 2
    HELLO = 3
    DISCONNECT = 4


# =========================
# Вспомогательные структуры
# =========================
class Peer:
    def __init__(self, ip, name="", sock=None, connected=False, reader_thread=None):
        self.ip = ip
        self.name = name
        self.sock = sock
        self.connected = connected
        self.reader_thread = reader_thread


class HistoryEvent:
    def __init__(self, event_type, peer_ip="", peer_name="", content="", timestamp=None):
        self.timestamp = timestamp or datetime.now().strftime("%H:%M:%S")
        self.event_type = event_type
        self.peer_ip = peer_ip
        self.peer_name = peer_name
        self.content = content

    def format_for_display(self):
        if self.event_type == "peer_discovered":
            return f"[{self.timestamp}] [INFO] Обнаружен узел: {self.peer_name} ({self.peer_ip})"
        if self.event_type == "peer_connected":
            return f"[{self.timestamp}] [INFO] Установлено соединение с: {self.peer_name} ({self.peer_ip})"
        if self.event_type == "incoming_message":
            return f"[{self.timestamp}] [CHAT] {self.peer_name} ({self.peer_ip}): {self.content}"
        if self.event_type == "outgoing_message":
            return f"[{self.timestamp}] [CHAT] Вы: {self.content}"
        if self.event_type == "peer_disconnected":
            return f"[{self.timestamp}] [INFO] Узел отключился: {self.peer_name} ({self.peer_ip})"
        if self.event_type == "system":
            return f"[{self.timestamp}] [INFO] {self.content}"
        if self.event_type == "warn":
            return f"[{self.timestamp}] [WARN] {self.content}"
        if self.event_type == "error":
            return f"[{self.timestamp}] [ERROR] {self.content}"
        return f"[{self.timestamp}] {self.content}"


# =========================
# Протокол
# =========================
def create_message(msg_type, payload_text=""):
    msg_type = MessageType(msg_type)
    payload = payload_text.encode("utf-8")
    header = struct.pack(HEADER_FORMAT, int(msg_type), len(payload))
    return header + payload


def parse_message_header(header_bytes):
    msg_type, payload_length = struct.unpack(HEADER_FORMAT, header_bytes)
    return MessageType(msg_type), payload_length


def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Соединение закрыто")
        data += chunk
    return data


def read_message(sock):
    header = recv_exact(sock, HEADER_SIZE)
    msg_type, payload_length = parse_message_header(header)

    if payload_length == 0:
        return msg_type, ""

    payload = recv_exact(sock, payload_length)
    return msg_type, payload.decode("utf-8")


# =========================
# Сетевые утилиты
# =========================
def get_all_local_ips():
    ips = {"127.0.0.1"}

    try:
        hostname = socket.gethostname()
        for ip in socket.gethostbyname_ex(hostname)[2]:
            ips.add(ip)
    except OSError:
        pass

    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass

    try:
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        test_sock.connect(("8.8.8.8", 80))
        ips.add(test_sock.getsockname()[0])
        test_sock.close()
    except OSError:
        pass

    return sorted(ips)


def choose_primary_ip(local_ips):
    for ip in local_ips:
        if ip.startswith("192.168.") and ip != "127.0.0.1":
            return ip
    for ip in local_ips:
        if ip.startswith("10.") and ip != "127.0.0.1":
            return ip
    for ip in local_ips:
        if ip.startswith("172.") and ip != "127.0.0.1":
            return ip
    for ip in local_ips:
        if ip != "127.0.0.1":
            return ip
    return "127.0.0.1"


def derive_broadcasts(primary_ip):
    broadcasts = {"255.255.255.255"}

    parts = primary_ip.split(".")
    if len(parts) == 4:
        broadcasts.add(f"{parts[0]}.{parts[1]}.{parts[2]}.255")

    return sorted(broadcasts)


# =========================
# Основное приложение
# =========================
class P2PChatApp:
    def __init__(self, name):
        self.name = name

        self.local_ips = get_all_local_ips()
        self.local_ip_set = set(self.local_ips)
        self.ip = choose_primary_ip(self.local_ips)
        self.broadcast_addresses = derive_broadcasts(self.ip)

        self.running = False

        self.peers = {}       # ip -> Peer
        self.history = []

        self.peers_lock = threading.Lock()
        self.history_lock = threading.Lock()

        self.udp_socket = None
        self.tcp_server_socket = None

    # -------------------------
    # История
    # -------------------------
    def add_history_event(self, event_type, peer_ip="", peer_name="", content=""):
        event = HistoryEvent(
            event_type=event_type,
            peer_ip=peer_ip,
            peer_name=peer_name,
            content=content
        )
        with self.history_lock:
            self.history.append(event)
        print(event.format_for_display())

    # -------------------------
    # Сокеты
    # -------------------------
    def create_udp_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", UDP_PORT))
        self.udp_socket = sock

    def create_tcp_server_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", TCP_PORT))
        sock.listen(10)
        self.tcp_server_socket = sock

    # -------------------------
    # UDP: обнаружение узлов
    # -------------------------
    def send_hello(self):
        if self.udp_socket is None:
            return

        msg = create_message(MessageType.HELLO, self.name)

        for broadcast_ip in self.broadcast_addresses:
            try:
                self.udp_socket.sendto(msg, (broadcast_ip, UDP_PORT))
            except OSError as e:
                self.add_history_event("warn", content=f"Ошибка отправки HELLO на {broadcast_ip}: {e}")

    def hello_sender_loop(self):
        while self.running:
            self.send_hello()
            time.sleep(HELLO_INTERVAL)

    def udp_listener(self):
        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(BUFFER_SIZE)
            except OSError:
                break

            sender_ip = addr[0]

            if sender_ip in self.local_ip_set:
                continue

            if len(data) < HEADER_SIZE:
                continue

            try:
                msg_type, payload_length = parse_message_header(data[:HEADER_SIZE])
            except (struct.error, ValueError):
                continue

            if msg_type != MessageType.HELLO:
                continue

            if len(data) < HEADER_SIZE + payload_length:
                continue

            try:
                sender_name = data[HEADER_SIZE:HEADER_SIZE + payload_length].decode("utf-8")
            except UnicodeDecodeError:
                continue

            discovered_now = False

            with self.peers_lock:
                if sender_ip not in self.peers:
                    self.peers[sender_ip] = Peer(ip=sender_ip, name=sender_name)
                    discovered_now = True
                else:
                    if sender_name:
                        self.peers[sender_ip].name = sender_name

                already_connected = self.peers[sender_ip].connected

            if discovered_now:
                self.add_history_event(
                    event_type="peer_discovered",
                    peer_ip=sender_ip,
                    peer_name=sender_name
                )

            self.add_history_event(
                event_type="system",
                content=f"Получен HELLO от {sender_name} ({sender_ip})"
            )

            # В этой версии каждый узел пытается подключиться сам.
            # Дубликаты соединений отсекаются проверками.
            if not already_connected:
                connect_thread = threading.Thread(
                    target=self.connect_to_peer,
                    args=(sender_ip, sender_name),
                    daemon=True
                )
                connect_thread.start()

    # -------------------------
    # TCP: исходящее подключение
    # -------------------------
    def connect_to_peer(self, peer_ip, peer_name=""):
        if peer_ip in self.local_ip_set:
            return

        with self.peers_lock:
            if peer_ip in self.peers and self.peers[peer_ip].connected:
                return

        self.add_history_event(
            event_type="system",
            content=f"Попытка TCP-подключения к {peer_name or peer_ip} ({peer_ip}:{TCP_PORT})"
        )

        tcp_sock = None
        try:
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.settimeout(SOCKET_TIMEOUT)
            tcp_sock.connect((peer_ip, TCP_PORT))
            tcp_sock.settimeout(None)

            self.add_history_event(
                event_type="system",
                content=f"TCP connect успешно выполнен к {peer_ip}:{TCP_PORT}"
            )

            tcp_sock.sendall(create_message(MessageType.NAME, self.name))

            peer = Peer(
                ip=peer_ip,
                name=peer_name or peer_ip,
                sock=tcp_sock,
                connected=True
            )

            reader_thread = threading.Thread(
                target=self.peer_reader_loop,
                args=(peer_ip, tcp_sock, peer.name),
                daemon=True
            )
            peer.reader_thread = reader_thread

            with self.peers_lock:
                # Если пока мы подключались, peer уже стал connected через входящее соединение,
                # дубликат закрываем.
                if peer_ip in self.peers and self.peers[peer_ip].connected:
                    tcp_sock.close()
                    return

                self.peers[peer_ip] = peer

            self.add_history_event(
                event_type="peer_connected",
                peer_ip=peer_ip,
                peer_name=peer.name
            )

            reader_thread.start()

        except OSError as e:
            self.add_history_event(
                event_type="warn",
                content=f"Не удалось подключиться к {peer_name or peer_ip} ({peer_ip}:{TCP_PORT}): {e}"
            )
            if tcp_sock is not None:
                try:
                    tcp_sock.close()
                except OSError:
                    pass

    # -------------------------
    # TCP: входящие подключения
    # -------------------------
    def tcp_listener(self):
        while self.running:
            try:
                peer_sock, peer_addr = self.tcp_server_socket.accept()
            except OSError:
                break

            thread = threading.Thread(
                target=self.handle_incoming_connection,
                args=(peer_sock, peer_addr),
                daemon=True
            )
            thread.start()

    def handle_incoming_connection(self, peer_sock, peer_addr):
        peer_ip = peer_addr[0]

        if peer_ip in self.local_ip_set:
            try:
                peer_sock.close()
            except OSError:
                pass
            return

        self.add_history_event(
            event_type="system",
            content=f"Принято входящее TCP-соединение от {peer_ip}"
        )

        try:
            peer_sock.settimeout(SOCKET_TIMEOUT)
            msg_type, payload = read_message(peer_sock)
            peer_sock.settimeout(None)

            if msg_type != MessageType.NAME:
                peer_sock.close()
                return

            peer_name = payload or peer_ip

            self.add_history_event(
                event_type="system",
                content=f"По TCP получено имя узла {peer_name} от {peer_ip}"
            )

            with self.peers_lock:
                if peer_ip in self.peers and self.peers[peer_ip].connected:
                    peer_sock.close()
                    return

                peer = Peer(
                    ip=peer_ip,
                    name=peer_name,
                    sock=peer_sock,
                    connected=True
                )

                reader_thread = threading.Thread(
                    target=self.peer_reader_loop,
                    args=(peer_ip, peer_sock, peer_name),
                    daemon=True
                )
                peer.reader_thread = reader_thread
                self.peers[peer_ip] = peer

            self.add_history_event(
                event_type="peer_connected",
                peer_ip=peer_ip,
                peer_name=peer_name
            )

            reader_thread.start()

        except (ConnectionError, OSError) as e:
            self.add_history_event(
                event_type="warn",
                content=f"Ошибка обработки входящего TCP-соединения от {peer_ip}: {e}"
            )
            try:
                peer_sock.close()
            except OSError:
                pass

    # -------------------------
    # Чтение TCP-сообщений
    # -------------------------
    def peer_reader_loop(self, peer_ip, peer_sock, peer_name=""):
        try:
            while self.running:
                msg_type, payload = read_message(peer_sock)

                if msg_type == MessageType.CHAT:
                    with self.peers_lock:
                        actual_name = self.peers[peer_ip].name if peer_ip in self.peers else peer_name

                    self.add_history_event(
                        event_type="incoming_message",
                        peer_ip=peer_ip,
                        peer_name=actual_name,
                        content=payload
                    )

                elif msg_type == MessageType.DISCONNECT:
                    with self.peers_lock:
                        actual_name = self.peers[peer_ip].name if peer_ip in self.peers else peer_name

                    self.add_history_event(
                        event_type="peer_disconnected",
                        peer_ip=peer_ip,
                        peer_name=actual_name
                    )
                    break

        except (ConnectionError, OSError):
            if self.running:
                with self.peers_lock:
                    actual_name = self.peers[peer_ip].name if peer_ip in self.peers else peer_name

                self.add_history_event(
                    event_type="peer_disconnected",
                    peer_ip=peer_ip,
                    peer_name=actual_name
                )
        finally:
            self.mark_peer_disconnected(peer_ip)
            try:
                peer_sock.close()
            except OSError:
                pass

    # -------------------------
    # Работа с peers
    # -------------------------
    def show_peers(self):
        with self.peers_lock:
            peers_copy = list(self.peers.values())

        connected = [p for p in peers_copy if p.connected]
        disconnected = [p for p in peers_copy if not p.connected]

        if not peers_copy:
            self.add_history_event(event_type="system", content="Узлы не обнаружены")
            return

        self.add_history_event(event_type="system", content="Список узлов:")

        for peer in connected:
            print(f"    [CONNECTED] {peer.name} ({peer.ip})")

        for peer in disconnected:
            print(f"    [SEEN]      {peer.name} ({peer.ip})")

    # -------------------------
    # Отправка сообщений всем
    # -------------------------
    def broadcast_chat_message(self, text):
        with self.peers_lock:
            peers_copy = list(self.peers.values())

        sent_to_anyone = False

        for peer in peers_copy:
            if peer.connected and peer.sock is not None:
                try:
                    peer.sock.sendall(create_message(MessageType.CHAT, text))
                    sent_to_anyone = True
                except OSError:
                    self.mark_peer_disconnected(peer.ip)

        if sent_to_anyone:
            self.add_history_event(
                event_type="outgoing_message",
                content=text
            )
        else:
            self.add_history_event(
                event_type="warn",
                content="Нет подключённых узлов для отправки сообщения"
            )

    # -------------------------
    # Отключение
    # -------------------------
    def mark_peer_disconnected(self, peer_ip):
        with self.peers_lock:
            if peer_ip in self.peers:
                self.peers[peer_ip].connected = False
                self.peers[peer_ip].sock = None
                self.peers[peer_ip].reader_thread = None

    def shutdown(self):
        self.running = False

        with self.peers_lock:
            peers_copy = list(self.peers.values())

        for peer in peers_copy:
            if peer.connected and peer.sock is not None:
                try:
                    peer.sock.sendall(create_message(MessageType.DISCONNECT, ""))
                except OSError:
                    pass

        for peer in peers_copy:
            if peer.sock is not None:
                try:
                    peer.sock.close()
                except OSError:
                    pass

        if self.udp_socket is not None:
            try:
                self.udp_socket.close()
            except OSError:
                pass

        if self.tcp_server_socket is not None:
            try:
                self.tcp_server_socket.close()
            except OSError:
                pass

        self.add_history_event(event_type="system", content="Чат остановлен")

    # -------------------------
    # Запуск
    # -------------------------
    def start(self):
        self.create_udp_socket()
        self.create_tcp_server_socket()

        self.running = True

        udp_thread = threading.Thread(target=self.udp_listener, daemon=True)
        tcp_thread = threading.Thread(target=self.tcp_listener, daemon=True)
        hello_thread = threading.Thread(target=self.hello_sender_loop, daemon=True)

        udp_thread.start()
        tcp_thread.start()
        hello_thread.start()

        self.add_history_event(
            event_type="system",
            content=f"Чат запущен. Имя: {self.name}, основной IP: {self.ip}"
        )
        self.add_history_event(
            event_type="system",
            content=f"Все локальные IP: {', '.join(self.local_ips)}"
        )
        self.add_history_event(
            event_type="system",
            content=f"Broadcast-адреса: {', '.join(self.broadcast_addresses)}"
        )
        self.add_history_event(
            event_type="system",
            content="Команды: /peers, /exit"
        )


# =========================
# Точка входа
# =========================
if __name__ == "__main__":
    user_name = input("Введите имя: ").strip()
    while not user_name:
        user_name = input("Имя не может быть пустым. Введите имя: ").strip()

    app = P2PChatApp(user_name)
    app.start()

    try:
        while True:
            text = input().strip()

            if not text:
                continue

            if text.lower() == "/exit":
                app.shutdown()
                break

            if text.lower() == "/peers":
                app.show_peers()
                continue

            app.broadcast_chat_message(text)

    except KeyboardInterrupt:
        app.shutdown()
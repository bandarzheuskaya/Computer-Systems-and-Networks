import socket
import struct
import threading
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

SOCKET_TIMEOUT = 5.0


# =========================
# Типы сообщений
# =========================
class MessageType(IntEnum):
    CHAT = 1
    NAME = 2
    HELLO = 3
    HELLO_ACK = 4
    DISCONNECT = 5


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
    payload = payload_text.encode("utf-8")
    header = struct.pack(HEADER_FORMAT, int(MessageType(msg_type)), len(payload))
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

    payload = b""
    if payload_length > 0:
        payload = recv_exact(sock, payload_length)

    return msg_type, payload.decode("utf-8")


# =========================
# Сетевые утилиты
# =========================
def get_all_local_ipv4():
    ips = set()

    try:
        hostname = socket.gethostname()
        for ip in socket.gethostbyname_ex(hostname)[2]:
            if "." in ip:
                ips.add(ip)
    except OSError:
        pass

    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ips.add(s.getsockname()[0])
        s.close()
    except OSError:
        pass

    ips.add("127.0.0.1")
    return sorted(ips)


def choose_ip_interactively(local_ips):
    candidates = [ip for ip in local_ips if ip != "127.0.0.1"]

    if not candidates:
        return "127.0.0.1"

    if len(candidates) == 1:
        return candidates[0]

    print("Найдены локальные IPv4-адреса:")
    for i, ip in enumerate(candidates, start=1):
        print(f"{i}. {ip}")

    while True:
        raw = input("Выберите IP для работы чата: ").strip()
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(candidates):
                return candidates[idx - 1]
        print("Неверный выбор. Введите номер из списка.")


def derive_broadcast_addresses(ip):
    result = {"255.255.255.255"}

    parts = ip.split(".")
    if len(parts) == 4:
        result.add(f"{parts[0]}.{parts[1]}.{parts[2]}.255")

    return sorted(result)


# =========================
# Основное приложение
# =========================
class P2PChatApp:
    def __init__(self, name):
        self.name = name

        self.local_ips = get_all_local_ipv4()
        self.ip = choose_ip_interactively(self.local_ips)
        self.local_ip_set = set(self.local_ips)

        self.broadcast_addresses = derive_broadcast_addresses(self.ip)

        self.running = False

        self.peers = {}    # ip -> Peer
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
    # Работа с peers
    # -------------------------
    def ensure_peer_exists(self, peer_ip, peer_name=""):
        discovered_now = False

        with self.peers_lock:
            if peer_ip not in self.peers:
                self.peers[peer_ip] = Peer(ip=peer_ip, name=peer_name or peer_ip)
                discovered_now = True
            else:
                if peer_name:
                    self.peers[peer_ip].name = peer_name

            current_name = self.peers[peer_ip].name

        if discovered_now:
            self.add_history_event(
                event_type="peer_discovered",
                peer_ip=peer_ip,
                peer_name=current_name
            )

    def get_peer_name(self, peer_ip, fallback=""):
        with self.peers_lock:
            if peer_ip in self.peers and self.peers[peer_ip].name:
                return self.peers[peer_ip].name
        return fallback or peer_ip

    def is_connected(self, peer_ip):
        with self.peers_lock:
            return peer_ip in self.peers and self.peers[peer_ip].connected

    def mark_peer_disconnected(self, peer_ip, emit_event=False):
        event_data = None

        with self.peers_lock:
            if peer_ip in self.peers:
                peer = self.peers[peer_ip]
                was_connected = peer.connected
                peer.connected = False
                peer.sock = None
                peer.reader_thread = None

                if emit_event and was_connected:
                    event_data = (peer.ip, peer.name)

        if event_data:
            self.add_history_event(
                event_type="peer_disconnected",
                peer_ip=event_data[0],
                peer_name=event_data[1]
            )

    def show_peers(self):
        with self.peers_lock:
            peers_copy = list(self.peers.values())

        if not peers_copy:
            self.add_history_event(event_type="system", content="Узлы не обнаружены")
            return

        self.add_history_event(event_type="system", content="Список узлов:")

        for peer in peers_copy:
            state = "CONNECTED" if peer.connected else "SEEN"
            print(f"    [{state}] {peer.name} ({peer.ip})")

    # -------------------------
    # Сокеты
    # -------------------------
    def create_udp_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.ip, UDP_PORT))
        self.udp_socket = sock

    def create_tcp_server_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.ip, TCP_PORT))
        sock.listen(10)
        self.tcp_server_socket = sock

    # -------------------------
    # UDP: обнаружение узлов
    # -------------------------
    def send_udp_packet(self, target_ip, msg_type, payload):
        try:
            self.udp_socket.sendto(create_message(msg_type, payload), (target_ip, UDP_PORT))
        except OSError as e:
            self.add_history_event("warn", content=f"Ошибка отправки UDP на {target_ip}: {e}")

    def send_hello_broadcast(self):
        for broadcast_ip in self.broadcast_addresses:
            self.send_udp_packet(broadcast_ip, MessageType.HELLO, self.name)

    def send_hello_ack(self, target_ip):
        self.send_udp_packet(target_ip, MessageType.HELLO_ACK, self.name)

    def rescan(self):
        self.add_history_event("system", content="Повторное обнаружение узлов...")
        self.send_hello_broadcast()

    def maybe_connect_by_rule(self, peer_ip):
        if peer_ip in self.local_ip_set:
            return

        if self.is_connected(peer_ip):
            return

        # Один TCP-канал на пару узлов:
        # подключение инициирует узел с меньшим IP.
        if self.ip < peer_ip:
            peer_name = self.get_peer_name(peer_ip)
            self.connect_to_peer(peer_ip, peer_name)

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

            if len(data) < HEADER_SIZE + payload_length:
                continue

            try:
                sender_name = data[HEADER_SIZE:HEADER_SIZE + payload_length].decode("utf-8")
            except UnicodeDecodeError:
                continue

            if msg_type == MessageType.HELLO:
                self.ensure_peer_exists(sender_ip, sender_name)
                self.send_hello_ack(sender_ip)
                self.maybe_connect_by_rule(sender_ip)

            elif msg_type == MessageType.HELLO_ACK:
                self.ensure_peer_exists(sender_ip, sender_name)
                self.maybe_connect_by_rule(sender_ip)

    # -------------------------
    # TCP: исходящее подключение
    # -------------------------
    def connect_to_peer(self, peer_ip, peer_name=""):
        if peer_ip in self.local_ip_set:
            return

        with self.peers_lock:
            if peer_ip in self.peers and self.peers[peer_ip].connected:
                return

        tcp_sock = None
        try:
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            try:
                tcp_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass

            tcp_sock.settimeout(SOCKET_TIMEOUT)
            tcp_sock.connect((peer_ip, TCP_PORT))
            tcp_sock.settimeout(None)

            tcp_sock.sendall(create_message(MessageType.NAME, self.name))

            with self.peers_lock:
                if peer_ip in self.peers and self.peers[peer_ip].connected:
                    tcp_sock.close()
                    return

                actual_name = peer_name or self.get_peer_name(peer_ip, peer_ip)
                peer = Peer(
                    ip=peer_ip,
                    name=actual_name,
                    sock=tcp_sock,
                    connected=True
                )
                reader_thread = threading.Thread(
                    target=self.peer_reader_loop,
                    args=(peer_ip, tcp_sock, actual_name),
                    daemon=True
                )
                peer.reader_thread = reader_thread
                self.peers[peer_ip] = peer

            self.add_history_event(
                event_type="peer_connected",
                peer_ip=peer_ip,
                peer_name=self.get_peer_name(peer_ip, peer_ip)
            )

            reader_thread.start()

        except OSError as e:
            if tcp_sock is not None:
                try:
                    tcp_sock.close()
                except OSError:
                    pass

            self.add_history_event(
                event_type="warn",
                content=f"Не удалось подключиться к {peer_name or peer_ip} ({peer_ip}:{TCP_PORT}): {e}"
            )

    # -------------------------
    # TCP: входящие подключения
    # -------------------------
    def tcp_listener(self):
        while self.running:
            try:
                peer_sock, peer_addr = self.tcp_server_socket.accept()
            except OSError:
                break

            t = threading.Thread(
                target=self.handle_incoming_connection,
                args=(peer_sock, peer_addr),
                daemon=True
            )
            t.start()

    def handle_incoming_connection(self, peer_sock, peer_addr):
        peer_ip = peer_addr[0]

        if peer_ip in self.local_ip_set:
            try:
                peer_sock.close()
            except OSError:
                pass
            return

        try:
            peer_sock.settimeout(SOCKET_TIMEOUT)
            msg_type, payload = read_message(peer_sock)
            peer_sock.settimeout(None)

            if msg_type != MessageType.NAME:
                peer_sock.close()
                return

            peer_name = payload or peer_ip
            self.ensure_peer_exists(peer_ip, peer_name)

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

        except (ConnectionError, OSError):
            try:
                peer_sock.close()
            except OSError:
                pass

    # -------------------------
    # TCP: чтение сообщений
    # -------------------------
    def peer_reader_loop(self, peer_ip, peer_sock, peer_name=""):
        disconnect_logged = False

        try:
            while self.running:
                msg_type, payload = read_message(peer_sock)

                if msg_type == MessageType.CHAT:
                    actual_name = self.get_peer_name(peer_ip, peer_name)
                    self.add_history_event(
                        event_type="incoming_message",
                        peer_ip=peer_ip,
                        peer_name=actual_name,
                        content=payload
                    )

                elif msg_type == MessageType.DISCONNECT:
                    actual_name = self.get_peer_name(peer_ip, peer_name)
                    self.add_history_event(
                        event_type="peer_disconnected",
                        peer_ip=peer_ip,
                        peer_name=actual_name
                    )
                    disconnect_logged = True
                    break

        except (ConnectionError, OSError):
            if self.running:
                actual_name = self.get_peer_name(peer_ip, peer_name)
                self.add_history_event(
                    event_type="peer_disconnected",
                    peer_ip=peer_ip,
                    peer_name=actual_name
                )
                disconnect_logged = True
        finally:
            self.mark_peer_disconnected(peer_ip, emit_event=False)

            try:
                peer_sock.close()
            except OSError:
                pass

            if not disconnect_logged and not self.running:
                pass

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
                    self.mark_peer_disconnected(peer.ip, emit_event=True)

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

        udp_thread.start()
        tcp_thread.start()

        self.add_history_event(
            event_type="system",
            content=f"Чат запущен. Имя: {self.name}, IP: {self.ip}"
        )
        self.add_history_event(
            event_type="system",
            content=f"Broadcast-адреса: {', '.join(self.broadcast_addresses)}"
        )
        self.add_history_event(
            event_type="system",
            content="Команды: /peers, /rescan, /exit"
        )

        # Один раз объявляем о себе в сети
        self.send_hello_broadcast()


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

            if text.lower() == "/rescan":
                app.rescan()
                continue

            app.broadcast_chat_message(text)

    except KeyboardInterrupt:
        app.shutdown()
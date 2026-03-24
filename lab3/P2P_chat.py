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
            return f"[{self.timestamp}] Обнаружен новый узел: {self.peer_name} ({self.peer_ip})"
        if self.event_type == "peer_connected":
            return f"[{self.timestamp}] Установлено соединение с: {self.peer_name} ({self.peer_ip})"
        if self.event_type == "incoming_message":
            return f"[{self.timestamp}] {self.peer_name} ({self.peer_ip}): {self.content}"
        if self.event_type == "outgoing_message":
            return f"[{self.timestamp}] Вы: {self.content}"
        if self.event_type == "peer_disconnected":
            return f"[{self.timestamp}] Узел отключился: {self.peer_name} ({self.peer_ip})"
        return f"[{self.timestamp}] {self.content}"


# =========================
# Протокол
# =========================
def create_message(msg_type, payload_text=""):
    msg_type = MessageType(msg_type)
    payload = payload_text.encode("utf-8")
    header = struct.pack(HEADER_FORMAT, msg_type, len(payload))
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
# Определение локального IP
# =========================
def get_local_ip():
    """
    Пытается определить IP текущего устройства в локальной сети.
    """
    test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Адрес никуда реально не отправляется, нужен только для выбора интерфейса
        test_sock.connect(("8.8.8.8", 80))
        ip = test_sock.getsockname()[0]
    except OSError:
        ip = "127.0.0.1"
    finally:
        test_sock.close()
    return ip


# =========================
# Основное приложение
# =========================
class P2PChatApp:
    def __init__(self, name):
        self.name = name
        self.ip = get_local_ip()

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
        self.udp_socket.sendto(msg, ("255.255.255.255", UDP_PORT))

    def udp_listener(self):
        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(BUFFER_SIZE)
            except OSError:
                break

            sender_ip = addr[0]

            if sender_ip == self.ip:
                continue

            if len(data) < HEADER_SIZE:
                continue

            try:
                msg_type, payload_length = parse_message_header(data[:HEADER_SIZE])
            except struct.error:
                continue

            if msg_type != MessageType.HELLO:
                continue

            if len(data) < HEADER_SIZE + payload_length:
                continue

            try:
                sender_name = data[HEADER_SIZE:HEADER_SIZE + payload_length].decode("utf-8")
            except UnicodeDecodeError:
                continue

            with self.peers_lock:
                already_connected = (
                        sender_ip in self.peers and self.peers[sender_ip].connected
                )

            if already_connected:
                continue

            self.add_history_event(
                event_type="peer_discovered",
                peer_ip=sender_ip,
                peer_name=sender_name
            )

            # Чтобы не было двух одновременных TCP-соединений:
            # подключается только узел с меньшим IP
            if self.ip < sender_ip:
                self.connect_to_peer(sender_ip, sender_name)

    # -------------------------
    # TCP: исходящее подключение
    # -------------------------
    def connect_to_peer(self, peer_ip, peer_name=""):
        if peer_ip == self.ip:
            return

        with self.peers_lock:
            if peer_ip in self.peers and self.peers[peer_ip].connected:
                return

        tcp_sock = None
        try:
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.connect((peer_ip, TCP_PORT))

            # Сразу отправляем своё имя
            tcp_sock.sendall(create_message(MessageType.NAME, self.name))

            peer = Peer(
                ip=peer_ip,
                name=peer_name,
                sock=tcp_sock,
                connected=True
            )

            reader_thread = threading.Thread(
                target=self.peer_reader_loop,
                args=(peer_ip, tcp_sock, peer_name),
                daemon=True
            )
            peer.reader_thread = reader_thread

            with self.peers_lock:
                self.peers[peer_ip] = peer

            self.add_history_event(
                event_type="peer_connected",
                peer_ip=peer_ip,
                peer_name=peer_name
            )

            reader_thread.start()

        except OSError as e:
            print(f"Ошибка подключения к {peer_ip}: {e}")
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

        try:
            msg_type, payload = read_message(peer_sock)

            if msg_type != MessageType.NAME:
                peer_sock.close()
                return

            peer_name = payload

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

        except ConnectionError:
            try:
                peer_sock.close()
            except OSError:
                pass

    # -------------------------
    # Общий цикл чтения TCP-сообщений
    # -------------------------
    def peer_reader_loop(self, peer_ip, peer_sock, peer_name=""):
        disconnected_logged = False

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
                    disconnected_logged = True
                    break

        except ConnectionError:
            if self.running:
                with self.peers_lock:
                    actual_name = self.peers[peer_ip].name if peer_ip in self.peers else peer_name

                self.add_history_event(
                    event_type="peer_disconnected",
                    peer_ip=peer_ip,
                    peer_name=actual_name
                )
                disconnected_logged = True

        finally:
            self.mark_peer_disconnected(peer_ip)
            try:
                peer_sock.close()
            except OSError:
                pass

    # -------------------------
    # Отправка сообщений всем
    # -------------------------
    def broadcast_chat_message(self, text):
        with self.peers_lock:
            peers_copy = list(self.peers.values())

        for peer in peers_copy:
            if peer.connected and peer.sock is not None:
                try:
                    peer.sock.sendall(create_message(MessageType.CHAT, text))
                except OSError:
                    self.mark_peer_disconnected(peer.ip)

        self.add_history_event(
            event_type="outgoing_message",
            content=text
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

        # Отправляем DISCONNECT всем активным peers
        for peer in peers_copy:
            if peer.connected and peer.sock is not None:
                try:
                    peer.sock.sendall(create_message(MessageType.DISCONNECT, ""))
                except OSError:
                    pass

        # Закрываем peer sockets
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

        self.send_hello()

        print(f"Чат запущен. Имя: {self.name}, IP: {self.ip}")
        print("Введите сообщение и нажмите Enter. Для выхода: /exit")


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

            app.broadcast_chat_message(text)

    except KeyboardInterrupt:
        app.shutdown()
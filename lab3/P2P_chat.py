import socket
import struct
import threading
import time
from datetime import datetime
from enum import IntEnum


UDP_PORT = 4242
TCP_PORT = 4243
BUFFER_SIZE = 4096

HEADER_FORMAT = "!BI"   # 1 байт тип + 4 байта длина полезной нагрузки
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

SOCKET_TIMEOUT = 5.0
HELLO_BURST_COUNT = 3
HELLO_BURST_DELAY = 0.3


class MessageType(IntEnum):
    CHAT = 1
    NAME = 2
    HELLO = 3
    DISCONNECT = 4


class Peer:
    def __init__(self, ip, name="", sock=None, connected=False, reader_thread=None, source=None):
        self.ip = ip
        self.name = name
        self.sock = sock
        self.connected = connected
        self.reader_thread = reader_thread
        self.source = source   # "incoming" или "outgoing"


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


def create_message(msg_type, payload_text=""):
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
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ips.add(s.getsockname()[0])
        s.close()
    except OSError:
        pass

    return sorted(ips)


def choose_primary_ip(local_ips):
    for prefix in ("192.168.", "10.", "172."):
        for ip in local_ips:
            if ip.startswith(prefix) and ip != "127.0.0.1":
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


class P2PChatApp:
    def __init__(self, name):
        self.name = name

        self.local_ips = get_all_local_ips()
        self.local_ip_set = set(self.local_ips)
        self.ip = choose_primary_ip(self.local_ips)
        self.broadcast_addresses = derive_broadcasts(self.ip)

        self.running = False

        self.peers = {}   # ip -> Peer
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
        sock.listen(16)
        self.tcp_server_socket = sock

    # -------------------------
    # Логика выбора "правильного" TCP
    # -------------------------
    def preferred_source_for_peer(self, peer_ip):
        # Если вдруг два узла одновременно установили два TCP между собой,
        # оставляем только одно соединение по детерминированному правилу:
        # узел с меньшим IP хранит outgoing,
        # узел с большим IP хранит incoming.
        if self.ip < peer_ip:
            return "outgoing"
        return "incoming"

    def register_connection(self, peer_ip, peer_name, sock, source):
        old_sock_to_close = None
        need_log_connected = False
        accepted = False

        with self.peers_lock:
            existing = self.peers.get(peer_ip)
            preferred = self.preferred_source_for_peer(peer_ip)

            if existing and existing.connected:
                if existing.source == preferred and source != preferred:
                    return False

                if source == preferred and existing.source != preferred:
                    old_sock_to_close = existing.sock
                else:
                    return False

            reader_thread = threading.Thread(
                target=self.peer_reader_loop,
                args=(peer_ip, sock, peer_name),
                daemon=True
            )

            peer = Peer(
                ip=peer_ip,
                name=peer_name or peer_ip,
                sock=sock,
                connected=True,
                reader_thread=reader_thread,
                source=source
            )
            self.peers[peer_ip] = peer

            if not existing or not existing.connected:
                need_log_connected = True

            accepted = True

        if old_sock_to_close is not None:
            try:
                old_sock_to_close.close()
            except OSError:
                pass

        if need_log_connected:
            self.add_history_event(
                event_type="peer_connected",
                peer_ip=peer_ip,
                peer_name=peer_name or peer_ip
            )

        reader_thread.start()
        return accepted

    # -------------------------
    # UDP
    # -------------------------
    def send_hello_once(self):
        if self.udp_socket is None:
            return

        msg = create_message(MessageType.HELLO, self.name)
        for broadcast_ip in self.broadcast_addresses:
            try:
                self.udp_socket.sendto(msg, (broadcast_ip, UDP_PORT))
            except OSError:
                pass

    def send_hello_burst(self):
        for _ in range(HELLO_BURST_COUNT):
            if not self.running:
                return
            self.send_hello_once()
            time.sleep(HELLO_BURST_DELAY)

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

            need_connect = False
            discovered_now = False

            with self.peers_lock:
                peer = self.peers.get(sender_ip)

                if peer is None:
                    self.peers[sender_ip] = Peer(ip=sender_ip, name=sender_name)
                    discovered_now = True
                    need_connect = True
                else:
                    if sender_name:
                        peer.name = sender_name
                    if not peer.connected:
                        need_connect = True

            if discovered_now:
                self.add_history_event(
                    event_type="peer_discovered",
                    peer_ip=sender_ip,
                    peer_name=sender_name
                )

            # По условию другие узлы, получившие HELLO, устанавливают TCP-соединение.
            if need_connect:
                threading.Thread(
                    target=self.connect_to_peer,
                    args=(sender_ip, sender_name),
                    daemon=True
                ).start()

    # -------------------------
    # TCP: исходящее подключение
    # -------------------------
    def connect_to_peer(self, peer_ip, peer_name=""):
        if peer_ip in self.local_ip_set:
            return

        with self.peers_lock:
            existing = self.peers.get(peer_ip)
            if existing and existing.connected:
                return

        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(SOCKET_TIMEOUT)
            sock.connect((peer_ip, TCP_PORT))
            sock.settimeout(None)

            sock.sendall(create_message(MessageType.NAME, self.name))

            accepted = self.register_connection(
                peer_ip=peer_ip,
                peer_name=peer_name or peer_ip,
                sock=sock,
                source="outgoing"
            )

            if not accepted:
                sock.close()

        except OSError as e:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

            with self.peers_lock:
                peer = self.peers.get(peer_ip)
                if peer and not peer.connected:
                    peer.name = peer_name or peer.name

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

            threading.Thread(
                target=self.handle_incoming_connection,
                args=(peer_sock, peer_addr),
                daemon=True
            ).start()

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

            with self.peers_lock:
                if peer_ip not in self.peers:
                    self.peers[peer_ip] = Peer(ip=peer_ip, name=peer_name)
                    self.add_history_event(
                        event_type="peer_discovered",
                        peer_ip=peer_ip,
                        peer_name=peer_name
                    )
                else:
                    if peer_name:
                        self.peers[peer_ip].name = peer_name

            accepted = self.register_connection(
                peer_ip=peer_ip,
                peer_name=peer_name,
                sock=peer_sock,
                source="incoming"
            )

            if not accepted:
                peer_sock.close()

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
    # Чтение TCP
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
            self.mark_peer_disconnected(peer_ip, peer_sock)
            try:
                peer_sock.close()
            except OSError:
                pass

    # -------------------------
    # Работа с peers
    # -------------------------
    def mark_peer_disconnected(self, peer_ip, disconnected_sock=None):
        with self.peers_lock:
            peer = self.peers.get(peer_ip)
            if peer is None:
                return

            if disconnected_sock is not None and peer.sock is not disconnected_sock:
                return

            peer.connected = False
            peer.sock = None
            peer.reader_thread = None
            peer.source = None

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
    # Отправка сообщений
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
                    self.mark_peer_disconnected(peer.ip, peer.sock)

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

        threading.Thread(target=self.udp_listener, daemon=True).start()
        threading.Thread(target=self.tcp_listener, daemon=True).start()

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

        # Отправляем HELLO только при подключении к сети, а не постоянно.
        threading.Thread(target=self.send_hello_burst, daemon=True).start()


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
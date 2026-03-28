import json
import socket
import struct
import threading
import time
from datetime import datetime
from enum import IntEnum


BUFFER_SIZE = 4096
HEADER_FORMAT = "!BI"   # 1 byte type, 4 bytes payload length
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
SOCKET_TIMEOUT = 5.0


class MessageType(IntEnum):
    CHAT = 1
    NAME = 2
    HELLO = 3
    HELLO_ACK = 4
    DISCONNECT = 5


class Peer:
    def __init__(
        self,
        ip,
        tcp_port,
        name="",
        sock=None,
        connected=False,
        reader_thread=None
    ):
        self.ip = ip
        self.tcp_port = tcp_port
        self.name = name
        self.sock = sock
        self.connected = connected
        self.reader_thread = reader_thread

    @property
    def key(self):
        return self.ip, self.tcp_port


class HistoryEvent:
    def __init__(self, event_type, peer_ip="", peer_name="", peer_tcp_port=None, content="", timestamp=None):
        self.timestamp = timestamp or datetime.now().strftime("%H:%M:%S")
        self.event_type = event_type
        self.peer_ip = peer_ip
        self.peer_name = peer_name
        self.peer_tcp_port = peer_tcp_port
        self.content = content

    def format_for_display(self):
        peer_suffix = ""
        if self.peer_ip:
            if self.peer_tcp_port is not None:
                peer_suffix = f"{self.peer_name} ({self.peer_ip}:{self.peer_tcp_port})"
            else:
                peer_suffix = f"{self.peer_name} ({self.peer_ip})"

        if self.event_type == "peer_discovered":
            return f"[{self.timestamp}] [INFO] Обнаружен узел: {peer_suffix}"
        if self.event_type == "peer_connected":
            return f"[{self.timestamp}] [INFO] Установлено соединение с: {peer_suffix}"
        if self.event_type == "incoming_message":
            return f"[{self.timestamp}] [CHAT] {peer_suffix}: {self.content}"
        if self.event_type == "outgoing_message":
            return f"[{self.timestamp}] [CHAT] Вы: {self.content}"
        if self.event_type == "peer_disconnected":
            return f"[{self.timestamp}] [INFO] Узел отключился: {peer_suffix}"
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

    payload = b""
    if payload_length > 0:
        payload = recv_exact(sock, payload_length)

    return msg_type, payload.decode("utf-8")


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
    print("Доступные локальные IPv4-адреса:")
    for i, ip in enumerate(local_ips, start=1):
        print(f"{i}. {ip}")

    print("Можно ввести номер из списка или IP вручную, например 127.0.0.2")

    while True:
        raw = input("Выберите IP для работы чата: ").strip()

        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(local_ips):
                return local_ips[idx - 1]

        try:
            socket.inet_aton(raw)
            return raw
        except OSError:
            print("Неверный ввод. Введите номер из списка или корректный IPv4-адрес.")


def choose_port_interactively(prompt):
    while True:
        raw = input(prompt).strip()
        if raw.isdigit():
            port = int(raw)
            if 1 <= port <= 65535:
                return port
        print("Введите целое число от 1 до 65535.")


def is_port_available(ip, port, sock_type):
    test_sock = socket.socket(socket.AF_INET, sock_type)
    try:
        test_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        test_sock.bind((ip, port))
        return True
    except OSError:
        return False
    finally:
        try:
            test_sock.close()
        except OSError:
            pass


def ensure_port_available(ip, port, sock_type, title):
    if not is_port_available(ip, port, sock_type):
        raise OSError(f"{title}-порт {port} уже занят или недоступен для IP {ip}")


def derive_broadcast_addresses(ip):
    result = set()

    if ip.startswith("127."):
        result.add("127.255.255.255")
    else:
        result.add("255.255.255.255")
        parts = ip.split(".")
        if len(parts) == 4:
            result.add(f"{parts[0]}.{parts[1]}.{parts[2]}.255")

    return sorted(result)


class P2PChatApp:
    def __init__(self, name, bind_ip, udp_port, tcp_port):
        self.name = name
        self.ip = bind_ip
        self.udp_port = udp_port
        self.tcp_port = tcp_port

        self.local_ips = get_all_local_ipv4()
        self.broadcast_addresses = derive_broadcast_addresses(self.ip)

        self.running = False

        # key = (peer_ip, peer_tcp_port)
        self.peers = {}
        self.history = []

        self.peers_lock = threading.Lock()
        self.history_lock = threading.Lock()

        self.udp_socket = None
        self.tcp_server_socket = None

    @property
    def self_key(self):
        return self.ip, self.tcp_port

    def add_history_event(self, event_type, peer_ip="", peer_name="", peer_tcp_port=None, content=""):
        event = HistoryEvent(
            event_type=event_type,
            peer_ip=peer_ip,
            peer_name=peer_name,
            peer_tcp_port=peer_tcp_port,
            content=content
        )
        with self.history_lock:
            self.history.append(event)
        print(event.format_for_display())

    def make_peer_key(self, peer_ip, peer_tcp_port):
        return peer_ip, peer_tcp_port

    def ensure_peer_exists(self, peer_ip, peer_tcp_port, peer_name=""):
        key = self.make_peer_key(peer_ip, peer_tcp_port)
        discovered_now = False

        with self.peers_lock:
            if key not in self.peers:
                self.peers[key] = Peer(
                    ip=peer_ip,
                    tcp_port=peer_tcp_port,
                    name=peer_name or peer_ip
                )
                discovered_now = True
            else:
                if peer_name:
                    self.peers[key].name = peer_name

            current_name = self.peers[key].name

        if discovered_now:
            self.add_history_event(
                event_type="peer_discovered",
                peer_ip=peer_ip,
                peer_name=current_name,
                peer_tcp_port=peer_tcp_port
            )

    def get_peer(self, peer_ip, peer_tcp_port):
        key = self.make_peer_key(peer_ip, peer_tcp_port)
        with self.peers_lock:
            return self.peers.get(key)

    def get_peer_name(self, peer_ip, peer_tcp_port, fallback=""):
        peer = self.get_peer(peer_ip, peer_tcp_port)
        if peer and peer.name:
            return peer.name
        return fallback or peer_ip

    def is_connected(self, peer_ip, peer_tcp_port):
        peer = self.get_peer(peer_ip, peer_tcp_port)
        return bool(peer and peer.connected)

    def mark_peer_disconnected(self, peer_ip, peer_tcp_port, emit_event=False):
        key = self.make_peer_key(peer_ip, peer_tcp_port)
        event_data = None

        with self.peers_lock:
            if key in self.peers:
                peer = self.peers[key]
                was_connected = peer.connected
                peer.connected = False
                peer.sock = None
                peer.reader_thread = None

                if emit_event and was_connected:
                    event_data = (peer.ip, peer.tcp_port, peer.name)

        if event_data:
            self.add_history_event(
                event_type="peer_disconnected",
                peer_ip=event_data[0],
                peer_name=event_data[2],
                peer_tcp_port=event_data[1]
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
            print(f"    [{state}] {peer.name} ({peer.ip}:{peer.tcp_port})")

    def create_udp_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.ip, self.udp_port))
        self.udp_socket = sock

    def create_tcp_server_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.ip, self.tcp_port))
        sock.listen(10)
        self.tcp_server_socket = sock

    def make_hello_payload(self):
        data = {
            "name": self.name,
            "tcp_port": self.tcp_port
        }
        return json.dumps(data, ensure_ascii=False)

    def parse_hello_payload(self, payload):
        try:
            data = json.loads(payload)
            name = str(data.get("name", "")).strip()
            tcp_port = int(data.get("tcp_port"))
            if not (1 <= tcp_port <= 65535):
                return None, None
            return name, tcp_port
        except (ValueError, TypeError, json.JSONDecodeError):
            return None, None

    def send_udp_packet(self, target_ip, msg_type, payload):
        try:
            self.udp_socket.sendto(
                create_message(msg_type, payload),
                (target_ip, self.udp_port)
            )
        except OSError as e:
            self.add_history_event("warn", content=f"Ошибка отправки UDP на {target_ip}:{self.udp_port} - {e}")

    def send_hello_broadcast(self):
        payload = self.make_hello_payload()
        for broadcast_ip in self.broadcast_addresses:
            self.send_udp_packet(broadcast_ip, MessageType.HELLO, payload)

    def send_hello_ack(self, target_ip):
        payload = self.make_hello_payload()
        self.send_udp_packet(target_ip, MessageType.HELLO_ACK, payload)

    def rescan(self):
        self.add_history_event("system", content="Повторное обнаружение узлов...")
        self.send_hello_broadcast()

    def maybe_connect_by_rule(self, peer_ip, peer_tcp_port):
        if (peer_ip, peer_tcp_port) == self.self_key:
            return

        if self.is_connected(peer_ip, peer_tcp_port):
            return

        # Один TCP-канал на пару узлов:
        # инициирует тот, у кого ключ меньше.
        if self.self_key < (peer_ip, peer_tcp_port):
            peer_name = self.get_peer_name(peer_ip, peer_tcp_port)
            self.connect_to_peer(peer_ip, peer_tcp_port, peer_name)

    def udp_listener(self):
        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(BUFFER_SIZE)
            except OSError:
                break

            sender_ip = addr[0]

            if len(data) < HEADER_SIZE:
                continue

            try:
                msg_type, payload_length = parse_message_header(data[:HEADER_SIZE])
            except (struct.error, ValueError):
                continue

            if len(data) < HEADER_SIZE + payload_length:
                continue

            try:
                payload = data[HEADER_SIZE:HEADER_SIZE + payload_length].decode("utf-8")
            except UnicodeDecodeError:
                continue

            if msg_type not in (MessageType.HELLO, MessageType.HELLO_ACK):
                continue

            sender_name, sender_tcp_port = self.parse_hello_payload(payload)
            if not sender_name or sender_tcp_port is None:
                continue

            if (sender_ip, sender_tcp_port) == self.self_key:
                continue

            self.ensure_peer_exists(sender_ip, sender_tcp_port, sender_name)

            if msg_type == MessageType.HELLO:
                self.send_hello_ack(sender_ip)

            self.maybe_connect_by_rule(sender_ip, sender_tcp_port)

    def connect_to_peer(self, peer_ip, peer_tcp_port, peer_name=""):
        if (peer_ip, peer_tcp_port) == self.self_key:
            return

        with self.peers_lock:
            key = self.make_peer_key(peer_ip, peer_tcp_port)
            if key in self.peers and self.peers[key].connected:
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

            # КРИТИЧНО ДЛЯ LOOPBACK:
            # явно привязываем исходящее TCP-соединение к IP текущего узла,
            # иначе Windows может отправить его с 127.0.0.1
            tcp_sock.bind((self.ip, 0))

            tcp_sock.settimeout(SOCKET_TIMEOUT)
            tcp_sock.connect((peer_ip, peer_tcp_port))
            tcp_sock.settimeout(None)

            tcp_sock.sendall(create_message(MessageType.NAME, self.name))

            with self.peers_lock:
                key = self.make_peer_key(peer_ip, peer_tcp_port)
                if key in self.peers and self.peers[key].connected:
                    tcp_sock.close()
                    return

                actual_name = peer_name or self.get_peer_name(peer_ip, peer_tcp_port, peer_ip)
                peer = Peer(
                    ip=peer_ip,
                    tcp_port=peer_tcp_port,
                    name=actual_name,
                    sock=tcp_sock,
                    connected=True
                )
                reader_thread = threading.Thread(
                    target=self.peer_reader_loop,
                    args=(peer_ip, peer_tcp_port, tcp_sock, actual_name),
                    daemon=True
                )
                peer.reader_thread = reader_thread
                self.peers[key] = peer

            self.add_history_event(
                event_type="peer_connected",
                peer_ip=peer_ip,
                peer_name=self.get_peer_name(peer_ip, peer_tcp_port, peer_ip),
                peer_tcp_port=peer_tcp_port
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
                content=f"Не удалось подключиться к {peer_name or peer_ip} ({peer_ip}:{peer_tcp_port}): {e}"
            )

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

        try:
            peer_sock.settimeout(SOCKET_TIMEOUT)
            msg_type, payload = read_message(peer_sock)
            peer_sock.settimeout(None)

            if msg_type != MessageType.NAME:
                peer_sock.close()
                return

            peer_name = payload.strip() or peer_ip

            # TCP-порт удалённого узла берём из уже полученного HELLO,
            # иначе оставляем None и не принимаем такое соединение.
            with self.peers_lock:
                candidates = [p for p in self.peers.values() if p.ip == peer_ip]
                peer = candidates[0] if candidates else None

            if peer is None or peer.tcp_port is None:
                peer_sock.close()
                return

            peer_tcp_port = peer.tcp_port
            self.ensure_peer_exists(peer_ip, peer_tcp_port, peer_name)

            with self.peers_lock:
                key = self.make_peer_key(peer_ip, peer_tcp_port)
                if key in self.peers and self.peers[key].connected:
                    peer_sock.close()
                    return

                peer_obj = Peer(
                    ip=peer_ip,
                    tcp_port=peer_tcp_port,
                    name=peer_name,
                    sock=peer_sock,
                    connected=True
                )

                reader_thread = threading.Thread(
                    target=self.peer_reader_loop,
                    args=(peer_ip, peer_tcp_port, peer_sock, peer_name),
                    daemon=True
                )
                peer_obj.reader_thread = reader_thread
                self.peers[key] = peer_obj

            self.add_history_event(
                event_type="peer_connected",
                peer_ip=peer_ip,
                peer_name=peer_name,
                peer_tcp_port=peer_tcp_port
            )

            reader_thread.start()

        except (ConnectionError, OSError):
            try:
                peer_sock.close()
            except OSError:
                pass

    def peer_reader_loop(self, peer_ip, peer_tcp_port, peer_sock, peer_name=""):
        disconnect_logged = False

        try:
            while self.running:
                msg_type, payload = read_message(peer_sock)

                if msg_type == MessageType.CHAT:
                    actual_name = self.get_peer_name(peer_ip, peer_tcp_port, peer_name)
                    self.add_history_event(
                        event_type="incoming_message",
                        peer_ip=peer_ip,
                        peer_name=actual_name,
                        peer_tcp_port=peer_tcp_port,
                        content=payload
                    )

                elif msg_type == MessageType.DISCONNECT:
                    actual_name = self.get_peer_name(peer_ip, peer_tcp_port, peer_name)
                    self.add_history_event(
                        event_type="peer_disconnected",
                        peer_ip=peer_ip,
                        peer_name=actual_name,
                        peer_tcp_port=peer_tcp_port
                    )
                    disconnect_logged = True
                    break

        except (ConnectionError, OSError):
            if self.running:
                actual_name = self.get_peer_name(peer_ip, peer_tcp_port, peer_name)
                self.add_history_event(
                    event_type="peer_disconnected",
                    peer_ip=peer_ip,
                    peer_name=actual_name,
                    peer_tcp_port=peer_tcp_port
                )
                disconnect_logged = True
        finally:
            self.mark_peer_disconnected(peer_ip, peer_tcp_port, emit_event=False)

            try:
                peer_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            try:
                peer_sock.close()
            except OSError:
                pass

            if not disconnect_logged and not self.running:
                pass

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
                    self.mark_peer_disconnected(peer.ip, peer.tcp_port, emit_event=True)

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

    def shutdown(self):
        self.running = False

        with self.peers_lock:
            peers_copy = list(self.peers.values())

        # Сначала сообщаем об отключении
        for peer in peers_copy:
            if peer.connected and peer.sock is not None:
                try:
                    peer.sock.sendall(create_message(MessageType.DISCONNECT, ""))
                except OSError:
                    pass

        # Даём данным уйти в сеть
        time.sleep(0.2)

        # Потом мягко закрываем соединения
        for peer in peers_copy:
            if peer.sock is not None:
                try:
                    peer.sock.shutdown(socket.SHUT_RDWR)
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

    def start(self):
        ensure_port_available(self.ip, self.udp_port, socket.SOCK_DGRAM, "UDP")
        ensure_port_available(self.ip, self.tcp_port, socket.SOCK_STREAM, "TCP")

        self.create_udp_socket()
        self.create_tcp_server_socket()

        self.running = True

        udp_thread = threading.Thread(target=self.udp_listener, daemon=True)
        tcp_thread = threading.Thread(target=self.tcp_listener, daemon=True)

        udp_thread.start()
        tcp_thread.start()

        self.add_history_event(
            event_type="system",
            content=f"Чат запущен. Имя: {self.name}, IP: {self.ip}, UDP: {self.udp_port}, TCP: {self.tcp_port}"
        )
        self.add_history_event(
            event_type="system",
            content=f"Broadcast-адреса: {', '.join(self.broadcast_addresses)}"
        )
        self.add_history_event(
            event_type="system",
            content="Команды: /peers, /rescan, /exit"
        )

        self.send_hello_broadcast()


if __name__ == "__main__":
    user_name = input("Введите имя: ").strip()
    while not user_name:
        user_name = input("Имя не может быть пустым. Введите имя: ").strip()

    local_ips = get_all_local_ipv4()
    bind_ip = choose_ip_interactively(local_ips)

    udp_port = choose_port_interactively("Введите UDP-порт для обнаружения узлов: ")
    tcp_port = choose_port_interactively("Введите TCP-порт для обмена сообщениями: ")

    app = None
    try:
        app = P2PChatApp(user_name, bind_ip, udp_port, tcp_port)
        app.start()

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
        if app is not None:
            app.shutdown()
    except OSError as e:
        print(f"[ERROR] {e}")
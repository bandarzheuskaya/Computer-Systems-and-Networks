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
CONFIG_FILE = "config.json"


class MessageType(IntEnum):
    CHAT = 1
    NAME = 2
    HELLO = 3
    DISCONNECT = 4
    HISTORY_REQUEST = 5
    HISTORY_RESPONSE = 6


class Peer:
    def __init__(
        self,
        ip,
        name="",
        sock=None,
        connected=False,
        reader_thread=None
    ):
        self.ip = ip
        self.name = name
        self.sock = sock
        self.connected = connected
        self.reader_thread = reader_thread

    @property
    def key(self):
        return self.ip


class HistoryEvent:
    def __init__(self, event_type, peer_ip="", peer_name="", content="", timestamp=None):
        self.timestamp = timestamp or datetime.now().strftime("%H:%M:%S")
        self.event_type = event_type
        self.peer_ip = peer_ip
        self.peer_name = peer_name
        self.content = content

    def format_for_display(self):
        peer_suffix = ""
        if self.peer_ip:
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

    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "peer_ip": self.peer_ip,
            "peer_name": self.peer_name,
            "content": self.content
        }

    @staticmethod
    def from_dict(data):
        return HistoryEvent(
            event_type=str(data.get("event_type", "system")),
            peer_ip=str(data.get("peer_ip", "")),
            peer_name=str(data.get("peer_name", "")),
            content=str(data.get("content", "")),
            timestamp=str(data.get("timestamp", datetime.now().strftime("%H:%M:%S")))
        )


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


def load_config():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        udp_port = int(data["udp_port"])
        tcp_port = int(data["tcp_port"])

        if not (1 <= udp_port <= 65535):
            raise ValueError("udp_port вне диапазона 1..65535")
        if not (1 <= tcp_port <= 65535):
            raise ValueError("tcp_port вне диапазона 1..65535")

        return udp_port, tcp_port

    except FileNotFoundError:
        raise OSError(f"Файл конфигурации {CONFIG_FILE} не найден")
    except KeyError as e:
        raise OSError(f"В {CONFIG_FILE} отсутствует поле: {e}")
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        raise OSError(f"Некорректный {CONFIG_FILE}: {e}")


class P2PChatApp:
    def __init__(self, name, bind_ip, udp_port, tcp_port):
        self.name = name
        self.ip = bind_ip
        self.udp_port = udp_port
        self.tcp_port = tcp_port

        self.local_ips = get_all_local_ipv4()
        self.broadcast_addresses = derive_broadcast_addresses(self.ip)

        self.running = False

        self.peers = {}
        self.history = []

        self.peers_lock = threading.Lock()
        self.history_lock = threading.Lock()
        self.history_request_lock = threading.Lock()

        self.udp_socket = None
        self.tcp_server_socket = None

        self.auto_history_requested = False
        self.auto_history_received = False

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

    def validate_not_self_target(self, peer_ip, context):
        if peer_ip == self.ip:
            return False
        return True

    def ensure_peer_exists(self, peer_ip, peer_name=""):
        discovered_now = False

        with self.peers_lock:
            if peer_ip not in self.peers:
                self.peers[peer_ip] = Peer(
                    ip=peer_ip,
                    name=peer_name or peer_ip
                )
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

    def get_peer(self, peer_ip):
        with self.peers_lock:
            return self.peers.get(peer_ip)

    def get_peer_name(self, peer_ip, fallback=""):
        peer = self.get_peer(peer_ip)
        if peer and peer.name:
            return peer.name
        return fallback or peer_ip

    def get_peer_by_name(self, peer_name):
        target = peer_name.casefold()
        with self.peers_lock:
            for peer in self.peers.values():
                if peer.name and peer.name.casefold() == target:
                    return peer
        return None

    def is_connected(self, peer_ip):
        peer = self.get_peer(peer_ip)
        return bool(peer and peer.connected)

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

    def show_help(self):
        print("Доступные команды:")
        print("/help - показать список команд")
        print("/peers - показать список узлов")
        print("/rescan - повторно отправить широковещательный HELLO")
        print("/history <ник> - запросить историю у узла по нику")
        print("/exit - выйти из чата")
        print("Любой другой текст отправляется как сообщение в чат")

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
        return self.name

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

    def rescan(self):
        self.add_history_event("system", content="Повторное обнаружение узлов...")
        self.send_hello_broadcast()

    def maybe_connect_by_rule(self, peer_ip):
        if not self.validate_not_self_target(peer_ip, "попытка TCP-подключения к самому себе"):
            return

        peer = self.get_peer(peer_ip)

        if peer is not None and not peer.connected:
            self.connect_to_peer(peer_ip, self.get_peer_name(peer_ip))
            return

        if self.is_connected(peer_ip):
            return

        if self.ip < peer_ip:
            peer_name = self.get_peer_name(peer_ip)
            self.connect_to_peer(peer_ip, peer_name)

    def request_history_from_peer_name(self, peer_name):
        peer = self.get_peer_by_name(peer_name)
        if peer is None:
            self.add_history_event("warn", content=f"Узел с ником '{peer_name}' не найден")
            return

        if not peer.connected or peer.sock is None:
            self.add_history_event("warn", content=f"Узел {peer.name} ({peer.ip}) не подключён")
            return

        try:
            peer.sock.sendall(create_message(MessageType.HISTORY_REQUEST, ""))
            self.add_history_event(
                event_type="system",
                content=f"Запрошена история у узла {peer.name} ({peer.ip})"
            )
        except OSError as e:
            self.add_history_event(
                event_type="warn",
                content=f"Не удалось запросить историю у {peer.name} ({peer.ip}): {e}"
            )

    def maybe_request_history_automatically(self, peer_ip):
        with self.history_request_lock:
            if self.auto_history_requested or self.auto_history_received:
                return

            peer = self.get_peer(peer_ip)
            if peer is None or not peer.connected or peer.sock is None:
                return

            try:
                peer.sock.sendall(create_message(MessageType.HISTORY_REQUEST, ""))
                self.auto_history_requested = True
                self.add_history_event(
                    event_type="system",
                    content=f"Автоматически запрошена история у узла {peer.name} ({peer.ip})"
                )
            except OSError as e:
                self.add_history_event(
                    event_type="warn",
                    content=f"Не удалось автоматически запросить историю у {peer.name} ({peer.ip}): {e}"
                )

    def serialize_history(self):
        with self.history_lock:
            return json.dumps([event.to_dict() for event in self.history], ensure_ascii=False)

    def send_history_response(self, peer_sock):
        payload = self.serialize_history()
        peer_sock.sendall(create_message(MessageType.HISTORY_RESPONSE, payload))

    def apply_received_history(self, payload, from_ip, from_name):
        try:
            raw_events = json.loads(payload)
            if not isinstance(raw_events, list):
                raise ValueError("history payload is not a list")
        except (json.JSONDecodeError, ValueError) as e:
            self.add_history_event("warn", content=f"Получена некорректная история: {e}")
            return

        with self.history_lock:
            existing_keys = {
                (e.timestamp, e.event_type, e.peer_ip, e.peer_name, e.content)
                for e in self.history
            }

        imported_events = []
        for item in raw_events:
            try:
                event = HistoryEvent.from_dict(item)
            except Exception:
                continue

            key = (event.timestamp, event.event_type, event.peer_ip, event.peer_name, event.content)
            if key in existing_keys:
                continue

            imported_events.append(event)
            existing_keys.add(key)

        if not imported_events:
            self.auto_history_received = True
            self.add_history_event(
                "system",
                content=f"История от узла {from_name} ({from_ip}) получена, новых записей нет"
            )
            return

        with self.history_lock:
            self.history.extend(imported_events)

        print(f"===== ПОЛУЧЕННАЯ ИСТОРИЯ ОТ {from_name} ({from_ip}) =====")
        for event in imported_events:
            print(event.format_for_display())
        print("===== КОНЕЦ ИСТОРИИ =====")

        self.auto_history_received = True
        self.add_history_event(
            event_type="system",
            content=f"Получена история от узла {from_name} ({from_ip}): {len(imported_events)} записей"
        )

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

            if msg_type != MessageType.HELLO:
                continue

            sender_name = payload.strip()
            if not sender_name:
                continue

            if not self.validate_not_self_target(sender_ip, "обнаружение собственного HELLO"):
                continue

            self.ensure_peer_exists(sender_ip, sender_name)
            self.maybe_connect_by_rule(sender_ip)

    def connect_to_peer(self, peer_ip, peer_name=""):
        if not self.validate_not_self_target(peer_ip, "исходящее TCP-подключение к самому себе"):
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

            tcp_sock.bind((self.ip, 0))

            tcp_sock.settimeout(SOCKET_TIMEOUT)
            tcp_sock.connect((peer_ip, self.tcp_port))
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
            self.maybe_request_history_automatically(peer_ip)

        except OSError as e:
            if tcp_sock is not None:
                try:
                    tcp_sock.close()
                except OSError:
                    pass

            self.add_history_event(
                event_type="warn",
                content=f"Не удалось подключиться к {peer_name or peer_ip} ({peer_ip}:{self.tcp_port}): {e}"
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

            peer_name = payload.strip()
            if not peer_name:
                peer_sock.close()
                return

            if not self.validate_not_self_target(peer_ip, "входящее TCP-подключение от самого себя"):
                try:
                    peer_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                peer_sock.close()
                return

            self.ensure_peer_exists(peer_ip, peer_name)

            with self.peers_lock:
                if peer_ip in self.peers and self.peers[peer_ip].connected:
                    peer_sock.close()
                    return

                peer_obj = Peer(
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
                peer_obj.reader_thread = reader_thread
                self.peers[peer_ip] = peer_obj

            self.add_history_event(
                event_type="peer_connected",
                peer_ip=peer_ip,
                peer_name=peer_name
            )

            reader_thread.start()
            self.maybe_request_history_automatically(peer_ip)

        except (ConnectionError, OSError):
            try:
                peer_sock.close()
            except OSError:
                pass

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

                elif msg_type == MessageType.HISTORY_REQUEST:
                    self.send_history_response(peer_sock)
                    self.add_history_event(
                        event_type="system",
                        content=f"История отправлена узлу {self.get_peer_name(peer_ip, peer_name)} ({peer_ip})"
                    )

                elif msg_type == MessageType.HISTORY_RESPONSE:
                    self.apply_received_history(payload, peer_ip, self.get_peer_name(peer_ip, peer_name))

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
                peer_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            try:
                peer_sock.close()
            except OSError:
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

        time.sleep(0.2)

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

        self.send_hello_broadcast()


if __name__ == "__main__":
    user_name = input("Введите имя: ").strip()
    while not user_name:
        user_name = input("Имя не может быть пустым. Введите имя: ").strip()

    local_ips = get_all_local_ipv4()
    bind_ip = choose_ip_interactively(local_ips)

    udp_port, tcp_port = load_config()

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

            if text.lower() == "/help":
                app.show_help()
                continue

            if text.lower() == "/peers":
                app.show_peers()
                continue

            if text.lower() == "/rescan":
                app.rescan()
                continue

            if text.lower().startswith("/history "):
                parts = text.split(maxsplit=1)
                if len(parts) != 2 or not parts[1].strip():
                    app.add_history_event("warn", content="Использование: /history <ник>")
                else:
                    app.request_history_from_peer_name(parts[1].strip())
                continue

            app.broadcast_chat_message(text)

    except KeyboardInterrupt:
        if app is not None:
            app.shutdown()
    except OSError as e:
        print(f"[ERROR] {e}")
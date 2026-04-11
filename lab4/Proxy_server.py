import json
import socket
import threading

CONFIG_FILE = 'config.json'
BUFFER_SIZE = 4096
BACKLOG = 10
HTTP_ENCODING = 'iso-8859-1'

print_lock = threading.Lock()

def safe_print(message):
    with print_lock:
        print(message)


def load_config(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        return json.load(file)


def parse_request_line(request_text):
    first_line = request_text.split('\r\n')[0]
    parts = first_line.split()

    if len(parts) != 3:
        return None, None, None

    method, target, version = parts
    return method, target, version


def parse_target_url(target):
    if not target.startswith('http://'):
        return None

    rest = target[len('http://'):]
    slash_pos = rest.find('/')

    if slash_pos == -1:
        host_port = rest
        path = '/'
    else:
        host_port = rest[:slash_pos]
        path = rest[slash_pos:]

    if ':' in host_port:
        host, port = host_port.split(':', 1)
        port = int(port)
    else:
        host = host_port
        port = 80

    return host, port, path


def build_http_request(request_text, method, path, version):
    lines = request_text.split('\r\n')
    result_lines = [f'{method} {path} {version}']
    has_connection_header = False

    for line in lines[1:]:
        if line == '':
            continue

        lower_line = line.lower()

        if lower_line.startswith('proxy-connection:'):
            continue

        if lower_line.startswith('connection:'):
            result_lines.append('Connection: close')
            has_connection_header = True
            continue

        result_lines.append(line)

    if not has_connection_header:
        result_lines.append('Connection: close')

    return '\r\n'.join(result_lines) + '\r\n\r\n'


def parse_response_status(response_bytes):
    response_text = response_bytes.decode(HTTP_ENCODING, errors='replace')
    first_line = response_text.split('\r\n')[0]
    parts = first_line.split(' ', 2)

    if len(parts) < 3:
        return None, None

    try:
        return int(parts[1]), parts[2]
    except ValueError:
        return None, None


def is_blocked(target, blocked_urls):
    target = target.lower()

    for blocked_url in blocked_urls:
        blocked_url = blocked_url.lower().strip()
        if target == blocked_url:
            return True

    return False


def send_error_response(client_socket, code, message):
    body = f'<html><body><h1>{code} {message}</h1></body></html>'

    response = (
        f'HTTP/1.1 {code} {message}\r\n'
        f'Content-Type: text/html; charset=utf-8\r\n'
        f'Content-Length: {len(body.encode("utf-8"))}\r\n'
        f'Connection: close\r\n'
        f'\r\n'
        f'{body}'
    )

    client_socket.sendall(response.encode('utf-8'))


def send_blocked_response(client_socket, target):
    body = f'''
<html>
<head><meta charset="utf-8"></head>
<body>
<h1>Доступ заблокирован</h1>
<p>Адрес: <b>{target}</b></p>
</body>
</html>
'''.strip()

    response = (
        f'HTTP/1.1 403 Forbidden\r\n'
        f'Content-Type: text/html; charset=utf-8\r\n'
        f'Content-Length: {len(body.encode("utf-8"))}\r\n'
        f'Connection: close\r\n'
        f'\r\n'
        f'{body}'
    )

    client_socket.sendall(response.encode('utf-8'))


def forward_request(server_request, host, port, client_socket, target):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        server_socket.connect((host, port))
        server_socket.sendall(server_request.encode(HTTP_ENCODING))

        first_chunk = server_socket.recv(BUFFER_SIZE)
        if not first_chunk:
            if not target.endswith('/favicon.ico'):
                safe_print(f'{target} – unknown')
            return

        response_code, response_status = parse_response_status(first_chunk)

        if response_code is None:
            if not target.endswith('/favicon.ico'):
                safe_print(f'{target} – unknown')
        else:
            if not target.endswith('/favicon.ico'):
                safe_print(f'{target} – {response_code} {response_status}')

        client_socket.sendall(first_chunk)

        while True:
            chunk = server_socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            client_socket.sendall(chunk)

    finally:
        server_socket.close()


class ProxyServer:
    def __init__(self, config):
        self.host = config.get('host', '127.0.0.1')
        self.port = config.get('port', 8080)
        self.blocked_urls = config.get('blocked_urls', [])

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))

    def start(self):
        self.server_socket.listen(BACKLOG)
        safe_print(f'Proxy server started on {self.host}:{self.port}')

        while True:
            client_socket, client_addr = self.server_socket.accept()

            thread = threading.Thread(
                target=self.handle_client,
                args=(client_socket, client_addr),
                daemon=True
            )
            thread.start()

    def handle_client(self, client_socket, client_addr):
        try:
            request_text = self.read_http_request(client_socket)
            if request_text is None:
                return

            method, target, version = parse_request_line(request_text)
            if target is None:
                send_error_response(client_socket, 400, 'Bad Request')
                return

            parsed_url = parse_target_url(target)
            if parsed_url is None:
                send_error_response(client_socket, 501, 'Not Implemented')
                return

            host, port, path = parsed_url

            if is_blocked(target, self.blocked_urls):
                if not target.endswith('/favicon.ico'):
                    safe_print(f'{target} – 403 Forbidden')
                send_blocked_response(client_socket, target)
                return

            server_request = build_http_request(request_text, method, path, version)

            forward_request(
                server_request,
                host,
                port,
                client_socket,
                target
            )

        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            safe_print(f'[ERROR] {client_addr[0]}:{client_addr[1]} - {e}')
            try:
                send_error_response(client_socket, 502, 'Bad Gateway')
            except OSError:
                pass
        finally:
            client_socket.close()

    def read_http_request(self, client_socket):
        data = b''

        while b'\r\n\r\n' not in data:
            chunk = client_socket.recv(BUFFER_SIZE)
            if not chunk:
                break

            data += chunk

            if len(data) > 65536:
                break

        if not data:
            return None

        return data.decode(HTTP_ENCODING, errors='replace')


def main():
    config = load_config(CONFIG_FILE)
    proxy_server = ProxyServer(config)
    proxy_server.start()


if __name__ == '__main__':
    main()
import socket
import argparse
import struct
from time import perf_counter
import os

ICMP_ID = os.getpid() & 0xFFFF
MAX_HOPS = 30
PROBES_PER_HOP = 3
TIMEOUT = 3


class ICMPPacketCreator:
    def __init__(self, target_ip, port=None, data=None, ttl=64, icmp_id=ICMP_ID):
        self.target_ip = target_ip
        self.port = port
        self.data = data
        self.ttl = ttl
        self.icmp_id = icmp_id

    def calculate_checksum(self, data):
        checksum = 0

        if len(data) % 2 != 0:
            data += b"\x00"

        for i in range(0, len(data), 2):
            checksum += (data[i] << 8) + data[i + 1]

        checksum = (checksum >> 16) + (checksum & 0xFFFF)
        checksum += checksum >> 16

        return (~checksum) & 0xFFFF

    def create_icmp_packet(self, icmp_sequence):
        icmp_type = 8
        icmp_code = 0
        icmp_checksum = 0

        if self.data:
            icmp_payload = self.data.encode()
        else:
            icmp_payload = b'abcdefghijklmnopqrstuvwabcdefghi'

        icmp_header = struct.pack(
            "!BBHHH",
            icmp_type,
            icmp_code,
            icmp_checksum,
            self.icmp_id,
            icmp_sequence
        )

        icmp_checksum = self.calculate_checksum(icmp_header + icmp_payload)

        icmp_header = struct.pack(
            "!BBHHH",
            icmp_type,
            icmp_code,
            icmp_checksum,
            self.icmp_id,
            icmp_sequence
        )

        return icmp_header + icmp_payload


def resolve_target(target):
    return socket.getaddrinfo(target, None, family=socket.AF_INET)[0][4][0]


def format_host(ip, resolve_names=False):
    if not resolve_names:
        return ip

    try:
        hostname = socket.gethostbyaddr(ip)[0]
        return f"{hostname} [{ip}]"
    except socket.herror:
        return ip


def format_target_header(target, target_ip, resolve_names=False):
    if target != target_ip:
        return f"{target} [{target_ip}]"

    if resolve_names:
        try:
            hostname = socket.gethostbyaddr(target_ip)[0]
            return f"{hostname} [{target_ip}]"
        except socket.herror:
            return target_ip

    return target_ip


def create_socket(ttl):
    s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
    s.settimeout(TIMEOUT)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_TTL, ttl)
    return s


def send_probe(target_ip, ttl, seq):
    s = create_socket(ttl)
    try:
        pack = ICMPPacketCreator(target_ip, port=None, icmp_id=ICMP_ID).create_icmp_packet(seq)
        sec1 = perf_counter()
        s.sendto(pack, (target_ip, 0))
        data, addr = s.recvfrom(65468)
        sec2 = perf_counter()
        return data, addr, (sec2 - sec1) * 1000
    except socket.timeout:
        return None, None, None
    finally:
        s.close()


def parse_icmp_header(data):
    ip_header_len = (data[0] & 0x0F) * 4
    icmp_header = data[ip_header_len:ip_header_len + 8]
    icmp_type, icmp_code, checksum, packet_id, sequence = struct.unpack("!BBHHH", icmp_header)
    return icmp_type, icmp_code, packet_id, sequence, ip_header_len


def check_echo_reply(data, expected_id, expected_seq):
    icmp_type, icmp_code, packet_id, packet_sequence, _ = parse_icmp_header(data)
    return icmp_type == 0 and packet_id == expected_id and packet_sequence == expected_seq


def check_time_exceeded(data, expected_id, expected_seq):
    outer_ip_header_len = (data[0] & 0x0F) * 4

    outer_icmp_header = data[outer_ip_header_len:outer_ip_header_len + 8]
    outer_type, outer_code, _, _, _ = struct.unpack("!BBHHH", outer_icmp_header)

    if outer_type != 11:
        return False

    inner_ip_start = outer_ip_header_len + 8
    inner_ip_header_len = (data[inner_ip_start] & 0x0F) * 4

    inner_icmp_start = inner_ip_start + inner_ip_header_len
    inner_icmp_header = data[inner_icmp_start:inner_icmp_start + 8]
    inner_type, inner_code, _, inner_id, inner_seq = struct.unpack("!BBHHH", inner_icmp_header)

    return inner_type == 8 and inner_id == expected_id and inner_seq == expected_seq


def trace_hop(target_ip, ttl, seq):
    last_valid_data = None
    last_valid_addr = None
    last_valid_seq = None

    for _ in range(PROBES_PER_HOP):
        seq += 1
        data, addr, rtt_ms = send_probe(target_ip, ttl, seq)

        if rtt_ms is None:
            print('*'.center(10, ' '), end='')
            continue

        is_echo_reply = check_echo_reply(data, ICMP_ID, seq)
        is_time_exceeded = check_time_exceeded(data, ICMP_ID, seq)

        if is_echo_reply or is_time_exceeded:
            if rtt_ms < 1:
                print("<1 ms".center(10, ' '), end='')
            else:
                print(f"{rtt_ms:.0f} ms".center(10, ' '), end='')
            last_valid_data = data
            last_valid_addr = addr
            last_valid_seq = seq
        else:
            print('*'.center(10, ' '), end='')

    return last_valid_data, last_valid_addr, seq, last_valid_seq


parser = argparse.ArgumentParser()
parser.add_argument(
    '-r',
    action='store_true',
    help='resolve route node IP addresses to hostnames using reverse DNS'
)
parser.add_argument('target', type=str, help='host to trace route to')
args = parser.parse_args()

target_ip = resolve_target(args.target)

print(f"Tracing route to {format_target_header(args.target, target_ip, args.r)}")
print(f"over a maximum of {MAX_HOPS} hops:\n")

seq = 0

for ttl in range(1, MAX_HOPS + 1):
    print(f"{ttl}".center(7, ' '), end='')

    data, addr, seq, last_seq = trace_hop(target_ip, ttl, seq)

    if addr is not None and data is not None and last_seq is not None:
        print(format_host(addr[0], args.r))

        if check_echo_reply(data, ICMP_ID, last_seq):
            print('\nTrace complete.')
            break
    else:
        print('Request timed out.')
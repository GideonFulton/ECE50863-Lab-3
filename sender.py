#!/usr/bin/env python3
from monitor import Monitor
import sys, socket, struct, configparser, math, time

DATA, ACK, END = 0, 1, 2
ACK_NONE = 0xFFFFFFFF

DATA_FMT = "!BII"
DATA_HDR_SIZE = 9

ACK_FMT = "!BII"
ACK_SIZE = 9

SACK_BITS = 32

if __name__ == "__main__":
    config_path = sys.argv[1]

    cfg = configparser.RawConfigParser(allow_no_value=True)
    cfg.read(config_path)

    receiver_id = int(cfg.get("receiver", "id"))
    file_to_send = cfg.get("nodes", "file_to_send")
    max_packet_size = int(cfg.get("network", "MAX_PACKET_SIZE"))
    prop_delay = float(cfg.get("network", "PROP_DELAY"))
    bandwidth = int(cfg.get("network", "LINK_BANDWIDTH"))
    max_packets_queued = int(cfg.get("network", "MAX_PACKETS_QUEUED"))

    send_monitor = Monitor(config_path, "sender")

    monitor_header_size = len(f"{send_monitor.id} {receiver_id}\n".encode("ascii"))
    payload_size = max_packet_size - monitor_header_size - DATA_HDR_SIZE

    tx_delay = max_packet_size / bandwidth
    srtt = max(0.001, 2 * prop_delay + 2 * tx_delay)
    rttvar = max(0.001, srtt / 2)
    timeout = max(0.35, srtt + 4 * rttvar)
    send_monitor.socketfd.settimeout(timeout)

    bdp_bytes = bandwidth * srtt
    bdp_packets = max(1, round(bdp_bytes / payload_size))

    if bandwidth <= 3000:
        start_cap = 1
        hard_cap = 2
    elif bandwidth <= 30000:
        start_cap = 2
        hard_cap = 4
    elif bandwidth <= 100000:
        start_cap = 6
        hard_cap = 12
    else:
        start_cap = 10
        hard_cap = 24

    queue_cap = max_packets_queued // 8 if max_packets_queued > 0 else 50
    max_window = max(1, min(50, hard_cap, queue_cap))
    cwnd = float(max(1, min(bdp_packets, start_cap, max_window)))
    ssthresh = max(2.0, float(max_window // 2))
    min_window = 1.0

    base = 0
    next_seq = 0
    eof = False
    end_seq = None

    outstanding = {}      
    send_times = {}    
    retransmitted = {}    

    last_cum_ack = ACK_NONE
    dup_acks = 0
    last_fast_retx_ack = None

    def make_data(seq: int, chunk: bytes) -> bytes:
        return struct.pack(DATA_FMT, DATA, seq, len(chunk)) + chunk

    def make_end(seq: int) -> bytes:
        return struct.pack(DATA_FMT, END, seq, 0)

    def send_packet(seq: int):
        send_monitor.send(receiver_id, outstanding[seq])
        send_times[seq] = time.time()

    with open(file_to_send, "rb") as f:
        while True:
            window_int = max(1, int(math.floor(cwnd)))

            while next_seq < base + window_int and not eof:
                chunk = f.read(payload_size)
                if chunk:
                    outstanding[next_seq] = make_data(next_seq, chunk)
                else:
                    outstanding[next_seq] = make_end(next_seq)
                    eof = True
                    end_seq = next_seq

                retransmitted[next_seq] = False
                send_packet(next_seq)
                next_seq += 1

            if eof and end_seq is not None and base > end_seq:
                break

            try:
                _, ack = send_monitor.recv(max_packet_size)
            except socket.timeout:
                if base in outstanding:
                    send_packet(base)
                    retransmitted[base] = True

                ssthresh = max(2.0, cwnd / 2.0)
                cwnd = 1.0

                timeout = min(3.0, max(0.35, timeout * 1.35))
                send_monitor.socketfd.settimeout(timeout)

                dup_acks = 0
                last_fast_retx_ack = None
                continue

            if ack is None or len(ack) < ACK_SIZE:
                continue

            ack_type, cum_ack, sack_bitmap = struct.unpack(ACK_FMT, ack[:ACK_SIZE])
            if ack_type != ACK:
                continue

            current_sacked = set()
            start = 0 if cum_ack == ACK_NONE else cum_ack + 1
            for i in range(SACK_BITS):
                if sack_bitmap & (1 << i):
                    current_sacked.add(start + i)

            if cum_ack == last_cum_ack:
                dup_acks += 1
            else:
                last_cum_ack = cum_ack
                dup_acks = 1
                last_fast_retx_ack = None

            if cum_ack != ACK_NONE and cum_ack >= base:
                newly_acked = cum_ack - base + 1

                if cum_ack in send_times and not retransmitted.get(cum_ack, True):
                    sample_rtt = time.time() - send_times[cum_ack]
                    if sample_rtt > 0:
                        alpha = 1 / 8
                        beta = 1 / 4
                        rttvar = (1 - beta) * rttvar + beta * abs(srtt - sample_rtt)
                        srtt = (1 - alpha) * srtt + alpha * sample_rtt
                        timeout = max(0.25, srtt + 4 * rttvar)
                        send_monitor.socketfd.settimeout(timeout)

                for seq in range(base, cum_ack + 1):
                    outstanding.pop(seq, None)
                    send_times.pop(seq, None)
                    retransmitted.pop(seq, None)

                base = cum_ack + 1
                dup_acks = 0
                last_fast_retx_ack = None

                if cwnd < ssthresh:
                    cwnd = min(float(max_window), cwnd + newly_acked)
                else:
                    cwnd = min(float(max_window), cwnd + (newly_acked / max(cwnd, 1.0)))

            elif dup_acks >= 3 and current_sacked and last_fast_retx_ack != cum_ack:
                missing = base
                while missing in current_sacked:
                    missing += 1

                if missing in outstanding:
                    send_packet(missing)
                    retransmitted[missing] = True
                    last_fast_retx_ack = cum_ack

                    ssthresh = max(2.0, cwnd / 2.0)
                    cwnd = max(min_window, cwnd / 2.0)

    send_monitor.send_end(receiver_id)
#!/usr/bin/env python3
from monitor import Monitor
import sys, socket, struct, configparser

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

    sender_id = int(cfg.get("sender", "id"))
    write_location = cfg.get("receiver", "write_location")
    max_packet_size = int(cfg.get("network", "MAX_PACKET_SIZE"))
    prop_delay = float(cfg.get("network", "PROP_DELAY"))
    bandwidth = int(cfg.get("network", "LINK_BANDWIDTH"))

    recv_monitor = Monitor(config_path, "receiver")

    tx_delay = max_packet_size / bandwidth
    end_timeout = max(0.35, 2 * prop_delay + 2 * tx_delay + 0.05)

    expected = 0
    final_seq = None
    buffer = {}   

    def build_ack(cum_ack: int) -> bytes:
        start = 0 if cum_ack == ACK_NONE else cum_ack + 1
        sack_bitmap = 0
        for i in range(SACK_BITS):
            if (start + i) in buffer:
                sack_bitmap |= (1 << i)

        encoded_cum = ACK_NONE if cum_ack < 0 else cum_ack
        return struct.pack(ACK_FMT, ACK, encoded_cum, sack_bitmap)

    out = open(write_location, "wb")

    while True:
        try:
            src, packet = recv_monitor.recv(max_packet_size)
        except socket.timeout:
            continue

        if src != sender_id or packet is None or len(packet) < DATA_HDR_SIZE:
            continue

        pkt_type, seq, payload_len = struct.unpack(DATA_FMT, packet[:DATA_HDR_SIZE])
        payload = packet[DATA_HDR_SIZE:]

        if len(payload) != payload_len:
            continue

        if pkt_type == DATA:
            if seq < expected:
                recv_monitor.send(sender_id, build_ack(expected - 1))

            elif seq == expected:
                out.write(payload)
                expected += 1

                while expected in buffer:
                    out.write(buffer.pop(expected))
                    expected += 1

                out.flush()
                recv_monitor.send(sender_id, build_ack(expected - 1))

            else:
                if seq not in buffer:
                    buffer[seq] = payload
                recv_monitor.send(sender_id, build_ack(expected - 1))

        elif pkt_type == END:
            if seq == expected:
                recv_monitor.send(sender_id, build_ack(seq))
                out.flush()
                out.close()
                recv_monitor.recv_end(write_location, sender_id)
                final_seq = seq
                recv_monitor.socketfd.settimeout(end_timeout)
                break
            else:
                recv_monitor.send(sender_id, build_ack(expected - 1))

    while True:
        try:
            src, packet = recv_monitor.recv(max_packet_size)
        except socket.timeout:
            break

        if src != sender_id or packet is None or len(packet) < DATA_HDR_SIZE:
            continue

        pkt_type, seq, payload_len = struct.unpack(DATA_FMT, packet[:DATA_HDR_SIZE])

        if pkt_type == END and seq == final_seq and payload_len == 0:
            recv_monitor.send(sender_id, build_ack(final_seq))
        else:
            recv_monitor.send(sender_id, build_ack(final_seq))
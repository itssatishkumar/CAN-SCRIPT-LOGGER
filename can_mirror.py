import time
from ctypes import c_ubyte
from PCANBasic import *

class CANMirrorEngine:
    def __init__(self, pcan, channel):
        self.pcan = pcan
        self.channel = channel
        self.rules = {}  # rx_id -> rule

    def add_rule(self, rx_id, tx_id, extended=False, interval_ms=0):
        self.rules[rx_id] = {
            "tx_id": tx_id,
            "extended": extended,
            "interval": interval_ms,
            "last_sent": 0,
            "enabled": True
        }

    def remove_rule(self, rx_id):
        self.rules.pop(rx_id, None)

    def handle_rx(self, msg):
        rule = self.rules.get(msg.ID)
        if not rule or not rule["enabled"]:
            return

        now = time.time() * 1000
        if rule["interval"] > 0:
            if now - rule["last_sent"] < rule["interval"]:
                return

        tx = TPCANMsg()
        tx.ID = rule["tx_id"]
        tx.LEN = msg.LEN

        data = [int(b) & 0xFF for b in msg.DATA[:msg.LEN]]
        tx.DATA = (c_ubyte * 8)(*data + [0] * (8 - len(data)))

        tx.MSGTYPE = (
            PCAN_MESSAGE_EXTENDED
            if rule["extended"]
            else PCAN_MESSAGE_STANDARD
        )

        if self.pcan.Write(self.channel, tx) == PCAN_ERROR_OK:
            rule["last_sent"] = now

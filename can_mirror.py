import time
from ctypes import c_ubyte
from PCANBasic import *


class CANMirrorEngine:

    def __init__(self, pcan, channel):
        self.pcan = pcan
        self.channel = channel

        # rx_id -> rule
        self.rules = {}

        # rx_id -> last received byte list
        self.last_rx_data = {}

        # callback(tx_msg)
        self.on_tx = None

    # ------------------------------------------------------------------
    # Rule management
    # ------------------------------------------------------------------
    def add_rule(
        self,
        rx_id,
        tx_id,
        extended=False,
        interval_ms=0,
        byte_rules=None,
    ):
        self.rules[rx_id] = {
            "tx_id": tx_id,
            "extended": extended,
            "interval": interval_ms,
            "last_sent": 0,
            "enabled": True,
            "byte_rules": byte_rules or [],
        }

    def remove_rule(self, rx_id):
        self.rules.pop(rx_id, None)

    def enable_rule(self, rx_id, enable=True):
        if rx_id in self.rules:
            self.rules[rx_id]["enabled"] = enable

    # ------------------------------------------------------------------
    # RX data access for UI (IMPORTANT)
    # ------------------------------------------------------------------
    def get_rx_bytes(self, rx_id):
        data = self.last_rx_data.get(rx_id, [])
        out = list(data[:8])
        while len(out) < 8:
            out.append(0)
        return out

    # ------------------------------------------------------------------
    # RX handler (called from main)
    # ------------------------------------------------------------------
    def handle_rx(self, msg):
        rx_id = msg.ID

        # Cache RX bytes ALWAYS (even if no rule)
        rx_data = [int(b) & 0xFF for b in msg.DATA[:msg.LEN]]
        self.last_rx_data[rx_id] = rx_data

        rule = self.rules.get(rx_id)
        if not rule or not rule["enabled"]:
            return

        # Interval handling
        now = time.time() * 1000
        if rule["interval"] > 0:
            if now - rule["last_sent"] < rule["interval"]:
                return

        # --------------------------------------------------------------
        # Prepare TX message
        # --------------------------------------------------------------
        tx = TPCANMsg()
        tx.ID = rule["tx_id"]
        tx.LEN = msg.LEN

        # Copy RX data (do NOT modify cached RX data)
        data = rx_data.copy()

        # --------------------------------------------------------------
        # APPLY BYTE RULES (OFFSET / REPLACE)
        # --------------------------------------------------------------
        for br in rule.get("byte_rules", []):
            try:
                idx = int(br.get("index", -1))
                mode = br.get("mode", "offset")
                val = int(br.get("value", 0))
            except Exception:
                continue

            if not (0 <= idx < len(data)):
                continue

            # -------- SAFE OFFSET LOGIC --------
            if mode == "offset":
                original = data[idx]

                # Absolute zero → no offset
                if original == 0:
                    continue

                new_val = original + val

                # Clamp at zero
                if new_val < 0:
                    new_val = 0

                # Clamp at 255 (extra safety)
                if new_val > 255:
                    new_val = 255

                data[idx] = new_val

            elif mode == "replace":
                data[idx] = val & 0xFF

        # Pad to 8 bytes
        tx.DATA = (c_ubyte * 8)(*data + [0] * (8 - len(data)))

        tx.MSGTYPE = (
            PCAN_MESSAGE_EXTENDED
            if rule["extended"]
            else PCAN_MESSAGE_STANDARD
        )

        # --------------------------------------------------------------
        # Transmit
        # --------------------------------------------------------------
        result = self.pcan.Write(self.channel, tx)
        if result == PCAN_ERROR_OK:
            rule["last_sent"] = now

            # Notify main app
            if self.on_tx:
                self.on_tx(tx)

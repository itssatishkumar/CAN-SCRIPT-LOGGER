#! /usr/bin/env python3
import can
import cantools
import csv
import time
import os
import datetime
import threading


class MarvelCANLogger:
    def __init__(self, dbc_path="Marvel_3W_all_variant.dbc", bitrate=250000):
        self.dbc_path = os.path.abspath(dbc_path)
        if not os.path.exists(self.dbc_path):
            raise FileNotFoundError(f"DBC file not found: {self.dbc_path}")

        self.db = cantools.database.load_file(self.dbc_path)
        timestamp = datetime.datetime.now().strftime("%d%b%Y_%H-%M-%S")
        self.filename = os.path.join(os.getcwd(), f"Marvel_log_{timestamp}.csv")

        print(f"[INFO] Using DBC: {self.dbc_path}")
        print(f"[INFO] Logging to: {self.filename}")

        try:
            print(f"[INFO] Opening PCAN channel (250 kbps) for passive logging...")
            self.bus = can.interface.Bus(
                channel="PCAN_USBBUS1", bustype="pcan", bitrate=bitrate
            )
        except Exception as e:
            print(
                "[INFO] MarvelCANLogger running silently — waiting for main software to feed CAN data."
            )
            print("[INFO] No standalone CAN initialization will be performed.")
            print(f"[WARN] Reason: {e}")
            self.bus = None

        self.stop_flag = False
        self.thread = threading.Thread(target=self._log_data, daemon=True)
        self.thread.start()

    def _log_data(self):
        with open(self.filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "CAN_ID", "Message_Name", "Signal", "Value"])
            f.flush()
            print("[INFO] Logging started. Press Ctrl+C to stop.\n")

            while not self.stop_flag:
                if self.bus:
                    msg = self.bus.recv(0.1)
                    if msg:
                        try:
                            decoded = self.db.decode_message(
                                msg.arbitration_id, msg.data
                            )
                            msg_name = self.db.get_message_by_frame_id(
                                msg.arbitration_id
                            ).name
                            for name, val in decoded.items():
                                writer.writerow(
                                    [
                                        round(msg.timestamp, 6),
                                        hex(msg.arbitration_id),
                                        msg_name,
                                        name,
                                        val,
                                    ]
                                )
                                f.flush()
                        except Exception:
                            pass
                else:
                    time.sleep(0.1)

    def stop(self):
        self.stop_flag = True
        if self.bus:
            self.bus.shutdown()
        print(f"\n[INFO] Logging stopped. Data saved to: {self.filename}")


if __name__ == "__main__":
    logger = MarvelCANLogger()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.stop()

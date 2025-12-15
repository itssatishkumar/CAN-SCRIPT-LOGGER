import sys
import time
from collections import deque
from ctypes import c_ubyte
from parse_tool import trc_to_csv, parse_log_to_compact_csv
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget, QSplitter, QStatusBar, QLabel,
    QToolBar, QPushButton, QHBoxLayout, QFileDialog, QHeaderView,
    QMenu, QDialog, QGridLayout, QLineEdit, QComboBox, QCheckBox,
    QTabWidget, QFrame, QToolButton, QWidgetAction, QMessageBox,
    QProgressDialog
)
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QPoint, QObject

# ----------------------------
# Import PCANBasic module
# ----------------------------
from PCANBasic import *  # noqa: F401,F403  (keep using constants directly like PCAN_USBBUS1)

# Safe fallback for missing constants
PCAN_ERROR_ILLEGAL_PARAMETER = getattr(
    sys.modules.get('PCANBasic', None), "PCAN_ERROR_ILLEGAL_PARAMETER", 0x40000
)
PCAN_ERROR_QRCVEMPTY = getattr(
    sys.modules.get('PCANBasic', None), "PCAN_ERROR_QRCVEMPTY", 0x00000003
)

import updater  # Import updater module
from filesize import LogFileHandler  # <-- ensure filesize.py is present
from signal_watch import SignalWatch

# New imports for CSV logger
import can
import cantools
import csv
import os
import datetime
import threading
import subprocess
from pathlib import Path
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ----------------------------
# CAN configuration
# ----------------------------
CAN_CHANNEL = PCAN_USBBUS1
CAN_BAUDRATE = PCAN_BAUD_250K

# ----------------------------
# UI-friendly constants / tuning
# ----------------------------
TRACE_ROW_LIMIT = 200            # keep only latest 200 rows
TRACE_FLUSH_INTERVAL_MS = 50     # flush pending messages to UI every 50 ms
TRACE_ROWS_PER_FLUSH = 25        # limit rows processed per flush to keep UI responsive

# ----------------------------
# CSV Logger (embedded marvel_data_logger functionality)
# - Listens to CANReader.message_received (msg, ts_us)
# - Maintains latest decoded signals from DBC
# - Writes rows periodically to CSV
# ----------------------------
class CSVLogger(QObject):
    """
    CSVLogger: receives CAN frames via handle_message(msg, ts_us),
    decodes using the DBC, maintains a latest_values snapshot and
    writes CSV rows periodically.
    """
    def __init__(self, dbc_filename=None, output_dir=None, csv_prefix="Marvel_csv", log_interval=0.5, parent=None):
        super().__init__(parent)
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.dbc_filename = dbc_filename or os.path.join(self.script_dir, "Marvel_3W_all_variant.dbc")
        if not os.path.exists(self.dbc_filename):
            raise FileNotFoundError(f"DBC file not found: {self.dbc_filename}")
        try:
            self.db = cantools.database.load_file(self.dbc_filename)
        except Exception as e:
            raise RuntimeError(f"Failed to load DBC: {e}")

        self.output_dir = output_dir or self.script_dir
        ts = datetime.datetime.now().strftime("%d%b%Y_%H-%M-%S")
        self.csv_filename = os.path.join(self.output_dir, f"{csv_prefix}_{ts}.csv")
        self.log_interval = float(log_interval)

        # Build ordered signal list & units (same stable order)
        self.signal_list = ['Time']
        self.units_list = ['s']
        for msg in sorted(self.db.messages, key=lambda m: m.name):
            for sig in msg.signals:
                self.signal_list.append(sig.name)
                self.units_list.append(sig.unit if sig.unit else '')

        self.signal_pos = {name: idx for idx, name in enumerate(self.signal_list)}
        self.latest_values = [0] * len(self.signal_list)
        self.lock = threading.Lock()

        self._writer_thread = None
        self._running = False
        # Use timestamp base consistent with GUI TRC: will use offset from first frame received
        self._base_ts = None

    def start(self):
        if self._running:
            return
        # open file and write header
        try:
            self._f = open(self.csv_filename, "w", newline="")
            self._writer = csv.writer(self._f)
            self._writer.writerow(self.signal_list)
            self._writer.writerow(self.units_list)
            self._f.flush()
        except Exception as e:
            raise RuntimeError(f"Failed to open CSV file: {e}")

        self._running = True
        self._writer_thread = threading.Thread(target=self._write_loop, daemon=True)
        self._writer_thread.start()
        print(f"[CSV] Started logging to: {self.csv_filename}")

    def stop(self):
        self._running = False
        if self._writer_thread:
            self._writer_thread.join(timeout=1.0)
        try:
            if hasattr(self, "_f") and self._f:
                self._f.flush()
                self._f.close()
        except Exception:
            pass
        print(f"[CSV] Stopped. Data saved to: {self.csv_filename}")

    def handle_message(self, msg, ts_us):
        """
        Slot to be connected to CANReader.message_received(msg, ts_us)
        msg is PCAN message (TPCANMsg-like) from PCANBasic.Read
        ts_us is timestamp in microseconds (as produced by CANReader)
        We decode message and update latest_values map.
        """
        if ts_us is None:
            return
        # first set base timestamp if missing
        if self._base_ts is None:
            self._base_ts = ts_us

        # compute relative seconds
        rel_s = (ts_us - self._base_ts) / 1_000_000.0

        # try decode by arbitration id — PCAN TPCANMsg uses ID field
        try:
            arbitration_id = getattr(msg, "ID", None)
            data_arr = getattr(msg, "DATA", None)
            length = getattr(msg, "LEN", 0)
            # convert data to bytes
            if data_arr is None:
                payload = bytes()
            else:
                # PCANBasic TPCANMsg.DATA is an array of c_ubyte (length 8)
                # create bytes up to length
                b = []
                for i in range(min(8, length or 8)):
                    try:
                        b.append(int(data_arr[i]) & 0xFF)
                    except Exception:
                        b.append(0)
                payload = bytes(b)
            # update Time
            with self.lock:
                self.latest_values[self.signal_pos['Time']] = round(rel_s, 6)
            # attempt decode via cantools
            try:
                decoded = self.db.decode_message(arbitration_id, payload)
                with self.lock:
                    for name, val in decoded.items():
                        if name in self.signal_pos:
                            self.latest_values[self.signal_pos[name]] = val
            except Exception:
                # not all frames decode or DBC mismatch — ignore
                pass
        except Exception:
            # robust: ignore any exceptions in message handling
            pass

    def _write_loop(self):
        next_tick = time.time()
        while self._running:
            now = time.time()
            if now >= next_tick:
                with self.lock:
                    row = list(self.latest_values)
                try:
                    self._writer.writerow(row)
                    self._f.flush()
                except Exception:
                    pass
                next_tick += self.log_interval
            time.sleep(0.01)


# ----------------------------
# MCU CSV Logger (variant-selected on first MCU frame)
# ----------------------------
class MCUCSVLogger(QObject):
    """
    MCUCSVLogger: waits for a selector frame (ID 0x0726) with a recognized
    first byte before loading the matching DBC and starting CSV logging.
    """
    def __init__(self, mcu_variant_map, output_dir=None, log_interval=0.5, parent=None):
        super().__init__(parent)
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.mcu_variant_map = mcu_variant_map or {}
        self.output_dir = output_dir or self.script_dir
        self.log_interval = float(log_interval)

        self.db = None
        self.csv_filename = None
        self.signal_list = []
        self.units_list = []
        self.signal_pos = {}
        self.latest_values = []

        self._writer_thread = None
        self._running = False
        self._activated = False
        self._base_ts = None
        self._selected_variant = None
        self.lock = threading.Lock()
        self._update_counter = 0
        self._last_written_counter = 0

    def start(self):
        """
        Enables the logger to watch for selector frames. Actual file creation
        and writer thread start only happen after a recognized selector.
        """
        if self._running:
            return
        self._running = True
        self._writer_thread = threading.Thread(target=self._write_loop, daemon=True)
        self._writer_thread.start()

    def stop(self):
        self._running = False
        if self._writer_thread:
            self._writer_thread.join(timeout=1.0)
        try:
            if hasattr(self, "_f") and self._f:
                self._f.flush()
                self._f.close()
        except Exception:
            pass
        if self._activated:
            print(f"[MCU CSV] Stopped. Data saved to: {self.csv_filename}")
        self._activated = False
        self._selected_variant = None

    def handle_message(self, msg, ts_us):
        """
        Detects selector frame 0x0726, loads matching DBC, and logs decoded signals.
        """
        if not self._running:
            return

        try:
            arbitration_id = getattr(msg, "ID", None)
            data_arr = getattr(msg, "DATA", None)
            length = getattr(msg, "LEN", 0)
        except Exception:
            return

        if arbitration_id is None:
            return

        # Initialize variant when selector frame (0x0726) arrives
        if not self._activated and arbitration_id == 0x0726:
            first_byte = None
            try:
                if data_arr is not None and length and length > 0:
                    first_byte = int(data_arr[0]) & 0xFF
            except Exception:
                first_byte = None
            if first_byte in self.mcu_variant_map:
                self._init_variant(first_byte)

        if not self._activated or self.db is None or ts_us is None:
            return

        # set base timestamp if missing
        if self._base_ts is None:
            self._base_ts = ts_us

        rel_s = (ts_us - self._base_ts) / 1_000_000.0

        # build payload
        b = []
        try:
            for i in range(min(8, length or 8)):
                try:
                    b.append(int(data_arr[i]) & 0xFF)
                except Exception:
                    b.append(0)
        except Exception:
            b = []
        payload = bytes(b)

        # update Time
        with self.lock:
            if self.signal_pos:
                self.latest_values[self.signal_pos['Time']] = round(rel_s, 6)

        # decode and update signals
        try:
            decoded = self.db.decode_message(arbitration_id, payload)
            with self.lock:
                for name, val in decoded.items():
                    if name in self.signal_pos:
                        self.latest_values[self.signal_pos[name]] = val
                self._update_counter += 1
        except Exception:
            # ignore undecodable frames
            with self.lock:
                self._update_counter += 1

    def _init_variant(self, selector_byte: int):
        if self._activated:
            return
        variant = self.mcu_variant_map.get(selector_byte)
        if not variant:
            return

        variant_name, dbc_relpath = variant
        dbc_path = dbc_relpath if os.path.isabs(dbc_relpath) else os.path.join(self.script_dir, dbc_relpath)
        if not os.path.exists(dbc_path):
            print(f"[MCU CSV] DBC not found: {dbc_path}")
            return

        try:
            self.db = cantools.database.load_file(dbc_path)
        except Exception as e:
            print(f"[MCU CSV] Failed to load DBC: {e}")
            return

        ts = datetime.datetime.now().strftime("%d%b%Y_%H-%M-%S")
        self.csv_filename = os.path.join(self.output_dir, f"{variant_name}_csv_{ts}.csv")

        # Build ordered signals and units
        self.signal_list = ['Time']
        self.units_list = ['s']
        for msg in sorted(self.db.messages, key=lambda m: m.name):
            for sig in msg.signals:
                self.signal_list.append(sig.name)
                self.units_list.append(sig.unit if sig.unit else '')

        self.signal_pos = {name: idx for idx, name in enumerate(self.signal_list)}
        self.latest_values = [0] * len(self.signal_list)
        self._base_ts = None
        self._update_counter = 0
        self._last_written_counter = 0

        try:
            self._f = open(self.csv_filename, "w", newline="")
            self._writer = csv.writer(self._f)
            self._writer.writerow(self.signal_list)
            self._writer.writerow(self.units_list)
            self._f.flush()
        except Exception as e:
            print(f"[MCU CSV] Failed to open CSV: {e}")
            return

        self._activated = True
        self._selected_variant = selector_byte
        print(f"[MCU CSV] Started logging ({variant_name}) to: {self.csv_filename}")

    def _write_loop(self):
        next_tick = time.time()
        while True:
            if not self._running:
                break
            if not self._activated:
                time.sleep(0.05)
                next_tick = time.time()
                continue
            if self._update_counter <= self._last_written_counter:
                time.sleep(0.05)
                next_tick = time.time()
                continue
            now = time.time()
            if now >= next_tick:
                with self.lock:
                    row = list(self.latest_values)
                    current_counter = self._update_counter
                try:
                    self._writer.writerow(row)
                    self._f.flush()
                except Exception:
                    pass
                self._last_written_counter = current_counter
                next_tick += self.log_interval
            time.sleep(0.01)


# ----------------------------
# Worker Thread for Receiving CAN Messages
# - Manages init/reconnect itself
# - Emits message_received(msg, ts_us) to GUI thread
# - Emits status_changed(bool) only after a successful initial connect to avoid false disconnects
# ----------------------------
class CANReader(QThread):
    message_received = Signal(object, object)  # (msg, ts_us)
    status_changed = Signal(bool)             # True = connected, False = disconnected
    error_occurred = Signal(str)

    def __init__(self, pcan, channel, baudrate, parent=None):
        super().__init__(parent)
        self.pcan = pcan
        self.channel = channel
        self.baudrate = baudrate
        self.running = True

        # state
        self.connected = False
        self.ever_connected = False  # important: avoid reporting disconnects before first success

    def run(self):
        # Loop: try to initialize, then read; on problems try to reconnect.
        while self.running:
            if not self.connected:
                try:
                    res = self.pcan.Initialize(self.channel, self.baudrate)
                except Exception as e:
                    self.error_occurred.emit(f"PCAN Initialize exception: {e}")
                    res = 1

                if res == PCAN_ERROR_OK:
                    self.connected = True
                    self.ever_connected = True
                    # Inform GUI we are connected
                    self.status_changed.emit(True)
                else:
                    # Not connected yet — don't spam the GUI with disconnect events.
                    time.sleep(0.8)
                    continue

            # Connected: perform read loop. Use non-blocking Read and check status.
            try:
                sts = self.pcan.GetStatus(self.channel)
            except Exception as e:
                # Treat as lost connection; force reconnect path
                self.error_occurred.emit(f"GetStatus exception: {e}")
                sts = PCAN_ERROR_ILLEGAL_PARAMETER

            if sts != PCAN_ERROR_OK:
                # Lost connection — uninitialize and report (only if we had connected before)
                try:
                    self.pcan.Uninitialize(self.channel)
                except Exception:
                    pass
                self.connected = False
                # Only emit disconnected if we had previously been connected (avoid false alarms during startup)
                if self.ever_connected:
                    self.status_changed.emit(False)
                time.sleep(0.8)
                continue

            # Try reading frames
            try:
                result, msg, timestamp = self.pcan.Read(self.channel)
            except Exception as e:
                # treat read exception as disconnect/reconnect cycle
                self.error_occurred.emit(f"PCAN Read exception: {e}")
                try:
                    self.pcan.Uninitialize(self.channel)
                except Exception:
                    pass
                if self.ever_connected:
                    self.status_changed.emit(False)
                self.connected = False
                time.sleep(0.8)
                continue

            if result == PCAN_ERROR_OK:
                # convert timestamp structure to microseconds (matches original behavior)
                ts_us = timestamp.micros + timestamp.millis * 1000
                # emit the message object and timestamp
                self.message_received.emit(msg, ts_us)
            elif result == PCAN_ERROR_QRCVEMPTY:
                # nothing to read, yield CPU briefly
                time.sleep(0.001)
            else:
                # unexpected error code — notify and check/connect in next loop
                self.error_occurred.emit(f"PCAN Read return: {hex(result)}")
                # optionally check status which will trigger reconnect
                time.sleep(0.002)

        # Thread stopping: ensure uninitialize
        try:
            if self.connected:
                self.pcan.Uninitialize(self.channel)
        except Exception:
            pass
        # If we were connected, inform GUI we are now disconnected
        if self.connected and self.ever_connected:
            self.status_changed.emit(False)
        self.connected = False

    def stop(self):
        self.running = False


# ----------------------------
# Generic Worker for file parsing (unchanged)
# ----------------------------
class WorkerThread(QThread):
    finished_signal = Signal(str)
    error_signal = Signal(str)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            res = self.func(*self.args, **self.kwargs)
            if isinstance(res, str) and res:
                msg = res
            else:
                msg = "Conversion completed."
            self.finished_signal.emit(msg)
        except Exception as e:
            self.error_signal.emit(str(e))


# ----------------------------
# Popup dialog for New Transmit Message (unchanged)
# ----------------------------
class NewMessageDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("New Transmit Message")
        layout = QGridLayout()
        layout.addWidget(QLabel("ID (hex):"), 0, 0)
        self.id_input = QLineEdit("000")
        layout.addWidget(self.id_input, 0, 1)
        layout.addWidget(QLabel("Length:"), 1, 0)
        self.len_combo = QComboBox()
        self.len_combo.addItems([str(i) for i in range(1, 9)])
        self.len_combo.setCurrentText("8")
        layout.addWidget(self.len_combo, 1, 1)
        layout.addWidget(QLabel("Data (hex):"), 2, 0)
        self.data_inputs = []
        data_layout = QHBoxLayout()
        for _ in range(8):
            box = QLineEdit("00")
            box.setMaxLength(2)
            box.setFixedWidth(30)
            self.data_inputs.append(box)
            data_layout.addWidget(box)
        layout.addLayout(data_layout, 2, 1)
        layout.addWidget(QLabel("Cycle Time (ms):"), 3, 0)
        self.cycle_input = QLineEdit("100")
        layout.addWidget(self.cycle_input, 3, 1)
        self.chk_extended = QCheckBox("Extended Frame")
        layout.addWidget(self.chk_extended, 4, 0)
        self.chk_remote = QCheckBox("Remote Request")
        layout.addWidget(self.chk_remote, 4, 1)
        layout.addWidget(QLabel("Comment:"), 5, 0)
        self.comment_input = QLineEdit("")
        layout.addWidget(self.comment_input, 5, 1)
        btn_layout = QHBoxLayout()
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout, 6, 0, 1, 2)
        self.setLayout(layout)

    def get_data(self):
        return {
            "id": self.id_input.text(),
            "length": int(self.len_combo.currentText()),
            "data": [box.text() for box in self.data_inputs],
            "cycle": self.cycle_input.text(),
            "extended": self.chk_extended.isChecked(),
            "remote": self.chk_remote.isChecked(),
            "comment": self.comment_input.text()
        }


# ----------------------------
# Main Window (kept UI exactly as you wanted)
# - Modified only connection & trace handling
# ----------------------------
class PCANViewClone(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PCAN-View (Logger & DebugTools)")
        self.resize(1300, 630)

        # PCAN instance
        self.pcan = PCANBasic()

        # Reader thread
        self.reader = None

        # connection / logging state
        self.is_connected = False
        self.live_data = {}
        self.log_handler = None
        self.log_start_time = None
        self.log_base_ts_us = None  # base timestamp (us) for TRC offsets
        self.recording_start_time = None
        self.message_count = 0
        self.logging = False
        self.current_log_filename = None
        self.header_written = False
        self.signal_watch = None
        # per-row format tracking
        self.row_id_format_rx = {}
        self.row_id_format_tx = {}
        self.row_data_format_rx = {}
        self.row_data_format_tx = {}
        # store canonical values for formatting
        self.rx_row_id_value = {}
        self.tx_row_id_value = {}
        self.rx_row_data_bytes = {}
        self.tx_row_data_bytes = {}
        self.rx_id_to_row = {}
        # ----- TEMPORARY CONNECT LOCK -----
        self.connect_locked = False

        # CSV logger (parallel)
        self.csv_logger = None
        self.csv_logging_enabled = False
        self.csv_log_interval = 0.5  # seconds
        # MCU CSV logger
        self.mcu_logger = None
        self.mcu_logging_enabled = False
        self.mcu_variant_map = {
            0x05: ("GTAKE", "GTAKE_MCU.dbc"),
            0x01: ("GTAKE", "GTAKE_MCU.dbc"),
            0x03: ("Pegasus", "Pegasus_MCU_BMS.dbc"),
        }

        # track connection start used for non-recording trace timestamps
        self.connection_start_time = None
        # log timestamp alignment (map driver µs to local monotonic µs)
        self._log_ts_offset_us = None

        # trace buffering
        self.trace_buffer = deque()            # store full rows as lists
        self.max_trace_messages = TRACE_ROW_LIMIT

        # pending messages from reader - flushed to UI on timer to avoid UI freeze
        self._pending_trace = deque()

        # --- UI setup (kept intact) ---
        toolbar = QToolBar("Main Toolbar")
        toolbar.setStyleSheet("QToolBar { background-color: #0078D7; }")
        self.addToolBar(toolbar)

        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self.toggle_connection)
        self.style_toolbar_button(self.connect_btn)
        toolbar.addWidget(self.connect_btn)

        self.log_start_btn = QPushButton("Start Logging")
        self.log_start_btn.clicked.connect(self.ask_log_filename)
        self.style_toolbar_button(self.log_start_btn, bg="green")
        toolbar.addWidget(self.log_start_btn)

        self.log_stop_btn = QPushButton("Stop Logging")
        self.log_stop_btn.clicked.connect(self.stop_logging)
        self.log_stop_btn.setEnabled(False)
        self.style_toolbar_button(self.log_stop_btn, bg="red")
        toolbar.addWidget(self.log_stop_btn)

        self.trace_btn = QPushButton("Trace")
        self.trace_btn.clicked.connect(self.switch_to_trace_tab)
        self.style_toolbar_button(self.trace_btn, bg="#444")
        toolbar.addWidget(self.trace_btn)

        # Tabs and receive/transmit/trace UI (unchanged)
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        self.recv_tx_tab = QWidget()
        self.setup_recv_tx_tab()
        self.tabs.addTab(self.recv_tx_tab, "Receive / Transmit")

        self.trace_tab = QWidget()
        self.setup_trace_tab()
        self.tabs.addTab(self.trace_tab, "Trace")

        self.signal_tab = QWidget()
        signal_layout = QVBoxLayout()
        signal_layout.addWidget(QLabel("Signal Watch will load here…"))
        self.signal_tab.setLayout(signal_layout)
        self.tabs.addTab(self.signal_tab, "Signal Watch")

        self.signal_watch = SignalWatch(self)
        self.signal_watch.attach_ui(self.signal_tab)

        self.service_tab = QWidget()
        service_layout = QVBoxLayout()
        service_btn = QPushButton("Service Mode")
        service_btn.setStyleSheet(
            "QPushButton {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4da3ff, stop:1 #1c64d1);"
            "color: white; font-weight: bold; padding: 8px 14px; border-radius: 5px;"
            "border: 2px solid #0c4da2;"
            "}"
            "QPushButton:hover {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #65b7ff, stop:1 #2b73dd);"
            "}"
            "QPushButton:pressed {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #1c64d1, stop:1 #0f3f8c);"
            "border-top-color: #0f3f8c; border-bottom-color: #072a62; padding-top: 10px; padding-bottom: 6px;"
            "}"
        )
        service_btn.clicked.connect(self._launch_service_mode)
        service_layout.addWidget(service_btn)
        service_layout.addStretch()
        self.service_tab.setLayout(service_layout)
        self.tabs.addTab(self.service_tab, "Service Mode")

        # Status bar (unchanged)
        self.status_bar = QStatusBar()
        self.status_conn = QLabel("Disconnected")
        self.status_bitrate = QLabel("Bit rate: ---")
        self.status_bus = QLabel("Status: ---")
        self.status_bar.addWidget(self.status_conn)
        self.status_bar.addWidget(self.status_bitrate)
        self.status_bar.addWidget(self.status_bus)
        self.setStatusBar(self.status_bar)

        # Parse tool button (unchanged)
        self.parse_toolbutton = QToolButton()
        self.parse_toolbutton.setText("Parse File")
        self.parse_toolbutton.setStyleSheet(
            "QToolButton { background-color: green; color: white; font-weight: bold; padding: 6px; }"
            "QToolButton:pressed { background-color: darkgreen; }"
            "QToolButton:hover { background-color: green; }"
        )
        self.parse_menu = QMenu(self.parse_toolbutton)

        def create_colored_action(text, color):
            action = QWidgetAction(self.parse_menu)
            btn = QPushButton(text)
            btn.setStyleSheet(f"""
                QPushButton {{
                    background-color: {color};
                    color: white;
                    font-weight: bold;
                    border: none;
                    padding: 6px 12px;
                    text-align: left;
                }}
                QPushButton:pressed {{
                    background-color: {color};
                }}
                QPushButton:hover {{
                    background-color: {color};
                }}
            """)
            btn.clicked.connect(lambda checked=False, t=text: self._parse_menu_action_triggered(t))
            action.setDefaultWidget(btn)
            return action

        self.parse_menu.addAction(create_colored_action("TRC → CSV", "#dc0d33"))
        self.parse_menu.addAction(create_colored_action("LOG → CSV", "#09ad3d"))
        self.parse_toolbutton.setMenu(self.parse_menu)
        self.parse_toolbutton.setPopupMode(QToolButton.InstantPopup)
        self.tabs.setCornerWidget(self.parse_toolbutton, Qt.TopRightCorner)

        self.setStyleSheet("""
            QMainWindow { background-color: #f0f0f0; }
            QTableWidget { background: white; alternate-background-color: #e6f2ff; gridline-color: #c0c0c0; }
            QHeaderView::section { background-color: #0078D7; color: white; padding: 4px; }
        """)

        # Auto-send timer (unchanged)
        self.auto_send_timer = QTimer()
        self.auto_send_timer.timeout.connect(self.auto_send_messages)
        self.auto_send_timer.start(100)

        # Worker/progress references
        self._worker_thread = None
        self._progress_dialog = None

        # Blink timer for logging status
        self._blink_timer = QTimer()
        self._blink_timer.timeout.connect(self._blink_status_text)
        self._blink_state = False

        # Timer to flush pending trace rows to UI (smoothing)
        self._flush_timer = QTimer()
        self._flush_timer.setInterval(TRACE_FLUSH_INTERVAL_MS)
        self._flush_timer.timeout.connect(self._flush_pending_trace)
        self._flush_timer.start()

    # parse menu helper
    def _parse_menu_action_triggered(self, text):
        if text == "TRC → CSV":
            self.convert_trc_to_csv()
        elif text == "LOG → CSV":
            self.convert_log_to_csv()

    # ----------------------------
    # UI setup helpers (unchanged)
    # ----------------------------
    def setup_recv_tx_tab(self):
        layout = QVBoxLayout()
        splitter = QSplitter(Qt.Vertical)

        receive_frame = QFrame()
        receive_layout = QVBoxLayout(receive_frame)
        lbl_rx = QLabel("Receive")
        lbl_rx.setStyleSheet("background:#e0e0e0; padding:2px; font-weight:bold;")
        receive_layout.addWidget(lbl_rx)

        self.receive_table = QTableWidget()
        self.receive_table.setColumnCount(4)
        self.receive_table.setHorizontalHeaderLabels(["CAN ID", "Count", "Cycle Time (ms)", "Data"])
        self.receive_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.receive_table.setAlternatingRowColors(True)
        self.receive_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.receive_table.customContextMenuRequested.connect(self.show_rx_context_menu)
        receive_layout.addWidget(self.receive_table)

        transmit_frame = QFrame()
        transmit_layout = QVBoxLayout(transmit_frame)
        lbl_tx = QLabel("Transmit")
        lbl_tx.setStyleSheet("background:#e0e0e0; padding:2px; font-weight:bold;")
        transmit_layout.addWidget(lbl_tx)

        self.transmit_table = QTableWidget()
        self.transmit_table.setColumnCount(8)
        self.transmit_table.setHorizontalHeaderLabels(
            ["Enable", "CAN-ID", "Type", "Length", "Data", "Cycle Time(ms)", "Count", "Comment"])
        self.transmit_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.transmit_table.setAlternatingRowColors(True)
        self.transmit_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.transmit_table.customContextMenuRequested.connect(self.show_tx_context_menu)
        transmit_layout.addWidget(self.transmit_table)

        splitter.addWidget(receive_frame)
        splitter.addWidget(transmit_frame)
        splitter.setSizes([600, 100])
        layout.addWidget(splitter)
        self.recv_tx_tab.setLayout(layout)

    def setup_trace_tab(self):
        layout = QVBoxLayout()
        self.trace_table = QTableWidget()
        self.trace_table.setColumnCount(5)
        self.trace_table.setHorizontalHeaderLabels(["Time (s)", "CAN ID", "Rx/Tx", "Length", "Data"])
        self.trace_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.trace_table.setAlternatingRowColors(True)
        self.trace_table.setRowCount(0)
        layout.addWidget(self.trace_table)
        self.trace_tab.setLayout(layout)

    # ----------------------------
    # Styling helper (unchanged)
    # ----------------------------
    def style_toolbar_button(self, button, bg="#0078D7"):
        button.setStyleSheet(f"""
            QPushButton {{ background-color: {bg}; color: white; font-weight: bold; padding: 6px; }}
            QPushButton:hover {{ background-color: #005fa3; }}
        """)

    # ----------------------------
    # Formatting helpers and context menus
    # ----------------------------
    def format_can_id(self, id_hex_string, fmt):
        try:
            id_val = int(id_hex_string, 16)
        except Exception:
            id_val = 0
        if fmt == "dec":
            return str(id_val)
        return f"{id_val:X}h"

    def format_bytes(self, bytes_src, fmt):
        if isinstance(bytes_src, str):
            parse_fmt = fmt if fmt in ("dec", "ascii") else "hex"
            bytes_list = self._parse_data_text(bytes_src, parse_fmt)
        else:
            bytes_list = list(bytes_src or [])
        if fmt == "dec":
            return " ".join(str(b) for b in bytes_list)
        if fmt == "ascii":
            return "".join(chr(b) if 32 <= b <= 126 else "." for b in bytes_list)
        return " ".join(f"{b:02X}" for b in bytes_list)

    def _parse_id_text(self, text, fmt):
        try:
            if fmt == "dec":
                return int(text)
            return int(text.replace("h", ""), 16)
        except Exception:
            return 0

    def _parse_data_text(self, text, fmt):
        if text is None:
            return []
        if fmt == "ascii":
            return [ord(ch) & 0xFF for ch in text]
        bytes_out = []
        for token in text.split():
            try:
                base = 10 if fmt == "dec" else 16
                bytes_out.append(int(token, base) & 0xFF)
            except Exception:
                bytes_out.append(0)
        return bytes_out

    def refresh_single_rx_row(self, row):
        id_fmt = self.row_id_format_rx.get(row, "hex")
        data_fmt = self.row_data_format_rx.get(row, "hex")

        can_id_val = self.rx_row_id_value.get(row)
        if can_id_val is None:
            item = self.receive_table.item(row, 0)
            can_id_val = self._parse_id_text(item.text() if item else "0", id_fmt)
            self.rx_row_id_value[row] = can_id_val
        id_text = self.format_can_id(f"{can_id_val:X}", id_fmt)
        self.receive_table.setItem(row, 0, QTableWidgetItem(id_text))

        data_bytes = self.rx_row_data_bytes.get(row)
        if data_bytes is None:
            data_item = self.receive_table.item(row, 3)
            data_bytes = self._parse_data_text(data_item.text() if data_item else "", data_fmt)
            self.rx_row_data_bytes[row] = data_bytes
        data_text = self.format_bytes(data_bytes, data_fmt)
        self.receive_table.setItem(row, 3, QTableWidgetItem(data_text))

    def refresh_single_tx_row(self, row):
        id_fmt = self.row_id_format_tx.get(row, "hex")
        data_fmt = self.row_data_format_tx.get(row, "hex")

        id_val = self.tx_row_id_value.get(row)
        if id_val is None:
            item = self.transmit_table.item(row, 1)
            id_val = self._parse_id_text(item.text() if item else "0", id_fmt)
        self.tx_row_id_value[row] = id_val
        id_text = self.format_can_id(f"{id_val:X}", id_fmt)
        self.transmit_table.setItem(row, 1, QTableWidgetItem(id_text))

        data_bytes = self.tx_row_data_bytes.get(row)
        if data_bytes is None:
            data_item = self.transmit_table.item(row, 4)
            data_bytes = self._parse_data_text(data_item.text() if data_item else "", data_fmt)
        self.tx_row_data_bytes[row] = data_bytes
        data_text = self.format_bytes(data_bytes, data_fmt)
        self.transmit_table.setItem(row, 4, QTableWidgetItem(data_text))

    def show_rx_context_menu(self, pos: QPoint):
        row = self.receive_table.rowAt(pos.y())
        if row < 0:
            return
        # ensure defaults
        self.row_id_format_rx.setdefault(row, "hex")
        self.row_data_format_rx.setdefault(row, "hex")
        current_id_fmt = self.row_id_format_rx.get(row, "hex")
        current_data_fmt = self.row_data_format_rx.get(row, "hex")
        id_item = self.receive_table.item(row, 0)
        data_item = self.receive_table.item(row, 3)
        if id_item:
            self.rx_row_id_value[row] = self._parse_id_text(id_item.text(), current_id_fmt)
        if data_item:
            self.rx_row_data_bytes[row] = self._parse_data_text(data_item.text(), current_data_fmt)

        menu = QMenu(self.receive_table)
        id_menu = menu.addMenu("CAN ID Format")
        id_hex = id_menu.addAction("Hexadecimal")
        id_hex.setCheckable(True)
        id_dec = id_menu.addAction("Decimal")
        id_dec.setCheckable(True)

        data_menu = menu.addMenu("Data Bytes Format")
        data_hex = data_menu.addAction("Hexadecimal")
        data_hex.setCheckable(True)
        data_dec = data_menu.addAction("Decimal")
        data_dec.setCheckable(True)
        data_ascii = data_menu.addAction("ASCII")
        data_ascii.setCheckable(True)

        id_hex.setChecked(current_id_fmt == "hex")
        id_dec.setChecked(current_id_fmt == "dec")
        data_hex.setChecked(current_data_fmt == "hex")
        data_dec.setChecked(current_data_fmt == "dec")
        data_ascii.setChecked(current_data_fmt == "ascii")

        action = menu.exec_(self.receive_table.viewport().mapToGlobal(pos))
        if action == id_hex:
            self.row_id_format_rx[row] = "hex"
        elif action == id_dec:
            self.row_id_format_rx[row] = "dec"
        elif action == data_hex:
            self.row_data_format_rx[row] = "hex"
        elif action == data_dec:
            self.row_data_format_rx[row] = "dec"
        elif action == data_ascii:
            self.row_data_format_rx[row] = "ascii"

        if action:
            self.refresh_single_rx_row(row)

    def _show_tx_rowless_context_menu(self, pos: QPoint):
        menu = QMenu()
        add_action = menu.addAction("New Message")
        del_action = menu.addAction("Delete Selected")
        action = menu.exec_(self.transmit_table.viewport().mapToGlobal(pos))
        if action == add_action:
            dialog = NewMessageDialog()
            if dialog.exec_() == QDialog.Accepted:
                data = dialog.get_data()
                self.add_transmit_row(data)
        elif action == del_action:
            selected = self.transmit_table.currentRow()
            if selected >= 0:
                self.transmit_table.removeRow(selected)
                # clean format tracking for remaining rows
                self._reindex_tx_row_maps(selected)

    def show_tx_context_menu(self, pos: QPoint):
        row = self.transmit_table.rowAt(pos.y())
        if row < 0:
            self._show_tx_rowless_context_menu(pos)
            return

        self.row_id_format_tx.setdefault(row, "hex")
        self.row_data_format_tx.setdefault(row, "hex")
        current_id_fmt = self.row_id_format_tx.get(row, "hex")
        current_data_fmt = self.row_data_format_tx.get(row, "hex")
        id_item = self.transmit_table.item(row, 1)
        data_item = self.transmit_table.item(row, 4)
        if id_item:
            self.tx_row_id_value[row] = self._parse_id_text(id_item.text(), current_id_fmt)
        if data_item:
            self.tx_row_data_bytes[row] = self._parse_data_text(data_item.text(), current_data_fmt)

        menu = QMenu(self.transmit_table)
        edit_action = menu.addAction("Edit Message")
        menu.addSeparator()
        id_menu = menu.addMenu("CAN ID Format")
        id_hex = id_menu.addAction("Hexadecimal")
        id_hex.setCheckable(True)
        id_dec = id_menu.addAction("Decimal")
        id_dec.setCheckable(True)

        data_menu = menu.addMenu("Data Bytes Format")
        data_hex = data_menu.addAction("Hexadecimal")
        data_hex.setCheckable(True)
        data_dec = data_menu.addAction("Decimal")
        data_dec.setCheckable(True)
        data_ascii = data_menu.addAction("ASCII")
        data_ascii.setCheckable(True)

        id_hex.setChecked(current_id_fmt == "hex")
        id_dec.setChecked(current_id_fmt == "dec")
        data_hex.setChecked(current_data_fmt == "hex")
        data_dec.setChecked(current_data_fmt == "dec")
        data_ascii.setChecked(current_data_fmt == "ascii")

        action = menu.exec_(self.transmit_table.viewport().mapToGlobal(pos))
        if action == edit_action:
            self._edit_transmit_row(row)
        elif action == id_hex:
            self.row_id_format_tx[row] = "hex"
        elif action == id_dec:
            self.row_id_format_tx[row] = "dec"
        elif action == data_hex:
            self.row_data_format_tx[row] = "hex"
        elif action == data_dec:
            self.row_data_format_tx[row] = "dec"
        elif action == data_ascii:
            self.row_data_format_tx[row] = "ascii"

        if action:
            self.refresh_single_tx_row(row)

    def _edit_transmit_row(self, row: int):
        # Gather current values respecting the row's format selection
        id_fmt = self.row_id_format_tx.get(row, "hex")
        data_fmt = self.row_data_format_tx.get(row, "hex")
        id_item = self.transmit_table.item(row, 1)
        data_item = self.transmit_table.item(row, 4)
        len_item = self.transmit_table.item(row, 3)
        cycle_item = self.transmit_table.item(row, 5)
        type_item = self.transmit_table.item(row, 2)
        comment_item = self.transmit_table.item(row, 7)

        can_id_val = self._parse_id_text(id_item.text() if id_item else "0", id_fmt)
        data_bytes = self._parse_data_text(data_item.text().strip() if data_item else "", data_fmt)
        try:
            length_val = int(len_item.text()) if len_item else len(data_bytes)
        except Exception:
            length_val = len(data_bytes)
        length_val = max(1, min(8, length_val if length_val else (len(data_bytes) or 8)))
        data_bytes = (data_bytes or [0] * length_val)[:8]
        while len(data_bytes) < 8:
            data_bytes.append(0)

        cycle_text = cycle_item.text() if cycle_item else "0"
        comment_text = comment_item.text() if comment_item else ""
        is_ext = (type_item.text().strip().upper() == "EXT") if type_item else False

        dialog = NewMessageDialog()
        dialog.id_input.setText(f"{can_id_val:03X}")
        dialog.len_combo.setCurrentText(str(length_val))
        for idx, box in enumerate(dialog.data_inputs):
            box.setText(f"{data_bytes[idx]:02X}")
        dialog.cycle_input.setText(cycle_text)
        dialog.chk_extended.setChecked(is_ext)
        dialog.chk_remote.setChecked(False)
        dialog.comment_input.setText(comment_text)

        if dialog.exec_() == QDialog.Accepted:
            data = dialog.get_data()
            self._apply_tx_row_edit(row, data)

    def _apply_tx_row_edit(self, row: int, data: dict):
        id_fmt = self.row_id_format_tx.get(row, "hex")
        data_fmt = self.row_data_format_tx.get(row, "hex")
        can_id_hex = (data.get("id") or "0").replace("h", "").upper()
        msg_type = "EXT" if data.get("extended") else "STD"
        length_str = str(data.get("length", "8"))
        databytes = " ".join([d if d else "00" for d in data.get("data", [])])
        data_bytes_list = self._parse_data_text(databytes, "hex")[:8]

        try:
            can_id_val = int(can_id_hex or "0", 16)
        except Exception:
            can_id_val = 0

        self.tx_row_id_value[row] = can_id_val
        self.tx_row_data_bytes[row] = data_bytes_list

        self.transmit_table.setItem(row, 1, QTableWidgetItem(self.format_can_id(can_id_hex or "0", id_fmt)))
        self.transmit_table.setItem(row, 2, QTableWidgetItem(msg_type))
        self.transmit_table.setItem(row, 3, QTableWidgetItem(length_str))
        self.transmit_table.setItem(row, 4, QTableWidgetItem(self.format_bytes(data_bytes_list, data_fmt)))
        self.transmit_table.setItem(row, 5, QTableWidgetItem(data.get("cycle", "")))
        self.transmit_table.setItem(row, 7, QTableWidgetItem(data.get("comment", "")))

    def _reindex_tx_row_maps(self, removed_row):
        def _shift_map(src):
            shifted = {}
            for row_idx, value in src.items():
                if row_idx == removed_row:
                    continue
                new_idx = row_idx if row_idx < removed_row else row_idx - 1
                shifted[new_idx] = value
            return shifted

        self.row_id_format_tx = _shift_map(self.row_id_format_tx)
        self.row_data_format_tx = _shift_map(self.row_data_format_tx)
        self.tx_row_id_value = _shift_map(self.tx_row_id_value)
        self.tx_row_data_bytes = _shift_map(self.tx_row_data_bytes)

    def add_transmit_row(self, data):
        row = self.transmit_table.rowCount()
        self.transmit_table.insertRow(row)
        enable_box = QCheckBox()
        self.transmit_table.setCellWidget(row, 0, enable_box)
        msg_type = "EXT" if data["extended"] else "STD"
        can_id_hex = (data["id"] or "0").replace("h", "").upper()
        databytes = " ".join([d if d else "00" for d in data["data"]])
        data_bytes_list = self._parse_data_text(databytes, "hex")
        self.transmit_table.setItem(row, 1, QTableWidgetItem(self.format_can_id(can_id_hex, "hex")))
        self.transmit_table.setItem(row, 2, QTableWidgetItem(msg_type))
        self.transmit_table.setItem(row, 3, QTableWidgetItem(str(data["length"])))
        self.transmit_table.setItem(row, 4, QTableWidgetItem(databytes))
        self.transmit_table.setItem(row, 5, QTableWidgetItem(data["cycle"]))
        self.transmit_table.setItem(row, 6, QTableWidgetItem("0"))
        self.transmit_table.setItem(row, 7, QTableWidgetItem(data["comment"]))
        self.row_id_format_tx[row] = "hex"
        self.row_data_format_tx[row] = "hex"
        try:
            self.tx_row_id_value[row] = int(can_id_hex, 16)
        except Exception:
            self.tx_row_id_value[row] = 0
        self.tx_row_data_bytes[row] = data_bytes_list
        self.refresh_single_tx_row(row)

    # ----------------------------
    # Connection control
    # ----------------------------
    def toggle_connection(self):

        # ----- TEMPORARY CONNECT LOCK -----
        if hasattr(self, "connect_locked") and self.connect_locked:
            QMessageBox.warning(self, "Locked", "Heyy — I'M LOCKED !")
            return

        # Start reader thread (it will attempt to init/reconnect automatically)
        if not self.reader or not self.reader.isRunning():
            self.reader = CANReader(self.pcan, CAN_CHANNEL, CAN_BAUDRATE)
            self.reader.message_received.connect(self.process_message)
            # If CSV logger exists and is active, connect message feed to it too
            if self.csv_logger and self.csv_logging_enabled:
                self.reader.message_received.connect(self.csv_logger.handle_message)
            if self.mcu_logger and self.mcu_logging_enabled:
                self.reader.message_received.connect(self.mcu_logger.handle_message)
            self.reader.status_changed.connect(self.on_hardware_status_changed)
            self.reader.error_occurred.connect(self.on_reader_error)
            self.reader.start()
            self.connect_btn.setText("Disconnect")
            self.status_conn.setText("Connecting...")
            self.status_conn.setStyleSheet("color: orange; font-weight: bold;")
        else:
            if self.logging:
                self.stop_logging()
            # Stop reader and uninitialize
            try:
                self.reader.stop()
                self.reader.wait(2000)
            except Exception:
                pass
            self.reader = None
            try:
                self.pcan.Uninitialize(CAN_CHANNEL)
            except Exception:
                pass
            self.is_connected = False
            self.connect_btn.setText("Connect")
            self.status_conn.setText("Disconnected")
            self.status_conn.setStyleSheet("color: black;")
            self.status_bitrate.setText("Bit rate: ---")
            self.connection_start_time = None
            self._log_ts_offset_us = None

    def on_reader_error(self, msg):
        # show non-fatal errors in status bar
        self.status_bus.setText(f"Reader Error: {msg}")

    def on_hardware_status_changed(self, connected: bool):
        prev = self.is_connected
        self.is_connected = connected

        # Only write events if actual change from previous state
        if connected and not prev:
            self.status_conn.setText("Connected to hardware PCAN-USB")
            self.status_conn.setStyleSheet("color: green; font-weight: bold;")
            self.status_bitrate.setText("Bit rate: 250 kbit/s")
            if self.recording_start_time is None:
                self.connection_start_time = time.time()
            msg = self._format_hw_event_comment("PCAN HARDWARE GOT CONNECTED BACK AT")
            self._log_comment_and_trace(msg)
        elif not connected and prev:
            self.status_conn.setText("Hardware Disconnected")
            self.status_conn.setStyleSheet("color: red; font-weight: bold;")
            self.status_bitrate.setText("Bit rate: ---")
            msg = self._format_hw_event_comment("PCAN HARDWARE GOT DISCONNECTED AT")
            self._log_comment_and_trace(msg)
            self._log_ts_offset_us = None

    def _format_hw_event_comment(self, prefix_text: str) -> str:
        lt = time.localtime()
        millis = int((time.time() % 1) * 1000)
        time_only = time.strftime("%H:%M:%S", lt) + f".{millis}.0"
        comment_line = f"; {prefix_text} {time_only}"
        return comment_line

    def _log_comment_and_trace(self, comment_line: str):
        # write comment into log file (do not close)
        if self.log_handler:
            try:
                self.log_handler.write(comment_line + "\n")
            except Exception:
                self.status_bus.setText("Failed writing log comment")

        # append a visible event row to trace table but via pending queue (smooth)
        lt = time.localtime()
        millis = int((time.time() % 1) * 1000)
        display_time = time.strftime("%H:%M:%S", lt) + f".{millis}"
        row = [display_time, "--", "!", "", comment_line]
        self._pending_trace.append(row)

    def handle_disconnect(self):
        # stop reader and uninitialize
        if self.reader and self.reader.isRunning():
            self.reader.stop()
            self.reader.wait(2000)
            self.reader = None
        if self.logging:
            self.stop_logging()
        try:
            self.pcan.Uninitialize(CAN_CHANNEL)
        except Exception:
            pass
        self.is_connected = False
        self.connect_btn.setText("Connect")
        self.status_conn.setText("Disconnected")
        self.status_conn.setStyleSheet("color: black;")
        self.status_bitrate.setText("Bit rate: ---")
        self.connection_start_time = None
        self._log_ts_offset_us = None

    # ----------------------------
    # Message processing & trace buffering (non-blocking UI)
    # ----------------------------
    def process_message(self, msg, ts_us):
        # update timestamp sync anchor so Tx logging can share a stable clock
        self._log_timestamp_us(ts_us)

        # Keep the same live-data update logic
        can_id = msg.ID
        length = msg.LEN
        data = ' '.join(f"{b:02X}" for b in msg.DATA[:length])
        data_bytes_list = self._parse_data_text(data, "hex")
        can_id_hex = f"{can_id:X}"

        # Update live_data and receive table (unchanged)
        if can_id not in self.live_data:
            self.live_data[can_id] = {"count": 1, "last_ts": ts_us, "cycle_time": 0, "data": data}
            row = self.receive_table.rowCount()
            self.receive_table.insertRow(row)
            self.receive_table.setItem(row, 1, QTableWidgetItem("1"))
            self.receive_table.setItem(row, 2, QTableWidgetItem("0"))
            self.row_id_format_rx[row] = "hex"
            self.row_data_format_rx[row] = "hex"
            self.rx_row_id_value[row] = can_id
            self.rx_row_data_bytes[row] = data_bytes_list
            self.rx_id_to_row[can_id_hex] = row
            self.refresh_single_rx_row(row)
        else:
            old = self.live_data[can_id]
            cycle = (ts_us - old["last_ts"]) / 1000.0
            if cycle < 0:
                cycle = 0
            old["count"] += 1
            old["last_ts"] = ts_us
            old["cycle_time"] = cycle
            old["data"] = data
            # update receive table row
            row = self.rx_id_to_row.get(can_id_hex)
            if row is None:
                for idx in range(self.receive_table.rowCount()):
                    if self.rx_row_id_value.get(idx) == can_id:
                        row = idx
                        break
                if row is not None:
                    self.rx_id_to_row[can_id_hex] = row
            if row is not None:
                self.receive_table.setItem(row, 1, QTableWidgetItem(str(old["count"])))
                self.receive_table.setItem(row, 2, QTableWidgetItem(f"{cycle:.1f}"))
                self.rx_row_id_value[row] = can_id
                self.rx_row_data_bytes[row] = data_bytes_list
                self.refresh_single_rx_row(row)

        # Trace timestamp selection
        if self.recording_start_time is not None:
            timestamp_s = time.time() - self.recording_start_time
        elif self.connection_start_time is not None:
            timestamp_s = time.time() - self.connection_start_time
        else:
            timestamp_s = ts_us / 1_000_000.0

        display_time = f"{timestamp_s:.4f}"
        trace_row = [display_time, f"{can_id:04X}", "Rx", str(length), data]

        # Enqueue instead of immediate UI insert to keep UI responsive
        self._pending_trace.append(trace_row)

        # Logging: write to TRC immediately (keeps sequence)
        if self.logging:
            ts_for_log = self._log_timestamp_us(ts_us)
            self.message_count += 1
            self.write_trc_entry(self.message_count, ts_for_log, msg, tx=False)

        if self.signal_watch:
            self.signal_watch.process_frame(msg, ts_us)

    def _flush_pending_trace(self):
        """
        Flushes up to TRACE_ROWS_PER_FLUSH pending rows to the trace_table.
        Ensures trace_table length stays capped to TRACE_ROW_LIMIT.
        """
        rows_this_flush = 0
        while self._pending_trace and rows_this_flush < TRACE_ROWS_PER_FLUSH:
            row = self._pending_trace.popleft()
            # append row to UI
            trace_row_idx = self.trace_table.rowCount()
            self.trace_table.insertRow(trace_row_idx)
            for col, val in enumerate(row):
                self.trace_table.setItem(trace_row_idx, col, QTableWidgetItem(str(val)))
            # append to internal buffer
            self.trace_buffer.append(row)
            # enforce limit: remove oldest rows beyond cap
            while self.trace_table.rowCount() > self.max_trace_messages:
                self.trace_table.removeRow(0)
                if self.trace_buffer:
                    self.trace_buffer.popleft()
            rows_this_flush += 1

        if rows_this_flush:
            # keep view at bottom
            self.trace_table.scrollToBottom()

    # ----------------------------
    # Transmit logic (unchanged behavior, but keep reader from false disconnects)
    # ----------------------------
    def auto_send_messages(self):
        if not self.is_connected:
            return
        now_ms = time.time() * 1000
        if not hasattr(self, "_last_send_times"):
            self._last_send_times = {}
        for row in range(self.transmit_table.rowCount()):
            enable_widget = self.transmit_table.cellWidget(row, 0)
            if not enable_widget or not enable_widget.isChecked():
                continue
            cycle_str = self.transmit_table.item(row, 5).text()
            try:
                cycle = float(cycle_str)
            except Exception:
                cycle = 0
            if cycle <= 0:
                continue
            last_sent = self._last_send_times.get(row, 0)
            if (now_ms - last_sent) >= cycle:
                self._send_can_row(row)
                self._last_send_times[row] = now_ms

    def _send_can_row(self, row):
        try:
            id_item = self.transmit_table.item(row, 1)
            data_item = self.transmit_table.item(row, 4)
            self.row_id_format_tx.setdefault(row, "hex")
            self.row_data_format_tx.setdefault(row, "hex")
            id_fmt = self.row_id_format_tx.get(row, "hex")
            data_fmt = self.row_data_format_tx.get(row, "hex")
            can_id = self._parse_id_text(id_item.text() if id_item else "0", id_fmt)
            data_bytes = self._parse_data_text(data_item.text().strip() if data_item else "", data_fmt)
            data_bytes = data_bytes[:8]
            self.tx_row_id_value[row] = can_id
            self.tx_row_data_bytes[row] = data_bytes
            length = len(data_bytes)
            msg = TPCANMsg()
            msg.ID = can_id
            msg.LEN = length
            msg.DATA = (c_ubyte * 8)(*data_bytes + [0] * (8 - length))
            msg.MSGTYPE = PCAN_MESSAGE_STANDARD
            result = self.pcan.Write(CAN_CHANNEL, msg)
            if result != PCAN_ERROR_OK:
                self.status_bus.setText(f"Send Error: {result}")
            else:
                count_item = self.transmit_table.item(row, 6)
                if count_item is None:
                    count_item = QTableWidgetItem("0")
                    self.transmit_table.setItem(row, 6, count_item)
                try:
                    count = int(count_item.text())
                except Exception:
                    count = 0
                count += 1
                count_item.setText(str(count))

                ts_us = self._log_timestamp_us()

                # Logging Tx frame
                if self.logging:
                    self.message_count += 1
                    self.write_trc_entry(self.message_count, ts_us, msg, tx=True)

                data = ' '.join(f"{b:02X}" for b in data_bytes)

                # Add TX to pending trace queue (so UI updates are batched)
                if self.recording_start_time is not None:
                    timestamp_s = time.time() - self.recording_start_time
                elif self.connection_start_time is not None:
                    timestamp_s = time.time() - self.connection_start_time
                else:
                    timestamp_s = ts_us / 1_000_000.0

                display_time = f"{timestamp_s:.4f}"
                trace_row = [display_time, f"{can_id:04X}", "Tx", str(length), data]
                self._pending_trace.append(trace_row)

        except Exception as e:
            self.status_bus.setText(f"Send Exception: {e}")

    # ----------------------------
    # Parse tool wrappers (unchanged)
    # ----------------------------
    def _start_background_task_with_progress(self, target_func):
        progress = QProgressDialog("Parsing file... Please wait.", "Cancel", 0, 0, self)
        progress.setWindowModality(Qt.ApplicationModal)
        progress.setWindowTitle("Parsing")
        progress.setMinimumDuration(200)
        progress.setAutoClose(False)
        progress.setAutoReset(False)
        progress.show()

        worker = WorkerThread(target_func)
        self._worker_thread = worker
        self._progress_dialog = progress

        def on_finished(msg):
            if progress:
                progress.close()
            self._worker_thread = None
            self._progress_dialog = None
            QMessageBox.information(self, "Done", msg if msg else "Conversion completed.")

        def on_error(err):
            if progress:
                progress.close()
            self._worker_thread = None
            self._progress_dialog = None
            QMessageBox.critical(self, "Error", f"Conversion failed: {err}")

        def on_cancel():
            if worker.isRunning():
                QMessageBox.information(self, "Cancel requested", "Cancellation requested. The conversion will stop when possible.")
            progress.setLabelText("Cancellation requested...")

        progress.canceled.connect(on_cancel)
        worker.finished_signal.connect(on_finished)
        worker.error_signal.connect(on_error)
        worker.start()

    def convert_trc_to_csv(self):
        trc_paths, _ = QFileDialog.getOpenFileNames(self, "Select one or more TRC Files", "", "TRC Files (*.trc)")
        if not trc_paths:
            QMessageBox.information(self, "No File Selected", "No TRC file selected. Conversion cancelled.")
            return
        dbc_path, _ = QFileDialog.getOpenFileName(self, "Select DBC File", "", "DBC Files (*.dbc)")
        if not dbc_path:
            QMessageBox.information(self, "No DBC Selected", "No DBC file selected. Conversion cancelled.")
            return
        output_path, _ = QFileDialog.getSaveFileName(self, "Save CSV Output", "", "CSV Files (*.csv)")
        if not output_path:
            QMessageBox.information(self, "No output selected", "No output CSV file selected. Conversion cancelled.")
            return

        def task():
            trc_to_csv(trc_paths, dbc_path, output_path)
            return f"TRC → CSV conversion completed.\nSaved: {output_path}"

        self._start_background_task_with_progress(task)

    def convert_log_to_csv(self):
        log_path, _ = QFileDialog.getOpenFileName(self, "Select Log File", "", "Log Files (*.log)")
        if not log_path:
            QMessageBox.information(self, "No File Selected", "No LOG file selected. Conversion cancelled.")
            return
        dbc_path, _ = QFileDialog.getOpenFileName(self, "Select DBC File", "", "DBC Files (*.dbc)")
        if not dbc_path:
            QMessageBox.information(self, "No DBC Selected", "No DBC file selected. Conversion cancelled.")
            return
        output_path, _ = QFileDialog.getSaveFileName(self, "Save CSV Output", "", "CSV Files (*.csv)")
        if not output_path:
            QMessageBox.information(self, "No output selected", "No output CSV file selected. Conversion cancelled.")
            return

        def task():
            parse_log_to_compact_csv(log_path, dbc_path, output_path)
            return f"LOG → CSV conversion completed.\nSaved: {output_path}"

        self._start_background_task_with_progress(task)

    # ----------------------------
    # Logging methods (auto-resume behavior)
    # ----------------------------
    def ask_log_filename(self):
        if not self.is_connected:
            QMessageBox.warning(self, "Not connected", "Please connect to PCAN device before start logging.")
            return
        filename, _ = QFileDialog.getSaveFileName(self, "Save Log File", "", "TRC Files (*.trc)")
        if filename:
            self.start_logging(filename)

    def start_logging(self, filename):
        try:
            if self.log_handler:
                try:
                    self.log_handler.close()
                except Exception:
                    pass
                self.log_handler = None

            self.log_handler = LogFileHandler(
                filename,
                on_rotate_callback=self._on_log_file_rotated
            )
            self.current_log_filename = filename
            self.log_start_time = time.time()
            self.log_base_ts_us = None
            self._log_ts_offset_us = None
            self.message_count = 0
            self.header_written = False
            self.write_trc_header()
            self.header_written = True

            # Start recording timestamps at now
            self.recording_start_time = time.time()

            self.logging = True
            self.log_start_btn.setEnabled(False)
            self.log_stop_btn.setEnabled(True)
            self.status_bus.setText(f"Logging Started: {filename}")

            self._blink_state = True
            self._blink_timer.start(500)

            # --- Start parallel CSV logger here ---
            # create CSV loggers and attach to reader if connected (reader emits message_received)
            try:
                # CSV files saved next to TRC file
                out_dir = os.path.dirname(filename) or os.getcwd()

                # Marvel CSV logger
                self.csv_logger = CSVLogger(
                    dbc_filename=os.path.join(os.path.dirname(__file__), "Marvel_3W_all_variant.dbc"),
                    output_dir=out_dir,
                    log_interval=self.csv_log_interval
                )
                self.csv_logger.start()
                self.csv_logging_enabled = True

                # MCU CSV logger (starts writing only after selector frame)
                self.mcu_logger = MCUCSVLogger(
                    mcu_variant_map=self.mcu_variant_map,
                    output_dir=out_dir,
                    log_interval=self.csv_log_interval
                )
                self.mcu_logger.start()
                self.mcu_logging_enabled = True

                # If reader is already running, connect messages
                if self.reader and self.reader.isRunning():
                    self.reader.message_received.connect(self.csv_logger.handle_message)
                    self.reader.message_received.connect(self.mcu_logger.handle_message)
            except Exception as e:
                # non-fatal: allow TRC to continue
                self.status_bus.setText(f"CSV Logger Error: {e}")
                self.csv_logger = None
                self.csv_logging_enabled = False
                self.mcu_logger = None
                self.mcu_logging_enabled = False

        except Exception as e:
            self.status_bus.setText(f"Logging Error: {e}")

    def stop_logging(self):
        self.logging = False
        self._blink_timer.stop()
        self.status_bus.setStyleSheet("color: black;")
        self.status_bus.setText("Logging Stopped")
        self.recording_start_time = None
        if self.log_handler:
            try:
                self.log_handler.close()
            except Exception:
                pass
            self.log_handler = None
        self.log_base_ts_us = None
        self.current_log_filename = None
        self.log_start_btn.setEnabled(True)
        self.log_stop_btn.setEnabled(False)

        # stop csv logger if running
        try:
            if self.csv_logger:
                # disconnect reader signal if connected
                try:
                    if self.reader and self.reader.isRunning():
                        self.reader.message_received.disconnect(self.csv_logger.handle_message)
                except Exception:
                    pass
                self.csv_logger.stop()
                self.csv_logger = None
                self.csv_logging_enabled = False
        except Exception:
            pass

        # stop MCU csv logger if running
        try:
            if self.mcu_logger:
                try:
                    if self.reader and self.reader.isRunning():
                        self.reader.message_received.disconnect(self.mcu_logger.handle_message)
                except Exception:
                    pass
                self.mcu_logger.stop()
                self.mcu_logger = None
                self.mcu_logging_enabled = False
        except Exception:
            pass

    def write_trc_header(self):
        if self.header_written:
            return
        dt_now = time.localtime()
        human_time = time.strftime("%d-%m-%Y %H:%M:%S", dt_now)
        millis = int((time.time() % 1) * 1000)
        epoch_days_fraction = time.time() / 86400
        if self.log_handler:
            self.log_handler.write(
                f";$FILEVERSION=1.1\n"
                f";$STARTTIME={epoch_days_fraction:.10f}\n"
                f";\n"
                f";   Start time: {human_time}.{millis}.0\n"
                f";   Generated by PCAN-View v5.0.1.007\n"
                f";\n"
                f";   Message Number\n"
                f";   |         Time Offset (ms)\n"
                f";   |         |        Type\n"
                f";   |         |        |        ID (hex)\n"
                f";   |         |        |        |     Data Length\n"
            f";   |         |        |        |     |   Data Bytes (hex) ...\n"
            f";   |         |        |        |     |   |\n"
            f";---+--   ----+----  --+--  ----+---  +  -+ -- -- -- -- -- -- --\n"
        )

    def _monotonic_us(self):
        return time.monotonic_ns() // 1000

    def _log_timestamp_us(self, driver_ts_us=None):
        """
        Map driver timestamps to a single monotonic clock so Rx/Tx share the same base.
        """
        mono_us = self._monotonic_us()
        if driver_ts_us is not None:
            if self._log_ts_offset_us is None:
                self._log_ts_offset_us = mono_us - driver_ts_us
            return driver_ts_us + (self._log_ts_offset_us or 0)
        return mono_us

    def write_trc_entry(self, msg_num, ts_us, msg, tx=False):
        if not self.log_handler:
            return

        # fallback to unified monotonic clock if no timestamp is provided
        if ts_us is None:
            ts_us = self._log_timestamp_us()

        # 1. FORCE ROTATION BEFORE WRITING
        rotated = False
        try:
            if os.path.getsize(self.log_handler.log_file.name) >= self.log_handler.max_size:
                self.log_handler.start_new_file(first_file=False)
                rotated = True
        except Exception:
            pass

        # Reset numbering/time base for the first frame in a rotated file
        if rotated:
            self.message_count = 1
            msg_num = 1
            self.log_base_ts_us = None

        # establish base timestamp once per file
        if self.log_base_ts_us is None:
            self.log_base_ts_us = ts_us

        # 2. Continue as usual
        direction = "Tx" if tx else "Rx"
        data_str = " ".join(f"{b:02X}" for b in msg.DATA[:msg.LEN])
        offset_ms = (ts_us - self.log_base_ts_us) / 1000.0

        self.log_handler.write(
            f"{msg_num:6}){offset_ms:11.1f}  {direction:<3}        "
            f"{msg.ID:04X}  {msg.LEN}  {data_str}\n"
        )

    # ----------------------------
    # Blink status text
    # ----------------------------
    def _on_log_file_rotated(self, new_filename):
        self.message_count = 0
        self.log_start_time = time.time()
        self.log_base_ts_us = None
        self.current_log_filename = new_filename

    def _blink_status_text(self):
        if self.logging:
            if self._blink_state:
                self.status_bus.setStyleSheet("color: red; font-weight: bold;")
            else:
                self.status_bus.setStyleSheet("color: black; font-weight: normal;")
            self._blink_state = not self._blink_state
        else:
            self.status_bus.setStyleSheet("color: black; font-weight: normal;")
            self._blink_timer.stop()

    # ----------------------------
    # Launch external Service Mode tool
    # ----------------------------
    def _launch_service_mode(self):
        script_path = Path(__file__).resolve().parent / "Service_Mode.py"
        if not script_path.exists():
            QMessageBox.warning(self, "Service Mode missing", f"Service_Mode.py not found:\n{script_path}")
            return
        try:
            subprocess.Popen([sys.executable, str(script_path)])
        except Exception as exc:
            QMessageBox.warning(self, "Service Mode failed", f"Could not start Service Mode:\n{exc}")

    # ----------------------------
    # Small helper to switch to Trace tab (was missing and caused AttributeError)
    # ----------------------------
    def switch_to_trace_tab(self):
        self.tabs.setCurrentWidget(self.trace_tab)


if __name__ == "__main__":
    try:
        with open("version.txt", "r") as f:
            LOCAL_VERSION = f.read().strip()
    except FileNotFoundError:
        LOCAL_VERSION = "0.0.0"  # fallback if file missing

    # Initialize QApplication before creating any QWidget
    app = QApplication(sys.argv)

    # Check for updates (safe to call now)
    updater.check_for_update(LOCAL_VERSION, app)

    # Launch main application window
    window = PCANViewClone()
    window.show()

    # Start the Qt event loop
    sys.exit(app.exec())

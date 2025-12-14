import cantools
import csv
from datetime import datetime
from pathlib import Path
from PySide6.QtCore import QObject, Qt, Signal, QTimer, QPoint
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLineEdit,
    QMenu,
    QPushButton,
    QWidgetAction,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QHeaderView,
    QMessageBox,
)


class SignalWatch(QObject):
    """
    Decodes CAN frames using a loaded DBC and shows live signal values.
    All decoding is kept here to avoid touching pcan_logger.py logic.
    """

    csv_logging_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.db = None
        self.row_map = {}  # (msg_name, sig_name) -> (table_idx, row)
        self.tables = []
        self.table = None  # keep legacy reference to left table
        self.db_path_edit = None
        self.search_edit = None
        self.filter_text = ""
        self._container = None
        self.activate_dbc_btn = None
        self._activate_menu = None
        self._active_dbc_label = "Select DBC..."
        self._custom_dbc_action_id = "__custom_dbc__"
        self.start_csv_btn = None
        self._csv_log_path = None
        self._csv_log_file = None
        self._csv_writer = None
        self._csv_headers = []
        self._csv_units = []
        self._csv_signal_pos = {}
        self._csv_latest_values = []
        self._csv_base_ts = None
        self._csv_dirty = False
        self._csv_logging_active = False
        self._csv_log_interval_ms = 500
        self._csv_write_timer = QTimer(self)
        self._csv_write_timer.timeout.connect(self._flush_csv_log)
        self._csv_blink_timer = QTimer(self)
        self._csv_blink_timer.timeout.connect(self._blink_csv_button)
        self._csv_blink_state = False
        self._csv_btn_style_idle = (
            "QPushButton {"
            "color: white; padding: 4px 12px; font-weight: bold; border-radius: 4px;"
            "border: 2px solid #1f5f3c;"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #3fae72, stop:1 #2e8b57);"
            "}"
            "QPushButton:hover {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #56c685, stop:1 #319c62);"
            "}"
            "QPushButton:pressed {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2e8b57, stop:1 #1f5f3c);"
            "border-top-color: #1f5f3c; border-bottom-color: #143823; padding-top: 6px; padding-bottom: 2px;"
            "}"
        )
        self._csv_btn_style_blink = (
            "QPushButton {"
            "color: white; padding: 4px 12px; font-weight: bold; border-radius: 4px;"
            "border: 2px solid #8b0000;"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #ff4d4d, stop:1 #d00000);"
            "}"
            "QPushButton:hover {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #ff6666, stop:1 #e00000);"
            "}"
            "QPushButton:pressed {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #d00000, stop:1 #8b0000);"
            "border-top-color: #8b0000; border-bottom-color: #5a0000; padding-top: 6px; padding-bottom: 2px;"
            "}"
        )
        self._activate_btn_style_idle = (
            "QPushButton {"
            "color: white; padding: 4px 12px; font-weight: bold; border-radius: 5px;"
            "border: 2px solid #0c4da2;"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4da3ff, stop:1 #1c64d1);"
            "}"
            "QPushButton:hover {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #65b7ff, stop:1 #2b73dd);"
            "}"
            "QPushButton:pressed {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #1c64d1, stop:1 #0f3f8c);"
            "border-top-color: #0f3f8c; border-bottom-color: #072a62; padding-top: 6px; padding-bottom: 2px;"
            "}"
        )
        self._activate_btn_style_active = (
            "QPushButton {"
            "color: white; padding: 4px 12px; font-weight: bold; border-radius: 5px;"
            "border: 2px solid #1f5f3c;"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #3fae72, stop:1 #2e8b57);"
            "}"
            "QPushButton:hover {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #56c685, stop:1 #319c62);"
            "}"
            "QPushButton:pressed {"
            "background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #2e8b57, stop:1 #1f5f3c);"
            "border-top-color: #1f5f3c; border-bottom-color: #143823; padding-top: 6px; padding-bottom: 2px;"
            "}"
        )
        self.predefined_dbcs = {
            "Select DBC...": None,
            "Marvel DBC": Path(__file__).resolve().parent / "Marvel_3W_all_variant.dbc",
            "nBMS DBC": None,
            "G2A DBC": Path(__file__).resolve().parent / "G2A nBMS.dbc",
            "G2B DBC": Path(__file__).resolve().parent / "G2B_LR200 nBMS.dbc",
            "Athena 4 or 5 DBC": Path(__file__).resolve().parent / "Athena 4&5.dbc",
            "CIP BMS-24X": None,
            "ION BMS": None,
            "GTAKE DBC": Path(__file__).resolve().parent / "GTAKE_MCU.dbc",
            "Pegasus DBC": Path(__file__).resolve().parent / "Pegasus_MCU_BMS.dbc",
            "HEPU DBC": Path(__file__).resolve().parent / "HEPU_MCU.dbc",
        }

    def attach_ui(self, signal_tab_widget: QWidget):
        """Builds the Signal Watch UI inside the provided tab widget."""
        self._container = signal_tab_widget
        layout = signal_tab_widget.layout()
        if layout is None:
            layout = QVBoxLayout()
            signal_tab_widget.setLayout(layout)
        else:
            # remove any placeholder widgets
            while layout.count():
                item = layout.takeAt(0)
                w = item.widget()
                if w is not None:
                    w.deleteLater()
                del item

        controls = QHBoxLayout()
        control_height = 28
        self.activate_dbc_btn = QPushButton("Activate DBC")
        self.activate_dbc_btn.setStyleSheet(self._activate_btn_style_idle)
        self.activate_dbc_btn.setFixedHeight(control_height)
        self._active_dbc_label = "Select DBC..."
        self._activate_menu = QMenu(self.activate_dbc_btn)
        self._build_activate_menu()
        self._activate_menu.triggered.connect(self._on_predefined_dbc_action)
        self.activate_dbc_btn.setMenu(self._activate_menu)
        controls.addWidget(self.activate_dbc_btn)
        self.db_path_edit = QLineEdit()
        self.db_path_edit.setReadOnly(True)
        self.db_path_edit.setFixedHeight(control_height)
        controls.addWidget(self.db_path_edit)
        self.start_csv_btn = QPushButton("Start Logging")
        self.start_csv_btn.setStyleSheet(self._csv_btn_style_idle)
        self.start_csv_btn.setFixedHeight(control_height)
        self.start_csv_btn.clicked.connect(self._on_start_csv_clicked)
        controls.addWidget(self.start_csv_btn)

        controls.addStretch()

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search signal...")
        self.search_edit.textChanged.connect(self.apply_filter)
        self.search_edit.setFixedWidth(220)
        controls.addWidget(self.search_edit)

        left_table = self._build_table()
        right_table = self._build_table()

        layout.addLayout(controls)
        splitter = QWidget()
        splitter_layout = QHBoxLayout()
        splitter_layout.setContentsMargins(0, 0, 0, 0)
        splitter_layout.addWidget(left_table)
        splitter_layout.addWidget(right_table)
        splitter.setLayout(splitter_layout)
        layout.addWidget(splitter)

        self.tables = [left_table, right_table]
        self.table = left_table
        # Keep Start Logging clickable for user feedback even when no DBC is active.
        self._update_csv_button_state(False)

    def load_dbc(self, path: str):
        if self._csv_logging_active:
            self._warn_stop_logging_before_switch()
            return
        # reset CSV logging when switching databases to keep headers in sync
        self._stop_csv_logging(user_requested=True)
        try:
            self.db = self._load_dbc_with_fallback(path)
            if self.db_path_edit is not None:
                self.db_path_edit.setText(path)
            self.clear()
            self._update_csv_button_state(True)
        except Exception as exc:
            # Show an error when a DBC cannot be parsed so the user knows why it failed.
            self.db = None
            if self.db_path_edit is not None:
                self.db_path_edit.setText("")
            self._stop_csv_logging(reason="DBC load failed")
            self._update_csv_button_state(False)
            self._show_dbc_load_error(path, exc)

    def _on_predefined_dbc_action(self, action):
        if action is None:
            return
        path = action.data()
        name = action.text() or "Activate DBC"
        self._handle_dbc_selection(path, name)

    def _browse_and_activate_custom_dbc(self):
        parent = self._container or self.parent()
        path, _ = QFileDialog.getOpenFileName(
            parent, "Select DBC File", "", "DBC Files (*.dbc)"
        )
        if not path:
            return
        name = Path(path).stem or "Custom DBC"
        self._active_dbc_label = name
        if self.activate_dbc_btn is not None:
            self.activate_dbc_btn.setText(f"Activated: {name}")
            self.activate_dbc_btn.setStyleSheet(self._activate_btn_style_active)
        self.load_dbc(path)
        if self.db is None:
            self._reset_activate_button()

    def _reset_activate_button(self):
        self._active_dbc_label = "Select DBC..."
        if self.activate_dbc_btn is not None:
            self.activate_dbc_btn.setText("Activate DBC")
            self.activate_dbc_btn.setStyleSheet(self._activate_btn_style_idle)

    def _handle_dbc_selection(self, path, name: str):
        if self._csv_logging_active:
            self._warn_stop_logging_before_switch()
            return
        if path == self._custom_dbc_action_id:
            self._browse_and_activate_custom_dbc()
            return
        self._active_dbc_label = name
        if not path:
            self._reset_activate_button()
            self._stop_csv_logging(user_requested=True)
            self._update_csv_button_state(False)
            return
        if self.activate_dbc_btn is not None:
            self.activate_dbc_btn.setText(f"Activated: {name}")
            self.activate_dbc_btn.setStyleSheet(self._activate_btn_style_active)
        resolved = Path(path)
        target = str(resolved) if resolved.is_file() else str(path)
        self.load_dbc(target)
        if self.db is None:
            self._reset_activate_button()

    def _warn_stop_logging_before_switch(self):
        parent = self._container or self.parent()
        try:
            QMessageBox.warning(
                parent,
                "Stop logging first",
                "Please stop logging before switching DBC.",
            )
        except Exception:
            pass

    def _build_activate_menu(self):
        if self._activate_menu is None:
            return
        self._activate_menu.clear()
        self._style_menu(self._activate_menu)

        # nBMS submenu
        nmbs_menu = QMenu(self._activate_menu)
        self._style_menu(nmbs_menu)
        nmbs_items = [
            ("G2A DBC", self.predefined_dbcs.get("G2A DBC"), ("#e54735", "#b71c1c"), ("#f24542", "#c62828"), "#ffffff"),
            ("G2B DBC", self.predefined_dbcs.get("G2B DBC"), ("#266e1f", "#0f3f12"), ("#258229", "#135016"), "#ffffff"),
            ("Athena 4 or 5 DBC", self.predefined_dbcs.get("Athena 4 or 5 DBC"), ("#1f3dd1", "#0f4d8f"), ("#2596eb", "#135da3"), "#ffffff"),
        ]
        for label, path, grad, hover_grad, fg in nmbs_items:
            self._add_menu_button_action(
                nmbs_menu, label, grad, hover_grad, fg, data=path, menu_to_hide=nmbs_menu
            )

        menu_items = [
            ("Select DBC...", self.predefined_dbcs.get("Select DBC..."), ("#177a8c", "#0c4a55"), ("#1b91a7", "#0f6a7e"), "#ffffff"),
            ("Marvel DBC", self.predefined_dbcs.get("Marvel DBC"), ("#36bc47", "#1f7a2a"), ("#3fd658", "#249232"), "#ffffff"),
            ("nBMS DBC", None, ("#79ce80", "#4e9f57"), ("#87dc8e", "#5cac65"), "#ffffff", nmbs_menu),
            ("CIP BMS-24X", self.predefined_dbcs.get("CIP BMS-24X"), ("#f59a3f", "#c56a11"), ("#f7ad62", "#d17812"), "#ffffff"),
            ("ION BMS", self.predefined_dbcs.get("ION BMS"), ("#7a5a22", "#4c3513"), ("#8a6a2b", "#5b4218"), "#ffffff"),
            ("GTAKE DBC", self.predefined_dbcs.get("GTAKE DBC"), ("#b8e2ec", "#83c3d3"), ("#c6ebf3", "#8fcad8"), "#0b3b4a"),
            ("Pegasus DBC", self.predefined_dbcs.get("Pegasus DBC"), ("#10acf3", "#0a7dbc"), ("#15bbff", "#0c8ed9"), "#ffffff"),
            ("HEPU DBC", self.predefined_dbcs.get("HEPU DBC"), ("#8a1f1f", "#5c0f0f"), ("#9a2929", "#661212"), "#ffffff"),
        ]
        for item in menu_items:
            label, path, grad, hover_grad, fg, *rest = item
            submenu = rest[0] if rest else None
            display_label = label if submenu is None else f"{label} ▸"
            self._add_menu_button_action(
                self._activate_menu,
                display_label,
                grad,
                hover_grad,
                fg,
                data=path,
                submenu=submenu,
                menu_to_hide=self._activate_menu,
            )

        self._activate_menu.addSeparator()
        self._add_menu_button_action(
            self._activate_menu,
            "Load DBC...",
            ("#2a82c5", "#1a5c8e"),
            ("#3296e0", "#1f6ca9"),
            "#ffffff",
            data=self._custom_dbc_action_id,
            menu_to_hide=self._activate_menu,
        )

    def _add_menu_button_action(
        self,
        menu,
        label: str,
        gradient_colors,
        hover_gradient_colors,
        text_color: str,
        data=None,
        submenu: QMenu = None,
        menu_to_hide: QMenu = None,
    ):
        btn = QPushButton(label)
        btn.setFlat(True)
        btn.setCursor(Qt.PointingHandCursor)
        top, bottom = gradient_colors
        hover_top, hover_bottom = hover_gradient_colors
        btn.setStyleSheet(
            "QPushButton {"
            f"background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 {top}, stop:1 {bottom});"
            f"color: {text_color};"
            "font-weight: bold;"
            "padding: 4px 10px;"
            "border: 1px solid #0f172a;"
            "border-top-color: rgba(255,255,255,0.35);"
            "border-left-color: rgba(255,255,255,0.35);"
            "border-bottom-color: rgba(0,0,0,0.3);"
            "border-right-color: rgba(0,0,0,0.3);"
            "border-radius: 2px;"
            "text-align: left;"
            "}"
            "QPushButton:hover {"
            f"background-color: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 {hover_top}, stop:1 {hover_bottom});"
            "border-top-color: rgba(255,255,255,0.45);"
            "border-left-color: rgba(255,255,255,0.45);"
            "}"
        )
        action = QWidgetAction(menu)
        action.setDefaultWidget(btn)
        action.setData(data)

        def _on_click():
            if submenu is not None:
                self._open_submenu(submenu, btn)
                return
            self._on_menu_button_clicked(data, label, menu_to_hide)

        btn.clicked.connect(_on_click)
        menu.addAction(action)

    def _on_menu_button_clicked(self, data, label: str, menu_to_hide: QMenu = None):
        if menu_to_hide is not None:
            try:
                menu_to_hide.hide()
            except Exception:
                pass
        if self._activate_menu is not None:
            try:
                self._activate_menu.hide()
            except Exception:
                pass
        self._handle_dbc_selection(data, label)

    def _style_menu(self, menu: QMenu):
        if menu is None:
            return
        menu.setStyleSheet("QMenu { border: 1px solid #9ca3af; padding: 2px; }")

    def _open_submenu(self, submenu: QMenu, source_btn: QPushButton):
        if submenu is None or source_btn is None:
            return
        pos = source_btn.mapToGlobal(source_btn.rect().topRight())
        submenu.exec(pos)

    def process_frame(self, msg, ts_us):
        if self.db is None or not self.tables:
            return

        try:
            db_msg = self.db.get_message_by_frame_id(msg.ID)
        except Exception:
            return

        if db_msg is None:
            return

        try:
            length = getattr(msg, "LEN", 0) or 0
            data_field = getattr(msg, "DATA", [])
            payload = bytes(int(data_field[i]) & 0xFF for i in range(min(length, len(data_field))))
            decoded = db_msg.decode(payload)
        except Exception:
            return

        for sig_name, phys_value in decoded.items():
            self.update_table(db_msg.name, sig_name, phys_value)
        self._record_csv_update(db_msg, decoded, ts_us)

    def update_table(self, msg_name: str, sig_name: str, phys_value):
        if not self.tables:
            return

        key = (msg_name, sig_name)
        phys_txt = self._fmt(phys_value)

        if key in self.row_map:
            table_idx, row = self.row_map[key]
            table = self.tables[table_idx]
            table.setItem(row, 1, QTableWidgetItem(phys_txt))
            return

        table_idx = self._choose_table()
        table = self.tables[table_idx]
        row = table.rowCount()
        table.insertRow(row)
        sig_item = QTableWidgetItem(sig_name)
        sig_item.setToolTip(f"{msg_name} / {sig_name}")
        sig_item.setData(Qt.UserRole, msg_name)
        table.setItem(row, 0, sig_item)
        table.setItem(row, 1, QTableWidgetItem(phys_txt))
        self.row_map[key] = (table_idx, row)
        self._apply_filter_to_row(table, row)

    # ----------------------------
    # CSV logging
    # ----------------------------
    def _on_start_csv_clicked(self):
        if self.db is None:
            parent = self._container or self.parent()
            QMessageBox.warning(
                parent, "DBC not selected", "Please Activate and select a DBC before logging CSV."
            )
            return
        if not self._hardware_connected():
            parent = self._container or self.parent()
            QMessageBox.warning(parent, "No hardware connection", "Please connect to PCAN device before start logging CSV.")
            return
        if self._csv_logging_active:
            self._stop_csv_logging(user_requested=True)
            return
        suggested = self._suggest_csv_path()
        parent = self._container
        path, _ = QFileDialog.getSaveFileName(
            parent, "Save CSV Output", suggested, "CSV Files (*.csv)"
        )
        if not path:
            return
        if not path.lower().endswith(".csv"):
            path += ".csv"
        self._start_csv_logging(path)

    def _start_csv_logging(self, path: str):
        if self.db is None:
            return
        if self._is_csv_path_forbidden(path):
            self._stop_csv_logging(reason="CSV logging inside CAN SCRIPT LOGGER folder is blocked")
            return
        # rebuild headers from the currently loaded DBC
        headers, units = self._build_csv_headers()
        if not headers:
            return

        # reset any existing logging session
        self._stop_csv_logging(user_requested=True)
        self._csv_headers = headers
        self._csv_units = units
        self._csv_signal_pos = {name: idx for idx, name in enumerate(headers)}
        self._csv_latest_values = [0] * len(headers)
        self._csv_base_ts = None
        self._csv_dirty = False

        try:
            self._csv_log_file = open(path, "w", newline="")
            self._csv_writer = csv.writer(self._csv_log_file)
            self._csv_writer.writerow(headers)
            self._csv_writer.writerow(units)
            self._csv_log_file.flush()
        except Exception as exc:
            self._stop_csv_logging(reason=f"Failed to open CSV: {exc}")
            return

        self._csv_log_path = path
        self._csv_logging_active = True
        self._csv_write_timer.start(self._csv_log_interval_ms)
        self._csv_blink_state = False
        self._csv_blink_timer.start(350)
        if self.start_csv_btn is not None:
            self.start_csv_btn.setText("Stop Logging")
            self.start_csv_btn.setStyleSheet(self._csv_btn_style_idle)
            self.start_csv_btn.setToolTip(path)
        # keep compatibility with external hooks
        self.csv_logging_requested.emit()
        self._update_csv_button_state(True)

    def _stop_csv_logging(self, reason=None, user_requested: bool = False):
        try:
            if self._csv_write_timer.isActive():
                self._csv_write_timer.stop()
        except Exception:
            pass
        try:
            if self._csv_blink_timer.isActive():
                self._csv_blink_timer.stop()
        except Exception:
            pass
        self._csv_blink_state = False
        self._csv_logging_active = False
        self._csv_dirty = False
        self._csv_base_ts = None

        if self.start_csv_btn is not None:
            self.start_csv_btn.setText("Start Logging")
            self.start_csv_btn.setStyleSheet(self._csv_btn_style_idle)
            tooltip = reason if reason and not user_requested else ""
            self.start_csv_btn.setToolTip(tooltip)

        try:
            if self._csv_log_file:
                self._csv_log_file.flush()
                self._csv_log_file.close()
        except Exception:
            pass

        self._csv_log_file = None
        self._csv_writer = None
        self._csv_log_path = None
        self._csv_headers = []
        self._csv_units = []
        self._csv_signal_pos = {}
        self._csv_latest_values = []
        self._update_csv_button_state(self.db is not None)

    def _build_csv_headers(self):
        if self.db is None:
            return [], []
        headers = ["Time"]
        units = ["s"]
        try:
            for msg in sorted(self.db.messages, key=lambda m: m.name):
                for sig in msg.signals:
                    headers.append(sig.name)
                    units.append(sig.unit if getattr(sig, "unit", None) else "")
        except Exception:
            return [], []
        return headers, units

    def _record_csv_update(self, db_msg, decoded, ts_us):
        if not self._csv_logging_active or not decoded:
            return
        if self._csv_base_ts is None and ts_us is not None:
            self._csv_base_ts = ts_us
        if self._csv_base_ts is None:
            return
        rel_s = (ts_us - self._csv_base_ts) / 1_000_000.0 if ts_us is not None else 0.0
        if self._csv_latest_values:
            self._csv_latest_values[0] = round(rel_s, 6)
        for sig_name, phys_value in decoded.items():
            idx = self._csv_signal_pos.get(sig_name)
            if idx is not None and idx < len(self._csv_latest_values):
                self._csv_latest_values[idx] = phys_value
        self._csv_dirty = True

    def _flush_csv_log(self):
        if not self._csv_logging_active or not self._csv_dirty:
            return
        if self._csv_writer is None or self._csv_log_file is None:
            return
        try:
            row = [self._fmt(v) for v in self._csv_latest_values]
            self._csv_writer.writerow(row)
            self._csv_log_file.flush()
            self._csv_dirty = False
        except Exception as exc:
            self._stop_csv_logging(reason=f"CSV write error: {exc}")

    def _blink_csv_button(self):
        if not self._csv_logging_active or self.start_csv_btn is None:
            return
        self._csv_blink_state = not self._csv_blink_state
        style = self._csv_btn_style_blink if self._csv_blink_state else self._csv_btn_style_idle
        self.start_csv_btn.setStyleSheet(style)

    def _suggest_csv_path(self) -> str:
        default_dir = self._safe_csv_directory()
        if not default_dir.exists() and default_dir.parent.exists():
            default_dir = default_dir.parent
        dbc_name = ""
        if self.db_path_edit is not None and self.db_path_edit.text():
            dbc_name = Path(self.db_path_edit.text()).stem
        elif self._active_dbc_label and self._active_dbc_label != "Select DBC...":
            dbc_name = self._active_dbc_label.replace(" ", "_")
        base = dbc_name or "signals"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str((default_dir / f"{base}_csv_{timestamp}.csv").resolve())

    def _is_csv_path_forbidden(self, path: str) -> bool:
        """Block writing logs inside the application folder to keep the repo clean."""
        try:
            target = Path(path).resolve()
            project_root = Path(__file__).resolve().parent
            target.relative_to(project_root)
            return True
        except ValueError:
            return False
        except Exception:
            return True

    def _safe_csv_directory(self) -> Path:
        home_dir = Path.home()
        candidates = [
            home_dir / "Documents" / "pcan_logs",
            home_dir / "Documents",
            home_dir / "Downloads" / "pcan_logs",
            home_dir / "Downloads",
            home_dir / "pcan_logs",
            home_dir,
        ]
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
            except Exception:
                continue
            if self._is_csv_path_forbidden(resolved):
                continue
            parent = resolved if resolved.exists() else resolved.parent
            if parent.exists():
                return resolved
        fallback = Path(__file__).resolve().parent.parent
        if fallback.exists() and not self._is_csv_path_forbidden(fallback):
            return fallback
        return home_dir

    def clear(self):
        for table in self.tables:
            table.setRowCount(0)
        self.row_map = {}

    def apply_filter(self, text: str):
        self.filter_text = (text or "").strip().lower()
        for table in self.tables:
            for row in range(table.rowCount()):
                self._apply_filter_to_row(table, row)

    # ----------------------------
    # Helpers
    # ----------------------------
    def _build_table(self) -> QTableWidget:
        table = QTableWidget()
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(["Signal", "Physical Value"])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        table.setAlternatingRowColors(True)
        table.setRowCount(0)
        return table

    def _choose_table(self) -> int:
        if len(self.tables) < 2:
            return 0
        if self.tables[0].rowCount() <= self.tables[1].rowCount():
            return 0
        return 1

    def _hardware_connected(self) -> bool:
        """Check PCAN hardware connection state from parent window, defaulting to True."""
        try:
            parent = self.parent()
            if parent is None:
                return True
            return bool(getattr(parent, "is_connected", True))
        except Exception:
            return True

    def _apply_filter_to_row(self, table: QTableWidget, row: int):
        if not self.filter_text:
            table.setRowHidden(row, False)
            return
        sig_item = table.item(row, 0)
        if sig_item is None:
            table.setRowHidden(row, False)
            return
        msg_name = sig_item.data(Qt.UserRole) or ""
        haystack = f"{sig_item.text()} {msg_name}".lower()
        table.setRowHidden(row, self.filter_text not in haystack)

    @staticmethod
    def _fmt(val):
        # Format floats to avoid long binary tails like 99.99000000000001
        if isinstance(val, float):
            txt = f"{val:.6f}".rstrip("0").rstrip(".")
            return txt if txt else "0"
        return str(val)

    def _load_dbc_with_fallback(self, path: str):
        """Load DBC with a permissive fallback for files that violate strict specs."""
        try:
            return cantools.database.load_file(path)
        except Exception as strict_exc:
            try:
                return cantools.database.load_file(path, strict=False)
            except Exception:
                raise strict_exc

    def _show_dbc_load_error(self, path: str, exc: Exception):
        parent = self._container or self.parent()
        try:
            QMessageBox.warning(
                parent,
                "DBC load failed",
                f"Could not load DBC:\n{path}\n\n{exc}",
            )
        except Exception:
            pass

    def _update_csv_button_state(self, enabled: bool):
        if self.start_csv_btn is None:
            return
        # Keep clickable for user feedback even when DBC is not ready.
        self.start_csv_btn.setEnabled(True)
        if not (enabled or self._csv_logging_active):
            self.start_csv_btn.setToolTip("Please choose and activate a DBC before logging")
            self.start_csv_btn.setStyleSheet(self._csv_btn_style_idle)
            return
        # Ready or active; restore normal tooltip/state.
        self.start_csv_btn.setToolTip(self._csv_log_path or "")
        self.start_csv_btn.setStyleSheet(self._csv_btn_style_idle)

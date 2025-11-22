import cantools
from PySide6.QtCore import QObject, Qt
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QHeaderView,
)


class SignalWatch(QObject):
    """
    Decodes CAN frames using a loaded DBC and shows live signal values.
    All decoding is kept here to avoid touching pcan_logger.py logic.
    """

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
        load_btn = QPushButton("Load DBC...")
        load_btn.clicked.connect(self.load_dbc_dialog)
        self.db_path_edit = QLineEdit()
        self.db_path_edit.setReadOnly(True)
        controls.addWidget(load_btn)
        controls.addWidget(self.db_path_edit)
        controls.addStretch()

        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search signal…")
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

    def load_dbc_dialog(self):
        if self._container is None:
            return
        path, _ = QFileDialog.getOpenFileName(
            self._container, "Select DBC File", "", "DBC Files (*.dbc)"
        )
        if path:
            self.load_dbc(path)

    def load_dbc(self, path: str):
        try:
            self.db = cantools.database.load_file(path)
            if self.db_path_edit is not None:
                self.db_path_edit.setText(path)
            self.clear()
        except Exception:
            # stay silent in UI; decoding will simply be disabled
            self.db = None
            if self.db_path_edit is not None:
                self.db_path_edit.setText("")

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

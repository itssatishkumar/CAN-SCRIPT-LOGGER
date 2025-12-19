import json
import os

TX_HISTORY_FILE = "tx_history.json"


class TxHistory:
    def __init__(self, filename: str = TX_HISTORY_FILE):
        self.filename = filename

    # ----------------------------
    # SAVE TX TABLE STATE
    # ----------------------------
    def save(self, view):
        """
        view = PCANViewClone instance
        """
        tx_rows = []

        for row in range(view.transmit_table.rowCount()):
            enable = view.transmit_table.cellWidget(row, 0)
            id_item = view.transmit_table.item(row, 1)
            type_item = view.transmit_table.item(row, 2)
            len_item = view.transmit_table.item(row, 3)
            data_item = view.transmit_table.item(row, 4)
            cycle_item = view.transmit_table.item(row, 5)
            comment_item = view.transmit_table.item(row, 7)

            id_fmt = view.row_id_format_tx.get(row, "hex")
            data_fmt = view.row_data_format_tx.get(row, "hex")

            try:
                can_id = view._parse_id_text(id_item.text(), id_fmt)
            except Exception:
                can_id = 0

            try:
                data_bytes = view._parse_data_text(data_item.text(), data_fmt)
            except Exception:
                data_bytes = []

            tx_rows.append({
                "enabled": enable.isChecked() if enable else False,
                "can_id": can_id,
                "extended": (type_item.text() == "EXT") if type_item else False,
                "length": int(len_item.text()) if len_item else len(data_bytes),
                "data": data_bytes[:8],
                "cycle": cycle_item.text() if cycle_item else "0",
                "comment": comment_item.text() if comment_item else ""
            })

        try:
            with open(self.filename, "w") as f:
                json.dump(tx_rows, f, indent=2)
        except Exception as e:
            print(f"[TxHistory] Save failed: {e}")

    # ----------------------------
    # LOAD TX TABLE STATE
    # ----------------------------
    def load(self, view):
        """
        view = PCANViewClone instance
        """
        if not os.path.exists(self.filename):
            return

        try:
            with open(self.filename, "r") as f:
                tx_rows = json.load(f)
        except Exception as e:
            print(f"[TxHistory] Load failed: {e}")
            return

        view.transmit_table.setRowCount(0)

        for row_data in tx_rows:
            view.add_transmit_row({
                "id": f"{row_data['can_id']:X}",
                "length": row_data.get("length", 8),
                "data": [f"{b:02X}" for b in row_data.get("data", [])],
                "cycle": row_data.get("cycle", "0"),
                "extended": row_data.get("extended", False),
                "remote": False,
                "comment": row_data.get("comment", "")
            })

            row = view.transmit_table.rowCount() - 1
            enable = view.transmit_table.cellWidget(row, 0)
            if enable:
                enable.setChecked(False)

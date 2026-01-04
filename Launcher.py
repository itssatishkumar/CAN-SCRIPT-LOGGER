import sys
import os
import subprocess
import time
import threading
from pathlib import Path

from PySide6.QtCore import Qt, QTimer, QPointF
from PySide6.QtGui import QPainter, QColor, QPen, QFont
from PySide6.QtWidgets import (
    QApplication, QWidget, QLabel, QVBoxLayout, QPushButton, QHBoxLayout, QFrame
)

# Try to import pywin32 window detection utilities (Windows-only)
try:
    import win32gui
    import win32process
    WIN32_AVAILABLE = True
except Exception:
    WIN32_AVAILABLE = False


class CircularSpinner(QWidget):
    """
    Circular segmented spinner with rotating animation.
    - segments: number of segments in the ring
    - radius: radius of the ring
    - thickness: line thickness
    - speed_ms: timer interval in ms (smaller -> faster)
    """
    def __init__(self, parent=None, segments=12, radius=44, thickness=8, speed_ms=60):
        super().__init__(parent)
        self.segments = segments
        self.radius = radius
        self.thickness = thickness
        self.angle_offset = 0  # degrees
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.on_timeout)
        self.timer.start(speed_ms)
        self.setMinimumSize((radius + thickness) * 2 + 4, (radius + thickness) * 2 + 4)
        self.base_color = QColor(0, 120, 215)  # blue
        self.background_color = QColor(30, 30, 36)  # dark

    def on_timeout(self):
        # advance rotation
        self.angle_offset = (self.angle_offset + (360 / self.segments)) % 360
        self.update()

    def paintEvent(self, event):
        size = min(self.width(), self.height())
        cx = self.width() / 2
        cy = self.height() / 2

        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.fillRect(self.rect(), self.background_color)

        # Draw segments around circle
        step = 360 / self.segments
        for i in range(self.segments):
            # compute segment alpha so that trailing segments fade out
            # leading segment will be brightest
            idx = (i + int(self.angle_offset / step)) % self.segments
            alpha_factor = (idx + 1) / self.segments
            alpha = int(30 + 225 * alpha_factor)  # between ~30 and 255
            color = QColor(self.base_color)
            color.setAlpha(alpha)

            pen = QPen(color)
            pen.setWidth(self.thickness)
            pen.setCapStyle(Qt.RoundCap)
            painter.setPen(pen)

            # start and end angles (in degrees). QPainter uses degrees*16 for arcs, but we'll draw lines for crisp segments.
            angle_deg = i * step + self.angle_offset
            # compute start point and end point for short radial line
            from math import radians, cos, sin
            inner = self.radius - self.thickness / 2
            outer = self.radius + self.thickness / 2

            ax = cx + inner * cos(radians(angle_deg))
            ay = cy + inner * sin(radians(angle_deg))
            bx = cx + outer * cos(radians(angle_deg))
            by = cy + outer * sin(radians(angle_deg))
            painter.drawLine(int(ax), int(ay), int(bx), int(by))


class SplashWindow(QWidget):
    def __init__(self, main_script_name="pcan_logger.py"):
        super().__init__(None, Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setWindowTitle("CAN Logger - Launching")
        self.main_script_name = main_script_name
        self.proc = None
        self._killed_by_user = False

        # Styling
        self.resize(560, 320)
        self.center_on_screen()

        # Outer frame with rounded look
        outer = QFrame(self)
        outer.setObjectName("outer")
        outer.setStyleSheet("""
            QFrame#outer {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #111216, stop:1 #1e2430);
                border-radius: 12px;
                border: 2px solid rgba(255,255,255,0.04);
            }
        """)
        outer.setGeometry(10, 10, self.width()-20, self.height()-20)

        layout = QVBoxLayout(outer)
        layout.setContentsMargins(18, 12, 18, 12)
        layout.setSpacing(12)
        layout.setAlignment(Qt.AlignCenter)

        # Header: Yellow strip w/ red text
        header = QLabel("CAN LOGGER v1.0.26")
        header.setFixedHeight(64)
        header.setAlignment(Qt.AlignCenter)
        header.setStyleSheet("""
            QLabel {
                background: #FFD900;
                color: #C40000;
                font-weight: 700;
                font-family: 'Segoe UI';
                font-size: 28px;
                border-radius: 6px;
                padding-top: 6px;
            }
        """)
        layout.addWidget(header)

        # Spacer
        # Message + spinner area
        mid_frame = QFrame()
        mid_layout = QHBoxLayout(mid_frame)
        mid_layout.setContentsMargins(10, 6, 10, 6)
        mid_layout.setSpacing(20)
        mid_layout.setAlignment(Qt.AlignCenter)

        # Spinner
        self.spinner = CircularSpinner(mid_frame, segments=14, radius=46, thickness=10, speed_ms=65)
        mid_layout.addWidget(self.spinner, 0, Qt.AlignCenter)

        # Text column
        txt_frame = QFrame()
        txt_layout = QVBoxLayout(txt_frame)
        txt_layout.setContentsMargins(0, 0, 0, 0)
        txt_layout.setSpacing(6)
        title = QLabel("Launching... Please wait")
        title.setStyleSheet("color: #FFFFFF; font-family: 'Segoe UI'; font-size: 18px;")
        detail = QLabel("This may take a moment while the application initializes.")
        detail.setStyleSheet("color: #c8cbd3; font-family: 'Segoe UI'; font-size: 11px;")
        txt_layout.addWidget(title)
        txt_layout.addWidget(detail)
        mid_layout.addWidget(txt_frame, 0, Qt.AlignLeft)

        layout.addWidget(mid_frame)

        # Buttons row (Cancel)
        btn_frame = QFrame()
        btn_layout = QHBoxLayout(btn_frame)
        btn_layout.setContentsMargins(10, 6, 10, 6)
        btn_layout.setSpacing(10)
        btn_layout.addStretch()

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setFixedHeight(36)
        self.cancel_btn.setFixedWidth(100)
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                background: #2b2f36;
                color: #ffffff;
                border: 1px solid rgba(255,255,255,0.06);
                font-family: 'Segoe UI';
                font-size: 12px;
                border-radius: 6px;
            }
            QPushButton:hover {
                background: #3a3f47;
            }
        """)
        self.cancel_btn.clicked.connect(self.on_cancel_clicked)
        btn_layout.addWidget(self.cancel_btn)

        layout.addWidget(btn_frame)

        # Failsafe: if main does not show after 120s, close splash (to avoid forever hang)
        self.failsafe_timer = QTimer(self)
        self.failsafe_timer.setSingleShot(True)
        self.failsafe_timer.timeout.connect(self.on_failsafe_timeout)
        self.failsafe_timer.start(120_000)  # 120 seconds

        # Poll timer to check main app window
        self.poll_timer = QTimer(self)
        self.poll_timer.setInterval(800)
        self.poll_timer.timeout.connect(self.check_main_window)
        self.poll_timer.start()

    def center_on_screen(self):
        screen = QApplication.primaryScreen()
        screen_geo = screen.availableGeometry()
        x = int((screen_geo.width() - self.width()) / 2)
        y = int((screen_geo.height() - self.height()) / 2.5)
        self.move(x, y)

    def launch_main_process(self):
        """Start the main app process (pcan_logger.py)"""
        script_path = Path(__file__).with_name(self.main_script_name)
        if not script_path.exists():
            # try current directory
            script_path = Path(os.getcwd()) / self.main_script_name
        if not script_path.exists():
            print(f"[launcher] ERROR: {self.main_script_name} not found at {script_path}")
            return

        try:
            # Launch Python interpreter with the target script.
            # No console, inherit environment
            self.proc = subprocess.Popen([sys.executable, str(script_path)])
        except Exception as ex:
            print(f"[launcher] Failed to start main script: {ex}")
            self.proc = None

    def check_main_window(self):
        """
        Attempt to detect whether the main process created a top-level window.
        On Windows uses win32gui + win32process to find windows for our process id.
        If detection succeeds, close splash.
        If pywin32 not available, fallback: if process exists and >2s since started, close splash.
        """
        if self.proc is None:
            return

        if self.proc.poll() is not None:
            # process exited prematurely — close splash and show nothing
            self.cleanup_and_close()
            return

        if WIN32_AVAILABLE:
            try:
                pid = self.proc.pid
                # iterate top-level windows and see if any belong to this pid and are visible
                def enum_windows_callback(hwnd, param):
                    try:
                        _, win_pid = win32process.GetWindowThreadProcessId(hwnd)
                        if win_pid == pid and win32gui.IsWindowVisible(hwnd):
                            param.append(hwnd)
                    except Exception:
                        pass
                    return True

                matches = []
                win32gui.EnumWindows(enum_windows_callback, matches)
                if matches:
                    # main GUI window found => close splash
                    self.cleanup_and_close()
                    return
            except Exception:
                # if detection fails, fallback to simpler path
                pass

        # fallback: if process has been alive longer than a short stabilization time, assume it's loaded
        # We use creation time heuristics by checking runtime age (>2s) then close after small delay.
        try:
            # process started time (approx): use proc.start_time if available else rely on time check
            # To avoid closing too early, only close if process has been alive > 2s and still running in next check
            # We'll use a simple counter approach saved on the object
            if not hasattr(self, "_alive_since"):
                self._alive_since = time.time()
            else:
                if (time.time() - self._alive_since) > 2.2:
                    # gave the main app a tiny stabilization time
                    self.cleanup_and_close()
        except Exception:
            # if anything goes wrong, don't crash; let failsafe handle it
            pass

    def cleanup_and_close(self):
        # stop timers and close splash
        try:
            self.poll_timer.stop()
            self.failsafe_timer.stop()
            self.spinner.timer.stop()
            self.close()
        except Exception:
            pass

    def on_cancel_clicked(self):
        # kill the launched process if running and close splash
        self._killed_by_user = True
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.kill()
            except Exception:
                pass
        self.cleanup_and_close()

    def on_failsafe_timeout(self):
        # If still running, close splash anyway
        self.cleanup_and_close()


def main():
    app = QApplication(sys.argv)
    splash = SplashWindow(main_script_name="pcan_logger.py")

    # Start the main app in a background thread so UI stays responsive
    threading.Thread(target=splash.launch_main_process, daemon=True).start()

    splash.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

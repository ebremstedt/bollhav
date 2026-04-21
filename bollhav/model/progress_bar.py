import atexit
import inspect
import os
import threading
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Callable
import sys
import time

_SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
_SPINNER_INTERVAL = 0.1


class ProgressLevel(Enum):
    MINIMAL = "minimal"
    MODEL = "model"
    BATCH = "batch"


def get_progress_level() -> ProgressLevel:
    raw = os.environ.get("PROGRESS_BAR", "model").lower()
    try:
        return ProgressLevel(raw)
    except ValueError:
        return ProgressLevel.MODEL


@dataclass
class _State:
    current_model: str = ""
    current_mode: str = (
        ""  # "interval" | "rows" | "" — set per-model from effective_reload_mode
    )
    start: float = 0.0
    count: int = 0
    total: int = 0
    finish_total: int = 0
    batch_times: list[float] = field(default_factory=list)
    name_width: int = 60
    batch_width: int = 0
    models_done: int = 0
    overall_start: float = 0.0
    finished: bool = False


def _format_duration(secs: float) -> str:
    if secs < 60:
        if secs >= 1:
            return f"{secs:>5.1f}s"
        return f"{secs * 1000:>4.0f}ms"
    mins = secs / 60
    if mins < 60:
        return f"{mins:>5.1f}m"
    hours = mins / 60
    return f"{hours:>5.1f}h"


def _format_progress(current: int, total: int, width: int = 20) -> str:
    if total == 0:
        return f"{'░' * width} {current}/???"
    filled = int(width * current / total)
    bar = "█" * filled + "░" * (width - filled)
    pct = current / total * 100
    pct_str = f"{pct:.2f}".rstrip("0").rstrip(".")
    total_width = len(str(total))
    return f"{bar} {pct_str:>6}% {current:>{total_width}}/{total}"


def progress_bar(func: Callable) -> Callable:
    """
    Execution decorator with three display levels (set via PROGRESS_BAR env var).

    PROGRESS_BAR=minimal
        ✓ 3 models done  5.2s

    PROGRESS_BAR=model  (default)
        ✓ customers       110ms 3/3 á  36ms
        ✓ orders          1.2s  9/9 á 130ms
        ✓ products        4.5s 25/25 á 180ms

    PROGRESS_BAR=batch
        ⠋ customers  █████████░░░░░░░░░░░ 45% 3/9 á 130ms ~1.2s left
        ✓ customers       1.2s  9/9 á 130ms
        ⠋ orders     ██░░░░░░░░░░░░░░░░░░ 12% 3/25 á 180ms ~4.0s left
        ✓ orders          4.5s 25/25 á 180ms
        ...
    """
    level = get_progress_level()
    s = _State()

    # ── stdout helpers ──────────────────────────────────────────────

    def _write(msg: str, newline: bool = False) -> None:
        suffix = "\n" if newline else ""
        sys.stdout.write(f"\r{msg}   {suffix}")
        sys.stdout.flush()

    def _clear_line() -> None:
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()

    # ── spinner (batch mode only) ───────────────────────────────────

    _spinner_thread: list[threading.Thread | None] = [None]
    _spinner_stop = threading.Event()
    _spinner_msg: list[str] = [""]

    def _spin() -> None:
        i = 0
        while not _spinner_stop.is_set():
            frame = _SPINNER_FRAMES[i % len(_SPINNER_FRAMES)]
            if _spinner_msg[0]:
                sys.stdout.write(f"\r{frame} {_spinner_msg[0]}   ")
                sys.stdout.flush()
            i += 1
            _spinner_stop.wait(_SPINNER_INTERVAL)

    def _start_spinner(msg: str) -> None:
        _spinner_stop.clear()
        _spinner_msg[0] = msg
        sys.stdout.write(f"\r{_SPINNER_FRAMES[0]} {msg}   ")
        sys.stdout.flush()
        t = threading.Thread(target=_spin, daemon=True)
        _spinner_thread[0] = t
        t.start()

    def _stop_spinner() -> None:
        _spinner_stop.set()
        if _spinner_thread[0]:
            _spinner_thread[0].join()
            _spinner_thread[0] = None

    # ── batch timing ────────────────────────────────────────────────

    def _avg_batch_seconds() -> float | None:
        return sum(s.batch_times) / len(s.batch_times) if s.batch_times else None

    def _avg_batch_str() -> str:
        avg = _avg_batch_seconds()
        return f" á {_format_duration(avg)}" if avg is not None else ""

    def _eta_str() -> str:
        avg = _avg_batch_seconds()
        if avg is None:
            return ""
        remaining = s.total - s.count
        return f" ~{_format_duration(avg * remaining)} left" if remaining > 0 else ""

    # ── batch count formatting ──────────────────────────────────────

    def _batch_count_str() -> str:
        if s.finish_total:
            tw = len(str(s.finish_total))
            return f"{s.count:>{tw}}/{s.finish_total}"
        return str(s.count)

    # ── model lifecycle ─────────────────────────────────────────────

    def _resolve_model(args, kwargs) -> tuple[str, str]:
        """Return (full_name, mode_label). mode_label is empty when the
        model isn't a bollhav Model (or doesn't expose effective_reload_mode).

        Examples of mode_label:
            "10000 rows"      (ROW mode, effective batch size)
            "hourly"          (INTERVAL with @hourly alias — @ stripped)
            "*/15 * * * *"    (INTERVAL with a raw cron)"""
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        model = bound.arguments.get("model")
        if not (
            model and hasattr(model, "target") and hasattr(model.target, "full_name")
        ):
            return func.__name__, ""

        name = model.target.full_name
        if not hasattr(model, "effective_reload_mode"):
            return name, ""

        m = model.effective_reload_mode().value.lower()
        if m == "row":
            size = model.effective_reload_batch_size()
            return name, f"{size} rows"

        # INTERVAL mode — show the effective interval expression.
        # Precedence: tag override > pipe override > model's static config.
        rt = model.runtime_override
        expr = (
            getattr(rt, "reload_interval_expression", None)
            or getattr(rt, "batch_expression", None)
            or model.batching.interval.expression
        )
        # Strip the leading "@" from cron aliases for cleaner display
        # (`@hourly` -> `hourly`); raw crons pass through unchanged.
        return name, expr.lstrip("@")

    def _begin_model(name: str, mode: str) -> None:
        s.current_model = name
        s.current_mode = mode
        s.start = time.time()
        s.count = 0
        s.finish_total = 0
        s.batch_times = []
        if level == ProgressLevel.BATCH:
            _update_batch_display(name)

    def _mode_label() -> str:
        return f" ({s.current_mode})" if s.current_mode else ""

    def _finish_current() -> None:
        if not s.current_model:
            return

        s.models_done += 1

        if level == ProgressLevel.MINIMAL:
            s.current_model = ""
            s.current_mode = ""
            return

        if level == ProgressLevel.BATCH:
            _stop_spinner()

        _clear_line()

        name_with_mode = f"{s.current_model}{_mode_label()}"
        w = max(s.name_width, len(name_with_mode))
        elapsed = _format_duration(time.time() - s.start)
        bw = max(s.batch_width, len(_batch_count_str()))

        _write(
            f"▸ {name_with_mode:<{w}} {elapsed} {_batch_count_str():>{bw}}{_avg_batch_str()}",
            newline=True,
        )
        s.current_model = ""
        s.current_mode = ""

    # ── batch-level spinner update ──────────────────────────────────

    def _update_batch_display(model_name: str) -> None:
        bar = _format_progress(current=s.count, total=s.total)
        name_with_mode = f"{model_name}{_mode_label()}"
        w = max(s.name_width, len(name_with_mode))
        msg = f"{name_with_mode:<{w}} {bar}{_avg_batch_str()}{_eta_str()}"

        if _spinner_thread[0] is None:
            _start_spinner(msg)
        else:
            _spinner_msg[0] = msg

    # ── main wrapper ────────────────────────────────────────────────

    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        if not s.overall_start:
            s.overall_start = time.time()

        model_name, model_mode = _resolve_model(args, kwargs)

        if model_name != s.current_model:
            _finish_current()
            _begin_model(model_name, model_mode)

        batch_start = time.time()
        result = func(*args, **kwargs)

        s.batch_times.append(time.time() - batch_start)
        s.count += 1
        s.finish_total = s.total

        if level == ProgressLevel.BATCH:
            _update_batch_display(model_name)

        return result

    # ── public api ──────────────────────────────────────────────────

    def set_total(total: int) -> None:
        s.total = total
        tw = len(str(total)) * 2 + 1
        s.batch_width = max(s.batch_width, tw)

    def set_name_width(width: int) -> None:
        s.name_width = width

    def finish() -> None:
        if s.finished:
            return
        _finish_current()
        if s.models_done > 0:
            _clear_line()
            elapsed = time.time() - s.overall_start if s.overall_start else 0
            _write(
                f"✓ {s.models_done} models done {_format_duration(elapsed)}",
                newline=True,
            )
        s.finished = True

    atexit.register(finish)

    wrapper.set_total = set_total  # type: ignore[attr-defined]
    wrapper.set_name_width = set_name_width  # type: ignore[attr-defined]
    wrapper.finish = finish  # type: ignore[attr-defined]
    return wrapper

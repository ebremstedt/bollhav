import atexit
import inspect
import threading
from functools import wraps
from typing import Callable
import sys
import time

_SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]


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
    log_execution decorator — two modes:


    ⏳ model: ░░░░░░░░░░░░░░░░░░░░ 3/??? (2026-01-03) (Without set_total)

    ⏳ model: █████████░░░░░░░░░░░ 45% 3/9 (2026-01-03) (With set_total)

    Call set_total before the batch loop to enable progress tracking:
        execute.set_total(len(intervals))
    """
    state: dict[str, str | float | int | list] = {
        "current_model": "",
        "start": 0.0,
        "count": 0,
        "total": 0,
        "finish_total": 0,
        "batch_start": 0.0,
        "batch_times": [],
        "name_width": 60,
        "batch_width": 0,
        "spinner_index": 0,
        "spinner_msg": "",
    }
    _spinner_thread: list[threading.Thread | None] = [None]
    _spinner_stop = threading.Event()

    def _write(msg: str, newline: bool = False) -> None:
        suffix = "\n" if newline else ""
        sys.stdout.write(f"\r{msg}   {suffix}")
        sys.stdout.flush()

    def _spin() -> None:
        i = 0
        while not _spinner_stop.is_set():
            frame = _SPINNER_FRAMES[i % len(_SPINNER_FRAMES)]
            msg = state["spinner_msg"]
            if msg:
                sys.stdout.write(f"\r{frame} {msg}   ")
                sys.stdout.flush()
            i += 1
            _spinner_stop.wait(0.1)

    def _start_spinner(msg: str) -> None:
        _spinner_stop.clear()
        state["spinner_msg"] = msg
        t = threading.Thread(target=_spin, daemon=True)
        _spinner_thread[0] = t
        t.start()

    def _update_spinner(msg: str) -> None:
        state["spinner_msg"] = msg

    def _stop_spinner() -> None:
        _spinner_stop.set()
        if _spinner_thread[0]:
            _spinner_thread[0].join()
            _spinner_thread[0] = None

    def _avg_batch_seconds() -> float | None:
        times = list(state["batch_times"])  # type: ignore[arg-type]
        if not times:
            return None
        return sum(times) / len(times)

    def _avg_batch_time() -> str:
        avg = _avg_batch_seconds()
        if avg is None:
            return ""
        return f" á {_format_duration(avg)}"

    def _eta() -> str:
        avg = _avg_batch_seconds()
        if avg is None:
            return ""
        total = int(state["total"])
        count = int(state["count"])
        remaining = total - count
        if remaining <= 0:
            return ""
        return f" ~{_format_duration(avg * remaining)} left"

    def _finish_current() -> None:
        _stop_spinner()
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()
        elapsed = time.time() - float(state["start"])
        count = int(state["count"])
        total = int(state["finish_total"])
        if total:
            total_width = len(str(total))
            batch_str = f"{count:>{total_width}}/{total}"
        else:
            batch_str = str(count)
        bw = max(int(state["batch_width"]), len(batch_str))
        name = state["current_model"]
        w = max(int(state["name_width"]), len(name))
        _write(
            f"✓ {name:<{w}} {_format_duration(elapsed)} {batch_str:>{bw}}{_avg_batch_time()}",
            newline=True,
        )
        state["current_model"] = ""

    def _on_exit() -> None:
        if state["current_model"]:
            _finish_current()

    atexit.register(_on_exit)

    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        model = bound.arguments.get("model")
        model_name = (
            model.target.full_name
            if model and hasattr(model, "target") and hasattr(model.target, "full_name")
            else func.__name__
        )

        if model_name != state["current_model"]:
            if state["current_model"]:
                _finish_current()
            state["current_model"] = model_name
            state["start"] = time.time()
            state["count"] = 0
            state["finish_total"] = 0
            state["batch_times"] = []

        batch_start = time.time()
        prev_batch_start = float(state["batch_start"])
        if prev_batch_start > 0:
            state["batch_times"] = list(state["batch_times"]) + [
                batch_start - prev_batch_start
            ]  # type: ignore[operator]
        state["batch_start"] = batch_start

        state["count"] = int(state["count"]) + 1
        state["finish_total"] = int(state["total"])
        count = int(state["count"])
        total = int(state["total"])
        bar = _format_progress(current=count, total=total)
        w = max(int(state["name_width"]), len(model_name))
        msg = f"{model_name:<{w}} {bar}{_avg_batch_time()}{_eta()}"
        if _spinner_thread[0] is None:
            _start_spinner(msg)
        else:
            _update_spinner(msg)

        return func(*args, **kwargs)

    def set_total(total: int) -> None:
        state["total"] = total
        # e.g. "25/25" = 5 chars, track max across all models
        tw = len(str(total)) * 2 + 1
        state["batch_width"] = max(int(state["batch_width"]), tw)

    def set_name_width(width: int) -> None:
        state["name_width"] = width

    wrapper.set_total = set_total  # type: ignore[attr-defined]
    wrapper.set_name_width = set_name_width  # type: ignore[attr-defined]
    wrapper.finish = _finish_current  # type: ignore[attr-defined]
    return wrapper

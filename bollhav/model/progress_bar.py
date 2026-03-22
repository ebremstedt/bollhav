import atexit
import inspect
from functools import wraps
from typing import Callable
import sys
import time


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
    }

    def _write(msg: str, newline: bool = False) -> None:
        suffix = "\n" if newline else ""
        sys.stdout.write(f"\r{msg}   {suffix}")
        sys.stdout.flush()

    def _avg_batch_time() -> str:
        times = list(state["batch_times"])  # type: ignore[arg-type]
        if not times:
            return ""
        avg = sum(times) / len(times)
        if avg >= 1:
            return f" avg {avg:.1f}s"
        return f" avg {avg * 1000:.0f}ms"

    def _finish_current() -> None:
        elapsed = time.time() - float(state["start"])
        count = int(state["count"])
        total = int(state["finish_total"])
        batch_str = f"{count}/{total}" if total else str(count)
        batch_word = "batch" if count == 1 else "batches"
        _write(
            f"✓ {state['current_model']}: finished ({elapsed:.1f}s, {batch_str} {batch_word}{_avg_batch_time()})",
            newline=True,
        )
        state["current_model"] = ""

    def _progress_bar(current: int, total: int, width: int = 20) -> str:
        if total == 0:
            return f"{'░' * width} {current}/???"
        filled = int(width * current / total)
        bar = "█" * filled + "░" * (width - filled)
        pct = current / total * 100
        return f"{bar} {pct:.0f}% {current}/{total}"

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
        batch_since = bound.arguments.get("batch_since")

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
        date_part = f" ({batch_since.strftime('%Y-%m-%d')})" if batch_since else ""
        bar = _progress_bar(current=count, total=total)
        _write(f"⏳ {model_name}: {bar}{date_part}{_avg_batch_time()}")

        return func(*args, **kwargs)

    def set_total(total: int) -> None:
        state["total"] = total

    wrapper.set_total = set_total  # type: ignore[attr-defined]
    wrapper.finish = _finish_current  # type: ignore[attr-defined]
    return wrapper

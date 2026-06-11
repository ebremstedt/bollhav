import atexit
import inspect
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Callable, Iterator
import sys
import time

_SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
_SPINNER_INTERVAL = 0.1


class ProgressLevel(Enum):
    MINIMAL = "minimal"
    MODEL = "model"
    EXECUTE = "execute"


def get_progress_level() -> ProgressLevel:
    raw = os.environ.get("PROGRESS_BAR", "model").lower()
    if raw == "batch":  # legacy alias — one tick is one execute, not a batch
        return ProgressLevel.EXECUTE
    try:
        return ProgressLevel(raw)
    except ValueError:
        return ProgressLevel.MODEL


def _model_display_label(model) -> str:
    """The row label for a model: `full_name` plus the mode in parens.
    Exposed so callers can pre-compute the width a row will take. Returns
    "" for anything that isn't a bollhav Model. Call *after* `apply_pipe`
    so the name reflects the active schema suffix and any tag-driven reload
    overrides already baked into `batching`."""
    name, mode = _name_and_mode(model)
    if not name:
        return ""
    return f"{name} ({mode})" if mode else name


def name_width_for(models) -> int:
    """Return the width the progress bar needs to align every row's mode
    column. Pass this to `set_name_width` after `apply_pipe` has run."""
    return max((len(_model_display_label(m)) for m in models), default=0)


def _name_and_mode(model) -> tuple[str, str]:
    """Return `(full_name, mode_label)`. `mode_label` is empty when the
    object isn't a bollhav Model (or has no batching configured).

    Examples of mode_label:
        "hourly"          (@hourly alias — @ stripped)
        "*/15 * * * *"    (a raw cron)"""
    if not (model and hasattr(model, "target") and hasattr(model.target, "full_name")):
        return ("", "")
    name = model.target.full_name
    # No batching = no mode label — model runs once, unfiltered.
    if getattr(model, "batching", None) is None:
        return (name, "")
    # Strip the leading "@" from cron aliases for cleaner display
    # (`@hourly` -> `hourly`); raw crons pass through unchanged.
    return (name, model.batching.time.chunk.lstrip("@"))


@dataclass
class _ProgressState:
    current_model: str = ""
    current_mode: str = ""  # interval expression label | "" — set per-model
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
        # No intervals to run (e.g. everything already applied) — a known zero,
        # not an unknown count, so show a complete bar rather than "???".
        return f"{'█' * width}   100% {current}/0"
    filled = int(width * current / total)
    bar = "█" * filled + "░" * (width - filled)
    pct = current / total * 100
    pct_str = f"{pct:.2f}".rstrip("0").rstrip(".")
    total_width = len(str(total))
    return f"{bar} {pct_str:>6}% {current:>{total_width}}/{total}"


class Progress:
    """Self-contained progress renderer with three display levels (chosen by
    `PROGRESS_BAR`). It owns all terminal I/O — spinner, in-place redraw,
    per-model rows, final summary — so callers only signal *events*
    (`begin_model` / `tick` / `finish_model`) and never touch the cursor.

    Drive it either:
      * implicitly via the `@progress_bar` decorator (one instance per
        decorated function, model boundaries inferred from the `model` arg), or
      * explicitly via the module-level `PROGRESS` singleton, whose
        `model()` / `interval()` context managers the lifecycle hooks use.

    A `Progress` is inert until `init()` (singleton) or decoration enables it,
    so importing/calling the hooks without `@load_models` produces no output.
    """

    def __init__(self, level: ProgressLevel | None = None) -> None:
        self.level = level if level is not None else get_progress_level()
        self.s = _ProgressState()
        self.enabled = False
        self._spinner_thread: threading.Thread | None = None
        self._spinner_stop = threading.Event()
        self._spinner_msg = ""
        self._atexit_registered = False

    # ── lifecycle entry points ──────────────────────────────────────

    def init(self, models, level: ProgressLevel | None = None) -> None:
        """Enable progress for a run: snapshot the alignment width from all
        models and start the overall clock. Called once by `@load_models`."""
        if level is not None:
            self.level = level
        self.s = _ProgressState()
        self.s.name_width = name_width_for(models) if models else 60
        self.s.overall_start = time.time()
        self.enabled = True
        if not self._atexit_registered:
            atexit.register(self.finish)
            self._atexit_registered = True

    @contextmanager
    def model(self, model) -> Iterator[None]:
        """Bracket one model's run as a single progress row. Reads the
        model's finalized interval count for the total."""
        self.begin_model_for(model, total=len(getattr(model, "intervals", (None,))))
        try:
            yield
        finally:
            self.finish_model()

    @contextmanager
    def interval(self) -> Iterator[None]:
        """Bracket one interval (one batch). Times the body and ticks on
        success — a raised body skips the tick and propagates."""
        start = time.time()
        yield
        self.tick(time.time() - start)

    def begin_model_for(self, model, total: int | None = None) -> None:
        name, mode = _name_and_mode(model)
        self.begin_model(name, mode, total)

    def begin_model(self, name: str, mode: str, total: int | None = None) -> None:
        if not self.enabled:
            return
        if name != self.s.current_model:
            self.finish_model()
        self.s.current_model = name
        self.s.current_mode = mode
        self.s.start = time.time()
        self.s.count = 0
        self.s.finish_total = 0
        self.s.batch_times = []
        if total is not None:
            self.set_total(total)
        if self.level == ProgressLevel.EXECUTE:
            self._update_batch_display(name)

    def tick(self, elapsed: float) -> None:
        if not self.enabled or not self.s.current_model:
            return
        self.s.batch_times.append(elapsed)
        self.s.count += 1
        self.s.finish_total = self.s.total
        if self.level == ProgressLevel.EXECUTE:
            self._update_batch_display(self.s.current_model)

    def finish_model(self) -> None:
        if not self.enabled or not self.s.current_model:
            return

        self.s.models_done += 1

        if self.level == ProgressLevel.MINIMAL:
            self.s.current_model = ""
            self.s.current_mode = ""
            return

        if self.level == ProgressLevel.EXECUTE:
            self._stop_spinner()

        self._clear_line()
        name_with_mode = f"{self.s.current_model}{self._mode_label()}"
        w = max(self.s.name_width, len(name_with_mode))
        elapsed = _format_duration(time.time() - self.s.start)
        bw = max(self.s.batch_width, len(self._batch_count_str()))
        self._write(
            f"▸ {name_with_mode:<{w}} {elapsed} "
            f"{self._batch_count_str():>{bw}}{self._avg_batch_str()}",
            newline=True,
        )
        self.s.current_model = ""
        self.s.current_mode = ""

    def finish(self) -> None:
        if not self.enabled or self.s.finished:
            return
        self.finish_model()
        if self.s.models_done > 0:
            self._clear_line()
            elapsed = time.time() - self.s.overall_start if self.s.overall_start else 0
            self._write(
                f"✓ {self.s.models_done} models done {_format_duration(elapsed)}",
                newline=True,
            )
        self.s.finished = True

    # ── public knobs ────────────────────────────────────────────────

    def set_total(self, total: int) -> None:
        self.s.total = total
        tw = len(str(total)) * 2 + 1
        self.s.batch_width = max(self.s.batch_width, tw)

    def set_name_width(self, width: int) -> None:
        self.s.name_width = width

    # ── stdout helpers ──────────────────────────────────────────────

    def _write(self, msg: str, newline: bool = False) -> None:
        suffix = "\n" if newline else ""
        sys.stdout.write(f"\r{msg}   {suffix}")
        sys.stdout.flush()

    def _clear_line(self) -> None:
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()

    # ── spinner (execute mode only) ─────────────────────────────────

    def _spin(self) -> None:
        i = 0
        while not self._spinner_stop.is_set():
            frame = _SPINNER_FRAMES[i % len(_SPINNER_FRAMES)]
            if self._spinner_msg:
                sys.stdout.write(f"\r{frame} {self._spinner_msg}   ")
                sys.stdout.flush()
            i += 1
            self._spinner_stop.wait(_SPINNER_INTERVAL)

    def _start_spinner(self, msg: str) -> None:
        self._spinner_stop.clear()
        self._spinner_msg = msg
        sys.stdout.write(f"\r{_SPINNER_FRAMES[0]} {msg}   ")
        sys.stdout.flush()
        t = threading.Thread(target=self._spin, daemon=True)
        self._spinner_thread = t
        t.start()

    def _stop_spinner(self) -> None:
        self._spinner_stop.set()
        if self._spinner_thread:
            self._spinner_thread.join()
            self._spinner_thread = None

    # ── batch timing / formatting ───────────────────────────────────

    def _avg_batch_seconds(self) -> float | None:
        times = self.s.batch_times
        return sum(times) / len(times) if times else None

    def _avg_batch_str(self) -> str:
        avg = self._avg_batch_seconds()
        return f" á {_format_duration(avg)}" if avg is not None else ""

    def _eta_str(self) -> str:
        avg = self._avg_batch_seconds()
        if avg is None:
            return ""
        remaining = self.s.total - self.s.count
        return f" ~{_format_duration(avg * remaining)} left" if remaining > 0 else ""

    def _batch_count_str(self) -> str:
        if self.s.finish_total:
            tw = len(str(self.s.finish_total))
            return f"{self.s.count:>{tw}}/{self.s.finish_total}"
        return str(self.s.count)

    def _mode_label(self) -> str:
        return f" ({self.s.current_mode})" if self.s.current_mode else ""

    def _update_batch_display(self, model_name: str) -> None:
        bar = _format_progress(current=self.s.count, total=self.s.total)
        name_with_mode = f"{model_name}{self._mode_label()}"
        w = max(self.s.name_width, len(name_with_mode))
        msg = f"{name_with_mode:<{w}} {bar}{self._avg_batch_str()}{self._eta_str()}"
        if self._spinner_thread is None:
            self._start_spinner(msg)
        else:
            self._spinner_msg = msg


# Module-level singleton driven by the lifecycle hooks (via `@load_models`).
PROGRESS = Progress()


def progress_bar(func: Callable) -> Callable:
    """
    Execution decorator with three display levels (set via PROGRESS_BAR env var).

    PROGRESS_BAR=minimal
        ✓ 3 models done  5.2s

    PROGRESS_BAR=model  (default)
        ✓ customers       110ms 3/3 á  36ms
        ✓ orders          1.2s  9/9 á 130ms
        ✓ products        4.5s 25/25 á 180ms

    PROGRESS_BAR=execute
        ⠋ customers  █████████░░░░░░░░░░░ 45% 3/9 á 130ms ~1.2s left
        ✓ customers       1.2s  9/9 á 130ms
        ⠋ orders     ██░░░░░░░░░░░░░░░░░░ 12% 3/25 á 180ms ~4.0s left
        ✓ orders          4.5s 25/25 á 180ms
        ...

    Legacy single-execute path. New lifecycle-based pipelines get progress
    for free from the `@load_models` / `@model_lifecycle` / `@execute_lifecycle`
    hooks and do not stack this decorator.
    """
    p = Progress()
    p.enabled = True
    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        if not p.s.overall_start:
            p.s.overall_start = time.time()

        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        name, mode = _name_and_mode(bound.arguments.get("model"))
        if not name:
            name = func.__name__

        if name != p.s.current_model:
            p.begin_model(name, mode)

        with p.interval():
            return func(*args, **kwargs)

    wrapper.set_total = p.set_total  # type: ignore[attr-defined]
    wrapper.set_name_width = p.set_name_width  # type: ignore[attr-defined]
    wrapper.finish = p.finish  # type: ignore[attr-defined]
    atexit.register(p.finish)
    return wrapper

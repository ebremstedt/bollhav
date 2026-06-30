import io
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from bollhav.model.intervals import TZInterval
from bollhav.model.progress_bar import (
    Progress,
    ProgressLevel,
    progress_bar,
    _format_duration,
    _format_progress,
    _format_span,
    _is_whole_chunk,
    _name_and_mode,
)

UTC = ZoneInfo("UTC")


def _iv(*, since: tuple, until: tuple) -> TZInterval:
    return TZInterval(datetime(*since, tzinfo=UTC), datetime(*until, tzinfo=UTC))


def make_batched_model(name: str = "lakehouse.s.FactCase", chunk: str = "@daily"):
    """Minimal stand-in carrying just what `_name_and_mode` reads:
    `target.full_name` and `batching.time.chunk`."""
    return SimpleNamespace(
        target=SimpleNamespace(full_name=name),
        batching=SimpleNamespace(time=SimpleNamespace(chunk=chunk)),
    )


def make_mock_model(name: str):
    class Target:
        full_name = name

    class Model:
        target = Target()

    return Model()


def finish_captured(execute) -> str:
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        execute.finish()
    finally:
        sys.stdout = old
    return buf.getvalue()


def test_single_model_correct_count():
    @progress_bar
    def execute(model):
        pass

    execute.set_total(3)
    model = make_mock_model("orders")
    execute(model=model)
    execute(model=model)
    execute(model=model)

    output = finish_captured(execute)
    assert "orders" in output
    assert "3/3" in output


def test_total_does_not_bleed_across_models():
    """
    Regression: set_total for the *next* model must not corrupt the
    finished-line of the *previous* model.

    Sequence that triggered the bug:
      1. model_a runs N batches  (set_total=N)
      2. set_total(M) is called for model_b   ← used to overwrite state["total"]
      3. first batch of model_b fires          ← _finish_current printed N/M (wrong)
    """

    @progress_bar
    def execute(model):
        pass

    model_a = make_mock_model("step_a")
    model_b = make_mock_model("step_b")

    execute.set_total(5)
    for _ in range(5):
        execute(model=model_a)

    # Simulate the problematic ordering: set_total for b BEFORE first b batch
    execute.set_total(3)

    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    for _ in range(3):
        execute(model=model_b)
    execute.finish()
    sys.stdout = old

    output = buf.getvalue()

    # step_a must finish with 5/5, NOT 5/3
    lines = [line for line in output.splitlines() if "▸" in line and "step_a" in line]
    assert lines, "step_a finished line not found"
    assert "5/5" in lines[0], f"Expected 5/5 in step_a line, got: {lines[0]}"

    # step_b must finish with 3/3, NOT 3/<some other total>
    lines_b = [line for line in output.splitlines() if "▸" in line and "step_b" in line]
    assert lines_b, "step_b finished line not found"
    assert "3/3" in lines_b[0], f"Expected 3/3 in step_b line, got: {lines_b[0]}"


def test_finished_elapsed_uses_ms_for_sub_second():
    @progress_bar
    def execute(model):
        pass

    execute.set_total(1)
    execute(model=make_mock_model("fast"))

    output = finish_captured(execute)
    lines = [line for line in output.splitlines() if "fast" in line]
    assert lines, "fast finished line not found"
    # elapsed should be sub-second in a test, so must show ms not 0.0s
    assert "0.0s" not in lines[0], f"Got 0.0s instead of ms: {lines[0]}"
    assert "ms" in lines[0], f"Expected ms unit in: {lines[0]}"


def test_model_without_set_total_shows_count_only():
    @progress_bar
    def execute(model):
        pass

    model_a = make_mock_model("no_total")
    for _ in range(4):
        execute(model=model_a)

    output = finish_captured(execute)
    lines = [line for line in output.splitlines() if "no_total" in line]
    assert lines, "no_total finished line not found"
    # total was never set, so it should just show "4 batches" (no slash)
    assert "4/0" not in lines[0]
    assert "4/" not in lines[0]


def test_format_duration_fixed_width():
    values = [0.001, 0.01, 0.1, 0.5, 1.0, 9.9, 59.9, 60, 120, 599, 3600, 36000]
    lengths = [len(_format_duration(v)) for v in values]
    assert all(length == lengths[0] for length in lengths), (
        f"Widths vary: {list(zip(values, [_format_duration(v) for v in values]))}"
    )


def test_format_progress_fixed_width():
    total = 100
    lengths = [len(_format_progress(i, total)) for i in range(1, total + 1)]
    assert all(length == lengths[0] for length in lengths), (
        f"Widths vary: {list(zip(range(1, total + 1), lengths))}"
    )


def test_batch_level_shows_finish_lines(monkeypatch):
    monkeypatch.setenv("PROGRESS_BAR", "batch")
    from bollhav.model.progress_bar import progress_bar as pb

    @pb
    def execute(model):
        pass

    model_a = make_mock_model("alpha")
    execute.set_total(2)
    execute(model=model_a)
    execute(model=model_a)

    output = finish_captured(execute)
    assert "▸" in output
    assert "alpha" in output
    assert "2/2" in output


def test_minimal_level_shows_summary(monkeypatch):
    monkeypatch.setenv("PROGRESS_BAR", "minimal")
    from bollhav.model.progress_bar import progress_bar as pb

    @pb
    def execute(model):
        pass

    model_a = make_mock_model("first")
    model_b = make_mock_model("second")

    execute.set_total(2)
    execute(model=model_a)
    execute(model=model_a)
    execute.set_total(1)
    execute(model=model_b)

    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        execute.finish()
    finally:
        sys.stdout = old
    output = buf.getvalue()

    assert "2 models done" in output
    assert "✓" in output  # final all-done summary uses ✓ regardless of level


# ── sub-chunk window labelling ──────────────────────────────────────────


def test_is_whole_chunk_detects_grid_alignment():
    day = _iv(since=(2026, 1, 14), until=(2026, 1, 15))
    hour = _iv(since=(2026, 1, 14, 12), until=(2026, 1, 14, 13))
    # a full daily slice spans exactly one @daily tick; the hour slice doesn't
    assert _is_whole_chunk(day, "@daily") is True
    assert _is_whole_chunk(hour, "@daily") is False
    # but that same hour IS a whole chunk under an @hourly grain
    assert _is_whole_chunk(hour, "@hourly") is True


def test_format_span_compact_units():
    assert _format_span(timedelta(hours=1)) == "1h"
    assert _format_span(timedelta(hours=2, minutes=30)) == "2h30m"
    assert _format_span(timedelta(minutes=45)) == "45m"
    assert _format_span(timedelta(days=1, hours=2)) == "1d2h"
    assert _format_span(timedelta(seconds=30)) == "30s"
    assert _format_span(timedelta(0)) == "0s"


def test_label_single_sub_chunk_slice_shows_span():
    """A 1-hour backfill on a @daily model labels by its real span, not 'daily'
    — the bug this change fixes."""
    m = make_batched_model(chunk="@daily")
    hour = _iv(since=(2026, 1, 14, 12), until=(2026, 1, 14, 13))
    assert _name_and_mode(m, [hour]) == (m.target.full_name, "1h")


def test_label_whole_chunk_slice_keeps_chunk_alias():
    m = make_batched_model(chunk="@daily")
    day = _iv(since=(2026, 1, 14), until=(2026, 1, 15))
    assert _name_and_mode(m, [day]) == (m.target.full_name, "daily")


def test_label_multiple_intervals_keep_chunk_alias():
    """Span is shown only for a SINGLE slice; a multi-slice run stays on the
    chunk alias even if no slice is a whole day."""
    m = make_batched_model(chunk="@daily")
    half1 = _iv(since=(2026, 1, 14, 12), until=(2026, 1, 15))
    half2 = _iv(since=(2026, 1, 15), until=(2026, 1, 15, 12))
    assert _name_and_mode(m, [half1, half2]) == (m.target.full_name, "daily")


def test_label_no_intervals_keeps_chunk_alias():
    m = make_batched_model(chunk="@daily")
    assert _name_and_mode(m) == (m.target.full_name, "daily")


def test_label_timeless_none_interval_keeps_chunk_alias():
    m = make_batched_model(chunk="@daily")
    assert _name_and_mode(m, [None]) == (m.target.full_name, "daily")


def test_label_unbatched_model_has_empty_mode():
    m = SimpleNamespace(target=SimpleNamespace(full_name="dim_thing"), batching=None)
    assert _name_and_mode(m, [None]) == ("dim_thing", "")


def test_avg_batch_marker_uses_at_sign_not_a_acute():
    """The average-batch-time marker renders as '@', never 'á'."""

    @progress_bar
    def execute(model):
        pass

    execute.set_total(2)
    model = make_mock_model("orders")
    execute(model=model)
    execute(model=model)

    output = finish_captured(execute)
    assert " @ " in output, f"expected '@' avg marker in: {output!r}"
    assert "á" not in output, f"stale 'á' marker present in: {output!r}"


def test_begin_model_for_threads_span_into_finished_line():
    """End-to-end through the renderer: begin_model_for given a single
    sub-chunk slice prints '(1h)', never '(daily)'."""
    p = Progress(level=ProgressLevel.MODEL)
    p.init([])
    m = make_batched_model(name="lakehouse.s.FactCase", chunk="@daily")
    hour = _iv(since=(2026, 1, 14, 12), until=(2026, 1, 14, 13))

    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        p.begin_model_for(m, total=1, intervals=[hour])
        p.tick(0.001)
        p.finish_model()
    finally:
        sys.stdout = old

    output = buf.getvalue()
    assert "FactCase (1h)" in output
    assert "(daily)" not in output

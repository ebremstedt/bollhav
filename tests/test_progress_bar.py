import io
import sys

from bollhav.model.progress_bar import progress_bar, _format_duration, _format_progress


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
    lines = [line for line in output.splitlines() if "✓" in line and "step_a" in line]
    assert lines, "step_a finished line not found"
    assert "5/5" in lines[0], f"Expected 5/5 in step_a line, got: {lines[0]}"

    # step_b must finish with 3/3, NOT 3/<some other total>
    lines_b = [line for line in output.splitlines() if "✓" in line and "step_b" in line]
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
    assert all(l == lengths[0] for l in lengths), (
        f"Widths vary: {list(zip(values, [_format_duration(v) for v in values]))}"
    )


def test_format_progress_fixed_width():
    total = 100
    lengths = [len(_format_progress(i, total)) for i in range(1, total + 1)]
    assert all(l == lengths[0] for l in lengths), (
        f"Widths vary: {list(zip(range(1, total + 1), lengths))}"
    )

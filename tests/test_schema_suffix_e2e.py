"""End-to-end tests for SCHEMA_SUFFIX flowing through the progress bar.

Runs the company_xyz_pipeline example's models through the same call
sequence main.py uses (match_models -> set_name_width -> apply_pipe ->
execute) and asserts what the progress bar actually prints.
"""

import io
import os
import re
import sys
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

import pytest

from bollhav.model import Model, match_models, name_width_for, progress_bar
from bollhav.model.ordering import UpstreamMode
from bollhav.pipe.pipe_config import (
    PipeConfig,
    LatestConfig,
    BackfillConfig,
)


EXAMPLE_ROOT = Path(__file__).parent.parent / "examples" / "company_xyz_pipeline"


@progress_bar
def _run(model: Model, since: datetime, until: datetime) -> None:
    pass


def _pipe(schema_suffix: str = "pr123") -> PipeConfig:
    return PipeConfig(
        tags="[customers]",
        latest=LatestConfig(enabled=False),
        backfill=BackfillConfig(
            enabled=True,
            since=datetime(2024, 1, 1, tzinfo=timezone.utc),
            until=datetime(2024, 1, 3, tzinfo=timezone.utc),
        ),
        schema_suffix=schema_suffix,
        use_schema_suffix=True,
    )


@pytest.fixture
def matched_models():
    cwd = os.getcwd()
    os.chdir(EXAMPLE_ROOT)
    sys.path.insert(0, str(EXAMPLE_ROOT))
    try:
        yield match_models(
            folder="src/models",
            tags="[customers]",
            upstream_mode=UpstreamMode.IGNORE_COMPLETELY,
        )
    finally:
        os.chdir(cwd)
        sys.path.remove(str(EXAMPLE_ROOT))


def _drive_pipeline(models, pipe: PipeConfig) -> str:
    """Mirror main.py: apply_pipe to every model first so full_name
    reflects the suffix, then measure name width, then execute."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        for model in models:
            model.apply_pipe(pipe)

        _run.set_name_width(name_width_for(models))

        for model in models:
            intervals = model.infer_intervals()
            _run.set_total(len(intervals))
            for interval in intervals:
                since, until = (
                    (interval.since, interval.until) if interval else (None, None)
                )
                _run(model=model, since=since, until=until)

        _run.finish()
    return buf.getvalue()


def test_progress_bar_shows_suffixed_schema(matched_models):
    """The progress bar must display the full suffixed schema name."""
    output = _drive_pipeline(matched_models, _pipe(schema_suffix="pr123"))

    assert "warehouse_clean_pr123" in output, (
        f"Suffixed schema missing from progress bar output:\n{output}"
    )
    assert "warehouse_views_pr123" in output, (
        f"Suffixed schema missing from progress bar output:\n{output}"
    )


def test_progress_bar_does_not_show_unsuffixed_schema(matched_models):
    """When a suffix is set, the bare unsuffixed schema must NOT appear —
    otherwise a test run looks like it hit production tables."""
    output = _drive_pipeline(matched_models, _pipe(schema_suffix="pr123"))

    for line in output.splitlines():
        if "▸" not in line:
            continue
        # A row like "warehouse_clean_pr123_2616_.customer_master_data (daily)"
        # should never contain the bare "warehouse_clean." form.
        assert "warehouse_clean." not in line, (
            f"Progress bar row shows unsuffixed schema:\n{line}"
        )
        assert "warehouse_views." not in line, (
            f"Progress bar row shows unsuffixed schema:\n{line}"
        )


def test_progress_bar_rows_align_with_suffix(matched_models):
    """Every row's elapsed-time column must start at the same position —
    otherwise the suffix has pushed names past the configured name_width
    and each row is picking its own column break."""
    output = _drive_pipeline(matched_models, _pipe(schema_suffix="pr123"))

    rows = [line for line in output.splitlines() if line.lstrip().startswith("▸")]
    assert len(rows) >= 2, f"Need 2+ rows to detect misalignment, got:\n{output}"

    elapsed_re = re.compile(r"\d+(?:\.\d+)?(?:ms|s|m|h)\b")

    def elapsed_col(row: str) -> int:
        body = row.split("▸", 1)[1]
        m = elapsed_re.search(body)
        assert m, f"Row has no elapsed time: {row!r}"
        return m.start()

    cols = [elapsed_col(r) for r in rows]
    assert len(set(cols)) == 1, (
        f"Progress bar columns are misaligned after suffix applied — "
        f"elapsed-column positions vary: {cols}\n{output}"
    )

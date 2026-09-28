"""`bollhav.iceberg` must import without the optional `iceberg` extra.

The model-config side (IcebergColumn / IcebergType) is used by apps that never
install pyiceberg, and CI installs only `.[dev,tui]`. The catalog / table side
is resolved lazily on first attribute access, so *that* is what pulls
pyiceberg in. Checked in a subprocess so the sys.modules tampering can't leak
into other tests.
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

SCRIPT = """
import sys

# Make the optional extra look uninstalled, whatever this venv has.
sys.modules["pyiceberg"] = None
sys.modules["pyarrow"] = None

from bollhav.iceberg import IcebergColumn, IcebergType

assert IcebergColumn(name="id").name == "id"
assert IcebergType.LONG.value == "long"

import bollhav.iceberg as ice

try:
    ice.write  # lazy: this is the access that needs pyiceberg
except ImportError:
    pass
else:
    raise SystemExit("expected ImportError from the lazy attribute")

try:
    ice.not_a_real_name
except AttributeError:
    pass
else:
    raise SystemExit("expected AttributeError for an unknown name")
"""


def test_iceberg_package_imports_without_pyiceberg():
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

# Runner image — Python + Microsoft ODBC Driver 18 + bollhav + pyodbc.
# Runs the staging example inside Docker so there's no host ODBC/driver
# setup; it reaches the `mssql` service over the compose network.
#
# Build context is the REPO ROOT (see docker-compose.yml) so we can
# install bollhav from source. Only pyproject.toml, README.md and the
# bollhav package are copied for the install — keeps setuptools'
# auto-discovery from tripping over examples/ and tests/.
FROM python:3.12-slim-bookworm

# Microsoft ODBC Driver 18. The dearmored key must land at exactly the
# path Microsoft's prod.list references (signed-by=/usr/share/keyrings/...).
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gnupg ca-certificates apt-transport-https \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
 && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list -o /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc unixodbc-dev gcc g++ \
 && rm -rf /var/lib/apt/lists/*

# Install bollhav from a minimal copy of the repo. `polars[rtcompat]` is
# needed because SQL Server forces this image to amd64, which on Apple
# Silicon runs under Rosetta — the emulated CPU lacks AVX/AVX2 and stock
# polars would crash.
COPY pyproject.toml README.md /src/
COPY bollhav /src/bollhav
RUN pip install --no-cache-dir /src pyodbc \
 && pip install --no-cache-dir 'polars[rtcompat]'

# The example itself.
COPY examples/mssql/staging /app
WORKDIR /app

CMD ["python", "main.py"]

"""Shared ingest libraries — and the one place the repo's .env is applied.

Every credential and connection string these scripts need lives in the repo's
`.env`, and nothing used to read it. Each module resolved its own values
straight from `os.environ` and fell back to a default, so a shell that had not
manually exported the file got:

  * `PDI_DATABASE_URL` -> port 5432, which on a developer machine is usually a
    different project's Postgres. A load there does not fail; it succeeds
    against the wrong database.
  * `CENSUS_API_KEY` -> empty, and the Census API answers a keyless request with
    a 302 to an HTML page. urllib follows it, so the symptom is a JSON parse
    error nowhere near the real cause.

Loading happens at package import rather than inside `db.py` because
`census.py` reads its key at module scope and never imports `db`. Anything that
does `from lib import ...` gets the environment; that is the enforced home.
"""
import os
import pathlib

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def load_dotenv(path: pathlib.Path | None = None) -> None:
    """Apply KEY=VALUE lines from .env, without overriding the real environment.

    Deliberately not python-dotenv: this is the only thing the dependency would
    buy, and ingest/requirements.txt is one line today.
    """
    path = path or _REPO_ROOT / ".env"
    try:
        text = path.read_text()
    except OSError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("'\"")
        # An empty value in .env is not a value — leave it absent so callers hit
        # their own "not set" error rather than a confusing empty-credential one.
        if value and key not in os.environ:
            os.environ[key] = value


load_dotenv()

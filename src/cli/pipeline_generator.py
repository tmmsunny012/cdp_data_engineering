"""Click-based CLI for scaffolding new ETL/ELT connectors in the CDP platform.

Provides three commands:

* ``generate-connector`` -- renders Jinja2 templates to create a new connector
  module under ``src/ingestion/`` and a matching unit-test scaffold under
  ``tests/``.
* ``list-connectors`` -- enumerates existing connectors discovered in
  ``src/ingestion/``.
* ``validate-schemas`` -- runs lightweight JSON-schema validation on all
  registered event schemas.
"""

from __future__ import annotations

import json
import pathlib
from typing import Final

import click
from jinja2 import BaseLoader, Environment

# --------------------------------------------------------------------------- #
# Project paths                                                                #
# --------------------------------------------------------------------------- #
_PROJECT_ROOT: Final[pathlib.Path] = pathlib.Path(__file__).resolve().parents[2]
_INGESTION_DIR: Final[pathlib.Path] = _PROJECT_ROOT / "src" / "ingestion"
_TESTS_DIR: Final[pathlib.Path] = _PROJECT_ROOT / "tests"
_SCHEMAS_DIR: Final[pathlib.Path] = _PROJECT_ROOT / "schemas"

# --------------------------------------------------------------------------- #
# Jinja2 inline templates                                                      #
# --------------------------------------------------------------------------- #
_CONNECTOR_TEMPLATE = """\
\"\"\"{{ name | title | replace('_', '') }} connector — auto-generated scaffold.\"\"\"
from __future__ import annotations

import structlog
from src.common.metrics import events_ingested_total

logger = structlog.get_logger(__name__)


class {{ name | title | replace('_', '') }}Connector:
    \"\"\"{{ connector_type | title }} connector that reads {{ format }} data.\"\"\"

    def __init__(self, config: dict) -> None:
        self.config = config
        self.source_name = "{{ name }}"

    def connect(self) -> None:
        logger.info("connector_open", source=self.source_name)

    def read(self) -> list[dict]:
        \"\"\"Read a batch of records from the upstream source.\"\"\"
        raise NotImplementedError("Implement read() for {{ name }}")

    def close(self) -> None:
        logger.info("connector_close", source=self.source_name)
"""

_TEST_TEMPLATE = """\
\"\"\"Unit tests for {{ name }} connector.\"\"\"
from __future__ import annotations

import pytest
from src.ingestion.{{ name }}_connector import {{ name | title | replace('_', '') }}Connector


@pytest.fixture()
def connector() -> {{ name | title | replace('_', '') }}Connector:
    return {{ name | title | replace('_', '') }}Connector(config={})


class TestConnect:
    def test_connect_does_not_raise(self, connector: {{ name | title | replace('_', '') }}Connector) -> None:
        connector.connect()


class TestRead:
    def test_read_raises_not_implemented(self, connector: {{ name | title | replace('_', '') }}Connector) -> None:
        with pytest.raises(NotImplementedError):
            connector.read()
"""

_jinja_env = Environment(loader=BaseLoader(), keep_trailing_newline=True)

# --------------------------------------------------------------------------- #
# CLI group                                                                    #
# --------------------------------------------------------------------------- #


@click.group()
def cli() -> None:
    """CDP Pipeline Generator -- scaffold connectors and validate schemas."""


@cli.command("generate-connector")
@click.option("--name", required=True, help="Snake-case connector name (e.g. whatsapp).")
@click.option(
    "--type",
    "connector_type",
    type=click.Choice(["streaming", "batch"], case_sensitive=False),
    default="batch",
    show_default=True,
    help="Connector execution model.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["json", "csv", "text"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Primary data format the connector consumes.",
)
def generate_connector(name: str, connector_type: str, fmt: str) -> None:
    """Generate a new ingestion connector and its test scaffold."""
    _INGESTION_DIR.mkdir(parents=True, exist_ok=True)
    _TESTS_DIR.mkdir(parents=True, exist_ok=True)

    context = {"name": name, "connector_type": connector_type, "format": fmt}

    connector_path = _INGESTION_DIR / f"{name}_connector.py"
    rendered_connector = _jinja_env.from_string(_CONNECTOR_TEMPLATE).render(context)
    connector_path.write_text(rendered_connector, encoding="utf-8")

    test_path = _TESTS_DIR / f"test_{name}_connector.py"
    rendered_test = _jinja_env.from_string(_TEST_TEMPLATE).render(context)
    test_path.write_text(rendered_test, encoding="utf-8")

    click.secho("Files created:", fg="green", bold=True)
    click.echo(f"  connector : {connector_path.relative_to(_PROJECT_ROOT)}")
    click.echo(f"  test      : {test_path.relative_to(_PROJECT_ROOT)}")


@cli.command("list-connectors")
def list_connectors() -> None:
    """List all existing connectors discovered in src/ingestion/."""
    if not _INGESTION_DIR.exists():
        click.secho("No ingestion directory found.", fg="yellow")
        return

    connectors = sorted(_INGESTION_DIR.glob("*_connector.py"))
    if not connectors:
        click.echo("No connectors found.")
        return

    click.secho(f"{'Connector':<30} {'Path'}", fg="cyan", bold=True)
    click.echo("-" * 60)
    for path in connectors:
        label = path.stem.replace("_connector", "")
        click.echo(f"  {label:<28} {path.relative_to(_PROJECT_ROOT)}")


@cli.command("validate-schemas")
def validate_schemas() -> None:
    """Run JSON-schema validation on all registered event schemas."""
    if not _SCHEMAS_DIR.exists():
        click.secho("No schemas directory found.", fg="yellow")
        return

    schema_files = sorted(_SCHEMAS_DIR.glob("*.json"))
    if not schema_files:
        click.echo("No schema files found.")
        return

    errors: list[str] = []
    for schema_file in schema_files:
        try:
            data = json.loads(schema_file.read_text(encoding="utf-8"))
            if "type" not in data and "$ref" not in data:
                errors.append(f"{schema_file.name}: missing top-level 'type' or '$ref'")
        except json.JSONDecodeError as exc:
            errors.append(f"{schema_file.name}: invalid JSON — {exc}")

    if errors:
        click.secho(f"Validation failed ({len(errors)} error(s)):", fg="red", bold=True)
        for err in errors:
            click.echo(f"  - {err}")
        raise SystemExit(1)

    click.secho(f"All {len(schema_files)} schema(s) valid.", fg="green", bold=True)


if __name__ == "__main__":
    cli()

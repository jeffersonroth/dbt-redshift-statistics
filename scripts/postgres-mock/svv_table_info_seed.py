from __future__ import annotations

import click
import psycopg2
import os
from datetime import datetime, timedelta
import random
import json
import sys
import math
from dataclasses import dataclass
from jinja2 import Environment, FileSystemLoader
import zlib

assert (sys.maxsize & (sys.maxsize + 1)) == 0

TARGET_SCHEMA = "mock_redshift"
TARGET_TABLE = "mock_svv_table_info"
TARGET_COLUMNS = {
    "database": "VARCHAR(255)",
    "schema": "VARCHAR(255)",
    "table_id": "BIGINT",
    "table": "VARCHAR(255)",
    "encoded": "VARCHAR(15)",
    "diststyle": "VARCHAR(50)",
    "sortkey1": "VARCHAR(255)",
    "max_varchar": "INT",
    "sortkey1_enc": "VARCHAR(32)",
    "sortkey_num": "INT",
    "size": "BIGINT",
    "pct_used": "DECIMAL(10, 4)",
    "empty": "BIGINT",
    "unsorted": "DECIMAL(5, 2)",
    "stats_off": "DECIMAL(5, 2)",
    "tbl_rows": "BIGINT",
    "skew_sortkey1": "DECIMAL(19, 2)",
    "skew_rows": "DECIMAL(19, 2)",
    "estimated_visible_rows": "BIGINT",
    "risk_event": "TEXT",
    "vacuum_sort_benefit": "DECIMAL(12, 2)",
    "create_time": "TIMESTAMP WITHOUT TIME ZONE",
    "mock_id": "BIGINT",
    "mock_created_at": "TIMESTAMP WITHOUT TIME ZONE",
}

with open("/app/scripts/source_tables.json", "r") as file:
    _source_tables = json.load(file)
    SOURCE_TABLES = [
        (table["database"], table["schema"], table["table"])
        for table in _source_tables
        if table["database"] and table["schema"] and table["table"]
    ]


@dataclass
class TableInfo:

    database: str
    schema: str
    table_id: int
    table: str
    encoded: str | None
    diststyle: str | None
    sortkey1: str | None
    max_varchar: int
    sortkey1_enc: str | None
    sortkey_num: int
    size: int
    pct_used: float
    empty: int
    unsorted: float | None
    stats_off: float
    tbl_rows: int
    skew_sortkey1: float | None
    skew_rows: float | None
    estimated_visible_rows: int
    risk_event: str | None
    vacuum_sort_benefit: float
    create_time: datetime
    mock_id: int
    mock_created_at: datetime

    @classmethod
    def from_mock_info(
        cls,
        mock_created_at: datetime,
        database_size: int,
        database_name: str,
        schema_name: str,
        table_name: str,
        table_hash: int,
    ) -> TableInfo:
        """Format table mock data."""
        mock_id = int(mock_created_at.timestamp())

        create_time = datetime.now() - timedelta(seconds=random.randint(1, 60))
        create_time_epoch = int(create_time.timestamp())

        table_id = (
            zlib.crc32((str(table_hash) + str(create_time_epoch)).encode()) & 0xFFFFFFFF
        )

        if encoded := random.choice([None, "Y", "N"]):
            encoded = ", ".join(
                filter(None, [encoded, random.choice([None, "AUTO(ENCODE)"])])
            )
        diststyle = random.choice(
            [
                "EVEN",
                "KEY(column)",
                "ALL",
                "AUTO(ALL)",
                "AUTO(EVEN)",
                "AUTO(KEY(column))",
            ]
        )
        sortkey1 = random.choice(
            [None, "column", "AUTO(SORTKEY)", "AUTO(SORTKEY(column))"]
        )
        max_varchar = random.randint(2**4, 2**8)
        sortkey1_enc = random.choice([None, "none", "az64", "lzo"])
        sortkey_num = 0 if not sortkey1 else random.randint(1, 10)
        size = random.randint(2**0, 2**12)
        pct_used = round(size / database_size, 6)
        empty = 0
        unsorted = None if not sortkey1 else round(random.uniform(0, 100), 2)
        stats_off = round(random.uniform(0, 100), 2)
        tbl_rows = size * random.randint(2**9, 2**11)
        skew_sortkey1 = None if not sortkey1 else round(random.uniform(0, 10), 2)
        skew_rows = random.choice([None, round(random.uniform(0, 10), 2)])
        estimated_visible_rows = math.floor(tbl_rows * random.uniform(0.75, 1))
        risk_event = random.choice(
            [
                None,
                f"risk_type|{random.randint(1,1000)}|{random_timestamp(create_time, mock_created_at).strftime('%Y-%m-%d %H:%M:%S')}",
            ]
        )
        vacuum_sort_benefit = round(random.uniform(0, 100), 2)

        return TableInfo(
            database=database_name,
            schema=schema_name,
            table_id=table_id,
            table=table_name,
            encoded=encoded,
            diststyle=diststyle,
            sortkey1=sortkey1,
            max_varchar=max_varchar,
            sortkey1_enc=sortkey1_enc,
            sortkey_num=sortkey_num,
            size=size,
            pct_used=pct_used,
            empty=empty,
            unsorted=unsorted,
            stats_off=stats_off,
            tbl_rows=tbl_rows,
            skew_sortkey1=skew_sortkey1,
            skew_rows=skew_rows,
            estimated_visible_rows=estimated_visible_rows,
            risk_event=risk_event,
            vacuum_sort_benefit=vacuum_sort_benefit,
            create_time=create_time,
            mock_id=mock_id,
            mock_created_at=mock_created_at,
        )


def random_timestamp(start, end):
    """Generate a random timestamp between start and end."""
    start_timestamp = datetime.timestamp(start)
    end_timestamp = datetime.timestamp(end)
    random_timestamp = (
        start_timestamp + (end_timestamp - start_timestamp) * random.random()
    )
    return datetime.fromtimestamp(random_timestamp)


@click.command()
@click.option(
    "--ddl-filename",
    type=str,
    default="svv_table_info_ddl.jinja",
    help="Target table ddl jinja file.",
)
@click.option(
    "--source-tables",
    type=(str, str, str),
    multiple=True,
    default=SOURCE_TABLES,
    help="Source tables info: (database, schema, table).",
)
@click.option(
    "--target-schema",
    type=str,
    default=TARGET_SCHEMA,
    help="Schema where Redshift Mocks will be loaded to.",
)
@click.option(
    "--target-table",
    type=str,
    default=TARGET_TABLE,
    help="Table where Redshift Mocks will be loaded to.",
)
@click.option(
    "--target-columns",
    type=(str, str),
    multiple=True,
    default=TARGET_COLUMNS.items(),
    help="Columns of the target table.",
)
@click.option(
    "--recreate",
    is_flag=True,
    default=False,
    help="Drop and recreate the target table.",
)
def seed_svv_table_info(
    ddl_filename: str,
    source_tables: str,
    target_schema: str,
    target_table: str,
    target_columns: list(tuple(str, str)),
    recreate: bool,
) -> None:
    """Insert mock data into the database."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([dbname, user, password]):
        click.echo(
            "Database name, user, and password must be set as environment variables."
        )
        return

    # Database connection
    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        cur = conn.cursor()
        click.echo("Database connection successful.")
    except psycopg2.Error as e:
        click.echo(f"Database connection failed: {e}")
        return

    # Render ddl statements
    env = Environment(loader=FileSystemLoader("/app/scripts"))
    template = env.get_template(ddl_filename)

    # Render the template with the specified schema, table, and columns
    click.echo(f"Creating {target_schema}.{target_table}...")
    ddl_statements = template.render(
        recreate=recreate,
        schema_name=target_schema,
        table_name=target_table,
        columns=dict(target_columns),
    )

    # Execute the ddl statements to create the target table and insert the mock data
    with psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    ) as conn:
        with conn.cursor() as cur:
            click.echo("Executing DDL statements...")
            cur.execute(ddl_statements)

            mock_created_at = datetime.now()
            database_size = random.randint(2**12, 2**16)

            for source_table in source_tables:
                database_name, schema_name, table_name = source_table

                table_hash = hash(frozenset(source_table)) & sys.maxsize
                table_info = TableInfo.from_mock_info(
                    mock_created_at=mock_created_at,
                    database_size=database_size,
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    table_hash=table_hash,
                )

                insert_sql = f"""
                INSERT INTO {target_schema}.{target_table} ("database", "schema", "table_id", "table", "encoded", "diststyle", "sortkey1", "max_varchar", "sortkey1_enc", "sortkey_num", "size", "pct_used", "empty", "unsorted", "stats_off", "tbl_rows", "skew_sortkey1", "skew_rows", "estimated_visible_rows", "risk_event", "vacuum_sort_benefit", "create_time", "mock_id", "mock_created_at")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                table_info_values = (
                    table_info.database,
                    table_info.schema,
                    table_info.table_id,
                    table_info.table,
                    table_info.encoded,
                    table_info.diststyle,
                    table_info.sortkey1,
                    table_info.max_varchar,
                    table_info.sortkey1_enc,
                    table_info.sortkey_num,
                    table_info.size,
                    table_info.pct_used,
                    table_info.empty,
                    table_info.unsorted,
                    table_info.stats_off,
                    table_info.tbl_rows,
                    table_info.skew_sortkey1,
                    table_info.skew_rows,
                    table_info.estimated_visible_rows,
                    table_info.risk_event,
                    table_info.vacuum_sort_benefit,
                    table_info.create_time,
                    table_info.mock_id,
                    table_info.mock_created_at,
                )

                click.echo(
                    f"Inserting mock data for {table_info.database}.{table_info.schema}.{table_info.table}..."
                )
                cur.execute(insert_sql, table_info_values)

            click.echo("Successfully inserted mock records.")


if __name__ == "__main__":
    seed_svv_table_info()

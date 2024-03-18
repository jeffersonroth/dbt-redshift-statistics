import os
import psycopg2
import click
import tempfile

SOURCE_SCHEMA = "monitoring"
SOURCE_TABLE = "table_info"
TARGET_SCHEMA = "mock_redshift"
TARGET_TABLE = "mock_svv_table_info"


@click.command()
@click.option(
    "--source-schema",
    default=SOURCE_SCHEMA,
    help="Schema where dbt creates the current svv_table_info data.",
)
@click.option(
    "--source-table",
    default=SOURCE_TABLE,
    help="Table where dbt creates the current svv_table_info data.",
)
@click.option(
    "--target-schema",
    default=TARGET_SCHEMA,
    help="Schema where Redshift Mocks are loaded to.",
)
@click.option(
    "--target-table",
    default=TARGET_TABLE,
    help="Table where Redshift Mocks are loaded to.",
)
def check_new_mocks(source_schema, source_table, target_schema, target_table):
    """Check for new mocks based on last_analyzed_at."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([dbname, user, password]):
        click.echo(
            "Database name, user, and password must be set as environment variables.",
            err=True,
        )
        return

    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        cur = conn.cursor()

        # Check if the source schema and table exist
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """,
            (source_schema, source_table),
        )
        exists = cur.fetchone()[0]

        if not exists:
            click.echo(
                f"{source_schema}.{source_table} does not exist. Returning all mock IDs.",
                err=True,
            )
            cur.execute(
                f"""
                SELECT mock_id
                FROM (
                    SELECT mock_id, MIN(mock_created_at) AS first_created_at
                    FROM {target_schema}.{target_table}
                    GROUP BY mock_id
                ) AS grouped_mocks
                ORDER BY first_created_at;
                """
            )
        else:
            # Get the most recent analysis date from the current table
            cur.execute(
                f"SELECT MAX(last_analyzed_at) FROM {source_schema}.{source_table};"
            )
            max_analyzed_at = cur.fetchone()[0]

            if max_analyzed_at is None:
                click.echo(
                    f"No data found in {source_schema}.{source_table}.", err=True
                )
                return

            click.echo(f"Max analyzed at: {max_analyzed_at}", err=True)

            # Get mock IDs that were created after the most recent analysis date
            cur.execute(
                f"""
                SELECT mock_id
                FROM (
                    SELECT mock_id, MIN(mock_created_at) AS first_created_at
                    FROM {target_schema}.{target_table}
                    WHERE mock_created_at > %s
                    GROUP BY mock_id
                ) AS grouped_mocks
                ORDER BY first_created_at;
                """,
                (max_analyzed_at,),
            )

        mock_ids = [mock_id[0] for mock_id in cur.fetchall()]

        if not mock_ids:
            click.echo("No new mocks found.", err=True)
            return

        # Write the mock IDs to a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            for mock_id in mock_ids:
                temp_file.write(f"{mock_id}\n".encode())

            # Output the path of the temporary file
            print(temp_file.name)

    except psycopg2.Error as e:
        click.echo(f"Database connection failed: {e}", err=True)


if __name__ == "__main__":
    check_new_mocks()

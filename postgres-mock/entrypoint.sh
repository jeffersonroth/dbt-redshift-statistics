#!/bin/bash
set -e

echo "Seeding mock Redshift statistics to Postgres..."
if ! python /app/scripts/svv_table_info_seed.py; then
    echo "Error seeding mock Redshift statistics to Postgres."
    exit 1
fi
echo "Done seeding mock Redshift statistics to Postgres."

cd /app/dbt_redshift_stats
echo "Running dbt deps..."
dbt debug

echo "Running dbt model..."
mock_ids_file=$(python /app/scripts/svv_table_info_select_mock_ids.py)
echo "Mock IDs file: $mock_ids_file"

# Check if the python script ran successfully and the file was created
if [ ! -f "$mock_ids_file" ]; then
    echo "Error retrieving mock IDs."
    exit 1
fi

while read mock_id; do
    echo "Running dbt model for mock id: $mock_id"
    DBT_ENV=dev dbt run --target test --models redshift_stats --vars "mock_id: $mock_id"
    if [ $? -ne 0 ]; then
        echo "Error running dbt model for mock id: $mock_id"
        echo "dbt command failed, exiting..."
        exit 1
    fi
done < "$mock_ids_file"

# Remove the temporary file
rm "$mock_ids_file"

echo "Done running dbt model."

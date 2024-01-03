This script runs a sequence of SQL commands, parameterized by one or two
variables, to populate a Postgres database, and then gets a query plan for one
final SQL query. The costs of the query plans are plotted against the free
variables, and regions with different query plan topologies are shown.

Install and use this script as follows:

```
# Create a new virtual environment.
python -m venv .venv
# Activate the virtual environment.
. .venv/bin/activate
# Install this package and its dependencies in editable mode.
pip install -e .
# Run the script.
python -m query_plan_charts samples/FILENAME.toml
```

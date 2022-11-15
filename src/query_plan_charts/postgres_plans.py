import json

import psycopg
from testcontainers.postgres import PostgresContainer  # type: ignore

from .base import Backend, QueryPlan


class Postgres(Backend):
    def __init__(self):
        # We need to provide extra shared memory as the Docker default of 64MB
        # may not be enough for some large queries.
        self.container = PostgresContainer(
            "postgres:15"
        ).with_kwargs(shm_size="1g")
        self.connection = None

    def __enter__(self):
        self.container.__enter__()
        connection_url = self.container.get_connection_url()
        connection_url = connection_url.replace(
            "postgresql+psycopg2:",
            "postgresql:",
        )
        self.connection = psycopg.connect(connection_url, autocommit=True)
        self.connection.autocommit = True

    def __exit__(self, exc_type, exc_val, traceback):
        self.connection.close()
        self.connection = None
        return self.container.__exit__(exc_type, exc_val, traceback)

    def execute_statement(self, statement: str, parameter_values: list[int]):
        with self.connection.cursor() as cursor:
            cursor.execute(statement, parameter_values)

    def prepare_indexes(self):
        with self.connection.cursor() as cursor:
            cursor.execute("VACUUM")
            cursor.execute("ANALYZE")

    def plan_query_text(self, query: str) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute(f"EXPLAIN {query};")
            return "\n".join(row[0] for row in cursor)

    def plan_query_structured(self, query: str) -> "PostgresPlan":
        with self.connection.cursor() as cursor:
            cursor.execute(f"EXPLAIN (FORMAT JSON) {query};")
            return json.loads("".join(row for row in cursor))


class PostgresPlan(QueryPlan):
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresPlan):
            return False
        raise NotImplementedError()

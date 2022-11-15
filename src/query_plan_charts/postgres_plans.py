from .base import Backend, QueryPlan


class Postgres(Backend):
    def __init__(self):
        pass

    def execute_statement(self, statement: str, parameter_values: list):
        raise NotImplementedError()

    def prepare_indexes(self):
        self.execute_statement("VACUUM")
        self.execute_statement("ANALYZE")

    def plan_query_text(self, query: str) -> str:
        f"EXPLAIN {query};"
        raise NotImplementedError()

    def plan_query_structured(self, query: str) -> "PostgresPlan":
        f"EXPLAIN (FORMAT JSON) {query};"
        raise NotImplementedError()


class PostgresPlan(QueryPlan):
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresPlan):
            return False
        raise NotImplementedError()

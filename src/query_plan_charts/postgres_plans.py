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

    def plan_query(self, query: str) -> "PostgresPlan":
        with self.connection.cursor() as cursor:
            cursor.execute(f"EXPLAIN {query};")
            text = "\n".join(row[0] for row in cursor)

            cursor.execute(f"EXPLAIN (FORMAT JSON) {query};")
            doc, = cursor.fetchone()
            if not isinstance(doc, list):
                raise Exception("Plan output was not a list")
            if len(doc) != 1:
                raise Exception(
                    "Outer list in plan output has {} elements"
                    .format(len(doc)))
            return PostgresPlan(doc[0]["Plan"], text)


PLAN_KEYS_SIMPLE_COMPARISONS = [
    "Node Type",
    "Alias",
    "Async Capable",
    "Command",
    "CTE Name",
    "Filter",
    "Group Key",
    "Hash Cond",
    "Index Cond",
    "Index Name",
    "Inner Unique",
    "Join Filter",
    "Join Type",
    "Merge Cond",
    "One-Time Filter",
    "Operation",
    "Parallel Aware",
    "Parent Relationship",
    "Partial Mode",
    "Presorted Key",
    "Recheck Cond",
    "Relation Name",
    "Scan Direction",
    "Schema",
    "Single Copy",
    "Sort Key",
    "Strategy",
    "Subplan Name",
    "Workers Planned",
]
PLAN_KEYS_EXPECTED = set(PLAN_KEYS_SIMPLE_COMPARISONS).union({
    "Plans",
    # We ignore the following keys when comparing.
    "Startup Cost",
    "Total Cost",
    "Plan Rows",
    "Plan Width",
})


def plan_eq(left, right) -> bool:
    for key in PLAN_KEYS_SIMPLE_COMPARISONS:
        if left.get(key) != right.get(key):
            return False

    for plan_dict in (left, right):
        for key in plan_dict.keys():
            if key not in PLAN_KEYS_EXPECTED:
                raise Exception(
                    "Unexpected key in plan: {} (with value {})"
                    .format(key, plan_dict[key]))

    left_subplans = left.get("Plans")
    right_subplans = right.get("Plans")
    if (left_subplans is None) != (right_subplans is None):
        return False
    if left_subplans is not None and right_subplans is not None:
        if len(left_subplans) != len(right_subplans):
            return False
        for (left_subplan, right_subplan) in zip(left_subplans,
                                                 right_subplans):
            if not plan_eq(left_subplan, right_subplan):
                return False
    return True


def plan_summary_gen(node):
    yield node["Node Type"]
    if "Plans" in node:
        yield "( "
        first = True
        for child in node["Plans"]:
            if first:
                first = False
            else:
                yield ", "
            yield from plan_summary_gen(child)
        yield " )"


class PostgresPlan(QueryPlan):
    def __init__(self, plan, text_plan):
        self.plan = plan
        self.text_plan = text_plan

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresPlan):
            return False
        return plan_eq(self.plan, other.plan)

    def summary(self) -> str:
        return "".join(plan_summary_gen(self.plan))

    def text(self) -> str:
        return self.text_plan

    def cost(self) -> float:
        return self.plan["Total Cost"]

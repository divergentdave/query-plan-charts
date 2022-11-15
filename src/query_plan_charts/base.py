class Backend:
    def execute_statement(self, statement: str, parameter_values: list[int]):
        raise NotImplementedError()

    def prepare_indexes(self):
        raise NotImplementedError()

    def plan_query_text(self, query: str) -> str:
        raise NotImplementedError()

    def plan_query_structured(self, query: str) -> "QueryPlan":
        raise NotImplementedError()


class QueryPlan:
    """
    Base class for query plans from each database backend. Subclasses must
    override __eq__ in a way that splits plans into equivalence classes based
    on their structure. It should ignore row counts, costs, time durations,
    and other values that will almost always vary between plans.
    """

    def summary(self) -> str:
        raise NotImplementedError()


class ParameterizedStatement:
    def __init__(self, statement: str, parameter_count: int):
        self.statement = statement
        self.parameter_count = parameter_count


class ParameterConfig:
    def __init__(self, start: int, stop: int, steps: int):
        self.start = start
        self.stop = stop
        self.steps = steps

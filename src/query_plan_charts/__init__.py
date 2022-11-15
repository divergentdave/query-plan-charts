from dataclasses import dataclass
import itertools
import typing

import numpy
import tqdm

from .base import ParameterizedStatement, ParameterConfig, QueryPlan
from .postgres_plans import Postgres


def choose_parameter_values(start, end, max_steps):
    array = numpy.geomspace(start, end, max_steps)
    array = numpy.rint(array)
    array = numpy.asarray(array, dtype="int")
    array = numpy.unique(array)
    # Iterate from largest to smallest so that the user can quickly determine
    # if parameters are so large that they make setup queries too slow.
    array = numpy.flip(array)
    return array


def run_single_case(setup_statements: list[ParameterizedStatement],
                    parameter_values: list[int],
                    target_query: str):
    backend = Postgres()
    with backend:
        parameter_offset = 0
        for statement in setup_statements:
            statement_params = parameter_values[
                parameter_offset:
                parameter_offset + statement.parameter_count
            ]
            backend.execute_statement(statement.statement, statement_params)
            parameter_offset += statement.parameter_count

        backend.prepare_indexes()

        return (
            backend.plan_query_structured(target_query),
            backend.plan_query_text(target_query),
        )


@dataclass
class EquivalenceClass:
    key: typing.Any
    members: list[QueryPlan]


class EquivalenceClasses:
    def __init__(self):
        self.classes = []

    def add(self, key, plan: QueryPlan):
        for ec in self.classes:
            if plan == ec.members[0]:
                ec.members.append(plan)
                break
        else:
            self.classes.append(EquivalenceClass(key, [plan]))


def run_0d(setup_statements: list[ParameterizedStatement],
           target_query: str):
    _plan, plan_text = run_single_case(setup_statements, [], target_query)
    print(plan_text)


def run_1d(setup_statements: list[ParameterizedStatement],
           parameter: ParameterConfig,
           target_query: str):
    parameter_values = choose_parameter_values(
        parameter.start, parameter.stop, parameter.steps)
    equivalence_classes = EquivalenceClasses()
    for parameter_value in tqdm.tqdm(parameter_values):
        plan, _plan_text = run_single_case(
            setup_statements, [int(parameter_value)], target_query)
        equivalence_classes.add(parameter_value, plan)


def run_2d(setup_statements: list[ParameterizedStatement],
           parameter_1: ParameterConfig,
           parameter_2: ParameterConfig,
           target_query: str):
    parameter_1_values = choose_parameter_values(
        parameter_1.start, parameter_1.stop, parameter_1.steps)
    parameter_2_values = choose_parameter_values(
        parameter_2.start, parameter_2.stop, parameter_2.steps)
    parameter_pairs = itertools.product(
        parameter_1_values, parameter_2_values)
    equivalence_classes = EquivalenceClasses()
    for (value_1, value_2) in tqdm.tqdm(parameter_pairs):
        plan, _plan_text = run_single_case(
            setup_statements, [int(value_1), int(value_2)], target_query)
        equivalence_classes.add((value_1, value_2), plan)

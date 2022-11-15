import itertools

import numpy
import tqdm

from .base import ParameterizedStatement, ParameterConfig
from .postgres_plans import Postgres


def choose_parameter_values(start, end, max_steps):
    array = numpy.geomspace(start, end, max_steps)
    array = numpy.rint(array)
    array = numpy.asarray(array, dtype="int")
    array = numpy.unique(array)
    # Iterate from largest to smallest so that the user can quickly determine
    # if parameters are so large that they make setup queries too slow.
    array = numpy.flip(array)
    return array.tolist()


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

        print()
        print(parameter_values)
        print(backend.plan_query_text(target_query))
        print()
        return backend.plan_query_structured(target_query)


def run_0d(setup_statements: list[ParameterizedStatement],
           target_query: str):
    run_single_case(setup_statements, [], target_query)


def run_1d(setup_statements: list[ParameterizedStatement],
           parameter: ParameterConfig,
           target_query: str):
    parameter_values = choose_parameter_values(
        parameter.start, parameter.stop, parameter.steps)
    for parameter_value in tqdm.tqdm(parameter_values):
        run_single_case(setup_statements, [parameter_value], target_query)


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
    last_plan = None
    for (value_1, value_2) in tqdm.tqdm(parameter_pairs):
        plan = run_single_case(
            setup_statements, [value_1, value_2], target_query)
        if last_plan is not None:
            print(last_plan == plan)
        last_plan = plan

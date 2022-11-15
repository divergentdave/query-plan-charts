from dataclasses import dataclass
import itertools
import typing

import numpy
import matplotlib.pyplot  # type: ignore
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
        for (i, ec) in enumerate(self.classes):
            if plan == ec.members[0]:
                ec.members.append(plan)
                return i
        else:
            i = len(self.classes)
            self.classes.append(EquivalenceClass(key, [plan]))
            return i


def centers_to_boundaries(centers):
    """
    Take an array of N center coordinates, and interpolate and extend it into
    N + 1 boundary coordinates. The geometric mean between any two points is
    chosen as a boundary, to align with the logarithmic scale used throughout.
    """
    temp = numpy.sqrt(numpy.multiply(centers[:-1], centers[1:]))
    first = temp[0] / (centers[1] / centers[0])
    last = temp[-1] * (centers[-1] / centers[-2])
    return numpy.concatenate(([first], temp, [last]))


def run_0d(setup_statements: list[ParameterizedStatement],
           target_query: str):
    _plan, plan_text = run_single_case(setup_statements, [], target_query)
    print(plan_text)


def run_1d(setup_statements: list[ParameterizedStatement],
           parameter: ParameterConfig,
           target_query: str):
    parameter_values = choose_parameter_values(
        parameter.start, parameter.stop, parameter.steps)

    if len(parameter_values) <= 1:
        raise Exception(
            "Degenerate input, the parameter can only take on a single value"
        )

    equivalence_classes = EquivalenceClasses()
    # Flatten the iterator from `enumerate` into a list, so that tqdm can see
    # its length and show a progress bar.
    enumerated = list(enumerate(parameter_values.tolist()))
    for (i, parameter_value) in tqdm.tqdm(enumerated):
        plan, _plan_text = run_single_case(
            setup_statements, [parameter_value], target_query)
        equivalence_classes.add(i, plan)


def run_2d(setup_statements: list[ParameterizedStatement],
           parameter_1: ParameterConfig,
           parameter_2: ParameterConfig,
           target_query: str):
    parameter_1_values = choose_parameter_values(
        parameter_1.start, parameter_1.stop, parameter_1.steps)
    parameter_2_values = choose_parameter_values(
        parameter_2.start, parameter_2.stop, parameter_2.steps)

    if len(parameter_1_values) <= 1 or len(parameter_2_values) <= 1:
        raise Exception(
            "Degenerate input, one of the parameters can only take on a "
            "single value"
        )

    parameter_pairs = list(itertools.product(
        enumerate(parameter_1_values.tolist()),
        enumerate(parameter_2_values.tolist()),
    ))
    equivalence_classes = EquivalenceClasses()
    colors = numpy.zeros(
        (len(parameter_1_values), len(parameter_2_values)),
        dtype="int8",
    )
    for ((i, value_1), (j, value_2)) in tqdm.tqdm(parameter_pairs):
        plan, _plan_text = run_single_case(
            setup_statements, [value_1, value_2], target_query)
        class_idx = equivalence_classes.add((i, j), plan)
        colors[i, j] = class_idx

    mesh_x = centers_to_boundaries(parameter_1_values)
    mesh_y = centers_to_boundaries(parameter_2_values)

    matplotlib.pyplot.xscale("log")
    matplotlib.pyplot.yscale("log")
    matplotlib.pyplot.pcolormesh(
        mesh_x,
        mesh_y,
        colors,
        cmap="tab20",
    )
    matplotlib.pyplot.show()

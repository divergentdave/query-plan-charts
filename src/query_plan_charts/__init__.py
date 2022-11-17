from dataclasses import dataclass
import itertools
import logging
import typing

import numpy
import matplotlib.pyplot  # type: ignore
from matplotlib.cm import get_cmap  # type: ignore
from matplotlib.colors import NoNorm  # type: ignore
from matplotlib.ticker import FuncFormatter, MultipleLocator  # type: ignore
import tqdm

from .base import ParameterizedStatement, ParameterConfig, QueryPlan
from .postgres_plans import Postgres


def undo_testcontainers_logging_changes():
    # Clear out extra logging handlers set up by the testcontainers library.
    # These would result in some messages being printed twice.
    logging.getLogger("testcontainers.core.container").handlers.clear()
    logging.getLogger("testcontainers.core.waiting_utils").handlers.clear()

    # Reset level on testcontainers loggers as well
    logging.getLogger("testcontainers.core.container").setLevel(logging.NOTSET)
    logging.getLogger("testcontainers.core.waiting_utils").setLevel(
        logging.NOTSET)


undo_testcontainers_logging_changes()


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

        return backend.plan_query(target_query)


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
           target_query: str,
           _title: str):
    plan = run_single_case(setup_statements, [], target_query)
    print(plan.text())


def run_1d(setup_statements: list[ParameterizedStatement],
           parameter: ParameterConfig,
           target_query: str,
           _title: str):
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
        plan = run_single_case(
            setup_statements, [parameter_value], target_query)
        equivalence_classes.add(i, plan)


def run_2d(setup_statements: list[ParameterizedStatement],
           parameter_1: ParameterConfig,
           parameter_2: ParameterConfig,
           target_query: str,
           title: str):
    # First parameter: x-axis, column index of numpy 2D arrays, and thus the
    # second index when indexing an array. Index variable `i`.
    # Second parameter: y-axis, row index of numpy 2D arrays, and thus the
    # first index when indexing an array. Index variable `j`.
    parameter_1_values = choose_parameter_values(
        parameter_1.start, parameter_1.stop, parameter_1.steps)
    parameter_2_values = choose_parameter_values(
        parameter_2.start, parameter_2.stop, parameter_2.steps)

    if len(parameter_1_values) <= 1 or len(parameter_2_values) <= 1:
        raise Exception(
            "Degenerate input, one of the parameters can only take on a "
            "single value"
        )

    # Evaluate the query plan with every combination of parameter values.
    parameter_pairs = list(itertools.product(
        enumerate(parameter_1_values.tolist()),
        enumerate(parameter_2_values.tolist()),
    ))
    equivalence_classes = EquivalenceClasses()
    colors = numpy.zeros(
        (len(parameter_2_values), len(parameter_1_values)),
        dtype="int8",
    )
    costs = numpy.zeros(
        (len(parameter_2_values), len(parameter_1_values)),
        dtype="float64",
    )
    for ((i, value_1), (j, value_2)) in tqdm.tqdm(parameter_pairs):
        plan = run_single_case(
            setup_statements, [value_1, value_2], target_query)
        class_idx = equivalence_classes.add((i, j), plan)
        colors[j, i] = class_idx
        costs[j, i] = plan.cost()
    class_count = len(equivalence_classes.classes)

    # Calculate node coordinates for the `pcolormesh` quads, such that each
    # parameter choice is in the center of a quad. (on a log-log plot)
    mesh_x = centers_to_boundaries(parameter_1_values)
    mesh_y = centers_to_boundaries(parameter_2_values)

    # Make the `pcolormesh` plot, and associated color bar. Color each plan
    # based on how we divided them into equivalence classes by topology.
    fig, ax = matplotlib.pyplot.subplots()
    ax.set_xscale("log")
    ax.set_yscale("log")
    color_map = get_cmap("viridis", class_count)
    norm = NoNorm(vmin=0, vmax=class_count - 1)
    quadmesh = ax.pcolormesh(
        mesh_x,
        mesh_y,
        colors,
        cmap=color_map,
        norm=norm,
    )
    ax.set_title(title)
    ax.set_xlabel(parameter_1.name)
    ax.set_ylabel(parameter_2.name)
    colorbar = fig.colorbar(
        quadmesh,
    )
    colorbar.set_ticks(
        list(range(class_count)),
        labels=[cls.members[0].summary()
                for cls in equivalence_classes.classes],
        wrap=True,
    )
    colorbar.ax.invert_yaxis()

    # Make a 3D surface plot of the query plan cost. 3D plots do not support
    # log scale, so we pre-transform the data instead and use substitute tick
    # labels.
    fig, ax = matplotlib.pyplot.subplots(subplot_kw={"projection": "3d"})
    surf_x, surf_y = numpy.meshgrid(
        numpy.log10(parameter_1_values),
        numpy.log10(parameter_2_values),
    )
    ax.plot_surface(
        surf_x,
        surf_y,
        costs,
    )
    locator = MultipleLocator(1)
    formatter = FuncFormatter(lambda val, _: "$10^{{{:.0f}}}$".format(val))
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    ax.yaxis.set_major_locator(locator)
    ax.yaxis.set_major_formatter(formatter)

    # Print more detailed information on each equivalence class to stdout,
    # including a representative text-format query plan.
    for (i, klass) in enumerate(equivalence_classes.classes):
        print(f"Equivalence class {i}")
        param_values = []
        for (idx_1, value_1) in enumerate(parameter_1_values):
            for (idx_2, value_2) in enumerate(parameter_2_values):
                if colors[idx_2, idx_1] == i:
                    param_values.append(f"({value_1}, {value_2})")
        print("Parameter values: {}".format(", ".join(param_values[::-1])))
        print(klass.members[0].summary())
        print(klass.members[0].text())
        print()

    matplotlib.pyplot.show()

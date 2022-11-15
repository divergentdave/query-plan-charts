import numpy

from query_plan_charts.postgres_plans import Postgres


def choose_parameter_values(start, end, max_steps):
    array = numpy.geomspace(start, end, max_steps)
    array = numpy.rint(array)
    array = numpy.asarray(array, dtype="int")
    array = numpy.unique(array)
    return array.tolist()


def run(setup_statements, parameters, target_query):
    backend = Postgres()

    backend.prepare_indexes()

    backend.plan_query_text(target_query)

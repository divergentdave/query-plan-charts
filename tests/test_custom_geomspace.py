import unittest

import numpy

from query_plan_charts import centers_to_boundaries, choose_parameter_values


class TestChooseParameterValues(unittest.TestCase):
    def test_choose_parameter_values(self):
        # Simple test case: if we ask for three values spanning 1 to 10, we
        # expect to get 1, 3, and 10 (in descending order).
        expected = [10, 3, 1]
        result = list(choose_parameter_values(1, 10, 3))
        self.assertEqual(result, expected)

        # If the number of steps is high enough, we shouldn't produce any
        # duplicate steps, even if the logarithmic spacing on the low end
        # would produce steps far smaller than one.
        expected = [4, 3, 2, 1]
        result = list(choose_parameter_values(1, 4, 20))
        self.assertEqual(result, expected)

    def test_centers_to_boundaries(self):
        # Give this a geometric sequence where the factor is a square, and
        # confirm the fenceposts we get back are another geometric sequence,
        # in between the first.
        expected = [0.125, 0.5, 2.0, 8.0, 32.0]
        result = list(centers_to_boundaries(numpy.array([
            0.25,
            1.0,
            4.0,
            16.0,
        ])))
        self.assertEqual(result, expected)

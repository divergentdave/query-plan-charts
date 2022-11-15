import unittest

from query_plan_charts import choose_parameter_values


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

import re
import unittest

from query_plan_charts.postgres_plans import Postgres


class TestPostgresPlan(unittest.TestCase):
    def test_get_plan(self):
        backend = Postgres()
        with backend:
            plan = backend.plan_query("SELECT 1")
            self.assertTrue(
                re.match(
                    "^Result +\\(cost=[.0-9]+ rows=[0-9]+ width=[0-9]+\\)$",
                    plan.text(),
                ),
                "Text-format plan didn't match the expected value, got {!r}"
                .format(plan.text()),
            )

            self.assertEqual(plan.plan["Node Type"], "Result")

            # Generate another very simple plan, with a differing "Plan Width"
            # value, and make sure that's ignored when comparing.
            other_plan = backend.plan_query("SELECT now()")
            self.assertEqual(plan, other_plan)

import re
import unittest

from query_plan_charts.postgres_plans import Postgres


class TestPostgresPlan(unittest.TestCase):
    def test_get_plan(self):
        backend = Postgres()
        with backend:
            text_plan = backend.plan_query_text("SELECT 1")
            self.assertTrue(
                re.match(
                    "^Result +\\(cost=[.0-9]+ rows=[0-9]+ width=[0-9]+\\)$",
                    text_plan,
                ),
                "Text-format plan didn't match the expected value, got {!r}"
                .format(text_plan),
            )

            rich_plan = backend.plan_query_structured("SELECT 1")
            self.assertEqual(rich_plan.plan["Node Type"], "Result")

            # Generate another very simple plan, with a differing "Plan Width"
            # value, and make sure that's ignored when comparing.
            other_plan = backend.plan_query_structured("SELECT now()")
            self.assertEqual(rich_plan, other_plan)

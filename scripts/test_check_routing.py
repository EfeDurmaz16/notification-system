"""The routing check must distinguish a rolling upgrade from a stranded queue."""

import importlib
import unittest
from unittest.mock import patch

routing = importlib.import_module("check-routing")


class RoutingCheckTest(unittest.TestCase):
    def test_running_modes(self):
        for apis, workers, pending, error in [
            (["channel"], ["shared"], None, "all workers read shared"),
            (["shared", "channel"], ["shared", "dual-read"], None, None),
            (["channel"], ["channel"], None, None),
            (["shared"], ["shared"], "t", "pending work"),
            (["shared"], ["shared"], "f", None),
        ]:
            with self.subTest(apis=apis, workers=workers, pending=pending):
                with patch.object(routing, "running_modes", side_effect=[apis, workers]), \
                        patch.object(routing, "output", return_value=pending) as query:
                    if error:
                        with self.assertRaisesRegex(ValueError, error):
                            routing.check()
                    else:
                        routing.check()
                    self.assertEqual(query.call_count, int(pending is not None))

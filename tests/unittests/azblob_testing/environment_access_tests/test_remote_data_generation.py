import os
import unittest


class CustomLocalEnvTestCase(unittest.TestCase):
    def test_set_osenvs(self):
        from tests.unittests.excluded_env_config.build_out_dummy_env import (
            set_osenvs,
        )

        def single_equality(a, b):
            return a == b

        def sequence_equality(a, b):
            if len(a) != len(b):
                return False
            for i, (_a, _b) in enumerate(zip(a, b)):
                if _a != _b:
                    return False
            return True

        # set_osenvs(be_silent=False, use_multi_proc=False)
        set_osenvs()
        expected_test_interval = (
            "hourly",
            "daily",
            "weekly",
            "monthly",
            "yearly",
        )
        expected_test_path_parts = (
            "protocol",
            "host",
            "container",
            "interval",
            "media_type",
            "fname_prefix",
            "fname",
        )
        expected_test_complete_remote_path_template = "{protocol}://{host_name}/{container_name}/{interval}/{media_type}/{fname_prefix}{fname}"
        env_vars = [
            ("test_intervals".upper(), ";", sequence_equality),
            ("test_path_parts".upper(), ";", sequence_equality),
            (
                "test_complete_remote_path_template".upper(),
                None,
                single_equality,
            ),
        ]
        expected_vals = [
            expected_test_interval,
            expected_test_path_parts,
            expected_test_complete_remote_path_template,
        ]
        dead_tests = []
        for i, (name, *_) in enumerate(env_vars):
            with self.subTest(
                objective="check if '{}' variable is in os.environ".format(
                    name
                ),
                environment_var=name,
            ):
                try:
                    check = os.environ[name]
                except BaseException as be:
                    dead_tests.append(i)
        for i in dead_tests[::-1]:
            env_vars.pop(i)
            expected_vals.pop(i)

        for (name, sep, comp), expected in zip(env_vars, expected_vals):
            val = os.environ[name]
            if sep:
                val = val.split(sep)
            with self.subTest(
                objective="confirm that the configured values match expectations",
                environment_var=name,
                environment_val=val,
                expected=expected,
            ):
                self.assertTrue(
                    comp(val, expected),
                    "{name} did not produce expected value:\n\tgot: {val}\n\texpected: {expected}".format(
                        name=name, val=val, expected=expected
                    ),
                )


if __name__ == "__main__":
    unittest.main()

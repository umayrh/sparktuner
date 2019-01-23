"""This module tests argument parsing"""

import unittest
from sparktuner.args import ArgumentParserError, ArgumentParser
from sparktuner.spark_default_param import REQUIRED_FLAGS, \
    FLAG_TO_CONF_PARAM


class ArgumentParserTest(unittest.TestCase):
    FLAGS = map(ArgumentParser.make_flag, REQUIRED_FLAGS)

    def setUp(self):
        self.parser = ArgumentParser()

    @staticmethod
    def make_required_flags():
        args = ["--path", "path/to"]
        for flag in ArgumentParserTest.FLAGS:
            args.extend([flag, "test_name"])
        return args

    def test_bad_flags(self):
        with self.assertRaises(ArgumentParserError):
            self.parser.parse_args(['--blah', "blah"])
        for flag in ArgumentParserTest.FLAGS:
            with self.assertRaises(ArgumentParserError):
                self.parser.parse_args(['--' + flag, flag + "_name"])

    def test_required_flags(self):
        args = self.make_required_flags()
        res = self.parser.parse_args(args)
        self.assertIsNotNone(res)
        res_dict = vars(res)
        for flag in REQUIRED_FLAGS:
            self.assertEqual(res_dict[flag].value, "test_name")

    def test_optional_flags(self):
        args = self.make_required_flags()
        for flag in map(ArgumentParser.make_flag, FLAG_TO_CONF_PARAM):
            args.extend([flag, "2"])

        res = self.parser.parse_args(args)
        self.assertIsNotNone(res)
        res_dict = vars(res)
        for flag in FLAG_TO_CONF_PARAM:
            self.assertEqual(res_dict[flag].value, 2)

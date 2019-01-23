"""This module handles argument parsing"""

from __future__ import print_function

from sys import stderr
import argparse
from chainmap import ChainMap
from spark_default_param import REQUIRED_FLAGS, \
    FLAG_TO_CONF_PARAM, FLAG_TO_DIRECT_PARAM


class ArgumentParserError(Exception):
    """Represents argument parsing errors"""
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class ArgumentParser(argparse.ArgumentParser):
    """
    Sets up arguments and overrides default ArgumentParser error
    """
    JAR_PATH_ARG_NAME = "path"
    PROGRAM_CONF_ARG_NAME = "program_conf"
    CONFIG_OUTPUT_PATH = "out_config"
    FIXED_SPARK_PARAM = "fixed_param"
    PROGRAM_FLAGS = ChainMap(FLAG_TO_DIRECT_PARAM, FLAG_TO_CONF_PARAM)

    @staticmethod
    def make_flag(param):
        return "--" + param

    @staticmethod
    def make_help_msg(desc):
        if isinstance(desc, tuple):
            return str(desc[1]).replace('\r\n', '').rstrip('.') + \
                   ". Default: " + str(desc[0]) + "."
        return desc

    def __init__(self, *args, **kwargs):
        super(ArgumentParser, self).__init__(*args, **kwargs)

        # Program information
        self.add_argument(ArgumentParser.make_flag(self.JAR_PATH_ARG_NAME),
                          type=str, required=True,
                          help="Fully qualified JAR path")
        self.add_argument(ArgumentParser.make_flag(self.PROGRAM_CONF_ARG_NAME),
                          type=str, required=False,
                          help="Program-specific parameters")
        self.add_argument(ArgumentParser.make_flag(self.CONFIG_OUTPUT_PATH),
                          type=str, required=False,
                          help="Output config storage location")
        self.add_argument(ArgumentParser.make_flag(self.FIXED_SPARK_PARAM),
                          type=str, required=False, default="",
                          help="List of fixed Spark parameters included as is"
                               "in every run")

        for param in ArgumentParser.PROGRAM_FLAGS:
            required = True if param in REQUIRED_FLAGS else False
            param_obj = ArgumentParser.PROGRAM_FLAGS[param]
            param_flag = ArgumentParser.make_flag(param)
            # param_obj.desc will be a tuple if a default value is
            # present (as it is for many param in spark_2_4_params.csv.).
            param_desc = ArgumentParser.make_help_msg(param_obj.desc)
            self.add_argument(param_flag, type=param_obj.make_param_from_str,
                              required=required, help=param_desc)

    def error(self, message):
        """Overwrites default error function"""
        print("Error: " + message, file=stderr)
        self.print_usage(stderr)
        raise ArgumentParserError("ArgumentParserError", message)

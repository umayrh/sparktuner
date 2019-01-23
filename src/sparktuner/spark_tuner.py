"""
Main module that is used for running a Spark application with different
configurable parameters, possibly a given range instead of a point value.
"""
import os
import logging

from args import ArgumentParser
from opentuner import (Result, argparsers)
from opentuner.search.manipulator import (ConfigurationManipulator,
                                          IntegerParameter,
                                          BooleanParameter)
from util import Util
from spark_param import (SparkParamType,
                         SparkIntType,
                         SparkMemoryType,
                         SparkBooleanType)
from spark_cmd import SparkSubmitCmd
from tuner_cfg import (MeasurementInterfaceExt,
                       MinimizeTimeAndResource,
                       ScaledIntegerParameter)
from spark_metrics import SparkMetrics

log = logging.getLogger(__name__)


class SparkTunerConfigError(Exception):
    """Represents errors in OpenTuner configuration setup"""
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class SparkConfigTuner(MeasurementInterfaceExt):
    """
    OpenTuner implementation for Spark configuration.
    Extends MeasurementInterface.
    """
    def __init__(self, *pargs, **kwargs):
        super(SparkConfigTuner, self).__init__(*pargs, **kwargs)
        self.arg_dict = vars(self.args)
        self.param_dict = SparkParamType.get_param_map(self.arg_dict)
        # Extract ranged SparkParamType objects from arguments
        self.ranged_param_dict = SparkParamType.get_param_map(
            self.param_dict, lambda p: p.is_range_val)
        log.info(str(self.ranged_param_dict))

    def manipulator(self):
        """
        Defines the search space across configuration parameters
        """
        manipulator = ConfigurationManipulator()

        for flag, param in self.ranged_param_dict.items():
            log.info("Adding flag: " + str(flag) + ", " + str(type(param)))
            if isinstance(param, SparkIntType):
                tuner_param = IntegerParameter(
                    flag,
                    param.get_range_start(),
                    param.get_range_end())
            elif isinstance(param, SparkMemoryType):
                tuner_param = ScaledIntegerParameter(
                    flag,
                    param.get_range_start(),
                    param.get_range_end(),
                    param.get_scale())
            elif isinstance(param, SparkBooleanType):
                tuner_param = BooleanParameter(flag)
            else:
                raise SparkTunerConfigError(
                    ValueError, "Invalid type for ConfigurationManipulator")
            log.info("Added config: " + str(type(tuner_param)) +
                     ": legal range " + str(tuner_param.legal_range(None)) +
                     ", search space size " +
                     str(tuner_param.search_space_size()))
            manipulator.add_parameter(tuner_param)
        return manipulator

    def run(self, desired_result, input, limit):
        """
        Runs a program for given configuration and returns the result
        """
        jar_path = self.arg_dict.get(
            ArgumentParser.JAR_PATH_ARG_NAME)
        program_conf = self.arg_dict.get(
            ArgumentParser.PROGRAM_CONF_ARG_NAME, "")
        fixed_args = self.arg_dict.get(
            ArgumentParser.FIXED_SPARK_PARAM, "")
        # This config dict is keyed by the program flag. See
        # manipulator().
        cfg_data = desired_result.configuration.data
        log.info("Config dict: " + str(cfg_data))
        # Extract all SparkParamType objects from map
        # Seems strange making a SparkParamType out of a value but it helps
        # maintain a consistent interface to SparkSubmitCmd
        tuner_cfg = {flag: self.param_dict[flag].make_param(
            cfg_data[flag]) for flag in cfg_data}

        # TODO figure out appropriate defaults
        spark_submit = SparkSubmitCmd({}, {}, fixed_args)
        # make_cmd() expects only dicts of flags to SparkParamType as input
        run_cmd = spark_submit.make_cmd(
            jar_path, program_conf, self.param_dict, tuner_cfg)
        log.info(run_cmd)

        run_result = self.call_program(run_cmd)
        # TODO differentiate between config errors and errors due to
        # insufficient resources
        log.debug(str(run_result))
        assert run_result[MeasurementInterfaceExt.RETURN_CODE] == 0, \
            run_result[MeasurementInterfaceExt.STDERR]

        # Log process performance metrics.
        metric_time = run_result[SparkMetrics.SECS]
        metric_mem_mb = Util.ratio(run_result[SparkMetrics.MEM_SECS],
                                   metric_time)
        metric_vcores = Util.ratio(run_result[SparkMetrics.VCORE_SECS],
                                   metric_time)
        log.info("Application metrics: time=%0.3fs, mem=%0.3fmb, vcores=%0.3f"
                 % (metric_time, metric_mem_mb, metric_vcores))

        # Unfortunately, size is a vector of tuner_cfg values at this point
        # and so cannot be resolved into a scalar value.
        return Result(time=run_result[MeasurementInterfaceExt.TIME],
                      size=0)

    def save_final_config(self, configuration):
        """Saves optimal configuration, after tuning, to a file"""
        if self.args.out_config is not None:
            file_name = os.path.join(
                self.args.output_config,
                self.args.name + "_final_config.json")
            log.info("Writing final config", file_name, configuration.data)
            self.manipulator().save_to_file(configuration.data, file_name)

    def objective(self):
        # TODO: add a flag to allow using a different objective function
        return MinimizeTimeAndResource(
            rel_tol=0.06, ranged_param_dict=self.ranged_param_dict)

    @staticmethod
    def make_parser():
        """Creates and returns the default parser"""
        return ArgumentParser(parents=argparsers(), add_help=True)


def main():
    SparkConfigTuner.main(SparkConfigTuner.make_parser().parse_args())


if __name__ == '__main__':
    main()

from os import environ
from chainmap import ChainMap
from util import Util
from spark_param import SparkParamType, SparkMemoryType
from spark_default_param import FLAG_TO_DIRECT_PARAM, FLAG_TO_CONF_PARAM


class SparkSubmitCmd:
    """
    Constructs spark-submit command
    """
    CMD_SEPARATOR = " "
    CONF_SEPARATOR = "="
    SPARK_SUBMIT_PATH = environ["SPARK_HOME"] + "/bin/spark-submit" \
        if "SPARK_HOME" in environ else "spark-submit"

    @staticmethod
    def make_spark_direct_flag(flag_param):
        """
        Converts a direct parameter into a flag by prefixing it with "--"

        :param flag_param: a non-null, non-empty str parameter
        :return: parameter prefixed with "--"
        """
        assert True if flag_param else False
        return "--" + flag_param

    @staticmethod
    def make_spark_conf_flag(flag_param):
        """
        Converts a parameter into a flag by prefixing it with "--conf "

        :param flag_param: a non-null, non-empty str parameter
        :return: parameter prefixed with "--conf "
        """
        assert True if flag_param else False
        return "--conf " + flag_param

    @staticmethod
    def make_subcmd(param_dict, is_direct=False):
        """
        Creates the sub-command for Spark direct param

        :param param_dict: dictionary mapping a Spark
        parameter to its value
        :param is_direct whether the dictionary parameters are
        direct or conf (default: False i.e. conf params)
        :return: a string representing the sub-command without leading
        or trailing whitespaces
        """
        dirc_flag = SparkSubmitCmd.make_spark_direct_flag
        conf_flag = SparkSubmitCmd.make_spark_conf_flag
        subcmd_list = []
        for param, value in param_dict.items():
            param_flag = dirc_flag(param) if is_direct else conf_flag(param)
            # TODO lower() seems hacky - find a better way
            value_str = value if isinstance(value, str) else str(value).lower()
            if is_direct:
                subcmd_list.append(param_flag)
                subcmd_list.append(value_str)
            else:
                cmd_phrase = SparkSubmitCmd.CONF_SEPARATOR.join(
                    [param_flag, value_str])
                subcmd_list.append(cmd_phrase)

        return SparkSubmitCmd.CMD_SEPARATOR.join(subcmd_list)

    def merge_params(self, arg_dict, tuner_cfg_dict):
        """
        Extracts all Spark direct and conf parameters from program
        arguments and from OpenTuner config dict, and merges them with
        their respective Spark default parameters. The function assumes
        that all configurable parameters (i.e. range types) in the
        arg_dict are over-written by specific param values in
        tuner_cfg_dict.

        :param arg_dict: program argument dict that maps a program flag
        to corresponding SparkParamType
        :param tuner_cfg_dict: OpenTuner config dict, which map a program
        flag to a corresponding SparkParamType, which is guaranteed to be
        a non-range value
        :return: a tuple of two dicts, the first containing all
        Spark direct parameters, and the second containing all
        Spark conf parameters. The keys for both are Spark parameter names,
        and not program flags.
        """
        input_direct_params = {}
        input_conf_params = {}

        # Extract direct and conf param from input dicts.
        # Note the order: tuner_cfg_dict takes precedence over arg_dict
        # to ensure that all configurable parameters (i.e. range
        # types) in the arg_dict are over-written by specific param
        # values. TODO Might want to assert:
        # type(param) is SparkParamType and type(param.value) is not tuple
        input_dict = dict(ChainMap({}, tuner_cfg_dict, arg_dict))
        for flag, param in input_dict.items():
            param_val = param.value
            # To ensure that we explicitly specify memory units - lest
            # Spark/YARN misinterprets the input - we use
            # `Util.format_size` here to 'round' all values to
            # kibibytes. For general units, there a small risk that the
            # rounding here - done outside of Opentuner configuration - may
            # throw off any underlying optimization algorithm.
            # TODO figure out when rounding here might cause issues
            if isinstance(param, SparkMemoryType):
                param_val = Util.format_size(param_val, 'k')

            if flag in FLAG_TO_DIRECT_PARAM:
                input_direct_params[param.spark_name] = param_val
            elif flag in FLAG_TO_CONF_PARAM:
                input_conf_params[param.spark_name] = param_val

        # merge input dicts with defaults
        direct_param_default = SparkParamType.get_value_map(
            self.direct_param_default)
        direct_params = ChainMap(
            {}, input_direct_params, direct_param_default)

        conf_defaults = SparkParamType.get_value_map(self.conf_defaults)
        conf_params = ChainMap({}, input_conf_params, conf_defaults)

        return dict(direct_params), dict(conf_params)

    def make_cmd(self, jar_path, program_conf, arg_dict, tuner_cfg_dict):
        """
        Constructs spark-submit command

        :param jar_path: string path to a JAR file
        :param program_conf: string representing arguments to program in
        the JAR
        :param arg_dict: maps program arguments to a SparkParamType object.
        This dict must contain the key: ArgumentParser.JAR_PATH_ARG_NAME.
        :param tuner_cfg_dict: config dict, which map a program
        flag to a corresponding SparkParamType, which is guaranteed to be
        a non-range value
        :return: a string representing an executable spark-submit
        command
        """
        # merge input parameter
        (direct_params, conf_params) = \
            self.merge_params(arg_dict, tuner_cfg_dict)
        # construct command from parameters
        return SparkSubmitCmd.CMD_SEPARATOR.join([
            SparkSubmitCmd.SPARK_SUBMIT_PATH,
            SparkSubmitCmd.make_subcmd(direct_params, True),
            SparkSubmitCmd.make_subcmd(conf_params, False),
            self.fixed_param,
            jar_path,
            program_conf
        ])

    def __init__(self,
                 direct_param_default=dict(),
                 conf_defaults=dict(),
                 fixed_param=""):
        """
        Note that these input dict are NOT keyed by program flags but
        by native Spark parameter names
        :param direct_param_default: maps direct Spark param name
        to a SparkParamType object
        :param conf_defaults: maps conf Spark param name
        to a SparkParamType object
        :param fixed_param: set of direct and/or conf flags, in the
        format expected by spark-submit, that's fixed across runs,
        and hence appended without modification to a spark-submit
        command
        """
        self.direct_param_default = direct_param_default
        self.conf_defaults = conf_defaults
        self.fixed_param = fixed_param
        return

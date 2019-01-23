"""This module tests spark-submit command formation"""

import unittest
from sparktuner.spark_cmd import SparkSubmitCmd
from sparktuner.spark_default_param import SparkParam, \
    REQUIRED_FLAGS, FLAG_TO_CONF_PARAM, \
    FLAG_TO_DIRECT_PARAM


class SparkSubmitCmdTest(unittest.TestCase):
    DEFAULT_PATH = "path/to"
    REQ_DIRECT_PARAM = {
        FLAG_TO_DIRECT_PARAM[k].spark_name: FLAG_TO_DIRECT_PARAM[k]
        for k in REQUIRED_FLAGS}
    DIRECT_PARAM = {
        FLAG_TO_DIRECT_PARAM[k].spark_name: FLAG_TO_DIRECT_PARAM[k]
        for k in FLAG_TO_DIRECT_PARAM}
    CONF_PARAM = {
        FLAG_TO_CONF_PARAM[k].spark_name: FLAG_TO_CONF_PARAM[k]
        for k in FLAG_TO_CONF_PARAM}

    def setUp(self):
        self.cmder = SparkSubmitCmd()
        self.jar = SparkSubmitCmdTest.DEFAULT_PATH

    @staticmethod
    def make_required_args_dict():
        """Return a dict that map a required program flag to a
        SparkParamType object"""
        args = {}
        for flag in REQUIRED_FLAGS:
            args[flag] = \
                FLAG_TO_DIRECT_PARAM[flag].make_param_from_str("test_value")
        return args

    @staticmethod
    def parse_spark_cmd(spark_cmd):
        """
        Extracts the Spark direct and conf parameters from a spark-submit
        command.
        :param spark_cmd: spark-submit command
        :return: tuple of two dict: first containing direct, the second conf
        """
        cmd = spark_cmd.replace(SparkSubmitCmd.SPARK_SUBMIT_PATH, "")
        cmd = cmd[:cmd.find(SparkSubmitCmdTest.DEFAULT_PATH)].strip()
        direct_param = {}
        conf_param = {}
        for line in cmd.split("--"):
            if not line:
                continue
            line = line.strip()
            if line.startswith("conf"):
                res = line.replace("conf", "").strip().split("=")
                conf_param[res[0]] = res[1]
            else:
                res = line.split(" ")
                direct_param[res[0]] = res[1]
        return direct_param, conf_param

    def test_merge_param_with_required_flags_only(self):
        args = self.make_required_args_dict()
        (direct, conf) = self.cmder.merge_params(args, {})
        self.assertTrue(len(conf) == 0)
        for param in REQUIRED_FLAGS:
            spark_name = FLAG_TO_DIRECT_PARAM[param].spark_name
            self.assertTrue(spark_name in direct)
            self.assertEqual(direct[spark_name], "test_value")
        # no conf param in direct
        conf_dict = {k for k in direct if k in SparkSubmitCmdTest.CONF_PARAM}
        self.assertTrue(len(conf_dict) == 0)

    def test_merge_param_with_both_flags(self):
        args = self.make_required_args_dict()
        for flag, param in FLAG_TO_CONF_PARAM.items():
            args[flag] = param.make_param_from_str("2,2")
        (direct, conf) = self.cmder.merge_params(args, {})
        for param in SparkSubmitCmdTest.REQ_DIRECT_PARAM:
            self.assertTrue(param in direct)
            self.assertEqual(direct[param], "test_value")
        for param in SparkSubmitCmdTest.CONF_PARAM:
            self.assertTrue(param in conf)
            self.assertEqual(conf[param], 2)

    def test_merge_param_with_tuner_cfg(self):
        args = self.make_required_args_dict()
        tuner_cfg = {}
        for flag, param in FLAG_TO_CONF_PARAM.items():
            args[flag] = param.make_param_from_str("2,2")
            tuner_cfg[flag] = param.make_param_from_str("3,3")
        (direct, conf) = self.cmder.merge_params(args, tuner_cfg)
        for spark_name in SparkSubmitCmdTest.CONF_PARAM:
            self.assertTrue(spark_name in conf)
            self.assertEqual(conf[spark_name], 3)

    def test_merge_param_with_defaults(self):
        args = self.make_required_args_dict()
        tuner_cfg = {
            "max_executors":
                SparkParam.MAX_EXECUTORS.make_param_from_str("23")}
        direct_default = {
            "executor-cores":
                SparkParam.EXECUTOR_CORES.make_param_from_str("111"),
            "driver-memory":
                SparkParam.DRIVER_MEM.make_param_from_str("12000")}
        conf_default = {
            "spark.eventLog.dir":
                SparkParam.EVENTLOG_DIR.make_param_from_str(
                    "file:///tmp/spark-events"),
            "spark.yarn.maxAppAttempts":
                SparkParam.YARN_MAX_ATTEMPTS.make_param_from_str("123")}
        expected_conf_dict = {
            "spark.dynamicAllocation.maxExecutors": 23,
            "spark.eventLog.dir": "file:///tmp/spark-events",
            "spark.yarn.maxAppAttempts": 123}

        spark_cmd = SparkSubmitCmd(direct_default, conf_default)
        (direct, conf) = spark_cmd.merge_params(args, tuner_cfg)

        self.assertEqual(conf, expected_conf_dict)
        self.assertTrue("executor-cores" in direct)
        self.assertEqual(direct["executor-cores"], 111)
        self.assertTrue("driver-memory" in direct)
        self.assertEqual(direct["driver-memory"], 12000)

    def test_make_cmd_no_prog_args(self):
        args = self.make_required_args_dict()
        tuner_cfg = {
            "spark_parallelism":
                SparkParam.PARALLELISM.make_param_from_str("231")}
        direct_default = {
            "executor-cores":
                SparkParam.EXECUTOR_CORES.make_param_from_str("111"),
            "driver-memory":
                SparkParam.DRIVER_MEM.make_param_from_str("12000")}
        conf_default = {
            "spark.dynamicAllocation.maxExecutors":
                SparkParam.MAX_EXECUTORS.make_param_from_str("23")}

        spark_cmd = SparkSubmitCmd(direct_default, conf_default)
        cmd = spark_cmd.make_cmd(self.jar, "", args, tuner_cfg).strip()

        self.assertTrue(cmd.startswith(SparkSubmitCmd.SPARK_SUBMIT_PATH))
        self.assertTrue(cmd.endswith(SparkSubmitCmdTest.DEFAULT_PATH))

        (actual_direct, actual_conf) = self.parse_spark_cmd(cmd)

        for param in SparkSubmitCmdTest.REQ_DIRECT_PARAM:
            self.assertTrue(param in actual_direct)
        self.assertTrue("executor-cores" in actual_direct)
        self.assertEqual(actual_direct["executor-cores"], "111")
        self.assertTrue("driver-memory" in actual_direct)
        self.assertEqual(actual_direct["driver-memory"], "12000")
        expected_conf = {"spark.default.parallelism": "231",
                         "spark.dynamicAllocation.maxExecutors": "23"}
        self.assertEqual(actual_conf, expected_conf)

    def test_make_cmd_with_prog_args(self):
        pass

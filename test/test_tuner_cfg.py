import os
import random
import shutil
import unittest
import tempfile
import requests_mock

from math import isnan
from requests.compat import urljoin
from argparse import ArgumentParser
from opentuner import (Result, argparsers)
from sparktuner.util import TestUtil
from sparktuner.spark_metrics import SparkMetrics
from sparktuner.yarn_metrics import YarnResourceManager
from sparktuner.spark_default_param import SparkParam
from sparktuner.tuner_cfg import (MeasurementInterfaceExt,
                                  ScaledIntegerParameter,
                                  MinimizeTimeAndResource)


class MeasurementInterfaceExtTest(unittest.TestCase):
    def setUp(self):
        self.dir_path = os.path.dirname(os.path.abspath(__file__))
        self.temp_file = os.path.join(
            tempfile.gettempdir(), next(tempfile._get_candidate_names()))
        self.yarn_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "resources")

    def tearDown(self):
        if os.path.exists(self.temp_file):
            shutil.rmtree(self.temp_file)

    @staticmethod
    def make_args(master="yarn", deploy_mode="cluster"):
        arg_list = ["--no-dups", "--test-limit", "1",
                    "--master", master, "--deploy_mode", deploy_mode]
        parser = ArgumentParser(parents=argparsers())
        parser.add_argument("--master",
                            type=SparkParam.MASTER.make_param_from_str)
        parser.add_argument("--deploy_mode",
                            type=SparkParam.DEPLOY_MODE.make_param_from_str)
        return parser.parse_args(arg_list)

    @unittest.skipIf("SPARK_HOME" not in os.environ,
                     "SPARK_HOME environment variable not set.")
    def test_call_program_local(self):
        spark_submit = os.environ.get("SPARK_HOME") + "/bin/spark-submit"
        jar_path = os.path.join(self.dir_path, TestUtil.JAR_NAME)
        basic_args = "--deploy-mode client --master \"local[*]\" " \
                     "--class com.umayrh.sort.Main --name sorter"
        cmd = " ".join(
            [spark_submit, basic_args, jar_path, "10", self.temp_file])
        controller = MeasurementInterfaceExt(
            MeasurementInterfaceExtTest.make_args("local[*]"))
        result = controller.call_program(cmd)
        self.assertTrue(result)
        self.assertEqual(result[MeasurementInterfaceExt.RETURN_CODE], 0)
        self.assertTrue(0 < result[SparkMetrics.SECS] < float("inf"))
        self.assertGreaterEqual(SparkMetrics.VCORE_SECS, 1)
        self.assertGreater(SparkMetrics.MEM_SECS, 0)

    @unittest.skipIf("SPARK_HOME" not in os.environ,
                     "SPARK_HOME environment variable not set.")
    @requests_mock.Mocker()
    def test_call_program_yarn(self, mocker):
        """
        Test that two successive runs of a spark-submit
        program produce distinct per-run data
        """
        spark_submit = os.environ.get("SPARK_HOME") + "/bin/spark-submit"
        jar_path = os.path.join(self.dir_path, TestUtil.JAR_NAME)
        basic_args = "--deploy-mode client --master \"local[*]\" " \
                     "--class com.umayrh.sort.Main --name sorter"
        cmd = " ".join(
            [spark_submit, basic_args, jar_path, "10", self.temp_file])
        yarn_resp_file = os.path.join(self.yarn_dir,
                                      "yarn-resp-app-info.json")
        # This creates a fake spark-submit output
        cmd = "cat " + yarn_resp_file + " && " + cmd

        # register YARN endpoints for mock requests
        app_id = "application_1476912658570_0002"
        proto, addr, port = ("http", "master", "8088")

        TestUtil.yarn_api_helper(
            self.yarn_dir, mocker,
            "yarn-resp-cluster-info.json",
            proto, addr, port,
            YarnResourceManager.ROUTE_INFO)
        TestUtil.yarn_api_helper(
            self.yarn_dir, mocker,
            "yarn-resp-app-info.json",
            proto, addr, port,
            urljoin(YarnResourceManager.ROUTE_APP_ID, app_id))

        with TestUtil.modified_environ(
                'YARN_CONF_DIR', HADOOP_CONF_DIR=self.yarn_dir):
            controller = MeasurementInterfaceExt(
                MeasurementInterfaceExtTest.make_args())
            result = controller.call_program(cmd)
        self.assertEqual(result[MeasurementInterfaceExt.RETURN_CODE], 0)
        self.assertTrue(0 < result[SparkMetrics.SECS] < float("inf"))
        self.assertGreaterEqual(SparkMetrics.VCORE_SECS, 1)
        self.assertGreater(SparkMetrics.MEM_SECS, 0)


class ScaledIntegerParameterTest(unittest.TestCase):
    def test_invalid_scaling_values(self):
        with self.assertRaises(AssertionError):
            ScaledIntegerParameter("a", 1, 2, 0)
        with self.assertRaises(AssertionError):
            ScaledIntegerParameter("a", 3, 1000, 20)
        with self.assertRaises(AssertionError):
            ScaledIntegerParameter("a", 30, 100, 1000)

    def test_scaling_boundedness(self):
        """
        Test if scaling and unscaling, in any order,
        cause a parameter values to go out of bounds.
        Disabled for now till ScaledIntegerParameter is
        reimplemented.
        """
        test_size = 100
        min_values = random.sample(range(1, 1000), test_size)
        max_values = random.sample(range(1000, 10000), test_size)

        for idx in range(0, test_size):
            min_val = min_values[idx]
            max_val = max_values[idx]
            scale = random.randint(1, min_val)

            param = ScaledIntegerParameter("a", min_val, max_val, scale)
            val = random.randint(min_val, max_val)

            result = param._unscale(param._scale(val))
            self.assertGreaterEqual(result, min_val)
            self.assertLessEqual(result, max_val)

            legal_range = param.legal_range(None)
            self.assertTrue(isinstance(legal_range, tuple))
            self.assertTrue(isinstance(legal_range[0], int))
            self.assertTrue(isinstance(legal_range[1], int))
            self.assertGreaterEqual(param._unscale(legal_range[0]), min_val)
            self.assertLessEqual(param._unscale(legal_range[1]), max_val)


class MinimizeTimeAndResourceTest(unittest.TestCase):
    def test_result_compare(self):
        obj = MinimizeTimeAndResource()
        # ((actual.time, actual.size), (expected.time, expected.size):
        #   result
        test_cases = {
            ((2, 3), (2, 3)): 0,
            ((2.0, 3.0), (2.0, 3.0)): 0,
            ((2.0, 3.0), (1.0, 3.0)): 1,
            ((2.0, 3.0), (2.0, 2.0)): 1,
            ((2.0, 3.0), (10.0, 3.0)): -1,
            ((2.0, 3.0), (2.0, 30)): -1,
        }
        for k, v in test_cases.items():
            a = Result(time=k[0][0], size=k[0][1])
            b = Result(time=k[1][0], size=k[1][1])
            self.assertEquals(obj.result_compare(a, b), v)

    def test_result_relative(self):
        obj = MinimizeTimeAndResource()
        # ((actual.time, actual.size), (expected.time, expected.size):
        #   result
        test_cases = {
            ((2, 3), (2, 3)): 1,
            ((2.0, 3.0), (2.0, 3.0)): 1,
            ((2.0, 3.0), (1.0, 3.0)): 2,
            ((2.0, 3.0), (2.0, 2.0)): 1.5,
            ((2.0, 3.0), (10.0, 3.0)): 0.2,
            ((2.0, 3.0), (2.0, 30)): 0.1,
            ((0, 0), (0, 0)): float('nan'),
            ((0, 1), (0, 0)): float('inf'),
        }
        for k, v in test_cases.items():
            a = Result(time=k[0][0], size=k[0][1])
            b = Result(time=k[1][0], size=k[1][1])
            if isnan(float(v)):
                self.assertTrue(isnan(float(obj.result_relative(a, b))))
            else:
                self.assertEquals(obj.result_relative(a, b), v)

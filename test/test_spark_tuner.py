"""Tests spark-tuner module"""

import unittest
import os
import shutil
import tempfile
from sparktuner.spark_tuner import SparkConfigTuner


class SparkTunerTest(unittest.TestCase):
    """
    Note: the JAR used here is a slim, unshaded file, and
    so should not be used for full functionality.
    """
    JAR_NAME = "sort-0.1-all.jar"

    @staticmethod
    def get_tempfile_name():
        return os.path.join(tempfile.gettempdir(),
                            next(tempfile._get_candidate_names()))

    @staticmethod
    def make_args(temp_file):
        program_conf = "10 " + temp_file
        dir_path = os.path.dirname(os.path.abspath(__file__))
        jar_path = os.path.join(dir_path, SparkTunerTest.JAR_NAME)
        return ["--no-dups",
                "--test-limit", "1",
                "--name", "sorter",
                "--class", "com.umayrh.sort.Main",
                "--master", "\"local[*]\"",
                "--deploy_mode", "client",
                "--path", jar_path,
                "--program_conf", program_conf]

    def setUp(self):
        self.temp_file = SparkTunerTest.get_tempfile_name()

    def tearDown(self):
        if os.path.exists(self.temp_file):
            shutil.rmtree(self.temp_file)

    def test_help(self):
        try:
            SparkConfigTuner.make_parser().print_help()
        except Exception:
            self.fail("Error using --help")

    @unittest.skipIf("SPARK_HOME" not in os.environ,
                     "SPARK_HOME environment variable not set.")
    def test_no_config_args(self):
        """
        build/deployable/bin/sparktuner --name blah
        --path test/sort-0.1-all.jar
        --deploy_mode client --master "local[*]" --class Main
        """
        arg_list = SparkTunerTest.make_args(self.temp_file)
        args = SparkConfigTuner.make_parser().parse_args(arg_list)
        SparkConfigTuner.main(args)

        if not os.path.exists(self.temp_file):
            self.fail("Expected output file")

    @unittest.skipIf("SPARK_HOME" not in os.environ,
                     "SPARK_HOME environment variable not set.")
    def test_with_driver_memory(self):
        arg_list = SparkTunerTest.make_args(self.temp_file)
        arg_list.extend(['--driver_memory', '0.75G,1G'])
        arg_list.extend(['--fixed_param',
                         '--conf spark.sql.autoBroadcastJoinThreshold=-1'])
        args = SparkConfigTuner.make_parser().parse_args(arg_list)
        SparkConfigTuner.main(args)

        if not os.path.exists(self.temp_file):
            self.fail("Expected output file")

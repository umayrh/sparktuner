import os
import unittest
import subprocess

from multiprocessing.pool import ThreadPool
from sparktuner.spark_logs import SparkLogProcessor


class SparkLogProcessorTest(unittest.TestCase):
    def test_init(self):
        logger = SparkLogProcessor("yarn", "cluster")
        self.assertTrue(logger.is_yarn)
        self.assertTrue(logger.is_cluster_deploy_mode)

        logger = SparkLogProcessor("local", "cluster")
        self.assertFalse(logger.is_yarn)
        self.assertTrue(logger.is_cluster_deploy_mode)

        logger = SparkLogProcessor("blah", "blah")
        self.assertFalse(logger.is_yarn)
        self.assertFalse(logger.is_cluster_deploy_mode)

    def test_process_spark_submit_log(self):
        log_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "resources")
        log_file = os.path.join(log_dir, "spark-submit-yarn.txt")
        kwargs = {'shell': True}
        # cat is not gonna work on non-Unix systems
        cmd_process = subprocess.Popen(
            "cat " + str(log_file),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        log_collector = SparkLogProcessor("yarn", "cluster")
        the_io_thread_pool = ThreadPool(1)
        stdout_result = the_io_thread_pool.apply_async(
            log_collector.process_spark_submit_log,
            args=(iter(cmd_process.stdout.readline, ''), )
        )
        actual_result = stdout_result.get(timeout=10)
        with open(log_file) as log_file_handle:
            expected_result = log_file_handle.read(-1)
            log_file_handle.flush()
        self.assertEqual(actual_result, expected_result)
        self.assertEqual(log_collector.yarn_app_id,
                         "application_1434263747091_0023")

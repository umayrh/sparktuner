"""This module tests Spark parameter parsing"""

import unittest
from sparktuner.spark_param import SparkParamParseError, \
    SparkStringType, SparkBooleanType, SparkIntType, SparkMemoryType


class SparkSubmitCmdTest(unittest.TestCase):
    def test_make_string_param(self):
        param = SparkStringType("main-class", "MainClass", "???")
        self.assertEqual(param.spark_name, "main-class")
        self.assertEqual(param.value, "MainClass")
        self.assertEqual(param.desc, "???")
        self.assertFalse(param.is_range_val)

        with self.assertRaises(AssertionError):
            SparkStringType("main-class", True, "???")

    def test_make_new_string_param(self):
        param = SparkStringType("main-class", "MainClass", "???")
        new_param = param.make_param_from_str("MainClass2")
        self.assertEqual(new_param.spark_name, "main-class")
        self.assertEqual(new_param.value, "MainClass2")
        self.assertEqual(new_param.desc, "???")
        self.assertFalse(new_param.is_range_val)

    def test_make_bool_param(self):
        param = SparkBooleanType("spark.dynamicAllocation.enabled", True, "?")
        self.assertEqual(param.value, True)
        param = SparkBooleanType("spark.dynamicAllocation.enabled", False, "?")
        self.assertEqual(param.value, False)

        with self.assertRaises(AssertionError):
            SparkBooleanType("spark.dynamicAllocation.enabled", "true", "?")
        with self.assertRaises(AssertionError):
            SparkBooleanType("spark.dynamicAllocation.enabled", "t", "?")

    def test_make_new_bool_param(self):
        param = SparkBooleanType("spark.dynamicAllocation.enabled", True, "?")
        new_param = param.make_param_from_str("False")
        self.assertFalse(new_param.value)
        new_param = new_param.make_param_from_str("true")
        self.assertTrue(new_param.value)

        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("t")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("F")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("0")

    def test_make_int_param(self):
        param = SparkIntType("executor-cores", 2, "???")
        self.assertEqual(param.spark_name, "executor-cores")
        self.assertEqual(param.value, 2)
        self.assertEqual(param.desc, "???")
        self.assertFalse(param.is_range_val)

        param = SparkIntType("executor-cores", (2, 2), "???")
        self.assertEqual(param.value, 2)
        self.assertFalse(param.is_range_val)

        param = SparkIntType("executor-cores", (2, 3), "???")
        self.assertTrue(param.is_range_val)

        with self.assertRaises(AssertionError):
            SparkIntType("main-class", "2", "???")

    def test_make_new_int_param(self):
        param = SparkIntType("executor-cores", 2, "???")
        new_param = param.make_param_from_str("3")
        self.assertEqual(new_param.value, 3)

        new_param = new_param.make_param_from_str("4,4")
        self.assertEqual(new_param.value, 4)
        new_param = new_param.make_param_from_str("4,5")
        self.assertEqual(new_param.value, (4, 5))

        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("4;5")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("4,5,oops")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("4,6,7")

    def test_make_mem_param(self):
        param = SparkMemoryType("executor-memory", 20, "???")
        self.assertEqual(param.spark_name, "executor-memory")
        self.assertEqual(param.value, 20)
        self.assertEqual(param.desc, "???")
        self.assertFalse(param.is_range_val)

        param = SparkMemoryType("executor-memory", (20, 20), "???")
        self.assertEqual(param.value, 20)
        self.assertFalse(param.is_range_val)

        param = SparkMemoryType("executor-memory", (20, 30), "???")
        self.assertTrue(param.is_range_val)

        with self.assertRaises(AssertionError):
            SparkMemoryType("main-class", "20", "???")

    def test_make_new_mem_param(self):
        param = SparkMemoryType("executor-memory", 20, "???")
        new_param = param.make_param_from_str("30")
        self.assertEqual(new_param.value, 30)

        new_param = new_param.make_param_from_str("4")
        self.assertEqual(new_param.value, 4)
        new_param = new_param.make_param_from_str("4k")
        self.assertEqual(new_param.value, 4096)
        new_param = new_param.make_param_from_str("4,4")
        self.assertEqual(new_param.value, 4)
        new_param = new_param.make_param_from_str("4k,4k")
        self.assertEqual(new_param.value, 4096)
        new_param = new_param.make_param_from_str("4,6")
        self.assertEqual(new_param.value, (4, 6, 1))
        new_param = new_param.make_param_from_str("4k,5k")
        self.assertEqual(new_param.value, (4096, 5120, 1))
        new_param = new_param.make_param_from_str("4,8,2")
        self.assertEqual(new_param.value, (4, 8, 2))
        new_param = new_param.make_param_from_str("4k,8k,2k")
        self.assertEqual(new_param.value, (4096, 8192, 2048))

        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("4;5")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("4,5,oops")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("4,6,7oops")
        with self.assertRaises(SparkParamParseError):
            new_param.make_param_from_str("4,6,7,8")

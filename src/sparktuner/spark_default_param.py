"""This module describes all Spark parameters and their defaults"""

import csv
import os
from spark_param import SparkStringType, \
    SparkBooleanType, SparkIntType, SparkMemoryType


class _SparkParam(object):
    # Parse the CSV file containing Spark conf param names, defaults,
    # and meaning. The result is placed into a dictionary that maps
    # parameter name to a tuple containing the parameter's Spark
    # default value, and the meaning.
    __DICT = {}
    # This indirection helps run these scripts from any path
    __dir_path = os.path.dirname(os.path.abspath(__file__))
    __conf_file = os.path.join(__dir_path, "spark_2_4_params.csv")
    with open(__conf_file, 'rb') as csv_file:
        param_reader = csv.reader(csv_file)
        next(param_reader)
        for row in param_reader:
            param_name = row[0].strip()
            __DICT[param_name] = (row[1].strip(), row[2].strip())

    # Maps Spark name to SparkParamType
    def __init__(self, spark_param_dict):
        self.spark_param_dict = spark_param_dict

    def mk_string(self, spark_name, default_val, desc=None):
        desc = desc if desc else _SparkParam.__DICT.get(spark_name, "")
        param = SparkStringType(spark_name, default_val, desc)
        self.spark_param_dict[spark_name] = param
        return param

    def mk_int(self, spark_name, default_val, desc=None):
        desc = desc if desc else _SparkParam.__DICT.get(spark_name, "")
        param = SparkIntType(spark_name, default_val, desc)
        self.spark_param_dict[spark_name] = param
        return param

    def mk_memory(self, spark_name, default_val, desc=None):
        desc = desc if desc else _SparkParam.__DICT.get(spark_name, "")
        param = SparkMemoryType(spark_name, default_val, desc)
        self.spark_param_dict[spark_name] = param
        return param

    def mk_boolean(self, spark_name, default_val, desc=None):
        desc = desc if desc else _SparkParam.__DICT.get(spark_name, "")
        param = SparkBooleanType(spark_name, default_val, desc)
        self.spark_param_dict[spark_name] = param
        return param


class SparkParam(object):
    """
    Holds all Spark parameter objects
    """
    # Map all parameters from Spark name to SparkParamType
    SPARK_PARAM_DICT = {}
    __sp = _SparkParam(SPARK_PARAM_DICT)
    mk_string = __sp.mk_string
    mk_memory = __sp.mk_memory
    mk_int = __sp.mk_int
    mk_boolean = __sp.mk_boolean

    # All Spark parameters
    NAME = mk_string("name", "spark_program", "Program name")
    CLASS = mk_string(
        "class", "MainClass", "Fully qualified main class name")
    MASTER = mk_string(
        "master", "local[*]", "Spark master type: local/yarn/mesos")
    DEPLOY_MODE = mk_string(
        "deploy-mode", "client", "Deployment mode: client/cluster")
    DRIVER_MEM = mk_memory(
        "driver-memory", 10485760, "Amount of driver memory")
    EXECUTOR_MEM = mk_memory(
        "executor-memory", 10485760, "Amount of executor memory")
    EXECUTOR_CORES = mk_int("executor-cores", 2, "Number of executor cores")
    MAX_EXECUTORS = mk_int("spark.dynamicAllocation.maxExecutors", 2)
    PARALLELISM = mk_int("spark.default.parallelism", 10)
    PARTITIONS = mk_int("spark.sql.shuffle.partitions", 10)
    DA_ENABLED = mk_boolean("spark.dynamicAllocation.enabled", True)
    EVENTLOG_DIR = mk_string("spark.eventLog.dir", "file:///tmp/spark-events")
    EVENTLOG_ENABLED = mk_boolean("spark.eventLog.enabled", True)
    MR_OUTCOM_ALGO = mk_string(
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    SHUFFLE_ENABLED = mk_boolean("spark.shuffle.service.enabled", True)
    JOIN_THRESH = mk_memory("spark.sql.autoBroadcastJoinThreshold", 10485760)
    YARN_MAX_ATTEMPTS = mk_int("spark.yarn.maxAppAttempts", 1)


# Required parameters for spark-submit when running Spark JARs
REQUIRED_FLAGS = ["name", "class", "master", "deploy_mode"]

FLAG_TO_DIRECT_PARAM = {
    "name": SparkParam.NAME,
    "class": SparkParam.CLASS,
    "master": SparkParam.MASTER,
    "deploy_mode": SparkParam.DEPLOY_MODE,
    "driver_memory": SparkParam.DRIVER_MEM,
    "executor_memory": SparkParam.EXECUTOR_MEM,
    "executor_cores": SparkParam.EXECUTOR_CORES
}

# Conf whitelist

# These are allowed in the sense that they may be
# manipulated by OpenTuner for tuning
FLAG_TO_CONF_PARAM = {
    "max_executors": SparkParam.MAX_EXECUTORS,
    "spark_parallelism": SparkParam.PARALLELISM,
    "spark_partitions": SparkParam.PARTITIONS
}

# These parameters are always included in a spark-submit command.
# Default values are used where user input is unavailable.
SPARK_CONF_PARAM = {
    "spark.dynamicAllocation.enabled": SparkParam.DA_ENABLED,
    "spark.dynamicAllocation.maxExecutors": SparkParam.MAX_EXECUTORS,
    "spark.default.parallelism": SparkParam.PARALLELISM,
    "spark.sql.shuffle.partitions": SparkParam.PARTITIONS,
    "spark.eventLog.dir": SparkParam.EVENTLOG_DIR,
    "spark.eventLog.enabled": SparkParam.EVENTLOG_ENABLED,
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version":
        SparkParam.MR_OUTCOM_ALGO,
    "spark.shuffle.service.enabled": SparkParam.SHUFFLE_ENABLED,
    "spark.sql.autoBroadcastJoinThreshold": SparkParam.JOIN_THRESH,
    "spark.yarn.maxAppAttempts": SparkParam.YARN_MAX_ATTEMPTS
}

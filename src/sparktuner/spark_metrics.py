import abc
import time
import psutil

from yarn_metrics import YarnMetricsCollector


class SparkMetrics:
    """
    Abstract base class for Spark platform
    resource metrics
    """
    __metaclass__ = abc.ABCMeta

    MEM_SECS = "memorySeconds"
    VCORE_SECS = "vcoreSeconds"
    SECS = "elapsedTime"

    @abc.abstractmethod
    def start(self, **kwargs):
        pass

    @abc.abstractmethod
    def update(self, **kwargs):
        pass

    @abc.abstractmethod
    def get_perf_metrics(self, **kwargs):
        """
        :param kwargs: key-worded arguments. They are:
        For YARN:
            yarn_app_id: YARN application id
        For local process:
            pid: the process id
        :return: a dictionary containing values for the
        following metrics (metric names are keys):
            * memorySeconds
                * (long) The amount of memory the application has allocated
                (megabyte-seconds)
            * vcoreSeconds
                * (long) The amount of CPU resources the application has
                allocated (virtual core-seconds)
            * elapsedTime
                * (long) The elapsed time since the application started
                (in ms)
        """
        pass


class YarnMetrics(SparkMetrics):
    """Reports YARN metrics"""

    def __init__(self):
        super(YarnMetrics, self).__init__()
        self.collector = YarnMetricsCollector()

    def start(self, **kwargs):
        """
        Not applicable for YARN
        """
        pass

    def update(self, **kwargs):
        """
        Not applicable for YARN
        """
        pass

    def get_perf_metrics(self, **kwargs):
        """
        :param kwargs: keyworded args that must contain
        'yarn_app_id'
        :return: dict of performance metrics for
        given YARN application
        """
        yarn_app_id = kwargs["yarn_app_id"]
        items = [SparkMetrics.MEM_SECS,
                 SparkMetrics.VCORE_SECS,
                 SparkMetrics.SECS]
        return self.collector.get_app_info(yarn_app_id, items)


class ProcessMetrics(SparkMetrics):
    """Reports local process metrics"""

    @staticmethod
    def get_process_metrics(pr, cpu_interval=1):
        """
        Gets process metrics in psutil.Process.oneshot() using
        psutil.memory_info() and psutil.cpu_percent().
        Note that cpu_percent(interval=...) is a blocking call,
        so this function will effectively sleep for cpu_interval
        seconds.
        Unique set size (USS) rather than resident set size
        (RSS) is probably a better metric but:
        1. USS may not be available on all platforms, and
        2. accessing USS may require higher privileges.
        Can these issues be resolved?

        :param pr: a psutil.Process object
        :param cpu_interval: interval (in secs) for
        psutil.cpu_percent(). May be None.
        :return: a tuple of RSS (in bytes) and CPU percent over
        cpu_interval
        """
        with pr.oneshot():
            try:
                mem = pr.memory_info().rss
                cpu = pr.cpu_percent(interval=cpu_interval)
                return mem, cpu
            except psutil.Error:
                # horrid it may seem but the process may be dead
                pass
        return 0, 0

    def __init__(self):
        super(ProcessMetrics, self).__init__()
        self.cpu_num = psutil.cpu_count(logical=True)
        self.process_dict = {}
        self.process_info_dict = {}

    def start(self, **kwargs):
        """
        :param kwargs: keyworded args that must contain
        'pid'
        :return: the psutil.Process object corresponding
        to given pid
        """
        pid = kwargs['pid']
        if pid not in self.process_dict:
            self.process_dict[pid] = psutil.Process(pid)
            # Warm up cache
            self.process_dict[pid].memory_info()
            # Tuple contents: memory, cpu, time, samples
            self.process_info_dict[pid] = (0, 0, time.time(), 0)
        return self.process_dict[pid]

    def update(self, **kwargs):
        """
        :param kwargs: keyworded args that must contain
        'pid' and 'cpu_interval'
        """
        pid = kwargs['pid']
        cpu_interval = kwargs['cpu_interval']
        process = self.start(pid=pid)
        mem_secs, cpu_secs = ProcessMetrics.get_process_metrics(
            process, cpu_interval)
        # mbyte_secs is scaled just before being returned as a result
        mbyte_secs, vcore_secs, st_time, samples = self.process_info_dict[pid]
        mbyte_secs += mem_secs
        vcore_secs += (max(cpu_secs, 100) * self.cpu_num) / 100.0
        samples += 1
        self.process_info_dict[pid] = \
            (mbyte_secs, vcore_secs, st_time, samples)

    def get_perf_metrics(self, **kwargs):
        """
        :param kwargs: keyworded args that must contain
        'pid'
        :return: dict of performance metrics for given process
        """
        pid = kwargs['pid']
        end_time = time.time()
        mbyte_secs, vcore_secs, start_time, _ = self.process_info_dict[pid]
        # Potentially, remove process information here from internal dicts
        return {SparkMetrics.MEM_SECS: mbyte_secs / (1024 ** 2),
                SparkMetrics.VCORE_SECS: vcore_secs,
                SparkMetrics.SECS: (end_time-start_time)}


class SparkMetricsCollector(object):
    """
    Utilities for collecting Spark job performance metrics.
    Following Hadoop-YARN's ResourceManager API
    (https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html),
    we focus on the following metrics:
    """
    SPARK_MASTERS = frozenset(["local", "yarn"])
    DEPLOY_MODES = frozenset(["cluster"])

    @staticmethod
    def make_collector(spark_master):
        if spark_master == "local":
            return ProcessMetrics()
        elif spark_master == "yarn":
            return YarnMetrics()
        raise NotImplementedError("Invalid Spark master: " + spark_master)

    def __init__(self, spark_master, deploy_mode):
        """
        :param spark_master: Spark master (local/yarn)
        :param deploy_mode: Spark deploy mode (cluster)
        """
        # "local[*]" -> local
        master = spark_master.split("[")[0].replace('"', '')
        assert master in SparkMetricsCollector.SPARK_MASTERS, \
            "Invalid master: " + spark_master
        # assert master != "yarn" or \
        # deploy_mode in SparkMetricsCollector.DEPLOY_MODES
        self.spark_master = master
        self.deploy_mode = deploy_mode
        self.collector = SparkMetricsCollector.make_collector(master)

    def get_collector(self):
        """
        :return: the performance data collector object. Currently,
        either ProcessMetrics or YarnMetrics.
        """
        return self.collector

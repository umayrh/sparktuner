"""Contains extensions to some Opentuner classes"""

import time
import logging
import subprocess

from util import Util
from multiprocessing.pool import ThreadPool

from spark_metrics import SparkMetricsCollector
from opentuner import MeasurementInterface
from opentuner.resultsdb.models import Result
from opentuner.search.manipulator import (NumericParameter,
                                          IntegerParameter,
                                          ScaledNumericParameter)
from opentuner.search.objective import SearchObjective
from opentuner.measurement.interface import (preexec_setpgid_setrlimit,
                                             goodwait,
                                             goodkillpg)

log = logging.getLogger(__name__)


class MeasurementInterfaceExt(MeasurementInterface):
    """
    Extends Opentuner's `MeasurementInterface` to override and add some
    new functionality.
    """
    # Keys in the dict returned by call_program()
    TIME = 'time'
    TIMEOUT = 'timeout'
    RETURN_CODE = 'returncode'
    STDOUT = 'stdout'
    STDERR = 'stderr'

    def __init__(self, *pargs, **kwargs):
        super(MeasurementInterfaceExt, self).__init__(*pargs, **kwargs)
        self.the_io_thread_pool = None
        self.metrics = SparkMetricsCollector(
            self.args.master.value, self.args.deploy_mode.value)

    def the_io_thread_pool_init(self, parallelism=1):
        if self.the_io_thread_pool is None:
            self.the_io_thread_pool = ThreadPool(2 * parallelism)
            # make sure the threads are started up
            self.the_io_thread_pool.map(int, range(2 * parallelism))

    def call_program(self, cmd, limit=None, memory_limit=None, **kwargs):
        """
        The function is meant to "call cmd and kill it if it
        runs for longer than limit." While the process is running, we
        collect CPU and memory usage data at a 1sec interval.

        This unfortunately is mostly copy-pasta from Opentuner's own
        MeasurementInterface.call_program. The copy-pasta is necessary
        since the original code isn't modular enough and we may want to
        collect process resource usage information during its lifespan.

        :param cmd: (str) command to run
        :param limit: (float) process runtime limit (in seconds)
        :param memory_limit: The maximum area of address space
        which may be taken by the process (in bytes)
        :param kwargs: keyworded args passed to the pipe opened
        for the given command.
        :return: a dictionary like
          {'returncode': 0,
           'stdout': '', 'stderr': '',
           'timeout': False, 'time': 1.89}
        """
        inf = float('inf')
        self.the_io_thread_pool_init(self.args.parallelism)

        if limit is inf:
            limit = None
        if type(cmd) in (str, unicode):
            kwargs['shell'] = True
        killed = False
        t0 = time.time()
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=preexec_setpgid_setrlimit(memory_limit),
            **kwargs)
        process_id = p.pid
        # TODO: extract this from spark-submit logs
        yarn_app_id = None
        self.metrics.collector.start(pid=process_id)
        # Add p.pid to list of processes to kill
        # in case of keyboard interrupt
        self.pid_lock.acquire()
        self.pids.append(p.pid)
        self.pid_lock.release()

        try:
            # TODO: to parse STDOUT asynchronously, we need something like
            # apply_async(
            #   _process_spark_submit_log,
            #   args = (iter(self._submit_sp.stdout.readline, ''), )
            # )
            # while also collecting the logs themselves
            stdout_result = self.the_io_thread_pool.apply_async(p.stdout.read)
            stderr_result = self.the_io_thread_pool.apply_async(p.stderr.read)
            while p.returncode is None:
                # No need to sleep since cpu_percent(interval=1)
                # is a blocking call.
                self.metrics.collector.update(pid=process_id, cpu_interval=1)
                # Leaving some of the code commented here so it
                # can be easily differentiated from the parent
                # interface's implementation.
                if limit is None:
                    # No need to goodwait(p) since
                    # we're sampling process metrics above in a
                    # blocking call.
                    pass
                elif limit and time.time() > t0 + limit:
                    killed = True
                    self.kill_app(p.pid, yarn_app_id)
                    goodwait(p)
                else:
                    # No need for the following wait block since
                    # we're already sampling process metrics above in a
                    # blocking call:
                    # sleep_for = limit - (time.time() - t0)
                    # if not stdout_result.ready():
                    #     stdout_result.wait(sleep_for)
                    # elif not stderr_result.ready():
                    #     stderr_result.wait(sleep_for)
                    # else:
                    #    time.sleep(0.001)
                    pass
                p.poll()
        except Exception:
            if p.returncode is None:
                self.kill_app(p.pid, yarn_app_id)
            raise
        finally:
            # No longer need to kill p
            self.pid_lock.acquire()
            if p.pid in self.pids:
                self.pids.remove(p.pid)
            self.pid_lock.release()

        t1 = time.time()
        metrics = self.metrics.collector.get_perf_metrics(
            pid=process_id, yarn_app_id=yarn_app_id)
        result = {
            MeasurementInterfaceExt.TIME: inf if killed else (t1 - t0),
            MeasurementInterfaceExt.TIMEOUT: killed,
            MeasurementInterfaceExt.RETURN_CODE: p.returncode,
            MeasurementInterfaceExt.STDOUT: stdout_result.get(),
            MeasurementInterfaceExt.STDERR: stderr_result.get()}
        result.update(metrics)
        return result

    def kill_app(self, pid=None, yarn_app_id=None):
        """
        Kills the current process. If YARN, kills the YARN application
        first.
        This implementation follows Apache Airflow's spark_submit_hook.py
        :param pid: process id
        :param yarn_app_id: YARN application id (only used if Spark master
        is yarn)
        """
        if self.args.master == "yarn":
            kill_cmd = "yarn application -kill {}".format(yarn_app_id).split()
            yarn_kill = subprocess.Popen(kill_cmd,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE)
            log.info("YARN killed with return code: %s", yarn_kill.wait())
        goodkillpg(pid)


class ScaledIntegerParameter(ScaledNumericParameter, IntegerParameter):
    SCALING = 1000
    """
    An integer parameter that is searched on a
    linear scale after normalization, but stored without scaling.

    TODO: revisit this implementation, and this class' utility.
    It seems hard to achieve all of the following goals simultaneously:
    - Keep this parameter type an integer,
    - Allow scaling the parameter range by another integer, with
    possibly irrational result,
    - Enforce that rounding, or some other way to convert a real
    value to an integer, doesn't cause a bounded value to go
    out of bounds after either scaling-unscaling or unscaling-scaling.
    A partial solution might be to ensure that scaling factor is
    _effectively_ a real between 0 and 1. We achieve this by rescaling
    values by 1000000.
    The current implementation may fail the following test because
    of rounding errors:
        result = param._scale(param._unscale(val))
        self.assertGreaterEqual(result, min_val)
        self.assertLessEqual(result, max_val)
    """
    def __init__(self, name, min_value, max_value, scaling, **kwargs):
        assert 0 < abs(scaling) <= abs(min_value), "Invalid scaling"
        kwargs['value_type'] = int
        super(ScaledNumericParameter, self).__init__(
            name, min_value, max_value, **kwargs)
        self.scaling = scaling

    def _scale(self, v):
        return (v - self.min_value) * ScaledIntegerParameter.SCALING \
               / float(self.scaling)

    def _unscale(self, v):
        v = (v * self.scaling) / ScaledIntegerParameter.SCALING + \
            self.min_value
        return int(round(v))

    def legal_range(self, config):
        low, high = NumericParameter.legal_range(self, config)
        # We avoid increasing the bounds (to account for rounding)
        # by rescaling using ScaledIntegerParameter.SCALING
        return int(self._scale(low)), int(self._scale(high))


class MinimizeTimeAndResource(SearchObjective):
    """
    Minimize Result().time (with epsilon-comparison), and
    break ties with Result().size, which is being overloaded to
    represent a single Spark resource amount (memory, partitions,
    CPU etc).
    Note: given how Result model is structured around a fixed set of
    objective function values, it seems that there's no clean way
    to support minimizing multiple kinds of resource usage.
    TODO: different tols for time and size
    """
    def __init__(self, rel_tol=1e-09, abs_tol=0.0, ranged_param_dict={}):
        """
        :param rel_tol: relative tolerance for math.isclose()
        :param abs_tol: absolute tolerance for math.isclose()
        :param ranged_param_dict: dict of ranged SparkParamType
        """
        super(SearchObjective, self).__init__()
        self.rel_tol = rel_tol
        self.abs_tol = abs_tol
        self.ranged_param_dict = ranged_param_dict

    def result_order_by_terms(self):
        """Return database columns required to order by the objective"""
        return [Result.time, Result.size]

    def result_compare(self, result1, result2):
        """cmp() compatible comparison of resultsdb.models.Result"""
        if Util.isclose(result1.time, result2.time,
                        self.rel_tol, self.abs_tol):
            return cmp(result1.size, result2.size)
        return cmp(result1.time, result2.time)

    def display(self, result):
        """
        Produce a string version of a resultsdb.models.Result()
        """
        return "time=%.2f, size=%.2f, config=%s" % \
               (result.time, result.size, str(result.configuration.data))

    @staticmethod
    def _ratio(a, b):
        return Util.ratio(a, b)

    def result_relative(self, result1, result2):
        """return None, or a relative goodness of resultsdb.models.Result"""
        if Util.isclose(result1.time, result2.time,
                        self.rel_tol, self.abs_tol):
            return MinimizeTimeAndResource._ratio(result1.size, result2.size)
        return MinimizeTimeAndResource._ratio(result1.time, result2.time)

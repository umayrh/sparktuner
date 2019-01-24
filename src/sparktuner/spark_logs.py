import re
import logging

log = logging.getLogger(__name__)


class SparkLogProcessor(object):
    """
    Processes Spark logs
    """
    def __init__(self, master, deploy_mode):
        self.yarn_app_id = ""
        self.is_yarn = master == "yarn"
        self.is_cluster_deploy_mode = deploy_mode == "cluster"

    def process_spark_submit_log(self, itr):
        """
        This function has been cribbed, with gratitude, from
        master/airflow/contrib/hooks/spark_submit_hook.py

        Processes the log files and extracts useful information out of it.
        If the deploy-mode is 'client', log the output of the submit
        command as those are the output logs of the Spark worker directly.
        Remark: If the driver needs to be tracked for its status, the
        log-level of the spark deploy needs to be at least INFO
        (log4j.logger.org.apache.spark.deploy=INFO)

        A potential way to use it is:
            ...
            stdout_result = self.the_io_thread_pool.apply_async(
                    self.log_collector.process_spark_submit_log,
                    args=(iter(cmd_process.stdout.readline, ''), )
                    )
            stdout_result.get()

        :param itr: An iterator which iterates over the input of the
        subprocess
        :return `str` representing a concatenation of all items in itr
        """
        spark_submit_stdout_log = []
        # Consume the iterator
        for line in itr:
            spark_submit_stdout_log.append(line)
            if not self.yarn_app_id and \
                    self.is_yarn and self.is_cluster_deploy_mode:
                line = line.strip()
                # If we run yarn cluster mode, we want to extract the
                # application id from the logs so we can kill the
                # application when we stop it unexpectedly
                match = re.search('(application[0-9_]+)', line)
                if match:
                    self.yarn_app_id = match.groups()[0]
                    log.info("Identified spark driver id: %s",
                             self.yarn_app_id)
        # Hopefully, the logs aren't eggregiously ginormous
        return ''.join(spark_submit_stdout_log)

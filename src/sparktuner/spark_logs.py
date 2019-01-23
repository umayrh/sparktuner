class SparkLogProcessor(object):
    """
    Parses Spark application logs
    """
    def parse_yarn_app_log(self):
        """
        Extracts YARN application id from logs
        :return:
        """
        raise NotImplementedError

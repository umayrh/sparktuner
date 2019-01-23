import os
import logging

from requests.compat import urljoin
from util import XmlParser, WebRequest, WebRequestError

log = logging.getLogger(__name__)


class YarnProperty(object):
    RM_ADDR = "yarn.resourcemanager.address"
    RM_WEBAPP_ADDR = "yarn.resourcemanager.webapp.address"
    RM_HAS_HA = "yarn.resourcemanager.ha.enabled"
    RM_PREFIX_WEBAPP_HTTPS = "yarn.resourcemanager.webapp.https.address."
    RM_PREFIX_WEBAPP_ADDR = "yarn.resourcemanager.webapp.address."
    RM_PREFIX_ADMIN_ADDR = "yarn.resourcemanager.admin.address."
    HTTP_POLICY = "yarn.http.policy"


class YarnResourceManager(object):
    DEFAULT_HEADER = {"accept": "application/json"}
    DEFAULT_PORTS = {"http": "8088", "https": "8090"}

    ROUTE_INFO = "/ws/v1/cluster/info"
    ROUTE_APP_ID = "/ws/v1/cluster/apps/"


class YarnMetricsError(Exception):
    pass


class YarnMetricsCollector(object):
    """
    Utilities for accessing YARN configuration parameters,
    and for querying YARN Resource Manager.

    This implementation follows Sparlyr:
    https://github.com/rstudio/sparklyr/blob/master/R/yarn_cluster.R

    See also YARN's ResourceManager API:
    https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
    """
    YARN_CONF_DIR = "YARN_CONF_DIR"
    HADOOP_CONF_DIR = "HADOOP_CONF_DIR"
    YARN_SITE = "yarn-site.xml"
    YARN_API_REQUEST_HEADER = {}

    @staticmethod
    def get_yarn_site_path():
        """
        Locates yarns-site.xml by using either the YARN_CONF_DIR
        or HADOOP_CONF_DIR environment variables.
        :return: the path, as a str, if the file exist. None otherwise
        """
        conf_dir = os.getenv(YarnMetricsCollector.YARN_CONF_DIR,
                             os.getenv(YarnMetricsCollector.HADOOP_CONF_DIR))
        if not conf_dir:
            return None

        yarn_site = os.path.join(conf_dir, YarnMetricsCollector.YARN_SITE)
        if os.path.exists(yarn_site):
            return yarn_site
        return None

    @staticmethod
    def get_yarn_property_map(yarn_site_path):
        """
        :param yarn_site_path: absolute path to yarn-site.xml
        :return: a dictionary of parameter names and values in the
        yarn-site.xml file, or an empty dict if path is an empty str.
        :raises XmlParserError if file not found or if XML cannot be parsed.
        """
        if not yarn_site_path:
            return {}
        xml_parsed = XmlParser.parse_file(yarn_site_path)
        # FIXME there must be a one-pass way to do this
        property_names = XmlParser.map_element_data(xml_parsed, 'name')
        property_values = XmlParser.map_element_data(xml_parsed, 'value')
        return dict(zip(property_names, property_values))

    @staticmethod
    def get_webapp_protocol(yarn_property_map):
        """
        :param yarn_property_map: dict of YARN properties
        :return: str ("http" or "https") representing
        YARN HTTP policy
        """
        proto = yarn_property_map.get(YarnProperty.HTTP_POLICY, "http")
        if proto.lower() == "https_only":
            return "https"
        return "http"

    @staticmethod
    def get_webapp_port(yarn_property_map):
        """
        :param yarn_property_map: dict of YARN properties
        :return: Default webapp port corresonding to YARN HTTP policy
        """
        return YarnResourceManager.DEFAULT_PORTS.get(
            YarnMetricsCollector.get_webapp_protocol(yarn_property_map))

    @staticmethod
    def _get_rm_ha_webapp_addr(yarn_webapp_proto, yarn_property_map):
        """
        TODO(unimplemented)
        :param yarn_webapp_proto: YARN HTTP protocol (http/https)
        :param yarn_property_map: dict of YARN properties
        :return: the web address of a live HA Resource Manager
        if server address can be found in yarn-site.xml and if
        the server is currently online.
        """
        raise NotImplementedError

    @staticmethod
    def _get_rm_webapp_addr(yarn_webapp_proto,
                            yarn_webapp_port,
                            yarn_property_map):
        """
        :param yarn_webapp_proto: YARN HTTP protocol (http/https)
        :param yarn_webapp_port: Resource Manager webapp port.
        :param yarn_property_map: dict of YARN properties
        :return: the web address of a live Resource Manager
        if server address can be found in yarn-site.xml and
        if the server is currently online.
        """
        yarn_rm_webapp_addr = yarn_property_map.get(
            YarnProperty.RM_WEBAPP_ADDR,
            yarn_property_map.get(
                YarnProperty.RM_ADDR,
                None))
        if not yarn_rm_webapp_addr:
            raise YarnMetricsError("No RM address in yarn-site.xml")
        # Remove the Hadoop IPC port (8032 e.g.) if it exists
        yarn_rm_webapp_addr = yarn_rm_webapp_addr.split(":")[0]
        # Return rm_addr if server is online
        try:
            # Check to see if the server is available
            resp = YarnMetricsCollector.get_yarn_info(yarn_webapp_proto,
                                                      yarn_rm_webapp_addr,
                                                      yarn_webapp_port)
            log.info("YARN cluster info: " + str(resp))
            return yarn_rm_webapp_addr
        except WebRequestError:
            raise YarnMetricsError("Cannot reach RM server")

    @staticmethod
    def get_rm_webapp_addr(yarn_webapp_proto,
                           yarn_webapp_port,
                           yarn_property_map):
        """
        :param yarn_webapp_proto: YARN HTTP protocol (http/https)
        :param yarn_webapp_port: Resource Manager webapp port.
        :param yarn_property_map: dict of YARN properties
        :return: the web address of a live Resource Manager
        if server address can be found in yarn-site.xml and
        if the server is currently online.
        """
        has_ha = yarn_property_map.get(YarnProperty.RM_HAS_HA, "false")
        if has_ha.lower() == "true":
            return YarnMetricsCollector._get_rm_ha_webapp_addr(
                yarn_webapp_proto, yarn_property_map)
        return YarnMetricsCollector._get_rm_webapp_addr(
            yarn_webapp_proto, yarn_webapp_port, yarn_property_map)

    @staticmethod
    def call_yarn_api(yarn_webapp_proto,
                      yarn_rm_addr,
                      yarn_rm_port,
                      yarn_rm_route,
                      req_data_dict=None):
        """
        From YARN REST API docs (as of 2019-01-19):
          "Currently the only fields used in the header is Accept and
          Accept-Encoding. Accept currently supports XML and JSON for
          the response type you accept. Accept-Encoding currently supports
          only gzip format and will return gzip compressed output if
          this is specified, otherwise output is uncompressed. All
          other header fields are ignored."
        For now, this function supports only JSOn responses.
        :param yarn_webapp_proto: YARN HTTP protocol (http/https)
        :param yarn_rm_addr: Resource Manager webapp domain
        :param yarn_rm_port: Resource Manager webapp port. Added
        to yarn_rm_addr iff yarn_rm_port is not None.
        :param yarn_rm_route: Resource Manager webapp route
        :param req_data_dict: HTTP request data dict
        :return: response content parsed as JSON
        """
        try:
            if yarn_rm_port:
                yarn_rm_addr = yarn_rm_addr + ":" + str(yarn_rm_port)
            resp = WebRequest.request_get(
                webapp=yarn_rm_addr,
                route=yarn_rm_route,
                data_dict=req_data_dict,
                scheme=yarn_webapp_proto,
                header_dict=YarnResourceManager.DEFAULT_HEADER)
            log.debug("YARN RM call to " + yarn_rm_route + ": " + str(resp))
            # Expecting only only json responses to avoid parsing
            # woeful XML.
            if resp.headers.get("content-type") != \
                    YarnResourceManager.DEFAULT_HEADER.get("accept"):
                raise YarnMetricsError("Cannot parse content-type " +
                                       resp.headers.get("content-type"))
            return resp.json()
        except WebRequestError:
            raise YarnMetricsError("API request failed")

    @staticmethod
    def get_yarn_info(yarn_webapp_proto,
                      yarn_rm_addr,
                      yarn_rm_port):
        """
        This function uses YARN Resource Manager's
        Cluster Information API. "The cluster information resource
        provides overall information about the cluster."
        :param yarn_webapp_proto: YARN HTTP protocol (http/https)
        :param yarn_rm_addr: Resource Manager webapp domain
        :param yarn_rm_port: Resource Manager webapp port.
        :param items: resource names to collect data for. If
        None, all information is collected.
        :return: a dict of resource name to resource values.
        """
        return YarnMetricsCollector.call_yarn_api(
            yarn_webapp_proto,
            yarn_rm_addr,
            yarn_rm_port,
            YarnResourceManager.ROUTE_INFO)

    @staticmethod
    def get_yarn_app_info(yarn_webapp_proto,
                          yarn_rm_addr,
                          yarn_rm_port,
                          yarn_app_id,
                          items=None):
        """
        This function uses YARN Resource Manager's
        Cluster Application API. "An application resource contains
        information about a particular application that was
        submitted to a cluster."
        :param yarn_webapp_proto: YARN HTTP protocol (http/https)
        :param yarn_rm_addr: Resource Manager webapp domain
        :param yarn_rm_port: Resource Manager webapp port.
        :param yarn_app_id: YARN application id
        :param items: resource names to collect data for. If
        None, all information is collected.
        :return: a dict of resource name to resource values.
        """
        app_route = urljoin(YarnResourceManager.ROUTE_APP_ID, yarn_app_id)

        info = YarnMetricsCollector.call_yarn_api(yarn_webapp_proto,
                                                  yarn_rm_addr,
                                                  yarn_rm_port,
                                                  app_route)["app"]
        if items:
            return {k: v for k, v in info.iteritems() if k in items}
        return info

    def __init__(self):
        # Some of these may raise, which shouldn't happen in ctor
        yarn_site_path = YarnMetricsCollector.get_yarn_site_path()
        yarn_property_map = YarnMetricsCollector.get_yarn_property_map(
            yarn_site_path)
        self.yarn_webapp_proto = YarnMetricsCollector.get_webapp_protocol(
            yarn_property_map)
        self.yarn_webapp_port = YarnMetricsCollector.get_webapp_port(
            yarn_property_map)
        self.yarn_rm_webapp_addr = YarnMetricsCollector.get_rm_webapp_addr(
            self.yarn_webapp_proto, self.yarn_webapp_port, yarn_property_map)

    def get_info(self):
        return YarnMetricsCollector.get_yarn_info(self.yarn_webapp_proto,
                                                  self.yarn_rm_webapp_addr,
                                                  self.yarn_webapp_port)

    def get_app_info(self, yarn_app_id, resp_items=None):
        return YarnMetricsCollector.get_yarn_app_info(self.yarn_webapp_proto,
                                                      self.yarn_rm_webapp_addr,
                                                      self.yarn_webapp_port,
                                                      yarn_app_id,
                                                      resp_items)

import os
import os.path
import unittest
import requests_mock

from requests.compat import urljoin
from sparktuner.util import TestUtil
from sparktuner.yarn_metrics import (YarnMetricsCollector,
                                     YarnProperty,
                                     YarnResourceManager)


class YarnMetricsConfigTest(unittest.TestCase):
    def setUp(self):
        self.yarn_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "resources")

    def test_get_yarn_site_path(self):
        with TestUtil.modified_environ(
                'YARN_CONF_DIR', HADOOP_CONF_DIR=self.yarn_dir):
            yarn_site_path = YarnMetricsCollector.get_yarn_site_path()
            self.assertIsNotNone(yarn_site_path)
            self.assertEqual(
                YarnMetricsCollector.YARN_SITE,
                os.path.basename(yarn_site_path))
            self.assertEqual(
                self.yarn_dir, os.path.dirname(yarn_site_path))

        with TestUtil.modified_environ(
                'HADOOP_CONF_DIR', YARN_CONF_DIR=self.yarn_dir):
            yarn_site_path = YarnMetricsCollector.get_yarn_site_path()
            self.assertIsNotNone(yarn_site_path)
            self.assertEqual(
                YarnMetricsCollector.YARN_SITE,
                os.path.basename(yarn_site_path))
            self.assertEqual(
                self.yarn_dir, os.path.dirname(yarn_site_path))

        with TestUtil.modified_environ(
                'YARN_CONF_DIR', 'HADOOP_CONF_DIR'):
            self.assertIsNone(YarnMetricsCollector.get_yarn_site_path())

    def test_get_yarn_property_map(self):
        yarn_site_path = os.path.join(
            self.yarn_dir, YarnMetricsCollector.YARN_SITE)
        yarn_properties = YarnMetricsCollector.get_yarn_property_map(
            yarn_site_path)

        self.assertIsNotNone(yarn_properties)
        self.assertEqual(
            "master",
            yarn_properties["yarn.resourcemanager.hostname"])
        self.assertEqual(
            "master:8032",
            yarn_properties["yarn.resourcemanager.address"])


class YarnMetricsServiceTest(unittest.TestCase):
    def setUp(self):
        self.yarn_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "resources")
        self.yarn_properties = YarnMetricsCollector.get_yarn_property_map(
            os.path.join(self.yarn_dir, YarnMetricsCollector.YARN_SITE))

    def test_get_webapp_protocol(self):
        proto = YarnMetricsCollector.get_webapp_protocol(
            self.yarn_properties)
        self.assertEqual("http", proto)

        proto = YarnMetricsCollector.get_webapp_protocol(
            {YarnProperty.HTTP_POLICY: "https_only"})
        self.assertEqual("https", proto)

    def test_get_webapp_port(self):
        port = YarnMetricsCollector.get_webapp_port(self.yarn_properties)
        self.assertEqual("8088", port)

        port = YarnMetricsCollector.get_webapp_port(
            {YarnProperty.HTTP_POLICY: "https_only"})
        self.assertEqual("8090", port)

    def test_get_rm_ha_webapp_addr(self):
        with self.assertRaises(NotImplementedError):
            YarnMetricsCollector._get_rm_ha_webapp_addr("", "")

    def rm_webapp_addr_helper(self, mocker, yarn_properties, addr_property):
        proto = YarnMetricsCollector.get_webapp_protocol(yarn_properties)
        port = YarnMetricsCollector.get_webapp_port(yarn_properties)
        addr = yarn_properties[addr_property]
        headers = {"content-type": "application/json"}

        mocker.get(urljoin(proto + "://" + addr + ":" + port,
                           YarnResourceManager.ROUTE_INFO),
                   json="[]", headers=headers)
        self.assertEqual(
            addr,
            YarnMetricsCollector._get_rm_webapp_addr(
                proto, port, yarn_properties)
        )

    @requests_mock.Mocker()
    def test_get_rm_webapp_addr(self, mocker):
        yarn_properties = {YarnProperty.RM_WEBAPP_ADDR: "spark.data"}
        self.rm_webapp_addr_helper(
            mocker, yarn_properties, YarnProperty.RM_WEBAPP_ADDR)

        yarn_properties = {YarnProperty.RM_ADDR: "10.1.5.2",
                           YarnProperty.HTTP_POLICY: "https_only"}
        self.rm_webapp_addr_helper(
            mocker, yarn_properties, YarnProperty.RM_ADDR)

    @requests_mock.Mocker()
    def test_get_yarn_info(self, mocker):
        proto, addr, port = ("http", "spark.data", "8088")
        expected_json_data = TestUtil.yarn_api_helper(
            self.yarn_dir,
            mocker,
            "yarn-resp-cluster-info.json",
            proto, addr, port,
            YarnResourceManager.ROUTE_INFO)
        self.assertDictEqual(
            expected_json_data,
            YarnMetricsCollector.get_yarn_info(proto, addr, port)
        )

    @requests_mock.Mocker()
    def test_get_yarn_app_info(self, mocker):
        proto, addr, port, app_id = ("http", "spark.data", "8088", "appid")
        expected_json_data = TestUtil.yarn_api_helper(
            self.yarn_dir,
            mocker,
            "yarn-resp-app-info.json",
            proto, addr, port,
            urljoin(YarnResourceManager.ROUTE_APP_ID, app_id))
        self.assertDictEqual(
            expected_json_data["app"],
            YarnMetricsCollector.get_yarn_app_info(proto, addr, port, app_id)
        )

    @requests_mock.Mocker()
    def test_get_info(self, mocker):
        # Need to set this up before yarn-site.xml
        # parsing since that involves calling the
        # api being tested here. _smh_
        expected_json_data = TestUtil.yarn_api_helper(
            self.yarn_dir,
            mocker,
            "yarn-resp-cluster-info.json",
            "http", "master", "8088",
            YarnResourceManager.ROUTE_INFO)
        with TestUtil.modified_environ(
                'YARN_CONF_DIR', HADOOP_CONF_DIR=self.yarn_dir):
            collector = YarnMetricsCollector()
            proto = collector.yarn_webapp_proto
            port = collector.yarn_webapp_port
            addr = collector.yarn_rm_webapp_addr
        self.assertDictEqual(
            expected_json_data,
            YarnMetricsCollector.get_yarn_info(proto, addr, port)
        )

    @requests_mock.Mocker()
    def test_get_app_info(self, mocker):
        TestUtil.yarn_api_helper(
            self.yarn_dir,
            mocker,
            "yarn-resp-cluster-info.json",
            "http", "master", "8088",
            YarnResourceManager.ROUTE_INFO)
        with TestUtil.modified_environ(
                'YARN_CONF_DIR', HADOOP_CONF_DIR=self.yarn_dir):
            collector = YarnMetricsCollector()
            proto = collector.yarn_webapp_proto
            port = collector.yarn_webapp_port
            addr = collector.yarn_rm_webapp_addr
        expected_json_data = TestUtil.yarn_api_helper(
            self.yarn_dir,
            mocker,
            "yarn-resp-app-info.json",
            proto, addr, port,
            urljoin(YarnResourceManager.ROUTE_APP_ID, "app_id"))
        self.assertDictEqual(
            expected_json_data["app"],
            YarnMetricsCollector.get_yarn_app_info(
                proto, addr, port, "app_id")
        )

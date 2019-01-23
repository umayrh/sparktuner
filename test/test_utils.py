"""Tests util module"""

import unittest
from sparktuner.util import Util, InvalidSize


class WebRequestTest(unittest.TestCase):
    @unittest.skip
    def test_query(self):
        pass

    @unittest.skip
    def test_parse_file(self):
        pass


class XmlParserTest(unittest.TestCase):
    @unittest.skip
    def test_parse_file(self):
        pass

    @unittest.skip
    def test_parse_string(self):
        pass

    @unittest.skip
    def test_map_elements_to_list(self):
        pass

    @unittest.skip
    def test_get_xml_element_data(self):
        pass


class UtilsTest(unittest.TestCase):
    @unittest.skip
    def test_isclose(self):
        pass

    @unittest.skip
    def test_ratio(self):
        pass

    def test_format_size(self):
        self.assertEqual("0b", Util.format_size(0))
        self.assertEqual("5b", Util.format_size(5))
        self.assertEqual("1k", Util.format_size(1024))
        self.assertEqual("10k", Util.format_size(10240))
        self.assertEqual("1m", Util.format_size(1024 ** 2))
        self.assertEqual("22m", Util.format_size(1024 ** 2 * 22))
        self.assertEqual("4g", Util.format_size(1024 ** 3 * 4))
        self.assertEqual("4218m", Util.format_size(int(1024 ** 3 * 4.12)))
        self.assertEqual("4t", Util.format_size(1024 ** 4 * 4))
        self.assertEqual("4218g", Util.format_size(int(1024 ** 4 * 4.12)))
        self.assertEqual("4p", Util.format_size(1024 ** 5 * 4))
        self.assertEqual("4218t", Util.format_size(int(1024 ** 5 * 4.12)))

        with self.assertRaises(InvalidSize):
            Util.format_size(1024 ** 3 * 4.12)

        self.assertEqual("4294967296b", Util.format_size(1024 ** 3 * 4, 'b'))
        self.assertEqual("4194304k", Util.format_size(1024 ** 3 * 4, 'k'))
        self.assertEqual("4096m", Util.format_size(1024 ** 3 * 4, 'm'))
        self.assertEqual("4g", Util.format_size(1024 ** 3 * 4, 'g'))
        self.assertEqual("4g", Util.format_size(1024 ** 3 * 4, 't'))
        self.assertEqual("4g", Util.format_size(1024 ** 3 * 4, 'p'))

        self.assertEqual("4096t", Util.format_size(1024 ** 5 * 4, 't'))
        self.assertEqual("4p", Util.format_size(1024 ** 5 * 4, 'p'))

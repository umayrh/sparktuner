"""Contains various data processing, parsing, or conversion utilities"""

import logging
import requests

from xml.dom import minidom
from xml.parsers.expat import ExpatError
from requests.compat import urljoin
from requests.utils import prepend_scheme_if_needed

log = logging.getLogger(__name__)


class WebRequestError(ValueError):
    pass


class WebRequest(object):
    """Utilities for making HTTP requests"""
    @staticmethod
    def request_get(webapp,
                    route,
                    data_dict=None,
                    scheme='',
                    header_dict={}):
        """
        :param webapp: web app address. If the address contains
        schema information, then the schema argument should be None.
        Port, if any, is assumed to already be appended to webapp.
        :param route: web application route
        :param data_dict: dict of url parameters if any
        :param scheme: web address scheme. If the address contains
        schema information, then this argument is ignored.
        :param header_dict: dict of header for request. Default: None.
        :return: the HTTP response object
        :raises WebRequestError if HTTP status is not 200
        """
        webapp_url = prepend_scheme_if_needed(webapp, scheme)
        url = urljoin(webapp_url, route)
        req = requests.get(url, params=data_dict, headers=header_dict)
        log.debug("HTTP get " + str(req.url))
        if req.status_code != 200:
            raise WebRequestError("Status code: " + str(req.status_code) +
                                  ", msg: " + req.text)
        return req

    def __init__(self, webapp):
        self.webapp = webapp

    def get(self, route, data_dict=None, scheme=None):
        """
        :param route: web application route
        :param data_dict: dict of url parameters if any
        :param scheme: web address scheme. If the address contains
        schema information, then this argument must be None.
        :return: the response contents
        :raises WebRequestError if HTTP status is not 200
        """
        return WebRequest.request_get(self.webapp, route, data_dict, scheme)


class XmlParserError(Exception):
    pass


class XmlParser(object):
    """
    Utilities for parsing XML data
    """
    @staticmethod
    def parse_file(file_path):
        """
        :param file_path: absolute path to an XML file
        :return: a parsed XML object
        :raises XmlParserError if file not found or
        if XML cannot be parsed.
        """
        try:
            return minidom.parse(file_path)
        except IOError as e:
            raise XmlParserError(e)
        except ExpatError as e:
            raise XmlParserError(e)

    @staticmethod
    def parse_string(data):
        """
        :param data: XML data as str
        :return: a parsed XML object
        :raises XmlParserError if XML cannot be parsed.
        """
        try:
            return minidom.parseString(data)
        except TypeError as e:
            raise XmlParserError(e)
        except ExpatError as e:
            raise XmlParserError(e)

    @staticmethod
    def map_element_data(xml_parsed, tag_name):
        """
        :param xml_parsed: a parsed XML object
        :param tag_name: (str) an XML tag name
        :return: the data in the first child for all
        elements matching a given tag name
        """
        return map(lambda e: e.childNodes[0].data,
                   xml_parsed.getElementsByTagName(tag_name))

    @staticmethod
    def get_element_data(xml_parsed, tag_name):
        """
        :param xml_parsed: a parsed XML object
        :param tag_name: (str) an XML tag name
        :return: the data in the first child for the first
        element matching a given tag name
        """
        return xml_parsed.getElementsByTagName(tag_name)[0].firstChild.data


class InvalidSize(Exception):
    ERROR_STR = "Size must be specified as bytes (b), " \
                "kibibytes (k), mebibytes (m), gibibytes (g), " \
                "tebibytes (t), or pebibytes(p). " \
                "E.g. 50b, 100kb, or 250mb."


class Util(object):
    """
    Utilities here may be available in other packages (such as 'humanfriendly'
    and 'humanize') but these are tailored for this package, and also avoid the
    non-essential requirements.
    """
    @staticmethod
    def ratio(a, b):
        """
        :param a: a number
        :param b: a number
        :return: the ratio of two numbers. If denominator is
        equal to 0, then return INFINITY * numerator.
        """
        if b == 0:
            return float('inf') * a
        return a / b

    @staticmethod
    def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
        """
        TODO: use Python 3's implementation once we've moved over
        :param a: a number
        :param b: a number
        :param rel_tol: relative tolerance (default: 1e-09)
        :param abs_tol: absolute tolerance (default: 0.0)
        :return: true iff the absolute difference between the given
        two numbers is within prescribed tolerance. False otherwise.
        """
        return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    @staticmethod
    def format_size(num_bytes, units=""):
        """
        This function formats sizes in bytes, kibibytes (k), mebibytes (m),
        gibibytes (g), tebibytes (t), or pebibytes(p). The format itself:
            a. contains only integral values,
            b. preserves, where possible, four significant figures
               to account for rounding errors, and
            c. converts to a byte string format that Spark finds
              readable. See Apache Spark's
              src/main/java/org/apache/spark/network/util/JavaUtils.java#L276
        Note that, in case of float values, the function doesn't round to avoid
        going out of some arbitrary min-max bounds.
        The choice of units in ('b', 'k', 'm', 'g', 't', 'p') instead of
        ('b', 'kb', 'mb', 'gb', 'tb', 'pb') was constrained by by JVM's format
        for heap size arguments (Xms and Xmx).

        Some examples:

        > format_size(0)
        '0b'
        > format_size(5)
        '5b'
        > format_size(1024)
        '1kb'
        > format_size(10240)
        '10kb'
        > format_size(1024 ** 3 * 4)
        '4gb'
        > format_size(int(1024 ** 3 * 4.12))
        '4218mb'

        :param num_bytes: a :class:`int` representing the number of bytes
        :param units: units to express the number bytes in. Must be a value
        in the set ('b', 'k', 'm', 'g', 't', 'p'), and defaults
        to an empty string allowing the function to choose an appropriate
        representation. The given unit may be ignored if it results in
        'significant' rounding error.
        :return: a human-readable, :class:`str` representation of the
        input number of bytes
        """
        if not isinstance(num_bytes, int):
            raise InvalidSize("Input bytes must be integral")

        num = num_bytes
        for x in ['b', 'k', 'm', 'g', 't']:
            if x == units or num < 1024 or (num != int(num) and num <= 9999):
                return "%d%s" % (num, x)
            num /= 1024.0
        return "%d%s" % (num, 'p')

    @staticmethod
    def parse_size(mem_str):
        """
        TODO implementing this would help remove dependency on
        the 'humanfriendly' package.
        :param mem_str: a `str` expressing a memory size.
        Size must be specified as bytes (b), kibibytes (k),
        mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p).
        E.g. 50b, 100kb, or 250mb.
        :return: the number of bytes represented as an int
        """
        raise NotImplementedError

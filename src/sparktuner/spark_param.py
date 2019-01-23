"""Defines different types of Spark parameters"""
import abc
from humanfriendly import parse_size, InvalidSize


class SparkParamParseError(Exception):
    """Represents Spark param value parsing errors"""
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

    @staticmethod
    def raise_if_empty(arg_value, msg="Empty range argument"):
        if len(arg_value) == 0:
            raise SparkParamParseError(IndexError, msg)

    @staticmethod
    def raise_invalid(msg="Invalid range argument"):
        raise SparkParamParseError(ValueError, msg)


class SparkParamType(object):
    """
    Abstract base class for Spark parameter types.
    """
    __metaclass__ = abc.ABCMeta

    RANGE_SEP = ","

    @staticmethod
    def get_value_map(arg_dict):
        """
        Returns the map from key to SparkParamType.value
        for a given key-SparkParamType dict
        """
        return {k: arg_dict[k].value for k in arg_dict}

    @staticmethod
    def get_param_map(param_dict, filter_lambda=lambda a: True):
        """
        :param param_dict: dict mapping to objects, including of
        type SparkParamType
        :param filter_lambda: one-arg lambda function that operates
        on a SparkParamType object
        :return: the map from key to SparkParamType iff both
        object type is SparkParamType and given lambda is True
        """
        return dict(filter(
            lambda p:
            isinstance(p[1], SparkParamType) and filter_lambda(p[1]),
            param_dict.items()))

    def __init__(self,
                 spark_name,
                 value,
                 maybe_range=False,
                 desc=""):
        """
        TODO
        :param spark_name: Spark parameter name
        :param value: parameter value (may be a default value)
        :param maybe_range: whether or not this parameter type accepts
        a range of values
        :param desc: parameter description, or meaning
        """
        self.spark_name = spark_name
        self.is_range_val = \
            maybe_range and isinstance(value, tuple) and value[0] < value[1]
        self.value = value
        # Inferring a non-range value that's supplied as a range e.g. (2, 2)
        if maybe_range and isinstance(value, tuple) and value[0] == value[1]:
            self.value = value[0]
        self.desc = desc

    def __str__(self):
        return ",".join([self.spark_name,
                         str(self.is_range_val),
                         str(type(self.value)),
                         str(self.value)])

    @abc.abstractmethod
    def cast_from_str(self, str_value):
        return str_value

    @abc.abstractmethod
    def make_param_from_str(self, str_value):
        """
        Creates a new SparkParamType initialized using this object but with
        value set by parsing the input string version of the value. str_value
        may be a point or a range value.
        """
        pass

    @abc.abstractmethod
    def make_param(self, value):
        """
        Creates a new SparkParamType initialized using this object but with
        value set using the given value, which must be a point (or, non-range)
        value, and hence not a tuple.
        """
        pass


class SparkNumericType(SparkParamType):
    @staticmethod
    def parse_range(str_value):
        SparkParamParseError.raise_if_empty(str_value)
        return str_value.split(SparkParamType.RANGE_SEP)

    @abc.abstractmethod
    def get_maybe_range_from_str(self, str_value):
        pass

    @abc.abstractmethod
    def make_param_from_str(self, str_value):
        pass

    def get_range_start(self):
        if self.is_range_val:
            return self.value[0]
        return self.value

    def get_range_end(self):
        if self.is_range_val:
            return self.value[1]
        return self.value

    def cast_range_1(self, range_list):
        return self.cast_from_str(range_list[0])

    def cast_range_2(self, range_list):
        return self.cast_from_str(range_list[0]), \
               self.cast_from_str(range_list[1])

    def cast_range_3(self, range_list):
        return self.cast_from_str(range_list[0]), \
               self.cast_from_str(range_list[1]), \
               self.cast_from_str(range_list[2])


class SparkStringType(SparkParamType):
    """
    String types are assumed to have fixed values
    """
    def __init__(self, name, value, desc):
        assert isinstance(value, str)
        super(SparkStringType, self).__init__(name, value, False, desc)

    def cast_from_str(self, str_value):
        return str_value

    def make_param_from_str(self, str_value):
        return SparkStringType(self.spark_name, str_value, self.desc)

    def make_param(self, value):
        return self.make_param_from_str(value)


class SparkIntType(SparkNumericType):
    @staticmethod
    def check_if_legal(int_value):
        assert isinstance(int_value, int) and int_value > 0

    def __init__(self, name, value, desc):
        assert isinstance(value, int) or isinstance(value, tuple)
        super(SparkIntType, self).__init__(name, value, True, desc)

    def cast_from_str(self, str_value):
        if str_value.isdigit():
            int_value = int(str_value)
            SparkIntType.check_if_legal(int_value)
            return int_value
        raise SparkParamParseError(ValueError, "Expected int value")

    def get_maybe_range_from_str(self, str_value):
        range_list = SparkNumericType.parse_range(str_value)
        range_len = len(range_list)
        if range_len == 1:
            return self.cast_range_1(range_list)
        elif range_len == 2:
            return self.cast_range_2(range_list)
        SparkParamParseError.raise_invalid()

    def make_param_from_str(self, str_value):
        return SparkIntType(self.spark_name,
                            self.get_maybe_range_from_str(str_value),
                            self.desc)

    def make_param(self, value):
        SparkIntType.check_if_legal(value)
        return SparkIntType(self.spark_name, value, self.desc)


class SparkMemoryType(SparkNumericType):
    def __init__(self, name, value, desc):
        assert isinstance(value, int) or isinstance(value, tuple)
        super(SparkMemoryType, self).__init__(name, value, True, desc)

    def get_scale(self):
        if isinstance(self.value, tuple):
            return self.value[2]
        return 1

    def cast_from_str(self, str_value):
        try:
            return parse_size(str_value, binary=True)
        except InvalidSize:
            raise SparkParamParseError(InvalidSize, "Invalid memory size")

    def get_maybe_range_from_str(self, str_value):
        range_list = SparkNumericType.parse_range(str_value)
        range_len = len(range_list)
        if range_len == 1:
            return self.cast_range_1(range_list)
        elif range_len == 2:
            (range_start, range_end) = self.cast_range_2(range_list)
            range_scale = 1
            return range_start, range_end, range_scale
        elif range_len == 3:
            return self.cast_range_3(range_list)
        SparkParamParseError.raise_invalid()

    def make_param_from_str(self, str_value):
        return SparkMemoryType(self.spark_name,
                               self.get_maybe_range_from_str(str_value),
                               self.desc)

    def make_param(self, value):
        SparkIntType.check_if_legal(value)
        return SparkMemoryType(self.spark_name, value, self.desc)


class SparkBooleanType(SparkParamType):
    @staticmethod
    def check_if_legal(value):
        assert isinstance(value, bool)

    def __init__(self, name, value, desc):
        assert isinstance(value, bool)
        super(SparkBooleanType, self).__init__(name, value, True, desc)

    def cast_from_str(self, str_value):
        lower_str = str_value.lower()
        if lower_str == "true":
            return True
        elif lower_str == "false":
            return False
        raise SparkParamParseError(ValueError, "Invalid boolean type")

    def make_param_from_str(self, str_value):
        return SparkBooleanType(self.spark_name,
                                self.cast_from_str(str_value),
                                self.desc)

    def make_param(self, value):
        SparkBooleanType.check_if_legal(value)
        return SparkBooleanType(self.spark_name, value, self.desc)

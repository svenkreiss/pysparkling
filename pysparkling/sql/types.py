#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from array import array
import ctypes
import datetime
import decimal
import itertools
import json as _json
import re
import sys

from ._row import create_row, Row

__all__ = [
    "DataType", "NullType", "StringType", "BinaryType", "BooleanType", "DateType",
    "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType",
    "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType"
]


class DataType:
    """Base class for data types."""

    def __repr__(self):
        return self.__class__.__name__

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def typeName(cls):
        return cls.__name__[:-4].lower()

    def simpleString(self):
        return self.typeName()

    def jsonValue(self):
        return self.typeName()

    def json(self):
        return _json.dumps(self.jsonValue(),
                           separators=(',', ':'),
                           sort_keys=True)

    def needConversion(self):
        """
        Return whether or not value of this type is converted by Spark
        between DF definition and internal SQL object.

        This is used to avoid the unnecessary conversion for ArrayType/MapType/StructType.
        """
        return False

    def toInternal(self, obj):
        """
        This method is not used by pysparkling

        Converts a Python object into an internal SQL object.
        """
        return obj

    def fromInternal(self, obj):
        """
        This method is not used by pysparkling

        Converts an internal SQL object into a native Python object.
        """
        return obj


# This singleton pattern does not work with pickle, you will get
# another object after pickle and unpickle
class DataTypeSingleton(type):
    """Metaclass for DataType"""

    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__()
        return cls._instances[cls]


class NullType(DataType, metaclass=DataTypeSingleton):
    """Null type.

    The data type representing None, used for the types that cannot be inferred.
    """


class AtomicType(DataType):
    """An internal type used to represent everything that is not
    null, UDTs, arrays, structs, and maps."""


class NumericType(AtomicType):
    """Numeric data types.
    """


class IntegralType(NumericType, metaclass=DataTypeSingleton):
    """Integral data types.
    """


class FractionalType(NumericType):
    """Fractional data types.
    """


class StringType(AtomicType, metaclass=DataTypeSingleton):
    """String data type.
    """


class BinaryType(AtomicType, metaclass=DataTypeSingleton):
    """Binary (byte array) data type."""


class BooleanType(AtomicType, metaclass=DataTypeSingleton):
    """Boolean data type."""


class DateType(AtomicType, metaclass=DataTypeSingleton):
    """Date (datetime.date) data type."""

    EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()

    def needConversion(self):
        return False


class TimestampType(AtomicType, metaclass=DataTypeSingleton):
    """Timestamp (datetime.datetime) data type."""

    def needConversion(self):
        return True

    def toInternal(self, obj):
        if obj.tzinfo is not None:
            return obj.astimezone()
        return obj


class DecimalType(FractionalType):
    """Decimal (decimal.Decimal) data type.

    The DecimalType must have fixed precision (the maximum total number of digits)
    and scale (the number of digits on the right of dot). For example, (5, 2) can
    support the value from [-999.99 to 999.99].

    The precision can be up to 38, the scale must be less or equal to precision.

    When create a DecimalType, the default precision and scale is (10, 0). When infer
    schema from decimal.Decimal objects, it will be DecimalType(38, 18).

    :param precision: the maximum total number of digits (default: 10)
    :param scale: the number of digits on right side of dot. (default: 0)
    """

    def __init__(self, precision=10, scale=0):
        self.precision = precision
        self.scale = scale
        self.hasPrecisionInfo = True  # this is public API

    def simpleString(self):
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def jsonValue(self):
        return "decimal(%d,%d)" % (self.precision, self.scale)

    def __repr__(self):
        return "DecimalType(%d,%d)" % (self.precision, self.scale)


class DoubleType(FractionalType, metaclass=DataTypeSingleton):
    """Double data type, representing double precision floats."""


class FloatType(FractionalType, metaclass=DataTypeSingleton):
    """Float data type, representing single precision floats."""


class ByteType(IntegralType):
    """Byte data type, i.e. a signed integer in a single byte.
    """

    def simpleString(self):
        return 'tinyint'


class IntegerType(IntegralType):
    """Int data type, i.e. a signed 32-bit integer.
    """

    def simpleString(self):
        return 'int'


class LongType(IntegralType):
    """Long data type, i.e. a signed 64-bit integer.

    If the values are beyond the range of [-9223372036854775808, 9223372036854775807],
    please use :class:`DecimalType`.
    """

    def simpleString(self):
        return 'bigint'


class ShortType(IntegralType):
    """Short data type, i.e. a signed 16-bit integer.
    """

    def simpleString(self):
        return 'smallint'


class ArrayType(DataType):
    """Array data type.

    :param elementType: :class:`DataType` of each element in the array.
    :param containsNull: boolean, whether the array can contain null (None) values.
    """

    def __init__(self, elementType, containsNull=True):
        """
        >>> ArrayType(StringType()) == ArrayType(StringType(), True)
        True
        >>> ArrayType(StringType(), False) == ArrayType(StringType())
        False
        """
        assert isinstance(elementType, DataType), \
            "elementType %s should be an instance of %s" % (elementType, DataType)
        self.elementType = elementType
        self.containsNull = containsNull

    def simpleString(self):
        return 'array<%s>' % self.elementType.simpleString()

    def __repr__(self):
        return "ArrayType(%s,%s)" % (self.elementType,
                                     str(self.containsNull).lower())

    def jsonValue(self):
        return {"type": self.typeName(),
                "elementType": self.elementType.jsonValue(),
                "containsNull": self.containsNull}

    # noinspection PyShadowingNames
    @classmethod
    def fromJson(cls, json):
        return ArrayType(_parse_datatype_json_value(json["elementType"]),
                         json["containsNull"])

    def needConversion(self):
        return self.elementType.needConversion()

    def toInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and [self.elementType.toInternal(v) for v in obj]

    def fromInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and [self.elementType.fromInternal(v) for v in obj]


class MapType(DataType):
    """Map data type.

    :param keyType: :class:`DataType` of the keys in the map.
    :param valueType: :class:`DataType` of the values in the map.
    :param valueContainsNull: indicates whether values can contain null (None) values.

    Keys in a map data type are not allowed to be null (None).
    """

    def __init__(self, keyType, valueType, valueContainsNull=True):
        """
        >>> (MapType(StringType(), IntegerType())
        ...        == MapType(StringType(), IntegerType(), True))
        True
        >>> (MapType(StringType(), IntegerType(), False)
        ...        == MapType(StringType(), FloatType()))
        False
        """
        assert isinstance(keyType, DataType), \
            "keyType %s should be an instance of %s" % (keyType, DataType)
        assert isinstance(valueType, DataType), \
            "valueType %s should be an instance of %s" % (valueType, DataType)
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def simpleString(self):
        return 'map<%s,%s>' % (self.keyType.simpleString(), self.valueType.simpleString())

    def __repr__(self):
        return "MapType(%s,%s,%s)" % (self.keyType, self.valueType,
                                      str(self.valueContainsNull).lower())

    def jsonValue(self):
        return {"type": self.typeName(),
                "keyType": self.keyType.jsonValue(),
                "valueType": self.valueType.jsonValue(),
                "valueContainsNull": self.valueContainsNull}

    # noinspection PyShadowingNames
    @classmethod
    def fromJson(cls, json):
        return MapType(_parse_datatype_json_value(json["keyType"]),
                       _parse_datatype_json_value(json["valueType"]),
                       json["valueContainsNull"])

    def needConversion(self):
        return self.keyType.needConversion() or self.valueType.needConversion()

    def toInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and dict((self.keyType.toInternal(k), self.valueType.toInternal(v))
                            for k, v in obj.items())

    def fromInternal(self, obj):
        if not self.needConversion():
            return obj
        return obj and dict((self.keyType.fromInternal(k), self.valueType.fromInternal(v))
                            for k, v in obj.items())


class StructField(DataType):
    """A field in :class:`StructType`.

    :param name: string, name of the field.
    :param dataType: :class:`DataType` of the field.
    :param nullable: boolean, whether the field can be null (None) or not.
    :param metadata: a dict from string to simple type that can be toInternald to JSON automatically
    """

    def __init__(self, name, dataType, nullable=True, metadata=None):
        """
        >>> (StructField("f1", StringType(), True)
        ...      == StructField("f1", StringType(), True))
        True
        >>> (StructField("f1", StringType(), True)
        ...      == StructField("f2", StringType(), True))
        False
        """
        assert isinstance(dataType, DataType), \
            "dataType %s should be an instance of %s" % (dataType, DataType)
        assert isinstance(name, str), "field name %s should be string" % name
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def simpleString(self):
        return '%s:%s' % (self.name, self.dataType.simpleString())

    def __repr__(self):
        return "StructField(%s,%s,%s)" % (self.name, self.dataType,
                                          str(self.nullable).lower())

    def jsonValue(self):
        return {"name": self.name,
                "type": self.dataType.jsonValue(),
                "nullable": self.nullable,
                "metadata": self.metadata}

    # noinspection PyShadowingNames
    @classmethod
    def fromJson(cls, json):
        return StructField(json["name"],
                           _parse_datatype_json_value(json["type"]),
                           json["nullable"],
                           json["metadata"])

    def needConversion(self):
        return self.dataType.needConversion()

    def toInternal(self, obj):
        return self.dataType.toInternal(obj)

    def fromInternal(self, obj):
        return self.dataType.fromInternal(obj)

    def typeName(self):
        raise TypeError(
            "StructField does not have typeName. "
            "Use typeName on its type explicitly instead.")


class StructType(DataType):
    """Struct type, consisting of a list of :class:`StructField`.

    This is the data type representing a :class:`Row`.

    Iterating a :class:`StructType` will iterate its :class:`StructField`\\s.
    A contained :class:`StructField` can be accessed by name or position.

    >>> struct1 = StructType([StructField("f1", StringType(), True)])
    >>> struct1["f1"]
    StructField(f1,StringType,true)
    >>> struct1[0]
    StructField(f1,StringType,true)
    """

    def __init__(self, fields=None):
        """
        >>> struct1 = StructType([StructField("f1", StringType(), True)])
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType([StructField("f1", StringType(), True)])
        >>> struct2 = StructType([StructField("f1", StringType(), True),
        ...     StructField("f2", IntegerType(), False)])
        >>> struct1 == struct2
        False
        """
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]
            assert all(isinstance(f, StructField) for f in fields), \
                "fields should be a list of StructField"
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)

    def add(self, field, data_type=None, nullable=True, metadata=None):
        """
        Construct a StructType by adding new elements to it to define the schema. The method accepts
        either:

            a) A single parameter which is a StructField object.
            b) Between 2 and 4 parameters as (name, data_type, nullable (optional),
               metadata(optional). The data_type parameter may be either a String or a
               DataType object.

        >>> struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)
        >>> struct2 = StructType([StructField("f1", StringType(), True), \\
        ...     StructField("f2", StringType(), True, None)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType().add(StructField("f1", StringType(), True))
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True
        >>> struct1 = StructType().add("f1", "string", True)
        >>> struct2 = StructType([StructField("f1", StringType(), True)])
        >>> struct1 == struct2
        True

        :param field: Either the name of the field or a StructField object
        :param data_type: If present, the DataType of the StructField to create
        :param nullable: Whether the field to add should be nullable (default True)
        :param metadata: Any additional metadata (default None)
        :return: a new updated StructType
        """
        if isinstance(field, StructField):
            self.fields.append(field)
            self.names.append(field.name)
        else:
            if isinstance(field, str) and data_type is None:
                raise ValueError("Must specify DataType if passing name of struct_field to create.")

            if isinstance(data_type, str):
                data_type_f = _parse_datatype_json_value(data_type)
            else:
                data_type_f = data_type
            self.fields.append(StructField(field, data_type_f, nullable, metadata))
            self.names.append(field)
        # Precalculated list of fields that need conversion with fromInternal/toInternal functions
        self._needConversion = [f.needConversion() for f in self]
        self._needSerializeAnyField = any(self._needConversion)
        return self

    def __iter__(self):
        """Iterate the fields"""
        return iter(self.fields)

    def __len__(self):
        """Return the number of fields."""
        return len(self.fields)

    def __getitem__(self, key):
        """Access fields by name or slice."""
        if isinstance(key, str):
            for field in self:
                if field.name == key:
                    return field
            raise KeyError(f'No StructField named {key}')
        if isinstance(key, int):
            try:
                return self.fields[key]
            except IndexError as e:
                raise IndexError('StructType index out of range') from e
        if isinstance(key, slice):
            return StructType(self.fields[key])
        raise TypeError('StructType keys should be strings, integers or slices')

    def simpleString(self):
        return 'struct<%s>' % (','.join(f.simpleString() for f in self))

    def treeString(self):
        """
        >>> schema = StructType.fromDDL('some_str: string, some_int: integer, some_date: date')
        >>> print(schema.treeString())
         |-- some_str: string (nullable = true)
         |-- some_int: integer (nullable = true)
         |-- some_date: date (nullable = true)

        >>> schema = StructType.fromDDL('some_str: string, arr: array<string>')
        >>> print(schema.treeString())
         |-- some_str: string (nullable = true)
         |-- arr: array (nullable = true)
         |    |-- element: string (containsNull = true)

        >>> schema = StructType.fromDDL('some_str: string, arr: array<array<string>>')
        >>> print(schema.treeString())
         |-- some_str: string (nullable = true)
         |-- arr: array (nullable = true)
         |    |-- element: array (containsNull = true)
         |    |    |-- element: string (containsNull = true)

        :return: str with the schema inside.
        """
        txt = []
        indent = 0

        def _dump(name: str, type_: DataType, nullable: bool, nullable_name: str = 'nullable') -> str:
            pre = ' |   ' * indent
            return f'{pre} |-- {name}: {type_.typeName()} ({nullable_name} = {"true" if nullable else "false"})'

        def _dump_array(data_type):
            if not isinstance(data_type, ArrayType):
                return []

            nonlocal indent

            txt = []

            indent += 1

            txt.append(
                _dump(
                    'element',
                    data_type.elementType,
                    nullable=data_type.containsNull,
                    nullable_name='containsNull'
                )
            )

            txt.extend(_dump_array(data_type.elementType))

            indent -= 1

            return txt

        for field in self.fields:
            data_type = field.dataType
            txt.append(_dump(field.name, data_type, field.nullable))
            txt.extend(_dump_array(data_type))

        return '\n'.join(txt)

    def __repr__(self):
        return ("StructType(List(%s))" %
                ",".join(str(field) for field in self))

    def jsonValue(self):
        return {"type": self.typeName(),
                "fields": [f.jsonValue() for f in self]}

    # noinspection PyShadowingNames
    @classmethod
    def fromJson(cls, json):
        return StructType([StructField.fromJson(f) for f in json["fields"]])

    def fieldNames(self):
        """
        Returns all field names in a list.

        >>> struct = StructType([StructField("f1", StringType(), True)])
        >>> struct.fieldNames()
        ['f1']
        """
        return list(self.names)

    def needConversion(self):
        # We need convert Row()/namedtuple into tuple()
        return True

    def toInternal(self, obj):
        if obj is None:
            return None

        if self._needSerializeAnyField:
            return self.to_serialized_internal(obj)

        if isinstance(obj, dict):
            return tuple(obj.get(n) for n in self.names)
        if isinstance(obj, Row):
            return obj
        if isinstance(obj, (list, tuple)):
            return tuple(obj)
        if hasattr(obj, "__dict__"):
            d = obj.__dict__
            return tuple(d.get(n) for n in self.names)
        raise ValueError("Unexpected tuple %r with StructType" % obj)

    def to_serialized_internal(self, obj):
        # Only calling toInternal function for fields that need conversion
        if isinstance(obj, dict):
            return tuple(f.toInternal(obj.get(n)) if c else obj.get(n)
                         for n, f, c in zip(self.names, self.fields, self._needConversion))
        if isinstance(obj, Row):
            return create_row(
                obj.__fields__,
                (f.toInternal(val) for f, val, c in zip(self.fields, obj, self._needConversion))
            )
        if isinstance(obj, (tuple, list)):
            return tuple(f.toInternal(v) if c else v
                         for f, v, c in zip(self.fields, obj, self._needConversion))
        if hasattr(obj, "__dict__"):
            d = obj.__dict__
            return tuple(f.toInternal(d.get(n)) if c else d.get(n)
                         for n, f, c in zip(self.names, self.fields, self._needConversion))
        raise ValueError("Unexpected tuple %r with StructType" % obj)

    def fromInternal(self, obj):
        if obj is None:
            return None
        if isinstance(obj, Row):
            # it's already converted by pickler
            return obj
        if self._needSerializeAnyField:
            # Only calling fromInternal function for fields that need conversion
            values = [f.fromInternal(v) if c else v
                      for f, v, c in zip(self.fields, obj, self._needConversion)]
        else:
            values = obj
        return create_row(self.names, values)

    @classmethod
    def fromDDL(cls, string):
        def get_class(type_: str) -> DataType:
            type_to_load = f'{type_.strip().title()}Type'

            if type_to_load not in globals():
                match = re.match(r'^\s*array\s*<(.*)>\s*$', type_, flags=re.IGNORECASE)
                if match:
                    return ArrayType(get_class(match.group(1)))

                raise ValueError(f"Couldn't find '{type_to_load}'?")

            return globals()[type_to_load]()

        fields = StructType()

        for description in string.split(','):
            name, type_ = [x.strip() for x in description.split(':')]

            fields.add(StructField(name.strip(), get_class(type_), True))

        return fields


class UserDefinedType(DataType):
    """User-defined type (UDT).

    .. note:: WARN: Spark Internal Use Only
    """

    @classmethod
    def typeName(cls):
        return cls.__name__.lower()

    @classmethod
    def sqlType(cls):
        """
        Underlying SQL storage type for this UDT.
        """
        raise NotImplementedError("UDT must implement sqlType().")

    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        raise NotImplementedError("UDT must implement module().")

    @classmethod
    def scalaUDT(cls):
        """
        The class name of the paired Scala UDT (could be '', if there
        is no corresponding one).
        """
        return ''

    def needConversion(self):
        return True

    @classmethod
    def _cachedSqlType(cls):
        """
        Cache the sqlType() into class, because it's heavy used in `toInternal`.
        """
        if not hasattr(cls, "_cached_sql_type"):
            cls._cached_sql_type = cls.sqlType()
        return cls._cached_sql_type

    def toInternal(self, obj):
        if obj is not None:
            return self._cachedSqlType().toInternal(self.serialize(obj))
        return None

    def fromInternal(self, obj):
        v = self._cachedSqlType().fromInternal(obj)
        if v is not None:
            return self.deserialize(v)
        return None

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        raise NotImplementedError("UDT must implement toInternal().")

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        raise NotImplementedError("UDT must implement fromInternal().")

    def simpleString(self):
        return 'udt'

    def json(self):
        return _json.dumps(self.jsonValue(), separators=(',', ':'), sort_keys=True)

    def jsonValue(self):
        if self.scalaUDT():
            assert self.module() != '__main__', 'UDT in __main__ cannot work with ScalaUDT'
            schema = {
                "type": "udt",
                "class": self.scalaUDT(),
                "pyClass": "%s.%s" % (self.module(), type(self).__name__),
                "sqlType": self.sqlType().jsonValue()
            }
        else:
            raise NotImplementedError("pysparkling does not implement jsonValue() for UDT")
            # ser = CloudPickleSerializer()
            # b = ser.dumps(type(self))
            # schema = {
            #     "type": "udt",
            #     "pyClass": "%s.%s" % (self.module(), type(self).__name__),
            #     "serializedClass": base64.b64encode(b).decode('utf8'),
            #     "sqlType": self.sqlType().jsonValue()
            # }
        return schema

    # noinspection PyShadowingNames
    @classmethod
    def fromJson(cls, json):
        pyUDT = str(json["pyClass"])  # convert unicode to str
        split = pyUDT.rfind(".")
        pyModule = pyUDT[:split]
        pyClass = pyUDT[split + 1:]
        m = __import__(pyModule, globals(), locals(), [pyClass])
        if not hasattr(m, pyClass):
            raise NotImplementedError("pysparkling does not implement fromJson() for UDT")
            # s = base64.b64decode(json['serializedClass'].encode('utf-8'))
            # UDT = CloudPickleSerializer().loads(s)
        UDT = getattr(m, pyClass)
        return UDT()

    def __eq__(self, other):
        return isinstance(self, type(other))


_atomic_types = [StringType, BinaryType, BooleanType, DecimalType, FloatType, DoubleType,
                 ByteType, ShortType, IntegerType, LongType, DateType, TimestampType, NullType]
_all_atomic_types = dict((t.typeName(), t) for t in _atomic_types)
_all_complex_types = dict((v.typeName(), v)
                          for v in [ArrayType, MapType, StructType])

_FIXED_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(-?\d+)\s*\)")


def _parse_datatype_string(s):
    """
    Parses the given data type string to a :class:`DataType`. The data type string format equals
    to :class:`DataType.simpleString`, except that top level struct type can omit
    the ``struct<>`` and atomic types use ``typeName()`` as their format, e.g. use ``byte`` instead
    of ``tinyint`` for :class:`ByteType`. We can also use ``int`` as a short name
    for :class:`IntegerType`. Since Spark 2.3, this also supports a schema in a DDL-formatted
    string and case-insensitive strings.
    """
    raise NotImplementedError("_parse_datatype_string is not yet supported by pysparkling")
    # pylint: disable=W0511
    # todo: implement in pure Python the code below
    # NB: it probably requires to use antl4r

    # sc = Context._active_spark_context
    #
    # def from_ddl_schema(type_str):
    #     return _parse_datatype_json_string(
    #         sc._jvm.org.apache.spark.sql.types.StructType.fromDDL(type_str).json())
    #
    # def from_ddl_datatype(type_str):
    #     return _parse_datatype_json_string(
    #         sc._jvm.org.apache.spark.sql.api.python.PythonSQLUtils.parseDataType(type_str).json())
    #
    # try:
    #     # DDL format, "fieldname datatype, fieldname datatype".
    #     return from_ddl_schema(s)
    # except Exception as e:
    #     try:
    #         # For backwards compatibility, "integer", "struct<fieldname: datatype>" and etc.
    #         return from_ddl_datatype(s)
    #     except:
    #         try:
    #             # For backwards compatibility, "fieldname: datatype, fieldname: datatype" case.
    #             return from_ddl_datatype("struct<%s>" % s.strip())
    #         except:
    #             raise e


def _parse_datatype_json_string(json_string):
    """Parses the given data type JSON string."""
    return _parse_datatype_json_value(_json.loads(json_string))


def _parse_datatype_json_value(json_value):
    if not isinstance(json_value, dict):
        if json_value in _all_atomic_types.keys():
            return _all_atomic_types[json_value]()
        if json_value == 'decimal':
            return DecimalType()
        if _FIXED_DECIMAL.match(json_value):
            m = _FIXED_DECIMAL.match(json_value)
            return DecimalType(int(m.group(1)), int(m.group(2)))
        raise ValueError("Could not parse datatype: %s" % json_value)
    tpe = json_value["type"]
    if tpe in _all_complex_types:
        return _all_complex_types[tpe].fromJson(json_value)
    if tpe == 'udt':
        return UserDefinedType.fromJson(json_value)
    raise ValueError("not supported type: %s" % tpe)


# Mapping Python types to Spark SQL DataType
_type_mappings = {
    type(None): NullType,
    bool: BooleanType,
    int: LongType,
    float: DoubleType,
    str: StringType,
    bytearray: BinaryType,
    decimal.Decimal: DecimalType,
    datetime.date: DateType,
    datetime.datetime: TimestampType,
    datetime.time: TimestampType,
    bytes: BinaryType,
}

# Mapping Python array types to Spark SQL DataType
# We should be careful here. The size of these types in python depends on C
# implementation. We need to make sure that this conversion does not lose any
# precision. Also, JVM only support signed types, when converting unsigned types,
# keep in mind that it required 1 more bit when stored as singed types.
#
# Reference for C integer size, see:
# ISO/IEC 9899:201x specification, chapter 5.2.4.2.1 Sizes of integer types <limits.h>.
# Reference for python array typecode, see:
# https://docs.python.org/2/library/array.html
# https://docs.python.org/3.6/library/array.html
# Reference for JVM's supported integral types:
# http://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.3.1

_array_signed_int_typecode_ctype_mappings = {
    'b': ctypes.c_byte,
    'h': ctypes.c_short,
    'i': ctypes.c_int,
    'l': ctypes.c_long,
}

_array_unsigned_int_typecode_ctype_mappings = {
    'B': ctypes.c_ubyte,
    'H': ctypes.c_ushort,
    'I': ctypes.c_uint,
    'L': ctypes.c_ulong
}


# noinspection PyShadowingNames
def _int_size_to_type(size):
    """
    Return the Catalyst datatype from the size of integers.
    """
    if size <= 8:
        return ByteType
    if size <= 16:
        return ShortType
    if size <= 32:
        return IntegerType
    if size <= 64:
        return LongType
    return None


# The list of all supported array typecodes is stored here
_array_type_mappings = {
    # Warning: Actual properties for float and double in C is not specified in C.
    # On almost every system supported by both python and JVM, they are IEEE 754
    # single-precision binary floating-point format and IEEE 754 double-precision
    # binary floating-point format. And we do assume the same thing here for now.
    'f': FloatType,
    'd': DoubleType
}

# compute array typecode mappings for signed integer types
for _typecode in _array_signed_int_typecode_ctype_mappings:
    _size = ctypes.sizeof(_array_signed_int_typecode_ctype_mappings[_typecode]) * 8
    _dt = _int_size_to_type(_size)
    if _dt is not None:
        _array_type_mappings[_typecode] = _dt

# compute array typecode mappings for unsigned integer types
for _typecode in _array_unsigned_int_typecode_ctype_mappings:
    # JVM does not have unsigned types, so use signed types that is at least 1
    # bit larger to store
    _size = ctypes.sizeof(_array_unsigned_int_typecode_ctype_mappings[_typecode]) * 8 + 1
    _dt = _int_size_to_type(_size)
    if _dt is not None:
        _array_type_mappings[_typecode] = _dt

# Type code 'u' in Python's array is deprecated since version 3.3, and will be
# removed in version 4.0. See: https://docs.python.org/3/library/array.html
if sys.version_info[0] < 4:
    _array_type_mappings['u'] = StringType


def _infer_type(obj):
    """Infer the DataType from obj
    """
    if obj is None:
        return NullType()

    if hasattr(obj, '__UDT__'):
        return obj.__UDT__

    dataType = _type_mappings.get(type(obj))
    if dataType is DecimalType:
        # the precision and scale of `obj` may be different from row to row.
        return DecimalType(38, 18)
    if dataType is not None:
        return dataType()

    struct_type = _infer_struct_type(obj)
    if struct_type is not None:
        return struct_type

    try:
        return _infer_schema(obj)
    except TypeError as e:
        raise TypeError("not supported type: %s" % type(obj)) from e


def _infer_struct_type(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key is not None and value is not None:
                return MapType(_infer_type(key), _infer_type(value), True)
        return MapType(NullType(), NullType(), True)
    if isinstance(obj, list):
        for v in obj:
            if v is not None:
                return ArrayType(_infer_type(obj[0]), True)
        return ArrayType(NullType(), True)
    if isinstance(obj, array):
        if obj.typecode in _array_type_mappings:
            return ArrayType(_array_type_mappings[obj.typecode](), False)
        raise TypeError("not supported type: array(%s)" % obj.typecode)
    return None


def _infer_schema(row, names=None):
    """Infer the schema from dict/namedtuple/object"""
    if isinstance(row, dict):
        items = sorted(row.items())

    elif isinstance(row, (tuple, list)):
        if hasattr(row, "__fields__"):  # Row
            items = zip(row.__fields__, tuple(row))
        elif hasattr(row, "_fields"):  # namedtuple
            # noinspection PyProtectedMember
            items = zip(row._fields, tuple(row))
        else:
            if names is None:
                names = ['_%d' % i for i in range(1, len(row) + 1)]
            elif len(names) < len(row):
                names.extend('_%d' % i for i in range(len(names) + 1, len(row) + 1))
            items = zip(names, row)

    elif hasattr(row, "__dict__"):  # object
        items = sorted(row.__dict__.items())

    else:
        raise TypeError("Can not infer schema for type: %s" % type(row))

    fields = [StructField(k, _infer_type(v), True) for k, v in items]
    return StructType(fields)


# noinspection PyShadowingNames
def _has_nulltype(dt):
    """ Return whether there is NullType in `dt` or not """
    if isinstance(dt, StructType):
        return any(_has_nulltype(f.dataType) for f in dt.fields)
    if isinstance(dt, ArrayType):
        return _has_nulltype(dt.elementType)
    if isinstance(dt, MapType):
        return _has_nulltype(dt.keyType) or _has_nulltype(dt.valueType)
    return isinstance(dt, NullType)


def _get_null_fields(field, prefix=""):
    """ Return all field names which have a NullType in `field`"""
    if isinstance(field, StructType):
        return list(itertools.chain(*(
            _get_null_fields(f) for f in field.fields
        )))

    if prefix:
        prefixed_field_name = f"{prefix}.{field.name}"
    else:
        prefixed_field_name = field.name

    if isinstance(field, ArrayType):
        return _get_null_fields(field.elementType, prefix=prefixed_field_name + ".elements")

    if isinstance(field, MapType):
        return [
            _get_null_fields(field.keyType, prefix=prefixed_field_name + ".keys")
            + _get_null_fields(field.valueType, prefix=prefixed_field_name + ".values")
        ]

    return [prefixed_field_name] if isinstance(field.dataType, NullType) else []


def _merge_type(a, b, name=None):
    if name is None:
        new_msg = lambda msg: msg
        new_name = lambda n: "field %s" % n
    else:
        new_msg = lambda msg: "%s: %s" % (name, msg)
        new_name = lambda n: "field %s in %s" % (n, name)

    if isinstance(a, NullType):
        return b
    if isinstance(b, NullType):
        return a
    if type(a) is not type(b):
        # pylint: disable=W0511
        # TODO: type cast (such as int -> long)
        raise TypeError(new_msg("Can not merge type %s and %s" % (type(a), type(b))))

    # same type
    if isinstance(a, StructType):
        nfs = dict((f.name, f.dataType) for f in b.fields)
        fields = [StructField(f.name, _merge_type(f.dataType, nfs.get(f.name, NullType()),
                                                  name=new_name(f.name)))
                  for f in a.fields]
        names = {f.name for f in fields}
        for n in nfs:
            if n not in names:
                fields.append(StructField(n, nfs[n]))
        return StructType(fields)

    if isinstance(a, ArrayType):
        return ArrayType(_merge_type(a.elementType, b.elementType,
                                     name='element in array %s' % name), True)

    if isinstance(a, MapType):
        return MapType(_merge_type(a.keyType, b.keyType, name='key of map %s' % name),
                       _merge_type(a.valueType, b.valueType, name='value of map %s' % name),
                       True)
    return a


def _need_converter(dataType):
    if isinstance(dataType, StructType):
        return True
    if isinstance(dataType, ArrayType):
        return _need_converter(dataType.elementType)
    if isinstance(dataType, MapType):
        return _need_converter(dataType.keyType) or _need_converter(dataType.valueType)
    if isinstance(dataType, NullType):
        return True
    return False


def _create_converter(dataType):
    """Create a converter to drop the names of fields in obj
    :type dataType: Union[DataType, StructField]
    """
    if not _need_converter(dataType):
        return lambda x: x

    if isinstance(dataType, ArrayType):
        conv = _create_converter(dataType.elementType)
        return lambda row: [conv(v) for v in row]

    if isinstance(dataType, MapType):
        kconv = _create_converter(dataType.keyType)
        vconv = _create_converter(dataType.valueType)
        return lambda row: dict((kconv(k), vconv(v)) for k, v in row.items())

    if isinstance(dataType, NullType):
        return lambda x: None

    if not isinstance(dataType, StructType):
        return lambda x: x

    # dataType must be StructType
    names = [f.name for f in dataType.fields]
    converters = [_create_converter(f.dataType) for f in dataType.fields]
    convert_fields = any(_need_converter(f.dataType) for f in dataType.fields)

    def convert_struct(obj):
        if obj is None:
            return None

        if isinstance(obj, Row):
            if convert_fields:
                return create_row(
                    obj.__fields__,
                    [converter(value) for converter, value in zip(converters, obj)]
                )
            return obj

        if isinstance(obj, (tuple, list)):
            if convert_fields:
                return tuple(c(v) for v, c in zip(obj, converters))
            return tuple(obj)

        return convert_dict(obj)

    def convert_dict(obj):
        if isinstance(obj, dict):
            d = obj
        elif hasattr(obj, "__dict__"):  # object
            d = obj.__dict__
        else:
            raise TypeError("Unexpected obj type: %s" % type(obj))

        if convert_fields:
            return tuple(convert(d.get(name)) for name, convert in zip(names, converters))

        return tuple(d.get(name) for name in names)

    return convert_struct


_acceptable_types = {
    BooleanType: (bool,),
    ByteType: (int,),
    ShortType: (int,),
    IntegerType: (int,),
    LongType: (int,),
    FloatType: (float,),
    DoubleType: (float,),
    DecimalType: (decimal.Decimal,),
    StringType: (str,),
    BinaryType: (bytearray, bytes),
    DateType: (datetime.date, datetime.datetime,),
    TimestampType: (datetime.datetime,),
    ArrayType: (list, tuple, array,),
    MapType: (dict,),
    StructType: (tuple, list, dict,),
}


# pylint: disable=too-many-locals, too-many-branches, too-many-statements
def _make_type_verifier(dataType, nullable=True, name=None):
    """
    Make a verifier that checks the type of obj against dataType and raises a TypeError if they do
    not match.

    This verifier also checks the value of obj against datatype and raises a ValueError if it's not
    within the allowed range, e.g. using 128 as ByteType will overflow. Note that, Python float is
    not checked, so it will become infinity when cast to Java float if it overflows.

    >>> _make_type_verifier(StructType([]))(None)
    >>> _make_type_verifier(StringType())("")
    >>> _make_type_verifier(LongType())(0)
    >>> _make_type_verifier(ArrayType(ShortType()))(list(range(3)))
    >>> _make_type_verifier(ArrayType(StringType()))(set()) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    TypeError:...
    >>> _make_type_verifier(MapType(StringType(), IntegerType()))({})
    >>> _make_type_verifier(StructType([]))(())
    >>> _make_type_verifier(StructType([]))([])
    >>> _make_type_verifier(StructType([]))([1]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    ValueError:...
    >>> # Check if numeric values are within the allowed range.
    >>> _make_type_verifier(ByteType())(12)
    >>> _make_type_verifier(ByteType())(1234) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    ValueError:...
    >>> _make_type_verifier(ByteType(), False)(None) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    ValueError:...
    >>> _make_type_verifier(
    ...  ArrayType(ShortType(), False)
    ... )([1, None]) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    ValueError:...
    >>> _make_type_verifier(
    ...  MapType(StringType(), IntegerType())
    ... )({None: 1}) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    ValueError:...
    >>> schema = StructType().add("a", IntegerType()).add("b", StringType(), False)
    >>> _make_type_verifier(schema)((1, None)) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    ValueError:...
    """

    if name is None:
        new_msg = lambda msg: msg
        new_name = lambda n: "field %s" % n
    else:
        new_msg = lambda msg: "%s: %s" % (name, msg)
        new_name = lambda n: "field %s in %s" % (n, name)

    def verify_nullability(obj):
        if obj is None:
            if nullable:
                return True

            raise ValueError(new_msg("This field is not nullable, but got None"))

        return False

    _type = type(dataType)

    def assert_acceptable_types(obj):
        assert _type in _acceptable_types, \
            new_msg("unknown datatype: %s for object %r" % (dataType, obj))

    def verify_acceptable_types(obj):
        # subclass of them can not be fromInternal in JVM
        if type(obj) not in _acceptable_types[_type]:
            raise TypeError(new_msg("%s can not accept object %r in type %s"
                                    % (dataType, obj, type(obj))))

    if isinstance(dataType, StringType):
        # StringType can work with any types
        verify_value = lambda _: _

    elif isinstance(dataType, UserDefinedType):
        verifier = _make_type_verifier(dataType.sqlType(), name=name)

        def verify_udf(obj):
            if not (hasattr(obj, '__UDT__') and obj.__UDT__ == dataType):
                raise ValueError(new_msg("%r is not an instance of type %r" % (obj, dataType)))
            verifier(dataType.toInternal(obj))

        verify_value = verify_udf

    elif isinstance(dataType, ByteType):
        def verify_byte(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -128 or obj > 127:
                raise ValueError(new_msg("object of ByteType out of range, got: %s" % obj))

        verify_value = verify_byte

    elif isinstance(dataType, ShortType):
        def verify_short(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -32768 or obj > 32767:
                raise ValueError(new_msg("object of ShortType out of range, got: %s" % obj))

        verify_value = verify_short

    elif isinstance(dataType, IntegerType):
        def verify_integer(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -2147483648 or obj > 2147483647:
                raise ValueError(
                    new_msg("object of IntegerType out of range, got: %s" % obj))

        verify_value = verify_integer

    elif isinstance(dataType, LongType):
        def verify_long(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            if obj < -9223372036854775808 or obj > 9223372036854775807:
                raise ValueError(
                    new_msg("object of LongType out of range, got: %s" % obj))

        verify_value = verify_long

    elif isinstance(dataType, ArrayType):
        element_verifier = _make_type_verifier(
            dataType.elementType, dataType.containsNull, name="element in array %s" % name)

        def verify_array(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            for i in obj:
                element_verifier(i)

        verify_value = verify_array

    elif isinstance(dataType, MapType):
        key_verifier = _make_type_verifier(dataType.keyType, False, name="key of map %s" % name)
        value_verifier = _make_type_verifier(
            dataType.valueType, dataType.valueContainsNull, name="value of map %s" % name)

        def verify_map(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)
            for k, v in obj.items():
                key_verifier(k)
                value_verifier(v)

        verify_value = verify_map

    elif isinstance(dataType, StructType):
        verifiers = []
        for f in dataType.fields:
            verifier = _make_type_verifier(f.dataType, f.nullable, name=new_name(f.name))
            verifiers.append((f.name, verifier))

        def verify_struct(obj):
            assert_acceptable_types(obj)

            if isinstance(obj, dict):
                for f, verifier in verifiers:
                    verifier(obj.get(f))
            elif isinstance(obj, (tuple, list)):
                if len(obj) != len(verifiers):
                    raise ValueError(
                        new_msg("Length of object (%d) does not match with "
                                "length of fields (%d)" % (len(obj), len(verifiers))))
                for v, (_, verifier) in zip(obj, verifiers):
                    verifier(v)
            elif hasattr(obj, "__dict__"):
                d = obj.__dict__
                for f, verifier in verifiers:
                    verifier(d.get(f))
            else:
                raise TypeError(new_msg("StructType can not accept object %r in type %s"
                                        % (obj, type(obj))))
        verify_value = verify_struct

    else:
        def verify_default(obj):
            assert_acceptable_types(obj)
            verify_acceptable_types(obj)

        verify_value = verify_default

    def verify(obj):
        if not verify_nullability(obj):
            verify_value(obj)

    return verify

# This is such an important import, I try to make it NOT be depending on ANYTHING else...


def row_from_keyed_values(keyed_values, metadata=None):
    """
    Create a Row from a list of tuples where the first element
    is the field name and the second its value.

    :type keyed_values: iterable
    :type metadata: Optional[dict]
    :return: pysparkling.sql.Row
    """
    # keyed_values might be an iterable
    keyed_values = tuple(keyed_values)
    values = (value for key, value in keyed_values)
    fields = (key for key, value in keyed_values)
    return create_row(fields, values, metadata)


def create_row(fields, values, metadata=None):
    """
    Create a Row from a list of fields and the corresponding list of values

    This functions preserves field order and duplicates, unlike Row(dict)

    :type fields: iterable
    :type values: iterable
    :type metadata: Optional[dict]
    :return: pysparkling.sql.Row
    """
    new_row = tuple.__new__(Row, values)
    new_row.__fields__ = tuple(fields)
    new_row.set_metadata(metadata)
    return new_row


class Row(tuple):
    """
    A row in L{DataFrame}.
    The fields in it can be accessed:

    * like attributes (``row.key``)
    * like dictionary values (``row[key]``)

    ``key in row`` will search through row keys.

    Row can be used to create a row object by using named arguments,
    the fields will be sorted by names. It is not allowed to omit
    a named argument to represent the value is None or missing. This should be
    explicitly set to None in this case.

    >>> row = Row(name="Alice", age=11)
    >>> row
    Row(age=11, name='Alice')
    >>> row['name'], row['age']
    ('Alice', 11)
    >>> row.name, row.age
    ('Alice', 11)
    >>> 'name' in row
    True
    >>> 'wrong_key' in row
    False

    Row also can be used to create another Row like class, then it
    could be used to create Row objects, such as

    >>> Person = Row("name", "age")
    >>> Person
    <Row(name, age)>
    >>> 'name' in Person
    True
    >>> 'wrong_key' in Person
    False
    >>> Person("Alice", 11)
    Row(name='Alice', age=11)
    """
    _metadata = None

    def __new__(cls, *args, **kwargs):
        if args and kwargs:
            raise ValueError("Can not use both args "
                             "and kwargs to create Row")
        if kwargs:
            # create row objects
            names = sorted(kwargs.keys())
            row = tuple.__new__(cls, [kwargs[n] for n in names])
            row.__fields__ = names
            row.__from_dict__ = True
            return row

        # create row class or objects
        return tuple.__new__(cls, args)

    def asDict(self, recursive=False):
        """
        Return as an dict

        :param recursive: turns the nested Row as dict (default: False).

        >>> Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}
        True
        >>> row = Row(key=1, value=Row(name='a', age=2))
        >>> row.asDict() == {'key': 1, 'value': Row(age=2, name='a')}
        True
        >>> row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}
        True
        """
        if not hasattr(self, "__fields__"):
            raise TypeError("Cannot convert a Row class into dict")

        if recursive:
            def conv(obj):
                if isinstance(obj, Row):
                    return obj.asDict(True)
                if isinstance(obj, list):
                    return [conv(o) for o in obj]
                if isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                return obj

            return dict(zip(self.__fields__, (conv(o) for o in self)))
        return dict(zip(self.__fields__, self))

    def __contains__(self, item):
        if hasattr(self, "__fields__"):
            return item in self.__fields__
        return super().__contains__(item)

    # let object acts like class
    def __call__(self, *args):
        """create new Row object"""
        if len(args) > len(self):
            raise ValueError("Can not create Row with fields %s, expected %d values "
                             "but got %s" % (self, len(self), args))
        return create_row(self, args)

    def __getitem__(self, item):
        if isinstance(item, (int, slice)):
            return super().__getitem__(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return super().__getitem__(idx)
        except IndexError as e:
            raise KeyError(item) from e
        except ValueError as e:
            raise ValueError(item) from e

    def __getattr__(self, item):
        if item.startswith("__"):
            raise AttributeError(item)
        try:
            # it will be slow when it has many fields,
            # but this will not be used in normal cases
            idx = self.__fields__.index(item)
            return self[idx]
        except IndexError as e:
            raise AttributeError(item) from e
        except ValueError as e:
            raise AttributeError(item) from e

    def __setattr__(self, key, value):
        if key not in ('__fields__', "__from_dict__", "_metadata"):
            raise Exception("Row is read-only")
        self.__dict__[key] = value

    def __reduce__(self):
        """Returns a tuple so Python knows how to pickle Row."""
        if hasattr(self, "__fields__"):
            return create_row, (self.__fields__, tuple(self))
        return tuple.__reduce__(self)

    def __repr__(self):
        """Printable representation of Row used in Python REPL."""
        if hasattr(self, "__fields__"):
            return "Row(%s)" % ", ".join("%s=%r" % (k, v)
                                         for k, v in zip(self.__fields__, tuple(self)))
        return "<Row(%s)>" % ", ".join(self)

    def set_grouping(self, grouping):
        # This method is specific to Pysparkling and should not be used by
        # user of the library who wants compatibility with PySpark
        if self._metadata is None:
            self.set_metadata({})
        self._metadata["grouping"] = grouping
        return self

    def set_input_file_name(self, input_file_name):
        # This method is specific to Pysparkling and should not be used by
        # user of the library who wants compatibility with PySpark
        if self._metadata is None:
            self.set_metadata({})
        self._metadata["input_file_name"] = input_file_name
        return self

    def set_metadata(self, metadata):
        # This method is specific to Pysparkling and should not be used by
        # user of the library who wants compatibility with PySpark
        self._metadata = metadata
        return self

    def get_metadata(self):
        # This method is specific to Pysparkling and should not be used by
        # user of the library who wants compatibility with PySpark
        return self._metadata

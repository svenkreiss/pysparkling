from pysparkling.sql.casts import get_caster
from pysparkling.sql.types import INTERNAL_TYPE_ORDER, DataType, StructField, python_to_spark_type
from pysparkling.sql.utils import AnalysisException

expression_registry = {}


class RegisterExpressions(type):
    pretty_name = None

    def __init__(cls, what, bases, dct):
        super().__init__(what, bases, dct)
        if cls.pretty_name is not None:
            expression_registry[cls.pretty_name] = cls


class Expression(object, metaclass=RegisterExpressions):
    pretty_name = None

    def __init__(self, *children):
        self.children = children
        self.pre_evaluation_schema = None

    def eval(self, row, schema):
        raise NotImplementedError

    def __str__(self):
        return "{0}({1})".format(self.pretty_name, ", ".join(str(arg) for arg in self.args()))

    def args(self):
        raise NotImplementedError

    def __repr__(self):
        return self.__class__.__name__

    def output_fields(self, schema):
        return [StructField(
            name=str(self),
            dataType=self.data_type,
            nullable=self.is_nullable
        )]

    @property
    def data_type(self):
        # pylint: disable=W0511
        # todo: be more specific
        return DataType()

    @property
    def is_nullable(self):
        return True

    @property
    def may_output_multiple_cols(self):
        return False

    @property
    def may_output_multiple_rows(self):
        return False

    @property
    def is_an_aggregation(self):
        return False

    def merge(self, row, schema):
        pass

    def recursive_merge(self, row, schema):
        self.merge(row, schema)
        self.children_merge(self.children, row, schema)

    @staticmethod
    def children_merge(children, row, schema):
        for child in children:
            if isinstance(child, Expression):
                child.recursive_merge(row, schema)
            elif hasattr(child, "expr") and isinstance(child.expr, Expression):
                child.expr.recursive_merge(row, schema)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge(child, row, schema)

    def mergeStats(self, other, schema):
        pass

    def recursive_merge_stats(self, other, schema):
        # Top level import would cause cyclic dependencies
        # pylint: disable=import-outside-toplevel
        from pysparkling.sql.expressions.operators import Alias
        if isinstance(other.expr, Alias):
            self.recursive_merge_stats(other.expr.expr, schema)
        else:
            self.mergeStats(other.expr, schema)
            self.children_merge_stats(self.children, other, schema)

    @staticmethod
    def children_merge_stats(children, other, schema):
        # Top level import would cause cyclic dependencies
        # pylint: disable=import-outside-toplevel
        from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_merge_stats(other, schema)
            elif isinstance(child, Column) and isinstance(child.expr, Expression):
                child.expr.recursive_merge_stats(other, schema)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_merge_stats(child, other, schema)

    def recursive_initialize(self, partition_index):
        """
        This methods adds once data to expressions that require it
        e.g. for non-deterministic expression so that their result is constant
        across several evaluations
        """
        self.initialize(partition_index)
        self.children_initialize(self.children, partition_index)

    @staticmethod
    def children_initialize(children, partition_index):
        # Top level import would cause cyclic dependencies
        # pylint: disable=import-outside-toplevel
        from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_initialize(partition_index)
            elif isinstance(child, Column) and isinstance(child.expr, Expression):
                child.expr.recursive_initialize(partition_index)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_initialize(child, partition_index)

    def initialize(self, partition_index):
        pass

    # Adding information about the schema that was defined in the step prior the evaluation
    def with_pre_evaluation_schema(self, schema):
        self.pre_evaluation_schema = schema

    def recursive_pre_evaluation_schema(self, schema):
        self.with_pre_evaluation_schema(schema)
        self.children_pre_evaluation_schema(self.children, schema)

    @staticmethod
    def children_pre_evaluation_schema(children, schema):
        # Top level import would cause cyclic dependencies
        # pylint: disable=import-outside-toplevel
        from pysparkling.sql.column import Column
        for child in children:
            if isinstance(child, Expression):
                child.recursive_pre_evaluation_schema(schema)
            elif isinstance(child, Column) and isinstance(child.expr, Expression):
                child.expr.recursive_pre_evaluation_schema(schema)
            elif isinstance(child, (list, set, tuple)):
                Expression.children_pre_evaluation_schema(child, schema)

    def get_literal_value(self):
        raise AnalysisException("Expecting a Literal, but got {0}: {1}".format(type(self), self))


class UnaryExpression(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row, schema):
        raise NotImplementedError

    def args(self):
        return (self.column,)


class BinaryOperation(Expression):
    """
    Represent a binary operation between 2 columns
    """

    def __init__(self, arg1, arg2):
        super().__init__(arg1, arg2)
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row, schema):
        raise NotImplementedError

    def args(self):
        return (
            self.arg1,
            self.arg2
        )


class TypeSafeBinaryOperation(BinaryOperation):
    """
    Represent a type and null-safe binary operation using *comparison* type cast rules:

    It converts values if they are of different types following PySpark rules:

    lit(datetime.date(2019, 1, 1))==lit("2019-01-01") is True
    """

    def eval(self, row, schema):
        value_1 = self.arg1.eval(row, schema)
        value_2 = self.arg2.eval(row, schema)
        if value_1 is None or value_2 is None:
            return None

        type_1 = value_1.__class__
        type_2 = value_2.__class__
        if type_1 == type_2:
            return self.unsafe_operation(value_1, value_2)

        try:
            order_1 = INTERNAL_TYPE_ORDER.index(type_1)
            order_2 = INTERNAL_TYPE_ORDER.index(type_2)
        except ValueError as e:
            raise AnalysisException("Unable to process type: {0}".format(e)) from e

        spark_type_1 = python_to_spark_type(type_1)
        spark_type_2 = python_to_spark_type(type_2)

        if order_1 > order_2:
            caster = get_caster(from_type=spark_type_2, to_type=spark_type_1, options={})
            value_2 = caster(value_2)
        elif order_1 < order_2:
            caster = get_caster(from_type=spark_type_1, to_type=spark_type_2, options={})
            value_1 = caster(value_1)

        return self.unsafe_operation(value_1, value_2)

    def __str__(self):
        raise NotImplementedError

    def unsafe_operation(self, value_1, value_2):
        raise NotImplementedError


class NullSafeBinaryOperation(BinaryOperation):
    """
    Represent a null-safe binary operation

    It does not converts values if they are of different types:
    lit(datetime.date(2019, 1, 1)) - lit("2019-01-01") raises an error
    """

    def eval(self, row, schema):
        value_1 = self.arg1.eval(row, schema)
        value_2 = self.arg2.eval(row, schema)
        if value_1 is None or value_2 is None:
            return None

        type_1 = value_1.__class__
        type_2 = value_2.__class__
        if type_1 == type_2 or (
                isinstance(value_1, (int, float))
                and isinstance(value_2, (int, float))
        ):
            return self.unsafe_operation(value_1, value_2)

        raise AnalysisException(
            "Cannot resolve {0} due to data type mismatch, first value is {1}, second value is {2}."
            "".format(self, type_1, type_2)
        )

    def __str__(self):
        raise NotImplementedError

    def unsafe_operation(self, value1, value2):
        raise NotImplementedError


class NullSafeColumnOperation(Expression):
    def __init__(self, column, *args):
        super().__init__(column, *args)
        self.column = column

    def eval(self, row, schema):
        value = self.column.eval(row, schema)
        return self.unsafe_operation(value)

    def args(self):
        raise NotImplementedError

    def unsafe_operation(self, value):
        raise NotImplementedError

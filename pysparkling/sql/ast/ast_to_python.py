import ast
import logging

from sqlparser import string_to_ast
from ..utils import ParseException

from ...sql import functions
from ..column import Column, parse
from ..expressions.expressions import expression_registry
from ..expressions.literals import Literal
from ..expressions.mappers import Concat, CreateStruct
from ..expressions.operators import (
    Add, Alias, And, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, Cast, Divide, Equal, GreaterThan,
    GreaterThanOrEqual, Invert, LessThan, LessThanOrEqual, Minus, Mod, Negate, Or, Time, UnaryPositive
)
from ..types import DoubleType, parsed_string_to_type, StringType, StructField, StructType


class SqlParsingError(ParseException):
    pass


class UnsupportedStatement(SqlParsingError):
    pass


def check_children(expected, children):
    if len(children) != expected:
        raise SqlParsingError(
            "Expecting {0} children, got {1}: {2}".format(
                expected, len(children), children
            )
        )


def unwrap(*children):
    check_children(1, children)
    return convert_tree(children[0])


def never_found(*children):
    logging.warning("We should never have encounter this node.")
    return unwrap(*children)


def unsupported(*children):
    raise UnsupportedStatement


def empty(*children):
    check_children(0, children)


def first_child_only(*children):
    return convert_tree(children[0])


def child_and_eof(*children):
    check_children(2, children)
    return convert_tree(children[0])


def convert_tree(tree):
    tree_type = tree.__class__.__name__
    if not hasattr(tree, "children"):
        return get_leaf_value(tree)
    try:
        converter = CONVERTERS[tree_type]
    except UnsupportedStatement:
        raise SqlParsingError("Unsupported statement {0}".format(tree_type)) from None
    children = tree.children or ()
    for c in children:
        if c.__class__.__name__ == 'ErrorNodeImpl':
            raise SqlParsingError(f'Unable to parse data type, unexpected {c.symbol}')
    return converter(*children)


def call_function(*children):
    raw_function_name = convert_tree(children[0])
    function_name = next(
        (name for name in functions.__all__ if name.lower() == raw_function_name.lower()),
        None
    )
    function_expression = expression_registry.get(function_name.lower())
    params = [convert_tree(c) for c in children[2:-1]]

    # todo: there must be a cleaner way to do that
    complex_function = any(
        not isinstance(param, Column) and param == ')'
        for param in params
    )
    if not complex_function:
        last_argument_position = None
        # filter_clause = None
        # over_clause = None
        # set_clause = None
    else:
        last_argument_position = params.index(")")
        # filter_clause = ...  # todo
        # over_clause = ...  # todo
        # set_clause = ...  # todo

    # parameters are comma separated
    function_arguments = params[0:last_argument_position:2]
    return function_expression(*function_arguments)


def binary_operation(*children):
    check_children(3, children)
    left, operator, right = children
    cls = binary_operations[convert_tree(operator).upper()]
    return cls(
        convert_tree(left),
        convert_tree(right)
    )


def cast_context(*children):
    """
    Children are:
    CAST '(' expression AS dataType ')'

    """
    check_children(6, children)
    expression = convert_tree(children[2])
    data_type = convert_tree(children[4])
    return parse(expression).cast(data_type)


def detect_data_type(*children):
    data_type = convert_tree(children[0])
    params = [convert_tree(c) for c in children[2:-1:2]]
    return parsed_string_to_type(data_type, params)


def unary_operation(*children):
    check_children(2, children)
    operator, value = children
    cls = unary_operations[convert_tree(operator).upper()]
    return cls(
        convert_tree(value)
    )


def parenthesis_context(*children):
    check_children(3, children)
    return convert_tree(children[1])


def convert_boolean(*children):
    check_children(1, children)
    value = convert_tree(children[0])
    if value.lower() == "false":
        return False
    if value.lower() == "true":
        return True
    raise SqlParsingError("Expecting boolean value, got {0}".format(children))


def convert_to_literal(*children):
    check_children(1, children)
    value = convert_tree(children[0])
    return Literal(value)


def convert_column(*children):
    check_children(1, children)
    value = convert_tree(children[0])
    return Column(value)


def convert_field(*children):
    name = convert_tree(children[0])
    data_type = convert_tree(children[1])
    return StructField(name, data_type)


def convert_table_schema(*children):
    check_children(2, children)
    return StructType(fields=list(convert_tree(children[0])))


def convert_to_null(*children):
    return None


def get_leaf_value(*children):
    check_children(1, children)
    value = children[0]
    if value.__class__.__name__ != "TerminalNodeImpl":
        raise SqlParsingError("Expecting TerminalNodeImpl, got {0}".format(type(value).__name__))
    if not hasattr(value, "symbol"):
        raise SqlParsingError("Got leaf value but without symbol")
    return value.symbol.text


def remove_delimiter(*children):
    delimited_value = get_leaf_value(*children)
    return delimited_value[1:-1]


def explicit_list(*children):
    return tuple(
        convert_tree(c)
        for c in children[1:-1:2]
    )


def convert_to_complex_col_type(*children):
    name = convert_tree(children[0])
    data_type = convert_tree(children[2])
    if len(children) > 3:
        params = [convert_tree(c) for c in children[3:]]
        nullable = not (params[0].lower() == 'not' and params[1].lower() == ['null'])
        metadata = params[3:] or None
    else:
        nullable = False
        metadata = None
    return name, data_type, nullable, metadata


def implicit_list(*children):
    return tuple(
        convert_tree(c)
        for c in children[::2]
    )


def concat_to_value(*children):
    return ast.literal_eval("".join(convert_tree(c) for c in children))


def concat_keywords(*children):
    return " ".join(convert_tree(c) for c in children)


def concat_strings(*children):
    return "".join(convert_tree(c) for c in children)


def build_struct(*children):
    return CreateStruct(*(convert_tree(c) for c in children[2:-1:2]))


def potential_alias(*children):
    if len(children) == 1:
        return convert_tree(children[0])
    if len(children) in (2, 3):
        return Alias(
            convert_tree(children[0]),
            convert_tree(children[-1])
        )
    raise SqlParsingError("Expecting 1, 2 or 3 children, got {0}".format(len(children)))


def get_quoted_identifier(*children):
    value = get_leaf_value(*children)
    return value[1:-1]


def check_identifier(*children):
    check_children(2, children)
    if children[0].children is None:
        raise SqlParsingError("Expected identifier not found") from children[0].exception
    identifier = convert_tree(children[0])
    if children[1].children:
        extra = convert_tree(children[1])
        raise SqlParsingError(
            "Possibly unquoted identifier {0}{1} detected. "
            "Please consider quoting it with back-quotes as `{0}{1}`".format(identifier, extra)
        )
    return identifier


CONVERTERS = {
    'AddTableColumnsContext': unsupported,
    'AddTablePartitionContext': unsupported,
    'AggregationClauseContext': unsupported,
    'AliasedQueryContext': unsupported,
    'AliasedRelationContext': unsupported,
    'AlterColumnActionContext': unsupported,
    'AlterTableAlterColumnContext': unsupported,
    'AlterViewQueryContext': unsupported,
    'AnalyzeContext': unsupported,
    'AnsiNonReservedContext': get_leaf_value,
    'ApplyTransformContext': unsupported,
    'ArithmeticBinaryContext': binary_operation,
    'ArithmeticOperatorContext': get_leaf_value,
    'ArithmeticUnaryContext': unary_operation,
    'AssignmentContext': unsupported,
    'AssignmentListContext': implicit_list,
    'BigDecimalLiteralContext': concat_to_value,
    'BigIntLiteralContext': concat_to_value,
    'BooleanExpressionContext': never_found,
    'BooleanLiteralContext': unwrap,
    'BooleanValueContext': convert_boolean,
    'BucketSpecContext': unsupported,
    'CacheTableContext': unsupported,
    'CastContext': cast_context,
    'ClearCacheContext': unsupported,
    'ColPositionContext': unsupported,
    'ColTypeContext': convert_field,
    'ColTypeListContext': implicit_list,
    'ColumnReferenceContext': convert_column,
    'CommentNamespaceContext': unsupported,
    'CommentSpecContext': unsupported,
    'CommentTableContext': unsupported,
    'ComparisonContext': binary_operation,
    'ComparisonOperatorContext': get_leaf_value,
    'ComplexColTypeContext': convert_to_complex_col_type,
    'ComplexColTypeListContext': implicit_list,
    'ComplexDataTypeContext': detect_data_type,
    'ConstantContext': never_found,
    'ConstantDefaultContext': convert_to_literal,
    'ConstantListContext': explicit_list,
    'CreateFileFormatContext': unsupported,
    'CreateFunctionContext': unsupported,
    'CreateHiveTableContext': unsupported,
    'CreateNamespaceContext': unsupported,
    'CreateTableClausesContext': unsupported,
    'CreateTableContext': unsupported,
    'CreateTableHeaderContext': unsupported,
    'CreateTableLikeContext': unsupported,
    'CreateTempViewUsingContext': unsupported,
    'CreateViewContext': unsupported,
    'CtesContext': unsupported,
    'CurrentDatetimeContext': unsupported,
    'DataTypeContext': never_found,
    'DecimalLiteralContext': concat_to_value,
    'DeleteFromTableContext': unsupported,
    'DereferenceContext': unsupported,
    'DescribeColNameContext': unsupported,
    'DescribeFuncNameContext': unwrap,
    'DescribeFunctionContext': unsupported,
    'DescribeNamespaceContext': unsupported,
    'DescribeQueryContext': unsupported,
    'DescribeRelationContext': unsupported,
    'DmlStatementContext': unsupported,
    'DmlStatementNoWithContext': never_found,
    'DoubleLiteralContext': concat_to_value,
    'DropFunctionContext': unsupported,
    'DropNamespaceContext': unsupported,
    'DropTableColumnsContext': unsupported,
    'DropTableContext': unsupported,
    'DropTablePartitionsContext': unsupported,
    'DropViewContext': unsupported,
    'ErrorCapturingIdentifierContext': check_identifier,
    'ErrorCapturingIdentifierExtraContext': never_found,
    'ErrorCapturingMultiUnitsIntervalContext': unsupported,
    'ErrorCapturingUnitToUnitIntervalContext': unsupported,
    'ErrorIdentContext': concat_strings,
    'ExistsContext': unsupported,
    'ExplainContext': unsupported,
    'ExponentLiteralContext': concat_to_value,
    'ExpressionContext': unwrap,
    'ExtractContext': unsupported,
    'FailNativeCommandContext': unsupported,
    'FileFormatContext': never_found,
    'FirstContext': unsupported,
    'FrameBoundContext': unsupported,
    'FromClauseContext': unsupported,
    'FromStatementBodyContext': unsupported,
    'FromStatementContext': unsupported,
    'FromStmtContext': unwrap,
    'FunctionCallContext': call_function,
    'FunctionIdentifierContext': unsupported,
    'FunctionNameContext': unwrap,
    'FunctionTableContext': unsupported,
    'GenericFileFormatContext': unwrap,
    'GroupingSetContext': unsupported,
    'HavingClauseContext': unsupported,
    'HintContext': unsupported,
    'HintStatementContext': unsupported,
    'HiveChangeColumnContext': unsupported,
    'HiveReplaceColumnsContext': unsupported,
    'IdentifierCommentContext': unsupported,
    'IdentifierCommentListContext': explicit_list,
    'IdentifierContext': unwrap,
    'IdentifierListContext': explicit_list,
    'IdentifierSeqContext': unsupported,
    'IdentityTransformContext': unwrap,
    'InlineTableContext': unsupported,
    'InlineTableDefault1Context': unwrap,
    'InlineTableDefault2Context': unwrap,
    'InsertIntoContext': never_found,
    'InsertIntoTableContext': unsupported,
    'InsertOverwriteDirContext': unsupported,
    'InsertOverwriteHiveDirContext': unsupported,
    'InsertOverwriteTableContext': unsupported,
    'IntegerLiteralContext': concat_to_value,
    'IntervalContext': unsupported,
    'IntervalLiteralContext': unwrap,
    'IntervalUnitContext': unwrap,
    'IntervalValueContext': unsupported,
    'JoinCriteriaContext': unsupported,
    'JoinRelationContext': unsupported,
    'JoinTypeContext': concat_keywords,
    'LambdaContext': unsupported,
    'LastContext': unsupported,
    'LateralViewContext': unsupported,
    'LegacyDecimalLiteralContext': concat_to_value,
    'LoadDataContext': unsupported,
    'LocationSpecContext': unsupported,
    'LogicalBinaryContext': binary_operation,
    'LogicalNotContext': unary_operation,
    'ManageResourceContext': unsupported,
    'MatchedActionContext': unsupported,
    'MatchedClauseContext': unsupported,
    'MergeIntoTableContext': unsupported,
    'MultiInsertQueryBodyContext': unsupported,
    'MultiInsertQueryContext': unsupported,
    'MultipartIdentifierContext': unsupported,
    'MultipartIdentifierListContext': implicit_list,
    'MultiUnitsIntervalContext': unsupported,
    'NamedExpressionContext': potential_alias,
    'NamedExpressionSeqContext': unsupported,
    'NamedQueryContext': unsupported,
    'NamedWindowContext': unsupported,
    'NamespaceContext': get_leaf_value,
    'NestedConstantListContext': explicit_list,
    'NonReservedContext': get_leaf_value,
    'NotMatchedActionContext': unsupported,
    'NotMatchedClauseContext': unsupported,
    'NullLiteralContext': convert_to_null,
    'NumberContext': never_found,
    'NumericLiteralContext': unwrap,
    'OrderedIdentifierContext': unsupported,
    'OrderedIdentifierListContext': explicit_list,
    'OverlayContext': unsupported,
    'ParenthesizedExpressionContext': parenthesis_context,
    'PartitionSpecContext': unsupported,
    'PartitionSpecLocationContext': unsupported,
    'PartitionValContext': unsupported,
    'PivotClauseContext': unsupported,
    'PivotColumnContext': unsupported,
    'PivotValueContext': unsupported,
    'PositionContext': unsupported,
    'PredicateContext': unsupported,
    'PredicatedContext': unwrap,
    'PredicateOperatorContext': get_leaf_value,
    'PrimaryExpressionContext': never_found,
    'PrimitiveDataTypeContext': detect_data_type,
    'QualifiedColTypeWithPositionContext': unsupported,
    'QualifiedColTypeWithPositionListContext': implicit_list,
    'QualifiedNameContext': concat_strings,
    'QualifiedNameListContext': implicit_list,
    'QueryContext': unsupported,
    'QueryOrganizationContext': unsupported,
    'QueryPrimaryContext': never_found,
    'QueryPrimaryDefaultContext': unwrap,
    'QuerySpecificationContext': unsupported,
    'QueryTermContext': never_found,
    'QueryTermDefaultContext': unwrap,
    'QuotedIdentifierAlternativeContext': unwrap,
    'QuotedIdentifierContext': get_quoted_identifier,
    'RealIdentContext': empty,
    'RecoverPartitionsContext': unsupported,
    'RefreshResourceContext': unsupported,
    'RefreshTableContext': unsupported,
    'RegularQuerySpecificationContext': unsupported,
    'RelationContext': unsupported,
    'RelationPrimaryContext': never_found,
    'RenameTableColumnContext': unsupported,
    'RenameTableContext': unsupported,
    'RenameTablePartitionContext': unsupported,
    'RepairTableContext': unsupported,
    'ReplaceTableContext': unsupported,
    'ReplaceTableHeaderContext': unsupported,
    'ResetConfigurationContext': unwrap,
    'ResourceContext': unsupported,
    'RowConstructorContext': unsupported,
    'RowFormatContext': never_found,
    'RowFormatDelimitedContext': unsupported,
    'RowFormatSerdeContext': unsupported,
    'SampleByBucketContext': unsupported,
    'SampleByBytesContext': unsupported,
    'SampleByPercentileContext': unsupported,
    'SampleByRowsContext': unsupported,
    'SampleContext': unsupported,
    'SampleMethodContext': never_found,
    'SearchedCaseContext': unsupported,
    'SelectClauseContext': unsupported,
    'SetClauseContext': unsupported,
    'SetConfigurationContext': unsupported,
    'SetNamespaceLocationContext': unsupported,
    'SetNamespacePropertiesContext': unsupported,
    'SetOperationContext': unsupported,
    'SetQuantifierContext': get_leaf_value,
    'SetTableLocationContext': unsupported,
    'SetTablePropertiesContext': unsupported,
    'SetTableSerDeContext': unsupported,
    'ShowColumnsContext': unsupported,
    'ShowCreateTableContext': unsupported,
    'ShowCurrentNamespaceContext': unsupported,
    'ShowFunctionsContext': unsupported,
    'ShowNamespacesContext': unsupported,
    'ShowPartitionsContext': unsupported,
    'ShowTableContext': unsupported,
    'ShowTablesContext': unsupported,
    'ShowTblPropertiesContext': unsupported,
    'ShowViewsContext': unsupported,
    'SimpleCaseContext': unsupported,
    'SingleDataTypeContext': child_and_eof,
    'SingleExpressionContext': child_and_eof,
    'SingleFunctionIdentifierContext': child_and_eof,
    'SingleInsertQueryContext': unsupported,
    'SingleMultipartIdentifierContext': child_and_eof,
    'SingleStatementContext': first_child_only,
    'SingleTableIdentifierContext': child_and_eof,
    'SingleTableSchemaContext': convert_table_schema,
    'SkewSpecContext': unsupported,
    'SmallIntLiteralContext': concat_to_value,
    'SortItemContext': unsupported,
    'SqlBaseParser': unsupported,
    'StarContext': unsupported,
    'StatementContext': never_found,
    'StatementDefaultContext': unwrap,
    'StorageHandlerContext': unsupported,
    'StrictIdentifierContext': never_found,
    'StrictNonReservedContext': get_leaf_value,
    'StringLiteralContext': remove_delimiter,
    'StructContext': build_struct,
    'SubqueryContext': parenthesis_context,
    'SubqueryExpressionContext': parenthesis_context,
    'SubscriptContext': unsupported,
    'SubstringContext': unsupported,
    'TableAliasContext': unsupported,
    'TableContext': unsupported,
    'TableFileFormatContext': unsupported,
    'TableIdentifierContext': unsupported,
    'TableNameContext': unsupported,
    'TablePropertyContext': unsupported,
    'TablePropertyKeyContext': unsupported,
    'TablePropertyListContext': explicit_list,
    'TablePropertyValueContext': unwrap,
    'TableProviderContext': unsupported,
    'onContext': unwrap,
    'TerminalNodeImpl': get_leaf_value,
    'TinyIntLiteralContext': concat_to_value,
    'TransformArgumentContext': unwrap,
    'TransformClauseContext': unsupported,
    'TransformContext': never_found,
    'TransformListContext': explicit_list,
    'TransformQuerySpecificationContext': unsupported,
    'TrimContext': unsupported,
    'TruncateTableContext': unsupported,
    'TypeConstructorContext': unsupported,
    'UncacheTableContext': unsupported,
    'UnitToUnitIntervalContext': unsupported,
    'UnquotedIdentifierContext': unwrap,
    'UnsetTablePropertiesContext': unsupported,
    'UnsupportedHiveNativeCommandsContext': unsupported,
    'UpdateTableContext': unsupported,
    'UseContext': unsupported,
    'ValueExpressionContext': never_found,
    'ValueExpressionDefaultContext': unwrap,
    'WhenClauseContext': unsupported,
    'WhereClauseContext': unsupported,
    'WindowClauseContext': unsupported,
    'WindowDefContext': unsupported,
    'WindowFrameContext': unsupported,
    'WindowRefContext': unsupported,
    'WindowSpecContext': never_found,
}

binary_operations = {
    "=": Equal,
    "==": Equal,
    "<>": lambda *args: Invert(Equal(*args)),
    "!=": lambda *args: Invert(Equal(*args)),
    "<": LessThan,
    "<=": LessThanOrEqual,
    "!>": LessThanOrEqual,
    ">": GreaterThan,
    ">=": GreaterThanOrEqual,
    "!<": GreaterThanOrEqual,
    "+": Add,
    "-": Minus,
    '*': Time,
    '/': lambda a, b: Divide(Cast(a, DoubleType), Cast(b, DoubleType)),
    '%': Mod,
    'DIV': lambda a, b: Divide(Cast(a, DoubleType), Cast(b, DoubleType)),
    '&': BitwiseAnd,
    '|': BitwiseOr,
    '||': lambda a, b: Concat([Cast(a, StringType), Cast(b, StringType)]),
    '^': BitwiseXor,
    'AND': And,
    'OR': Or,
}

unary_operations = {
    "+": UnaryPositive,
    "-": Negate,
    "~": BitwiseNot,
    'NOT': Invert
}


def parse_sql(string, rule, debug=False):
    tree = string_to_ast(string, rule, debug=debug)
    return convert_tree(tree)


def parse_data_type(string, debug=False):
    return parse_sql(string, "singleDataType", debug)


def parse_schema(string, debug=False):
    return parse_sql(string, "singleTableSchema", debug)


def parse_expression(string, debug=False):
    return parse_sql(string, "singleExpression", debug)


def parse_ddl_string(string, debug=False):
    try:
        return parse_schema(string, debug)
    except ParseException:
        try:
            return parse_data_type(string, debug)
        except ParseException:
            return parse_data_type(f"struct<{string.strip()}>")

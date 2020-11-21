import ast
import logging

from pysparkling.sql.column import parse, Column
from pysparkling.sql.expressions.literals import Literal
from pysparkling.sql.expressions.mappers import CreateStruct, Concat
from pysparkling.sql.expressions.operators import Equal, Invert, LessThan, LessThanOrEqual, GreaterThan, \
    GreaterThanOrEqual, Add, Minus, Time, Divide, Mod, Cast, And, BitwiseAnd, BitwiseOr, BitwiseXor, Or, Negate, \
    BitwiseNot, UnaryPositive, Alias
from pysparkling.sql import functions
from pysparkling.sql.types import DoubleType, StringType, parsed_string_to_type
from sqlparser import string_to_ast


class SqlParsingError(Exception):
    pass


def check_children(expected, children):
    if len(children) != expected:
        raise SqlParsingError("Expecting {0} children, got {1}: {2}".format(expected, len(children), children))


def unwrap(*children):
    check_children(1, children)
    return convert_tree(children[0])


def never_found(*children):
    logging.warning("We should never have encounter this node.")
    return unwrap(*children)


def unsupported(*children):
    raise SqlParsingError("Unsupported statement")


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
    converter = CONVERTERS[tree_type]
    return converter(*tree.children)


def call_function(*children):
    raw_function_name = convert_tree(children[0])
    function_name = next(
        (name for name in functions.__all__ if name.lower() == raw_function_name.lower()),
        None
    )
    # function = getattr(functions, function_name)
    this should get an Expression init, not a function
    params = [convert_tree(c) for c in children[2:-1]]

    complex_function = ')' in params
    if not complex_function:
        last_argument_position = None
        filter_clause = None
        over_clause = None
        set_clause = None
    else:
        last_argument_position = params.index(")")
        filter_clause = ...  # todo
        over_clause = ...  # todo
        set_clause = ...  # todo

    # parameters are comma separated
    function_arguments = params[0:last_argument_position:2]
    return function(*function_arguments)


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


def parse_boolean(*children):
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


def parse_column(*children):
    check_children(1, children)
    value = convert_tree(children[0])
    return Column(value)


def convert_to_null(*children):
    return None


def get_leaf_value(*children):
    check_children(1, children)
    value = children[0]
    if value.__class__.__name__ != "TerminalNodeImpl":
        raise SqlParsingError("Expecting TerminalNodeImpl, got {0}".format(value.__class__.__name__))
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
    return CreateStruct([convert_tree(c) for c in children[2:-1:2]])


def potential_alias(*chidren):
    if len(chidren) == 1:
        return convert_tree(chidren[0])
    if len(chidren) in (2, 3):
        return Alias(
            convert_tree(chidren[0]),
            convert_tree(chidren[-1])
        )
    raise SqlParsingError("Expecting 1, 2 or 3 children, got {0}".format(len(chidren)))


def check_identifier(*children):
    check_children(2, children)
    identifier = convert_tree(children[0])
    if children[1].children:
        extra = convert_tree(children[1])
        raise SqlParsingError((
                                  "Possibly unquoted identifier {0}{1} detected. "
                                  "Please consider quoting it with back-quotes as `{0}{1}`"
                              ).format(identifier, extra))
    return identifier


def debug(*children):
    pass


CONVERTERS = {
    "SingleStatementContext": first_child_only,
    "SingleExpressionContext": child_and_eof,
    "SingleTableIdentifierContext": child_and_eof,
    "SingleMultipartIdentifierContext": child_and_eof,
    "SingleFunctionIdentifierContext": child_and_eof,
    "SingleDataTypeContext": child_and_eof,
    "SingleTableSchemaContext": child_and_eof,
    'NamespaceContext': get_leaf_value,
    'SetQuantifierContext': get_leaf_value,
    'ComparisonOperatorContext': get_leaf_value,
    'ArithmeticOperatorContext': get_leaf_value,
    'PredicateOperatorContext': get_leaf_value,
    'BooleanValueContext': parse_boolean,
    'QuotedIdentifierContext': get_leaf_value,
    'AnsiNonReservedContext': get_leaf_value,
    'StrictNonReservedContext': get_leaf_value,
    'NonReservedContext': get_leaf_value,
    'TerminalNodeImpl': get_leaf_value,
    "StringLiteralContext": remove_delimiter,
    "UnquotedIdentifierContext": get_leaf_value,
    'DescribeFuncNameContext': unwrap,
    'TablePropertyValueContext': unwrap,
    'TransformArgumentContext': unwrap,
    'ExpressionContext': unwrap,
    'IntervalUnitContext': unwrap,
    'FunctionNameContext': unwrap,
    'StatementDefaultContext': unwrap,
    'ResetConfigurationContext': unwrap,
    'GenericFileFormatContext': unwrap,
    'QueryTermDefaultContext': unwrap,
    'QueryPrimaryDefaultContext': unwrap,
    'FromStmtContext': unwrap,
    'InlineTableDefault1Context': unwrap,
    'InlineTableDefault2Context': unwrap,
    'TableValuedFunctionContext': unwrap,
    'IdentityTransformContext': unwrap,
    'ValueExpressionDefaultContext': unwrap,
    'ConstantDefaultContext': convert_to_literal,
    'ColumnReferenceContext': parse_column,
    'NullLiteralContext': convert_to_null,
    'IntervalLiteralContext': unwrap,
    'NumericLiteralContext': unwrap,
    'BooleanLiteralContext': unwrap,
    'QuotedIdentifierAlternativeContext': unwrap,
    "IdentifierContext": unwrap,
    "BooleanExpressionContext": never_found,
    "ConstantContext": never_found,
    "DataTypeContext": never_found,
    "DmlStatementNoWithContext": never_found,
    "ErrorCapturingIdentifierExtraContext": never_found,
    "FileFormatContext": never_found,
    "InsertIntoContext": never_found,
    "NumberContext": never_found,
    "PrimaryExpressionContext": never_found,
    "QueryPrimaryContext": never_found,
    "QueryTermContext": never_found,
    "RelationPrimaryContext": never_found,
    "RowFormatContext": never_found,
    "SampleMethodContext": never_found,
    "StatementContext": never_found,
    "StrictIdentifierContext": never_found,
    "TransformContext": never_found,
    "ValueExpressionContext": never_found,
    "WindowSpecContext": never_found,
    'ExponentLiteralContext': concat_to_value,
    'DecimalLiteralContext': concat_to_value,
    'LegacyDecimalLiteralContext': concat_to_value,
    'IntegerLiteralContext': concat_to_value,
    'BigIntLiteralContext': concat_to_value,
    'SmallIntLiteralContext': concat_to_value,
    'TinyIntLiteralContext': concat_to_value,
    'DoubleLiteralContext': concat_to_value,
    'BigDecimalLiteralContext': concat_to_value,
    'TablePropertyListContext': explicit_list,
    'ConstantListContext': explicit_list,
    'NestedConstantListContext': explicit_list,
    'IdentifierListContext': explicit_list,
    'OrderedIdentifierListContext': explicit_list,
    'IdentifierCommentListContext': explicit_list,
    'TransformListContext': explicit_list,
    'AssignmentListContext': implicit_list,
    'MultipartIdentifierListContext': implicit_list,
    'QualifiedColTypeWithPositionListContext': implicit_list,
    'ColTypeListContext': implicit_list,
    'ComplexColTypeListContext': implicit_list,
    'QualifiedNameListContext': implicit_list,
    'ComplexColTypeContext': implicit_list,
    "ComparisonContext": binary_operation,
    "ArithmeticBinaryContext": binary_operation,
    "LogicalBinaryContext": binary_operation,
    "RealIdentContext": empty,
    "ParenthesizedExpressionContext": parenthesis_context,
    "SubqueryContext": parenthesis_context,
    "SubqueryExpressionContext": parenthesis_context,
    "ArithmeticUnaryContext": unary_operation,
    "LogicalNotContext": unary_operation,
    'CreateTableHeaderContext': unsupported,
    'ReplaceTableHeaderContext': unsupported,
    'CreateTableClausesContext': unsupported,
    'InlineTableContext': unsupported,
    'FunctionTableContext': unsupported,
    'CreateTableContext': unsupported,
    'CreateHiveTableContext': unsupported,
    'CreateTableLikeContext': unsupported,
    'ReplaceTableContext': unsupported,
    'AnalyzeContext': unsupported,
    'AddTableColumnsContext': unsupported,
    'RenameTableColumnContext': unsupported,
    'DropTableColumnsContext': unsupported,
    'RenameTableContext': unsupported,
    'SetTablePropertiesContext': unsupported,
    'UnsetTablePropertiesContext': unsupported,
    'AlterTableAlterColumnContext': unsupported,
    'HiveChangeColumnContext': unsupported,
    'HiveReplaceColumnsContext': unsupported,
    'SetTableSerDeContext': unsupported,
    'AddTablePartitionContext': unsupported,
    'RenameTablePartitionContext': unsupported,
    'DropTablePartitionsContext': unsupported,
    'SetTableLocationContext': unsupported,
    'RecoverPartitionsContext': unsupported,
    'DropTableContext': unsupported,
    'ShowTablesContext': unsupported,
    'ShowTableContext': unsupported,
    'ShowCreateTableContext': unsupported,
    'DescribeRelationContext': unsupported,
    'CommentTableContext': unsupported,
    'RefreshTableContext': unsupported,
    'CacheTableContext': unsupported,
    'UncacheTableContext': unsupported,
    'LoadDataContext': unsupported,
    'TruncateTableContext': unsupported,
    'RepairTableContext': unsupported,
    'InsertOverwriteTableContext': unsupported,
    'InsertIntoTableContext': unsupported,
    'DeleteFromTableContext': unsupported,
    'UpdateTableContext': unsupported,
    'MergeIntoTableContext': unsupported,
    'UnsupportedHiveNativeCommandsContext': unsupported,
    'CreateFileFormatContext': unsupported,
    'CreateNamespaceContext': unsupported,
    'CreateViewContext': unsupported,
    'CreateTempViewUsingContext': unsupported,
    'CreateFunctionContext': unsupported,
    'DropNamespaceContext': unsupported,
    'DropViewContext': unsupported,
    'DropFunctionContext': unsupported,
    'LateralViewContext': unsupported,
    'AlterViewQueryContext': unsupported,
    'ShowViewsContext': unsupported,
    'SetNamespacePropertiesContext': unsupported,
    'SetNamespaceLocationContext': unsupported,
    'ShowNamespacesContext': unsupported,
    'ShowCurrentNamespaceContext': unsupported,
    'DescribeNamespaceContext': unsupported,
    'CommentNamespaceContext': unsupported,
    'UseContext': unsupported,
    'JoinTypeContext': concat_keywords,
    'CastContext': cast_context,
    'PrimitiveDataTypeContext': detect_data_type,
    'ComplexDataTypeContext': detect_data_type,
    'StructContext': build_struct,
    "NamedExpressionContext": potential_alias,
    "ErrorCapturingIdentifierContext": check_identifier,
    "ErrorIdentContext": concat_strings,
    "FunctionCallContext": call_function,
    "QualifiedNameContext": concat_strings,
    # WIP!
    # todo: check that all context are there
    #  including yyy: definition
    #  and definition #xxx
    "PredicatedContext": unwrap,
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


def parse_expression(string, debug=False):
    return parse_sql(string, "singleExpression", debug)

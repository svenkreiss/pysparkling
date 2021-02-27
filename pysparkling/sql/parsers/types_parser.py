import re
import sys

from ply import yacc
import ply.lex as lex

from ..types import ArrayType, DataType, DecimalType, MapType, StringType, StructField, StructType
from ..utils import ParseException

reserved = {
    # These are defined in sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala line 2313
    # visitPrimitiveDataType
    'BooleanType': ('boolean',),
    'ByteType': ('tinyint', 'byte',),
    'ShortType': ('smallint', 'short',),
    'IntegerType': ('int', 'integer',),
    'LongType': ('bigint', 'long',),
    'FloatType': ('float', 'real',),
    'DoubleType': ('double',),
    'DateType': ('date',),
    'TimestampType': ('timestamp',),
    'StringType': ('string',),
    'BinaryType': ('binary',),
    'NullType': ('void',),
    'DecimalType': ('decimal', 'dec', 'numeric',),

    # The next 2 are truly special cases... They're translated into a StringType in pyspark (without any length).
    # #       case ("character" | "char", length :: Nil) => CharType(length.getText.toInt)
    'CharType': ('character', 'char',),
    # #       case ("varchar", length :: Nil) => VarcharType(length.getText.toInt)
    'VarcharType': ('varchar',),

    # #       case ("interval", Nil) => CalendarIntervalType
    # #       case (dt, params) =>
    # #         val dtStr = if (params.nonEmpty) s"$dt(${params.mkString(",")})" else dt
    # #         throw QueryParsingErrors.dataTypeUnsupportedError(dtStr, ctx)

    # visitComplexDataType
    'ArrayType': ('array',),
    'MapType': ('map',),
    'StructType': ('struct',),
}

tokens = ['NUMBER', 'COMMENT', 'TEXT', 'ID'] + list(reserved.keys())


def t_COMMENT(t):
    """COMMENT"""
    return t


def t_TEXT(t):
    r"""
    "(\\"|[^"])*"|'(\\'|[^'])*'
    """
    txt = t.value
    first_char = txt[0]
    # Strip first & last char
    txt = txt[1:-1]
    # Remove any escaped characters that are my first char.
    t.value = txt.replace(f'\\{first_char}', first_char)
    return t


def t_NUMBER(t):
    r"""\d+"""
    t.value = int(t.value)
    return t


literals = ':,()<>`'
start = 'expression'
t_ignore = ' \t\r\n'


# Error handling rule
def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


# https://ply.readthedocs.io/en/latest/ply.html#specification-of-tokens
def t_ID(t):
    r"""
    [a-zA-Z_][a-zA-Z_0-9]*
    |
    `(?:``|[^`])*?`
    """
    if t.value[0] == '`':
        t.value = t.value[1:-1].replace('``', '`')
    else:
        try:
            t.type = next(k for k, v in reserved.items() if t.value.lower() in v)
        except StopIteration:
            pass

    return t


def p_expression(p):
    """
    expression : expression_with_colon
               | expression_without_colon
               | datatype
    """
    if isinstance(p[1], DataType):
        p[0] = p[1]
    else:
        p[0] = StructType(p[1])


def p_component_without_colon(p):
    """
    component_without_colon : ID datatype

    component_with_colon : ID ':' datatype
    """
    if len(p) > 3:
        p[0] = StructField(name=p[1], dataType=p[3])
    else:
        p[0] = StructField(name=p[1], dataType=p[2])


def p_expression_handling(p):
    """
    expression_with_colon : expression_with_colon ',' component_with_colon
                          | component_with_colon

    expression_without_colon : expression_without_colon ',' component_without_colon
                             | component_without_colon

    structfields : structfields ',' structfield
                 | structfield
    """
    if len(p) > 2:
        p[0] = p[1] + [p[3]]
    else:
        p[0] = [p[1]]


def p_decimal(p):
    """
    decimal : DecimalType '(' NUMBER ',' NUMBER ')'
            | DecimalType '(' NUMBER ')'
            | DecimalType
    """
    if len(p) == 7:
        p[0] = DecimalType(precision=int(p[3]), scale=int(p[5]))
    elif len(p) == 4:
        p[0] = DecimalType(precision=int(p[3]))
    else:
        p[0] = DecimalType()


def p_primitive_datatype(p):
    """
    primitive_datatype : BooleanType
                       | ByteType
                       | ShortType
                       | IntegerType
                       | LongType
                       | FloatType
                       | DoubleType
                       | DateType
                       | TimestampType
                       | StringType
                       | BinaryType
                       | NullType
    """
    sql_types = sys.modules['pysparkling.sql.types']
    try:
        to_find = p.slice[1].type
        if to_find in ['CharType', 'VarcharType']:
            to_find = 'StringType'  # Special case in pyspark as these types are translated this way.

        p[0] = next(v for k, v in vars(sql_types).items() if k == to_find)()
    except StopIteration:
        raise ParseException(f"Invalid type encountered '{p[1]}'.") from None


def p_datatype_name(p):
    """
    datatype_name : BooleanType
                  | ByteType
                  | ShortType
                  | IntegerType
                  | LongType
                  | FloatType
                  | DoubleType
                  | DateType
                  | TimestampType
                  | StringType
                  | BinaryType
                  | NullType
                  | DecimalType
                  | ArrayType
                  | MapType
                  | StructType
                  | CharType
                  | VarcharType
    """
    p[0] = p[1]


def p_datatype(p):
    """
    datatype : primitive_datatype
             | decimal
             | array
             | map
             | struct
             | empty_struct
             | varchar
    """
    p[0] = p[1]


def p_varchar(p):
    """
    varchar : CharType '(' NUMBER ')'
            | VarcharType '(' NUMBER ')'
    """
    p[0] = StringType()


def p_array(p):
    """
    array : ArrayType '<' datatype '>'
    """
    p[0] = ArrayType(p[3])


def p_map(p):
    """
    map : MapType '<' datatype ',' datatype '>'
    """
    p[0] = MapType(keyType=p[3], valueType=p[5])


def p_structfield(p):
    """
    structfield : ID ':' datatype
                | ID ':' datatype COMMENT TEXT
                | datatype_name ':' datatype
                | datatype_name ':' datatype COMMENT TEXT
    """
    metadata = None
    if len(p) > 4:
        metadata = {'comment': p[5]}

    p[0] = StructField(name=p[1], dataType=p[3], metadata=metadata)


def p_struct(p):
    """
    struct : StructType '<' structfields '>'
    """
    p[0] = StructType(p[3])


def p_empty_struct(p):
    """
    empty_struct : StructType '<' '>'
    """
    p[0] = StructType([])


# Error rule for syntax errors
def p_error(p):
    raise ParseException("Syntax error in input!")


# Build the lexer
lexer = lex.lex(reflags=re.IGNORECASE | re.VERBOSE, debug=False)
parser = yacc.yacc(debug=False, tabmodule='__tabmodule_types_parser')

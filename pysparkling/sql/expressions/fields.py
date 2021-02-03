from pysparkling.sql.expressions.expressions import Expression
from pysparkling.sql.types import StructField
from pysparkling.sql.utils import AnalysisException


class FieldAsExpression(Expression):
    def __init__(self, field):
        super().__init__()
        self.field = field

    def eval(self, row, schema):
        return row[find_position_in_schema(schema, self.field)]

    def __str__(self):
        return self.field.name

    def output_fields(self, schema):
        return [self.field]

    def args(self):
        return (self.field,)


def find_position_in_schema(schema, expr):
    if isinstance(expr, str):
        show_id = False
        field_name = expr
        matches = set(i for i, field in enumerate(schema.fields) if field_name == field.name)
    elif isinstance(expr, FieldAsExpression):
        return find_position_in_schema(schema, expr.field)
    elif isinstance(expr, StructField) and hasattr(expr, "id"):
        show_id = True
        field_name = format_field(expr, show_id=show_id)
        matches = set(i for i, field in enumerate(schema.fields) if expr.id == field.id)
    else:
        if isinstance(expr, StructField):
            expression = "Unbound field {0}".format(expr.name)
        else:
            expression = "Expression type '{0}'".format(type(expr))

        raise NotImplementedError(
            "{0} is not supported. "
            "As a user you should not see this error, feel free to report a bug at "
            "https://github.com/svenkreiss/pysparkling/issues".format(expression)
        )

    return get_checked_matches(matches, field_name, schema, show_id)


def get_checked_matches(matches, field_name, schema, show_id):
    if not matches:
        raise AnalysisException("Unable to find the column '{0}' among {1}".format(
            field_name,
            format_schema(schema, show_id)
        ))

    if len(matches) > 1:
        raise AnalysisException(
            "Reference '{0}' is ambiguous, found {1} columns matching it.".format(
                field_name,
                len(matches)
            )
        )

    return matches.pop()


def format_schema(schema, show_id):
    return [format_field(field, show_id=show_id) for field in schema.fields]


def format_field(field, show_id):
    if show_id:
        return "{0}#{1}".format(field.name, field.id)
    return field.name

from . import Expression
from ..types import StructField
from ..utils import AnalysisException


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
            expression = f"Unbound field {expr.name}"
        else:
            expression = f"Expression type '{type(expr)}'"

        raise NotImplementedError(
            f"{expression} is not supported. "
            "As a user you should not see this error, feel free to report a bug at "
            "https://github.com/svenkreiss/pysparkling/issues"
        )

    return get_checked_matches(matches, field_name, schema, show_id)


def get_checked_matches(matches, field_name, schema, show_id):
    if not matches:
        raise AnalysisException(f"Unable to find the column '{field_name}'"
                                f" among {format_schema(schema, show_id)}")

    if len(matches) > 1:
        raise AnalysisException(
            f"Reference '{field_name}' is ambiguous, found {len(matches)} columns matching it."
        )

    return matches.pop()


def format_schema(schema, show_id):
    return [format_field(field, show_id=show_id) for field in schema.fields]


def format_field(field, show_id):
    if show_id:
        return f"{field.name}#{field.id}"
    return field.name

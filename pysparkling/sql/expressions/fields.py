def format_schema(schema, show_id):
    return [format_field(field, show_id=show_id) for field in schema.fields]


def format_field(field, show_id):
    if show_id:
        return "{0}#{1}".format(field.name, field.id)
    return field.name

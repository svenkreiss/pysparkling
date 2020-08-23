def resolve_column(col, row, schema, allow_generator=True):
    """
    Return the list of column names corresponding to a column value and a schema and:
    If allow generator is False, a list of values corresponding to a row
    If allow generator is True, a list of list of values, each list correspond to a row
    """
    output_cols = [field.name for field in col.output_fields(schema)]

    output_values = col.eval(row, schema)

    if not allow_generator and col.may_output_multiple_rows:
        raise Exception("Generators are not supported when it's nested in expressions, "
                        "but got: {0}".format(col))

    if not col.may_output_multiple_rows:
        output_values = [output_values]
        if not col.may_output_multiple_cols:
            output_values = [output_values]

    return output_cols, output_values

from pysparkling.sql.expressions.expressions import Expression


class StarOperator(Expression):
    @property
    def may_output_multiple_cols(self):
        return True

    def output_fields(self, schema):
        return schema.fields

    def eval(self, row, schema):
        return [row[col] for col in row.__fields__]

    def __str__(self):
        return "*"


class CaseWhen(Expression):
    def __init__(self, conditions, values):
        super(CaseWhen, self).__init__(conditions, values)

        self.conditions = conditions
        self.values = values

    def eval(self, row, schema):
        for condition, function in zip(self.conditions, self.values):
            condition_value = condition.eval(row, schema)
            if condition_value:
                return function.eval(row, schema)
        return None

    def __str__(self):
        return "CASE {0} END".format(
            " ".join(
                "WHEN {0} THEN {1}".format(condition, value)
                for condition, value in zip(self.conditions, self.values)
            )
        )

    def add_when(self, condition, value):
        return CaseWhen(
            self.conditions + [condition],
            self.values + [value]
        )

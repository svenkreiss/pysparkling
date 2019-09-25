from pysparkling.sql.expressions.expressions import Expression


class StringTrim(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row, schema):
        return self.column.eval(row, schema).strip()

    def __str__(self):
        return "trim({0})".format(self.column)


class StringLTrim(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row, schema):
        return self.column.eval(row, schema).lstrip()

    def __str__(self):
        return "ltrim({0})".format(self.column)


class StringRTrim(Expression):
    def __init__(self, column):
        super().__init__(column)
        self.column = column

    def eval(self, row, schema):
        return self.column.eval(row, schema).rstrip()

    def __str__(self):
        return "rtrim({0})".format(self.column)

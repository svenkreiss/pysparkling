from pysparkling.sql.expressions.expressions import Expression


class StarOperator(Expression):
    def output_cols(self, row):
        return (col for col in row.__fields__)

    def eval(self, row):
        return (row[col] for col in row.__fields__)

    def __str__(self):
        return "*"


class IsNull(Expression):
    def __init__(self, expr):
        self.expr = expr

    def eval(self, row):
        return self.expr.eval(row) is None

    def __str__(self):
        return "({0} IS NULL)".format(self.expr)


class IsNotNull(Expression):
    def __init__(self, expr):
        self.expr = expr

    def eval(self, row):
        return self.expr.eval(row) is not None

    def __str__(self):
        return "({0} IS NOT NULL)".format(self.expr)


class Contains(Expression):
    def __init__(self, expr, value):
        self.expr = expr
        self.value = value

    def eval(self, row):
        return self.value in self.expr.eval(row)

    def __str__(self):
        return "contains({0}, {1})".format(self.expr, self.value)


class Negate(Expression):
    def __init__(self, expr):
        self.expr = expr

    def eval(self, row):
        return not self.expr.eval(row)

    def __str__(self):
        return "(- {0})".format(self.expr)


class Add(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) + self.arg2.eval(row)

    def __str__(self):
        return "({0} + {1})".format(self.arg1, self.arg2)


class Minus(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) - self.arg2.eval(row)

    def __str__(self):
        return "({0} - {1})".format(self.arg1, self.arg2)


class Time(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) * self.arg2.eval(row)

    def __str__(self):
        return "({0} * {1})".format(self.arg1, self.arg2)


class Divide(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) / self.arg2.eval(row)

    def __str__(self):
        return "({0} / {1})".format(self.arg1, self.arg2)


class Mod(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) % self.arg2.eval(row)

    def __str__(self):
        return "({0} % {1})".format(self.arg1, self.arg2)


class Pow(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) ** self.arg2.eval(row)

    def __str__(self):
        return "POWER({0}, {1})".format(self.arg1, self.arg2)


class Equal(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        value_1 = self.arg1.eval(row)
        value_2 = self.arg2.eval(row)
        if value_1 is None or value_2 is None:
            return None
        return value_1 == value_2

    def __str__(self):
        return "({0} = {1})".format(self.arg1, self.arg2)


class EqNullSafe(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) == self.arg2.eval(row)

    def __str__(self):
        return "({0} <=> {1})".format(self.arg1, self.arg2)


class LessThan(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) < self.arg2.eval(row)

    def __str__(self):
        return "({0} < {1})".format(self.arg1, self.arg2)


class LessThanOrEqual(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) <= self.arg2.eval(row)

    def __str__(self):
        return "({0} <= {1})".format(self.arg1, self.arg2)


class GreaterThan(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) > self.arg2.eval(row)

    def __str__(self):
        return "({0} > {1})".format(self.arg1, self.arg2)


class GreaterThanOrEqual(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) >= self.arg2.eval(row)

    def __str__(self):
        return "({0} >= {1})".format(self.arg1, self.arg2)


class And(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) and self.arg2.eval(row)

    def __str__(self):
        return "({0} AND {1})".format(self.arg1, self.arg2)


class Or(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) or self.arg2.eval(row)

    def __str__(self):
        return "({0} OR {1})".format(self.arg1, self.arg2)


class Invert(Expression):
    def __init__(self, arg1):
        self.arg1 = arg1

    def eval(self, row):
        return not self.arg1.eval(row)

    def __str__(self):
        return "(NOT {0})".format(self.arg1)


class BitwiseOR(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) | self.arg2.eval(row)

    def __str__(self):
        return "({0} | {1})".format(self.arg1, self.arg2)


class BitwiseAnd(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) & self.arg2.eval(row)

    def __str__(self):
        return "({0} & {1})".format(self.arg1, self.arg2)


class BitwiseXor(Expression):
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def eval(self, row):
        return self.arg1.eval(row) ^ self.arg2.eval(row)

    def __str__(self):
        return "({0} ^ {1})".format(self.arg1, self.arg2)


class StartsWith(Expression):
    def __init__(self, arg1, substr):
        self.arg1 = arg1
        self.substr = substr

    def eval(self, row):
        return str(self.arg1.eval(row)).startswith(self.substr)

    def __str__(self):
        return "startswith({0}, {1})".format(self.arg1, self.substr)


class EndsWith(Expression):
    def __init__(self, arg1, substr):
        self.arg1 = arg1
        self.substr = substr

    def eval(self, row):
        return str(self.arg1.eval(row)).endswith(self.substr)

    def __str__(self):
        return "endswith({0}, {1})".format(self.arg1, self.substr)


class Substr(Expression):
    def __init__(self, expr, start, end):
        self.expr = expr
        self.start = start
        self.end = end

    def eval(self, row):
        return str(self.expr.eval(row))[self.start:self.end]

    def __str__(self):
        return "substring({0}, {1}, {2})".format(self.expr, self.arg1, self.substr)


class IsIn(Expression):
    def __init__(self, arg1, cols):
        self.arg1 = arg1
        self.cols = cols

    def eval(self, row):
        return str(self.arg1.eval(row)) in [col.eval(row) for col in self.cols]

    def __str__(self):
        return "({0} IN ({1}))".format(
            self.arg1,
            ", ".join(str(col) for col in self.cols)
        )


class Alias(Expression):
    def __init__(self, expr, alias):
        self.expr = expr
        self.alias = alias

    def eval(self, row):
        return self.expr.eval(row)

    def __str__(self):
        return self.alias

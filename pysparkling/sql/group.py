class GroupedData(object):
    def __init__(self, jgd, df):
        self._jgd = jgd
        self._df = df
        self.sql_ctx = df.sql_ctx

from pysparkling.sql.column import Column


def col(colName):
    """
    :rtype: Column
    """
    return Column(colName)

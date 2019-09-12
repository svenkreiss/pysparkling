class CapturedException(Exception):
    pass


class AnalysisException(CapturedException):
    pass


class ParseException(CapturedException):
    pass


class IllegalArgumentException(CapturedException):
    pass

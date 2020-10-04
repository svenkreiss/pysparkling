from pysparkling.fileio import TextFile


def get_records(f_name, linesep, encoding):
    f_content = TextFile(f_name).load(encoding=encoding).read()
    records = f_content.split(linesep) if linesep is not None else f_content.splitlines()
    return records

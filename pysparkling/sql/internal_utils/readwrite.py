from pysparkling.sql.utils import IllegalArgumentException


def to_option_stored_value(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


class OptionUtils(object):
    def _set_opts(self, schema=None, **options):
        """
        Set named options (filter out those the value is None)
        """
        if schema is not None:
            self.schema(schema)
        for k, v in options.items():
            if v is not None:
                self.option(k, v)

    def option(self, key, value):
        raise NotImplementedError

    def schema(self, schema):
        # By default OptionUtils subclass do not support schema
        raise IllegalArgumentException(
            "schema is not a valid argument for {0}".format(self.__class__)
        )

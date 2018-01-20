.. _api_context:

.. currentmodule:: pysparkling

Context
-------

A :class:`~pysparkling.Context` describes the setup. Instantiating a Context with the default
arguments using ``Context()`` is the most lightweight setup. All data is just
in the local thread and is never serialized or deserialized.

If you want to process the data in parallel, you can use the `multiprocessing`
module. Given the limitations of the default `pickle` serializer, you can
specify to serialize all methods with `cloudpickle` instead. For example,
a common instantiation with `multiprocessing` looks like this:

.. code-block:: python

  sc = pysparkling.Context(
      multiprocessing.Pool(4),
      serializer=cloudpickle.dumps,
      deserializer=pickle.loads,
  )

This assumes that your data is serializable with `pickle` which is generally
faster. You can also specify a custom serializer/deserializer for data.

.. autoclass:: pysparkling.Context
   :members:

from IPython.parallel import Client
rc = Client(packer='pickle')

view = rc[:]
results = view.map(lambda x: x ** 30, range(8))
print(results.get())

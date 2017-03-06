from IPython.parallel import Client

rc = Client('/Users/sven/.starcluster/ipcluster/'
            'SecurityGroup:@sc-smallcluster-us-east-1.json',
            sshkey='/Users/sven/.ssh/starclusterkey.rsa', packer='pickle')

view = rc[:]
results = view.map(lambda x: x ** 30, range(8))
print(results.get())

from starcluster.clustersetup import ClusterSetup
import string
import random


class PackageInstaller(ClusterSetup):
    def __init__(self):
        '''From: https://stackoverflow.com/questions/18319101/whats-the-best-way-to-generate-random-strings-of-a-specific-length-in-python'''
        self.secretkey = ''.join(random.choice(string.ascii_uppercase) for i in range(12))

    def init_node(self, node, master):
        node.ssh.execute('mkdir -p /tmp && curl -0 http://www.parallelpython.com/downloads/pp/pp-1.6.4.tar.gz | tar -zxC /tmp/')
        node.ssh.execute('cd /tmp/pp-1.6.4/ && python ./setup.py install')

    def _init_slave_node(self, node):
        node.ssh.execute_async('/tmp/pp-1.6.4/ppserver.py -a -s "' + self.secretkey + '" -P /tmp/ppserver.pid')

    def run(self, nodes, master, user, user_shell, volumes):
        slave_aliases = []
        for node in nodes:
            self.init_node(node, master)
            if node != master:
                self._init_slave_node(node)
                slave_aliases.append(node.alias)

        hostf = master.ssh.remote_file('/tmp/otherhosts', 'w')
        hostf.write('\n'.join(slave_aliases))
        hostf.write('\n')
        hostf.close()

        secretf = master.ssh.remote_file('/tmp/secretkey', 'w')
        secretf.write(self.secretkey)
        secretf.write('\n')
        secretf.close()

    def stop_node(self, node):
        node.ssh.execute('(test -e /tmp/ppserver.pid && kill `cat /tmp/ppserver.pid` && rm /tmp/ppserver.pid) || true')

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        self.stop_node(node)

    def on_shutdown(self, nodes, master, user, user_shell, volumes):
        for node in nodes:
            self.stop_node(node)

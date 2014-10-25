from starcluster.clustersetup import ClusterSetup

class PackageInstaller(ClusterSetup):
    def __init__(self):
        self.setupRun = True

    def _init_master_node(self, master):
        master.ssh.execute('mkdir -p /tmp && curl -0 https://codeload.github.com/s3tools/s3cmd/tar.gz/v1.5.0-rc1 | tar -zxC /tmp/')
        master.ssh.execute('cd /tmp/s3cmd-1.5.0-rc1/ && python ./setup.py install')

    def run(self, nodes, master, user, user_shell, volumes):
        self._init_master_node(master)



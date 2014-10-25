
Setup:
# NOTE: you will need to create a 
export MYGITREPO=<base directory of this git repository>
mkdir -p ~/.starcluster/plugins
mkdir -p ~/.starcluster/configs 
nano ~/.starcluster/configs/aws # put your aws credential section in here
nano ~/.starcluster/configs/keys # put your private key information here
ln -s $MYGITREPO/config ~/.starcluster/config
ln -s $MYGITREPO/plugins/* ~/.starcluster/plugins/
nano ~/.starcluster/config # change the key section to refer to your keys

Running:
starcluster start test

starclsuter start crawler

Requirements:
* Python 2.7
* pp module from http://www.parallelpython.com/
* s3cmd from http://s3tools.org/, I was using 1.5.0-rc1 (http://sourceforge.net/projects/s3tools/files/s3cmd/1.5.0-rc1/s3cmd-1.5.0-rc1.tar.gz)
* UNIX-like environment (so python can work correctly with s3cmd)

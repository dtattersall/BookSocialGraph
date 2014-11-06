# Distributed URL Crawler
Customized for crawling Amazon.com's book catalog

## Requirements:
* Python 2.7
* STARCluster from [http://star.mit.edu/cluster](http://star.mit.edu/cluster/)
* ParallelPython (called pp) module from [http://www.parallelpython.com](http://www.parallelpython.com/)
* s3cmd from [http://s3tools.org](http://s3tools.org), tested with [1.5.0-rc1 from sourceforge](http://sourceforge.net/projects/s3tools/files/s3cmd/1.5.0-rc1/s3cmd-1.5.0-rc1.tar.gz)
* If you wish to run it locally, a UNIX-like environment (so python's os.system() call to s3cmd works correctly)

## Setup:
### STARCluster
Install STARCluster and take a brief look over the [Quickstart guide](http://star.mit.edu/cluster/docs/latest/quickstart.html) and the first part of the [User Guide](http://star.mit.edu/cluster/docs/latest/manual/configuration.html#creating-the-configuration-file). This document will go through most of the configuration (much of the heavy lifting has already been done in the `starcluster/config` file) but, but you will likely need to modify it to suit your own EC2/EBS/S3 configuration and preferences

### AWS Account
I'm not going to go through the details but you will need to pick a region (the `starcluster/config` uses US-West in Oregon) and create an [S3 bucket](http://docs.aws.amazon.com/AmazonS3/latest/gsg/CreatingABucket.html), an [EC2 keypair](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair) (or [use starcluster to create it](http://star.mit.edu/cluster/docs/latest/manual/configuration.html#amazon-ec2-keypairs)), and a small (2GB or `1500 * (median webpage size)`) [EBS volume](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-volume.html). Make sure all three are in the same region as you will be charged for moving data between regions.

Creating a useful EBS volume can be a bit pedantic. You actually need to start up an EC2 instance and use that to [attach](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-attaching-volume.html) and then [format](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html) the volume for scratch space before the crawler can use it. I found it easiest to create the volume (so you have a volume id) but hold off on the rest until after you have installed and run starcluster. Then you can use the `single` template to start a single instance and do the necessary legwork.

### Crawler Setup
Execute the following to set up all the links and files:
```
export MYGITREPO=<base directory of this git repository>
mkdir -p ~/.starcluster/plugins
mkdir -p ~/.starcluster/configs 
touch ~/.starcluster/configs/aws
touch ~/.starcluster/configs/keys 
touch ~/.starcluster/configs/volumes
touch ~/.starcluster/configs/sthree_credentials
ln -s $MYGITREPO/config ~/.starcluster/config
ln -s $MYGITREPO/plugins/* ~/.starcluster/plugins/
nano ~/.starcluster/config # change the key section to refer to your keys
```
#### Tell STARCluster about your AWS account
Open `~/.starcluster/configs/aws` and fill it with the following:
```
[aws info]
aws_access_key_id = #your aws access key id here
aws_secret_access_key = #your secret aws access key here
aws_user_id = #your 12-digit aws user id here
```
#### Tell STARCluster about your EC2 Keypair
__Note:__ Frustratingly the section name (`dtt_awskey` in the example below) must be the same as the pair name in the EC2 console. You will likely need to change this configuration and the cluster template configurations in `starcluster/config` so that they all refer to the correct EC2 keypair

Open `~/.starcluster/configs/keys` and fill it with the following:
```
[key dtt_awskey]
KEY_LOCATION = # Full path to your AWS private key
```
#### Tell STARCluster about your EBS Volume
__Note:__ you can do this even if you haven't formatted the volume yet

Open `~/.starcluster/configs/volumes` and fill it with the following:
```
[volume crawlerscratch]
VOLUME_ID = # Your volume id 
MOUNT_PATH = /vol
```

#### Create an S3 credentials file
If you want the crawled HTML pages to be uploaded to S3 you need a separate file with the credentials in it.

Open `~/.starcluster/configs/sthree_credentials` and fill it with the following:
```
bucket= # your bucket name
accesskey= # your aws access key id here
secretkey= # your secret aws access key here
```

## Running:
```
starcluster start -c <config> crwl
starcluster put crwl $MYGITREPO/crawler.py /tmp/crawler.py
starcluster put crwl ~/.starcluster/configs/sthree_credentials /tmp/sthree_credentials
starcluster put crwl <isbn text file> /tmp/<isbn text file name>
starcluster sshmaster
# on master host: 
/tmp/crawler.py -i /tmp/<isbn text files> -n /tmp/otherhosts -s /tmp/sthree_credentials -k `cat /tmp/secretkey`
``` 

#!/usr/bin/python

# Note: python >= 2.7 required
# Note: you will need s3cmd installed from http://s3tools.org/, I was using 1.5.0-rc1 (http://sourceforge.net/projects/s3tools/files/s3cmd/1.5.0-rc1/s3cmd-1.5.0-rc1.tar.gz)

import pp
import time         # for sleep
import os
import shutil
import sys
import string
import urllib2
import random
import errno
import argparse
import zipfile


parser = argparse.ArgumentParser(description='Crawl a set of ISBNs on amazon.com')
parser.add_argument('-i','--isbns', type=file, required=True, help='The file that contains the ISBNs to crawl')
parser.add_argument('-n','--nodes', type=file, help='The file that contains the cluster slave nodes to use')
parser.add_argument('-k','--secretkey', type=str, help='The secret key used to communicate with the cluster slave nodes')
parser.add_argument('-s','--s3credentials', type=file, help='The file containing the S3 credentials required to upload:\n\tbucket=<bucketname>\n\taccessKey=<access key>\n\tsecretKey=<secret key>')
args = parser.parse_args()

LocalRun = (args.nodes == None)
if LocalRun:
    print "No slave nodes provided, assuming you just want to run on this machine and this is a LOCAL RUN"

s3info = {}
if (args.s3credentials != None):
    s3lines = args.s3credentials.read().splitlines()
    for line in s3lines:
        kv = line.split('=')
        s3info[str.lower(kv[0])] = kv[1]
else:
    print "No S3 credentials provided, we will store the files locally"

userAgents = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0"
]
basePath = '/tmp/books/' if LocalRun else '/vol/temp_data/'

# download the contents of a url to the book's directory
# TODO fix the names 
#def _dummy_job(bookid, headers):
def _fetch_url_ (bookid, headers):
    url = 'http://www.amazon.com/dp/'+str(bookid)
    success = False
    maxTries = 5
    ntries = 0
    sleep = 2
    request = urllib2.Request(url,None,headers)
    
    while not success and ntries < maxTries:
        text = ''
        try: 
            response = urllib2.urlopen(request)
            if response.getcode() == 200:
                success = True
                text = response.read()
            else:
                print "Received {0} code when retrieving {1}".format(response.getcode(), url)
        except urllib2.HTTPError, e:
            print "When retrieving {0} received HTTP Error: {1}".format(url, e.code)
            if e.code == 404:
                print "received 404, so skipping further retries"
                ntries = maxTries
        except urllib2.URLError, e:
            print "URL Error:", e.reason , url
        finally:
            try:
                response.close()
            except NameError: 
                pass
            
        if success: 
            time.sleep(sleep) # sleep before trying next task 
        else:
            # exponential backoff
            time.sleep(sleep**ntries)
        ntries+=1
    # note if text is '' then we assume this is an error
    return (bookid, text)

#def _fetch_url_ (bookid, headers):
def _dummy_job(bookid, headers):
    time.sleep(1)
    text = 'Fake book data'
    return (bookid, text)


def startJob(bookid,jobserver):
    path = basePath + str(bookid) 
    index = random.randint(0,len(userAgents)-1)
    headers = {
        'User-Agent' : userAgents[index],
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9',
        'Accept-Language' : 'en-US,en;q=0.8'
    }
    def inner():
        return _fetch_url_(bookid, headers)

    return inner if LocalRun else jobserver.submit(_fetch_url_, (bookid, headers),(),('time','urllib2','pp'))

def slicer(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def rmall(filepath):
    if os.path.isdir(filepath):
        shutil.rmtree(filepath)
    else:
        os.remove(filepath)

def uploadToS3(filepath):
    if not bool(s3info): # test if the dictionary is empty
        print "S3 credentials not defined... not uploading", filepath
        return False

    destination = os.path.basename(filepath)
    cmd = "/usr/local/bin/s3cmd --access_key={0} --secret_key={1} --mime-type=application/zip --no-progress --quiet put {2} s3://{3}/{4}".format(
            s3info['accesskey'], s3info['secretkey'], filepath, s3info['bucket'], destination)
    exitcode = os.system(cmd)
    print "uploading",filepath,"to S3"
    return (exitcode == 0)



########## main ##########

starttime = time.gmtime() 
print "Start Time:", time.strftime("%Y-%m-%d %H:%M:%S UTC", starttime)

book_ids = args.isbns.read().splitlines()
book_ids = map(lambda x: string.strip(x), book_ids)

# start servers
totalServers = 1
if LocalRun:
    job_server = pp.Server()
else:
    ppservers = tuple(args.nodes.read().splitlines())
    totalServers = len(ppservers)+1 # +1 for the local node
    #print ppservers
    #print args.secretkey
    # TODO figure out why we can't communicate with other servers
    job_server = pp.Server(ppservers=ppservers) if (args.secretkey == None) else pp.Server(ppservers=ppservers, secret=args.secretkey)

print "Starting pp with", job_server.get_ncpus(), "cpus"
currentNodeCount = len(job_server.get_active_nodes())
print "We have", currentNodeCount, "nodes"
while (currentNodeCount < totalServers):
    print "Expecting {0} cluster nodes, have {1}, waiting for others to join...".format(totalServers, currentNodeCount)
    time.sleep(2)
    currentNodeCount = len(job_server.get_active_nodes())

# set up the temp directory
try:
    os.makedirs(basePath)
except OSError as exc: # Python >2.5
    if exc.errno == errno.EEXIST and os.path.isdir(basePath):
        # if the directory is already there, delete all contents
        files=os.listdir(basePath)
        for f in files:
            rmall(basePath + f)
    else: raise


# grab the books 1000 at a time and fetch the urls; wait for the results and then zip them up
batchSize = 10 if LocalRun else 100
print "Total number of books to process is",len(book_ids)
failedBooks = []
succeededBooks = []
count = 0
for bslice in slicer(book_ids, batchSize):
    jobs=[]
    sliceName = str(min(bslice)) + '_' + str(max(bslice)) + '_' + str(count) 
    zpath = basePath + sliceName +  '.zip'
    outputPath = basePath + sliceName + '/'
    processedBooks = []
    
    # Kick off the jobs
    jobs = map(lambda x: startJob(x,job_server), bslice)

    # set up the slice's directory
    try:
        os.makedirs(outputPath)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(outputPath):
            # if the directory is already there, delete all contents
            files=os.listdir(outputPath)
            for f in files:
                rmall(outputPath + f)
            pass
        else: raise

    # block waiting for all the jobs to complete, then write outputs as appropriate
    for job in jobs:
        (bkid, output) = job()
        if output == '': # fetch failed
            failedBooks.append(bkid) 
        else:
            processedBooks.append(bkid)
            local_file = open(outputPath + str(bkid), "w")
            local_file.write(output)
            local_file.close()

    if (len(processedBooks) > 0):
        # zip everything up
        zout = zipfile.ZipFile(zpath,'w',zipfile.ZIP_DEFLATED)
        for root,dirs,files in os.walk(outputPath):
            for f in files:
                zout.write(os.path.join(root, f))
        zout.close()

        #delete all the files we just zipped 
        rmall(outputPath)

        uploadSuccess = uploadToS3(zpath)
        
        # if we stored it in s3, delete the zipfile and the slice's directory
        if (uploadSuccess or LocalRun):
            if not LocalRun: # if it's a local run, leave the zip file where it is
                rmall(zpath)
            for b in processedBooks:
                succeededBooks.append((b, zpath))
        else:
            failedBooks = failedBooks + processedBooks

    print "Finished batch of size", len(bslice)
    count += len(bslice)
    time.sleep(1)

if (len(failedBooks) > 0):
    failedpath = basePath + 'incompleteBooks'
    local_file = open(failedpath, "w")
    for bkid in failedBooks:
      local_file.write(str(bkid) + "\n")
    local_file.close()
    upsucc = uploadToS3(failedpath)
    if (upsucc):
        rmall(failedpath)

if (len(succeededBooks) > 0):
    manifestPath = basePath + 'book_manifest'
    local_file = open(manifestPath, "w")
    for (bkid, zp) in succeededBooks:
      local_file.write("{0},{1}\n".format(bkid, os.path.basename(zp)))
    local_file.close()
    upsucc = uploadToS3(manifestPath)
    if (upsucc):
        rmall(manifestPath)


endtime = time.gmtime() 
print "End Time:", time.strftime("%Y-%m-%d %H:%M:%S UTC", endtime)
print "Total Time:", time.mktime(endtime)-time.mktime(starttime)

#!/usr/bin/python

import argparse

# imports also required by remote pp instance
import pp
import boto
import zipfile
import os
import shutil
import re
import random
import string
import time
# requires puthon >= 2.7 and probably < 3.0


########## Parse arguments ###############
parser = argparse.ArgumentParser(description='extract a set of parent ASINs given .zip of HTML downloads of ISBNs on amazon.com')
parser.add_argument('-i','--zipfiles', type=file, required=True, help='The file that contains the list of .zip files to download and process')
parser.add_argument('-n','--nodes', type=file, help='The file that contains the cluster slave nodes to use')
parser.add_argument('-k','--secretkey', type=str, help='The secret key used to communicate with the cluster slave nodes')
parser.add_argument('-s','--s3credentials', type=file, help='The file containing the S3 credentials required to upload:\n\tbucket=<bucketname>\n\taccessKey=<access key>\n\tsecretKey=<secret key>')
args = parser.parse_args()

LocalRun = (args.nodes == None)
if LocalRun:
    print "No slave nodes provided, assuming you just want to run on this machine and this is a LOCAL RUN"

s3data = {}
if (args.s3credentials != None):
    s3lines = args.s3credentials.read().splitlines()
    for line in s3lines:
        kv = line.split('=')
        s3data[str.lower(kv[0])] = kv[1]
else:
    print "No S3 credentials provided, we will store the files locally"

basePath = '/tmp/books/' if LocalRun else '/vol/extracted_asins/'
basePath = basePath + 'run_' + ''.join(random.choice(string.ascii_uppercase) for i in range(8)) + '/'


########## Helper Functions ###############

# Sadly pp is a bit limited and does not include external functions that are defined in 
# other parts of the file, therefore we have to pack them all into this one function
def download_and_analyze(zipfiles, safepath, s3info):
    def downloadFromS3(remotefile, destdir):
        if not bool(s3info): # test if the dictionary is empty
            print "S3 credentials not defined... not downloading",remotefile
            return False
        
        destination = (destdir if destdir[-1] == '/' else destdir + '/') + remotefile
        conn = boto.connect_s3(s3info['accesskey'], s3info['secretkey'])
        bucket = conn.get_bucket(s3info['bucket'])
        key = bucket.get_key(remotefile)
        if key == None:
            print "Could not find key {0} in bucket {1}!".format(remotefile, s3info['bucket'])
            return False
        test = key.get_contents_to_filename(destination)
        print "Downloaded {0} to {1}".format(remotefile, destination)

        success = False
        timeout = time.mktime(time.gmtime()) + 60 # in seconds
        while (not success) and (time.mktime(time.gmtime()) < timeout):
            success = os.path.exists(destination)
            time.sleep(1)

        if not success:
            print "File {0} does not exist despite download 'succeeding'".format(destination)
        
        return success

    def uniquePath(d, prefix=""):
        directory = (d if d[-1] == '/' else d + '/')
        unique = None
        while not unique or os.path.exists(unique):
            name = prefix + ''.join(random.choice(string.ascii_uppercase) for i in range(8)) + '/'
            unique = directory + name
        return unique

    def rmall(filepath):
        if os.path.isdir(filepath):
            shutil.rmtree(filepath)
        else:
            os.remove(filepath)

    def makeCleanDir(outputPath):
        try:
            os.makedirs(outputPath)
        except OSError as exc: 
            if exc.errno == errno.EEXIST and os.path.isdir(outputPath):
                # if the directory is already there, delete all contents
                files=os.listdir(outputPath)
                for f in files:
                    rmall(outputPath + f)
                pass
            else: 
                raise exc

    def downloadFile(zipfilename, path):
        makeCleanDir(path)
        successful = downloadFromS3(zipfilename, path)
        if not successful:
            raise IOError('Download of ' + zipfilename + ' from S3 failed')

        outputPath = path + 'files' + '/' 
        makeCleanDir(outputPath)
        with zipfile.ZipFile(path + zipfilename, "r") as z:
            z.extractall(outputPath)

        filespath = None
        # keep descending in the unzipped directories until you find the files
        for dirName, subdirList, fileList in os.walk(outputPath):
            if len(fileList) > 0:
                filespath = dirName
                break

        return filespath

    def analyzeDirectory(directory, zipfilename):
        captchaText = '<form method="get" action="/errors/validateCaptcha" name="">'
        directory = (directory if directory[-1] == '/' else directory + '/')

        badBooks = []
        goodBooks = []
        # for each file in resulting directory
        files=os.listdir(directory)
        for isbn in files:
            with open(directory + isbn) as f:
                text = f.read()
                if captchaText in text:
                    badBooks.append((isbn, zipfilename, 'captcha'))
                else:
                    parentasins = set() # limit to unique parent asins

                    ##### extracting parent asins ######
                    m = re.findall('parentASIN=([0-9]{13}|[0-9\-]{14}|[0-9A-Z]{10})', text)
                    if m != None:
                        parentasins.update(m)
                    ##### extracting parent asins ######

                    if len(parentasins) != 0:
                        goodBooks.append((isbn,parentasins))
                    else:
                        badBooks.append((isbn, zipfilename, 'no_parent_asins_found'))

        return (goodBooks, badBooks)

    failedBooksPath = safepath + 'failedBooks'
    succeededBooksPath = safepath + 'succeededBooks'
    for filename in zipfiles:
        outputPath = uniquePath(safepath, 'zipfile_')
        actualDirectory = downloadFile(filename, outputPath)
        # for each file in list
        (succeededBooks, failedBooks) = analyzeDirectory(actualDirectory, filename)

        # write to the appropriate files 
        if (len(failedBooks) > 0):
            with open(failedBooksPath , "a") as local_file:
                for items in failedBooks:
                  local_file.write(','.join(items) + '\n')

        if (len(succeededBooks) > 0):
            with open(succeededBooksPath, "a") as local_file:
                for (bkid, pasins) in succeededBooks:
                  local_file.write("{0},[{1}]\n".format(bkid, ','.join(pasins)))

        rmall(outputPath)

def uniquePath(d, prefix=""):
    directory = (d if d[-1] == '/' else d + '/')
    unique = None
    while not unique or os.path.exists(unique):
        name = prefix + ''.join(random.choice(string.ascii_uppercase) for i in range(8)) + '/'
        unique = directory + name
    return unique

def rmall(filepath):
    if os.path.isdir(filepath):
        shutil.rmtree(filepath)
    else:
        os.remove(filepath)


def makeCleanDir(outputPath):
    try:
        os.makedirs(outputPath)
    except OSError as exc: 
        if exc.errno == errno.EEXIST and os.path.isdir(outputPath):
            # if the directory is already there, delete all contents
            files=os.listdir(outputPath)
            for f in files:
                rmall(outputPath + f)
            pass
        else: 
            raise exc

def startJob(zipfiles, jobserver, startingPath):
    path = uniquePath(startingPath,'job_')
    makeCleanDir(path)
    def inner():
        return download_and_analyze(zipfiles, path, s3data)
    imports = ('pp','zipfile','os', 'shutil','re', 'random','string', 'time', 'boto')
    return inner if LocalRun else jobserver.submit(download_and_analyze, (zipfiles, path, s3data),(),imports)

def slicer(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def writeFailures(failures, path):
    # write unprocessed zipfiles to a file somewhere
    if (len(failures) > 0):
        # Note: the name of the file to store these in needs to be longer or shorter or different somehow
        # than the one that uniquePath() makes otherwise you need to test for a conflict
        with open(path + 'failed_to_process_zip_files', "a") as local_file:
            for fids in failures:
                for f in fids:
                    local_file.write("s3://{0}/{1}\n".format(s3data['bucket'],f))

########## main ###############

starttime = time.gmtime() 
print "Start Time:", time.strftime("%Y-%m-%d %H:%M:%S UTC", starttime)

file_ids = args.zipfiles.read().splitlines()
file_ids = map(lambda x: os.path.basename(string.strip(x)), file_ids)

# start servers
totalServers = 1
if LocalRun:
    job_server = pp.Server()
else:
    ppservers = tuple(args.nodes.read().splitlines())
    totalServers = len(ppservers)+1 # +1 for the local node
    job_server = pp.Server(ppservers=ppservers) if (args.secretkey == None) else pp.Server(ppservers=ppservers, secret=args.secretkey)

print "Starting pp with", job_server.get_ncpus(), "cpus"
currentNodeCount = len(job_server.get_active_nodes())
print "We have", currentNodeCount, "nodes"
while (currentNodeCount < totalServers):
    print "Expecting {0} cluster nodes, have {1}, waiting for others to join...".format(totalServers, currentNodeCount)
    time.sleep(2)
    currentNodeCount = len(job_server.get_active_nodes())
print "All expected nodes have joined"

# set up the temp directory
makeCleanDir(basePath)

batchSize = 10 if LocalRun else 200 
perJobSize = 1
print "Total number of files to process is",len(file_ids)

count = 0
failures = []
for fslice in slicer(list(slicer(file_ids, perJobSize)), batchSize):
    # Kick off the jobs
    # TODO we have to think carefully how to catch failures here so we can figure out how to
    # record which fids we need to re-run
    jobs = map(lambda fids: (fids, startJob(fids, job_server, basePath)), fslice)
    print "New round of jobs launched...awaiting responses"

    # block waiting for all the jobs to complete
    for (fids,job) in jobs:
        success = False
        try:
            job()
            count = count+1 
        except (KeyboardInterrupt, SystemExit):
            print "Caught keyboard interrupt/system exit"
            writeFailures(failures, basePath)
            exit(1) 
        except Exception, e:
            print "Encountered exception when executing job: ", e
            failures.append(fids)

    print "Successfully processed {0} zip files and skipped {1}".format(count, len(failures))
    print "This has been running for {0} seconds".format(time.mktime(time.gmtime())-time.mktime(starttime))

writeFailures(failures, basePath)

endtime = time.gmtime() 
print "End Time:", time.strftime("%Y-%m-%d %H:%M:%S UTC", endtime)
print "Total Time:", time.mktime(endtime)-time.mktime(starttime)

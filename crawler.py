#!/usr/bin/python

# Note: python > 2.5 required

import pp
import time         # for sleep
import os
import sys
import urllib2
import random
import errno
#import zipfile
#import codecs       # for handling UTF

LocalRun = (len(sys.argv) > 2 and (sys.argv[2] == '--local'))

userAgents = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0"
]
basePath = '/tmp/books/' if LocalRun else '/vol/temp_data/'

# download the contents of a url to the book's directory
def _fetch_url_ (bookid, fpath, headers):
    url = 'http://www.amazon.com/dp/'+str(bookid)
    success = False
    maxTries = 5
    ntries = 0
    sleep = 2
    request = urllib2.Request(url,None,headers)
    
    while not success and ntries < maxTries:
        try: 
            response = urllib2.urlopen(request)
            if response.getcode() == 200:
                success = True
        except urllib2.HTTPError, e:
            print "HTTP Error:",e.code , url
            if e.code == 404:
                print "received 404, so skipping further retries"
                ntries = maxTries
        except urllib2.URLError, e:
            print "URL Error:",e.reason , url
            
        if success: 
            # Open our local file for writing
            local_file = open(fpath, "w")
            #Write to our local file
            local_file.write(response.read())
            local_file.close()
            time.sleep(sleep) # sleep before trying next task 
        else:
            # exponential backoff
            time.sleep(sleep**ntries)
            ntries+=1
    # need a good way to pass back the status of the call
    return '' if success else bookid 

def startJob(bookid,jobserver):
    path = basePath + str(bookid) 
    index = random.randint(0,len(userAgents)-1)
    headers = {
        'User-Agent' : userAgents[index],
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9',
        'Accept-Language' : 'en-US,en;q=0.8'
    }
    def inner():
        return _fetch_url_(bookid,path,headers)

    return inner if LocalRun else jobserver.submit(_fetch_url_, (bookid,path,headers),(),('time','urllib2','pp'))

def slicer(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


########## main ##########

filename = sys.argv[1]

with open(filename) as f:
    book_ids = f.read().splitlines()

# start servers
ppservers = ('node001','node002','node003','node004','node005','node006','node007','node008','node009','node010','node011','node012','node013','node014')
job_server = pp.Server() if LocalRun else pp.Server(ppservers=ppservers)
print "Starting pp with", job_server.get_ncpus(), "workers"


# set up the temp directory
try:
    os.makedirs(basePath)
except OSError as exc: # Python >2.5
    if exc.errno == errno.EEXIST and os.path.isdir(basePath):
        # if the directory is already there, delete all contents
        files=os.listdir(basePath)
        for f in files:
            os.remove(basePath + f)
        pass
    else: raise

# grab the books 1000 at a time and fetch the urls; wait for the results and then zip them up
batchSize = 10 if LocalRun else 1000
print "Total number of books to process is",len(book_ids)
failedBooks = []
for bslice in slicer(book_ids,batchSize):
    jobs=[]
    previous_time=time.time()

    jobs = map(lambda x: startJob(x,job_server), bslice)
    for job in jobs:
        r = job()
        if r != '': # fetch failed
            failedBooks.append(r) 
    print "Finished batch of size",batchSize
    time.sleep(1)

if (len(failedBooks) > 0):
    local_file = open(basePath + 'incompleteBooks', "w")
    for bkid in failedBooks:
      local_file.write("%s\n" % bkid)
    local_file.close()



    '''
    #print book
#   print count, len(book_set)
    if count%1000==999 or count-4710000==len(book_set)-2:
        print "count is ", count
        print count,"time elapsed:", time.time()-previous_time
        job_server.print_stats()
        files = os.listdir("/vol/temp_data/")
        zout = zipfile.ZipFile("/vol/book_data/"+str(count)+".zip",'w',zipfile.ZIP_DEFLATED)
        for f in files:
            zout.write("/vol/temp_data/"+f)
        zout.close()
        #to delete all the files in data/
        files = os.listdir("/vol/temp_data/")
        for f in files:
            os.remove("/vol/temp_data/"+f)

    count+=1
    #url = 'http://www.amazon.com/dp/'+str(book)
    #file='raw_data1031/'+str(book)+'.txt'
    #result, success = _fetch_url_(url)
    if success==True:
        count+=1
        print count, book, 'has been crawled'
        #print result
        w = open(file,'w')
        w.write(result)
        w.close()
    '''

    #irl = 'http://www.amazon.com/dp/'+str(book)
   
    

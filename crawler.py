#!/usr/bin/python

import pp
#import re
import time         # for sleep
#import codecs       # for handling UTF
#import os
import sys
import urllib2
import zipfile
import random

userAgents = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.94 Safari/537.36",

        ]
basePath = '/vol/temp_data/'

# download the contents of a url to the book's directory
def _fetch_url_ ( url, fpath, headers):
    success = False
    ntries = 0
    sleep = 2
    request = urllib2.Request(url,None,headers)
    
    while not ntries < 5 :
        try: 
            response = urllib2.urlopen(request)
            if response.getcode() == 200:
                success = True
        except HTTPError, e:
            print "HTTP Error:",e.code , url
        except URLError, e:
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
    return success

def startJob(bookid,jobserver):
    url = 'http://www.amazon.com/dp/'+str(bookid)
    path = basePath + bookid 
    headers = { 'User-Agent' : userAgents[random.RandInt(0,len(userAgents)],
                'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9',
                'Accept-Language' : 'en-US,en;q=0.8'
    }
    return job_server.submit(_fetch_url_, (url,path,headers),(),('re','codecs','time','os','urllib','pp'))

def slicer(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


## main ##

filename = sys.argv[1]

with open(filename) as f:
    book_ids = f.read().splitlines()

# start servers
ppservers = ('node001','node002','node003','node004','node005','node006','node007','node008','node009','node010','node011','node012','node013','node014')
job_server = pp.Server(ppservers=ppservers)
print "Starting pp with", job_server.get_ncpus(), "workers"


# empty the temp directory
files=os.listdir(basePath)
for f in files:
    os.remove(basePath + f)

# grab the books 1000 at a time and fetch the urls; wait for the results and then zip them up
total=len(book_ids)
batchSize = 1000
print "Total books to process are ",total
for bslice in slicer(book_ids,batchSize)
    jobs=[]
    previous_time=time.time()

    jobs = map(lambda x: startJob(x,job_server), bslice)
    for job in jobs:
        job()   
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
   
    

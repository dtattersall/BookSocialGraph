import pp
import re
import time         # for sleep
import codecs       # for handling UTF
import os
import urllib
import zipfile

def _fetch_url_ ( url,book ):
	fpath='/vol/temp_data/'+book
	#fpath=None
	result = None
	success = False
	ntries = 0
   	sleep=1
	while not success :
        	ntries+=1
        	try: 
#			print "url"
            		urllib.urlretrieve(url, fpath)
           		time.sleep(sleep) # be nice
            		success = True
        	except:
            		pass
        	if ntries>5:
			success==False
	return success



#############
f = open('/vol/tobe_crawled_01061_new.txt','r')

for count, line in enumerate(f):
	if count==0:
        	book_set = line.split()
f.close()

ppservers = ('node001','node002','node003','node004','node005','node006','node007','node008','node009','node010','node011','node012','node013','node014')
job_server = pp.Server(ppservers=ppservers)
print "Starting pp with", job_server.get_ncpus(), "workers"


files=os.listdir("/vol/temp_data/")
for f in files:
	os.remove("/vol/temp_data/"+f)


count=4710000
print "total books are ",len(book_set)
for book in book_set:
#	if count>500:
#		break
	if count%1000==0:
		jobs=[]
		previous_time=time.time()

	#print book
	url = 'http://www.amazon.com/dp/'+str(book)
	jobs.append(job_server.submit(_fetch_url_, (url,book),(),('re','codecs','time','os','urllib','pp')))
#	print count, len(book_set)
	if count%1000==999 or count-4710000==len(book_set)-2:
		print "count is ", count
		for job in jobs:
			job()	
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
	'''
	if success==True:
        count+=1
        print count, book, 'has been crawled'
        #print result
        w = open(file,'w')
        w.write(result)
        w.close()
	'''

    #irl = 'http://www.amazon.com/dp/'+str(book)
   
    

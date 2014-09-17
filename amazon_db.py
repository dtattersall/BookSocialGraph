'''
this script is used to create a sqlite databse for all the book pages we collected. It reads from book_info.txt and writes the data to the book_attribute table in amazon.db. It also creates an edgelist table in the database.
c1 to c10 are copurchases with the book.
'''
import sqlite3 as lite
import re

rPrice=re.compile(r'\d+\.\d+')
amazon_db = lite.connect("amazon.db")
amazon_db.text_factory = str
with amazon_db:
	cur=amazon_db.cursor()
	cur.execute("drop table if exists book_attribute")
	cur.execute("create table book_attribute(id int, lang text, asin text, isbn text, nrevs int, format text, url text, price real, title text, publisher text, rank int, c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, c8 text, c9 text, c10 text, stars real, ncps int, listprice real)")
	# parse the data into a db table
	f = open('book_info.txt','r')
	id=0
	test_lim=100000000000
	for count, line in enumerate(f):
		if count%1000000==0:
			print count
		if count%18==1 and count<test_lim:
			id+=1
			lang, asin, isbn, nrevs, format, url, price, title, publisher, rank = None, None, None, None, None, None, None, None, None, None
			c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 = None, None, None, None, None, None, None, None, None, None
			categories, stars, ncps, listprice = None, None, None, None
			lang = line.lstrip('lang:--').rstrip('\n')
		if count%18==2 and count<test_lim:
			asin = line.lstrip('asin:--').rstrip('\n')
		if count%18==3 and count<test_lim:
			isbn = line.lstrip('isbn:--').rstrip('\n')
		if count%18==4 and count<test_lim:
			nrevs = line.lstrip('nrevs:--').rstrip('\n')
		if count%18==5 and count<test_lim:
			format = line.lstrip('format:--').rstrip('\n')
		if count%18==6 and count<test_lim:
			url = line.lstrip('url:--').rstrip('\n')
		if count%18==7 and count<test_lim:
			price = line.lstrip('price:--').rstrip('\n').replace(',','')
		if count%18==8 and count<test_lim:
			title = line.lstrip('title:--').rstrip('\n')
		if count%18==9 and count<test_lim:
			publisher = line.lstrip('publisher:--').rstrip('\n')
		if count%18==10 and count<test_lim:
			rank = line.lstrip('rank:--').rstrip('\n')
		if count%18==11 and count<test_lim:
			categories = line.lstrip('categories:--').rstrip('\n')
		if count%18==12 and count<test_lim:
			stars = line.lstrip('stars:--').rstrip('\n')
		if count%18==14 and count<test_lim:
			copurchasing_list = line.lstrip('copurchasing_list:--').rstrip('\n')
		if count%18==15 and count<test_lim:
			listprice = line.lstrip('listprice:--').rstrip('\n').replace(',','')
		if count%18==17 and count<test_lim:
			if nrevs!="None": nrevs=int(nrevs)
			else: nrevs=0
			if price!="None": 
				try:
					price=float(rPrice.findall(price)[0])
				except:
					price=-1
					print "price error!!", isbn
			else: price=-1
			
			if listprice!="None":
				try:
					listprice=float(rPrice.findall(listprice)[0])
				except:
					listprice=-1
					print "listprice error!!", isbn
			else: listprice=-1
			if rank!='None': rank=int(rank.replace(',',''))
			else: rank=-1
			
			categories=categories.lstrip('None').replace(' ','').split('>>--')
			try:
				c1=categories[0]
				c2=categories[1]
				c3=categories[2]
				c4=categories[3]
				c5=categories[4]
				c6=categories[5]
				c7=categories[6]
				c8=categories[7]
				c9=categories[8]
				c10=categories[9]
			except:
				a=0
			
			ncps=len(categories)
			cur.execute("insert into book_attribute values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
			id, lang, asin, isbn, nrevs,
			format, url, price, title, publisher, rank, 
			c1, c2, c3, c4, c5, c6, c7, c8, c9, c10,  
			stars, ncps, listprice))
		if count>test_lim:
			break
	f.close()




# build the cop_list table in which the column entries are book nodes of the copurchasing ties.
amazon_db = lite.connect("amazon.db")
with amazon_db:
	cur=amazon_db.cursor()
	isbn_=set(cur.execute("select isbn from book_attribute where isbn!='None'"))
	isbn_set = set()
	for item in isbn_:
		isbn_set.add(item[0])
	print len(isbn_set)

with amazon_db:
	cur=amazon_db.cursor()
	cur.execute("drop table if exists cop_list")
	cur.execute("create table cop_list(book1 text, book2 text)")
	
	edge_list = list()
	f = open('book_info.txt','r')
	for count, line in enumerate(f):
		if count%1000000==0:
			print count
		if count%18==3:
			book1, book2 = None, None
			book1 = line.lstrip('isbn:--').rstrip('\n')
		if count%18==14:
			copurchaisng=line.lstrip("copurchasing_list:--").rstrip('\n')
			copurchaisng=copurchaisng.split(',')
			
			for book2 in copurchaisng:
				if book2 in isbn_set:
					edge_list.append((book1, book2))
	cur.executemany("insert into cop_list values(?,?)", edge_list)
				
	f.close()





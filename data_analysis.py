
import cPickle as pickle
import networkx as nx
from igraph import *
import math, random
import sqlite3 as lite
from scipy.stats import binom

##########################################
# prepare the copurchasing network object#
##########################################

# a. construct the edgelist
amazon_db = lite.connect("amazon.db")
cur=amazon_db.cursor()
cur.execute("select book1, book2 from cop_list")
items = cur.fetchall()

cur=amazon_db.cursor()
cur.execute("select id, isbn from unique_book_attribute")
book_entries = cur.fetchall()

# write cop_list table into a edgelist which doesn't have duplicate edges.
book_id_dict={}
for item in book_entries:
	book_id_dict[item[1]] = int(item[0]) 
w = open('edgelist_ids.txt','w') 
count=0
for item in items:
	count+=1
	if count%100000==0:
		print count
	nd1 = book_id_dict[item[0]] 	
	nd2 = book_id_dict[item[1]] 
	w.write(str(nd1)+"\t"+str(nd2)+"\n")
w.close()

# b. grab all the categories of these books. and save them into index_category
# b.1 select books that have at least one cop with red/blue books, put them in rel_books.
l=open('liberals.txt','r')
liberals=list()
for count, line in enumerate(l):
	liberals.append(line.rstrip('\n'))
l.close()
c=open('conservatives.txt','r')
conservatives=list()
for count, line in enumerate(c):
	conservatives.append(line.rstrip('\n'))
c.close()
poli_books = set.union(set(liberals), set(conservatives))

rel_books = set()
amazon_db = lite.connect("amazon.db")
amazon_db.text_factory = str
with amazon_db:
	cur=amazon_db.cursor()
	#for book in poli_books:
	poli_str = ', '.join("'"+x+"'" for x in poli_books)
	returned = cur.execute("select * from cop_list where book1 in (%s) or book2 in (%s)" % (poli_str, poli_str))
	for item in returned:
		rel_books.add(item[0])
		rel_books.add(item[1])

# b.2 find all the topics/categories that rel_books belong to and put them in category_set. 
# By doing so, we could ignore those categories that have no copurchases with political books and speed the process up.  
amazon_db = lite.connect("amazon.db")
amazon_db.text_factory = str
cur=amazon_db.cursor()
cur.execute("select id, isbn, c1,c2,c3,c4,c5,c6,c7,c8,c9,10 from unique_book_attribute")
book_entries = cur.fetchall()

category_list = list()
for entry in book_entries:
	if entry[1] in rel_books:
		category_list = category_list+list(entry[2:12])

category_set = set(category_list)
#category_set.add('red'); category_set.add('blue')
category_set.remove('');category_set.remove(None);category_set.remove(10)

# b.3 we indexize all the cateogory, because it is costly to sort a dictionary by the string keys.
index_category = dict() # keys are indices and values are category names
category_index = dict() # keys are category names and values are indices
index=0
for cate in category_set:
	index+=1
	index_category[index] = cate
	category_index[cate] = index
index_category['red']='red'
index_category['blue']='blue'
category_index['red']='red'
category_index['blue']='blue'
pickle.dump(index_category, open('index_category_pickle','w'))
pickle.dump(category_index, open('category_index_pickle','w'))



# c. construct igraph g from edgelist_ids.txt 
amazon_db = lite.connect("amazon.db")
amazon_db.text_factory = str
cur=amazon_db.cursor()
cur.execute("select id, isbn, c1,c2,c3,c4,c5,c6,c7,c8,c9,10 from unique_book_attribute")
book_entries = cur.fetchall()
print 'fetching items and book_entries done'

category_index = pickle.load(open('category_index_pickle','r'))
book_id_dict={}
for item in book_entries:
	book_id_dict[item[1]] = int(item[0])
g = Graph.Read_Edgelist("edgelist_ids.txt")
for item in book_entries:
	v_index = int(item[0])
	g.vs[v_index]['isbn'] = item[1]
	categories = []
	for cate in item[2:12]:
		if category_index.__contains__(cate)==True:
			categories.append(category_index[cate])
	g.vs[v_index]['categories'] = categories


# d. attribute the nodes with their political ideologies
l=open('liberals.txt','r')
liberals=list()
for count, line in enumerate(l):
	liberals.append(line.rstrip('\n'))
l.close()
c=open('conservatives.txt','r')
conservatives=list()
for count, line in enumerate(c):
	conservatives.append(line.rstrip('\n'))
c.close()
for book in liberals:
	g.vs.select(isbn=book)[0]['categories'].append('blue')
for book in conservatives:
	try:
		g.vs.select(isbn=book)[0]['categories'].append('red')
	except:
		a=1
pickle.dump(g, open('booknet_igraph','w')) # dump the g to a pickle file.




####################################################################################################################
#building category network and select the politically relevant topics based on two criteria detailed in the project description.
####################################################################################################################

# e. access the amazon.db and fetch the sales rank for each of the isbns and save them in a dict.
amazon_db = lite.connect("amazon.db")
amazon_db.text_factory = str
cur=amazon_db.cursor()
cur.execute("select isbn, title, rank from unique_book_attribute")
book_entries = cur.fetchall()
sales = []
book_entries_dict={}
for entry in book_entries:
	book_entries_dict[entry[0]] = int(entry[2])


# f. create a networkx object G_cate.
#cate_book_dict = pickle.load(open('cate_book_dict_pickle','r')) # keys are category names and values are a list of books in that category
category_index = pickle.load(open('category_index_pickle','r')) # keys are category names and values are category indices
index_category = pickle.load(open('index_category_pickle','r')) # keys are category indices and values are category names

G_cate = nx.Graph() # cateogory graph, in which nodes are categories and edges are copurchasing patterns between categories. 
for i in category_index.values():
	for j in category_index.values():
		 G_cate.add_edge(i,j)
		 G_cate[i][j]['strength']=0. # sum of tie strength of all the copurchasing ties between topics i and j. 
		 G_cate[i][j]['cops']=0. # number of copurchasing ties between i and j.

for nd in G_cate.nodes():
	G_cate.node[nd]['sales'] = 0.
	G_cate.node[nd]['pol_books'] = set() # a set of book isbns that have copurchases with political books.
	#print nd
	G_cate.node[nd]['cops']=0 # total copurchases, including non-political, made from topic nd.
	if index_category[nd] not in ['red','blue']:
		G_cate.node[nd]['size'] = len(cate_book_dict[index_category[nd]])
	if index_category[nd]=='red':
		G_cate.node[nd]['size'] = 472
	if index_category[nd]=='blue':
		G_cate.node[nd]['size'] = 246




# g. update each vertex in g with rank and sales.
g = pickle.load(open('booknet_igraph','r')) # read the copurchase network in igraph object; 
g.simplify() # remove all redundant edges;
g=g.as_undirected() 
g.vs['rank']=None
g.vs['sales']=None

# sales rank converted to actual sales. See reference, The Longer Tail: The Changing Shape of Amazon’s Sales Distribution Curve; http://papers.ssrn.com/sol3/papers.cfm?abstract_id=1679991
for v in g.vs:
	try: # not every book has sales rank. Less than 2000 books don't have sales rank.
		if book_entries_dict[v['isbn']]>0:
			v['rank'] = book_entries_dict[v['isbn']]
			v['sales'] =  math.exp(8.046-0.613*math.log(v['rank']))
	except:
		pass

# h. add up sales of books for each category in G_cate
for v in g.vs:
	try:
		sales = v['sales']
		for cate in  v['categories']:
			G_cate.node[cate]['sales'] += sales
	except:	
		pass

# i. compute the tie strength and cops between every pair of topics in the G_cate.
count=0
for edge in g.es:
	if count%10000==0:
		print count
	count+=1
	v1 = g.vs[edge.target]
	v2 = g.vs[edge.source]
	strength=0
	try: # try-except block because not every book has sales rank
		strength = v1['sales'] * v2['sales']
	except:
		pass
	cates1 = v1['categories']
	cates2 = v2['categories']
	if len(cates1)>0 and len(cates2)>0:
		for cate1 in cates1:
			G_cate.node[cate1]['cops'] += 1			
		for cate2 in cates2:
			G_cate.node[cate2]['cops'] += 1
		
		for cate1 in cates1:
			for cate2 in cates2:
				G_cate[cate1][cate2]['cops'] += 1
				G_cate[cate1][cate2]['strength'] += strength

# j. add all the books that have copurchases with political books to each topic. This information will be used for criterion 1.
for v in g.vs:
	if v['categories'] is not None:
		if 'red' in v['categories'] or 'blue' in v['categories']:
			for nbr_index in g.neighbors(v):
				v_nbr = g.vs[nbr_index]
				if v_nbr['categories'] is not None:
					for cate in v_nbr['categories']:
						G_cate.node[cate]['pol_books'].add(g.vs[nbr_index]['isbn'])

pickle.dump(G_cate, open('G_cate_full_pickle5','w'))



# k. significance test for tie strength scenario
def stripping(cate_str):
	cate_str=cate_str.replace('Books>>', '')
	cate_str=cate_str.replace('>>','::')
	return cate_str

G = pickle.load(open('G_cate_full_pickle5','r'))
base_nodes = 1447538. # number of books that have non-none sales and categories, otherwise, a book will not be included.
pol_size = 246+472
red_books = 472
blue_books = 246

'''
base_nodes = sum([G.node[nd]['cops'] for nd in G.nodes()])
red_books = G.node['red']['cops']
blue_books = G.node['blue']['cops']
pol_size = G.node['red']['cops'] + G.node['blue']['cops']
'''
G = G_cate
red_books = G.node['red']['sales']
blue_books = G.node['blue']['sales']

w=open('political_relevants.txt','w')

for nd in G.nodes():
	if nd not in ['red', 'blue']:
		Cr1, Cr2 = False, False
		# criterion one
		uni_books=len(G.node[nd]['pol_books'])
		if len(G.node[nd]['pol_books'])>=10:
			Cr2=True
			
		# criterion two
		p =  pol_size / (base_nodes - G.node[nd]['size'])
		n = G.node[nd]['cops']
		x = G[nd]['red']['cops'] + G[nd]['blue']['cops'] 
		p_value = binom.sf(x, n, p) # binomial test
		if p_value<0.05:
			Cr1=True			

		if Cr1==True and Cr2==True:
			#print index_category[nd], stripping(index_category[nd])
			scale = (G[nd]['red']['strength']/red_books) / (G[nd]['red']['strength']/red_books + G[nd]['blue']['strength']/blue_books) # disregard this scale measure.
			w.write(str(nd)+'\t'+str(x)+'\t'+str(G[nd]['red']['cops'])+'\t'+str(G[nd]['blue']['cops'])+'\t'+str(scale)+
			'\t'+str(n)+'\t'+str(G.node[nd]['size'])+'\t'+str(H)+'\t'+str(uni_books)+'\t'+str(format(p_value, '.10f'))+'\t'+stripping(index_category[nd])+'\n')
w.close()





# l. 
red_customers, blue_customers = 0., 0. # the total sales for all the red books and blue books, will be used for adjusting the difference in cluster sizes.
for v in g.vs:
	try:
		if 'red' in v['categories']:
			red_customers += v['sales']
		if 'blue' in v['categories']:
			blue_customers += v['sales']
	except:
		pass

# m. compute the ideology scale for each of the books that have ties with political books.
w=open('relevant_books.txt','w')
relevant_books = dict() # in which keys are book isbn, values are political scales
for v in g.vs():
	x=0 # counting the number of ties with political books.
	reds=0
	blues=0
	for nbr in g.neighbors(v):
		try:
			if (v['sales'] is not None) and (g.vs[nbr]['sales'] is not None):
				if g.vs[nbr]['categories'].__contains__('red') or g.vs[nbr]['categories'].__contains__('blue'):
					x+=1
					if g.vs[nbr]['categories'].__contains__('red'):
						reds += v['sales']* g.vs[nbr]['sales']
					if g.vs[nbr]['categories'].__contains__('blue'):
						blues += v['sales']* g.vs[nbr]['sales']
		except:
			pass
	if x>0: #
		scale = (reds/red_customers) / (reds/red_customers + blues/blue_customers)
		relevant_books[v['isbn']] = {}
		relevant_books[v['isbn']]['scale']=scale
		relevant_books[v['isbn']]['cops']=x
		relevant_books[v['isbn']]['sales'] = v['sales']
		w.write(str(v.index)+'\t'+v['isbn']+'\t'+str(x)+'\t'+str(reds)+'\t'+str(blues)+'\t'+str(scale)+'\t'+str(x)+'\n')
w.close()

# n. method that uses std as an indicator of internal polarizing and cohesion.
# to find all the books that are in a certain topic.
f=open('political_relevants.txt','r')
political_relevants_cat = {}
for count, line in enumerate(f):
	items = line.split('\t')
	political_relevants_cat[int(items[0])] = items[10]
f.close()
cate_scales={} # where the keys are cate indices, and values are a dictionary.
for cate in political_relevants_cat:
	cate_scales[cate] = {}
	cate_scales[cate]['scales']=[]
	cate_scales[cate]['topic'] = political_relevants_cat[cate]
	cate_scales[cate]['means']=-1.
	cate_scales[cate]['std']=-1.


# o. compute mean and std of books in each topic. 
scale_means=[]
scale_stds=[]
w=open('scale_plot.txt','w')
for cate in cate_scales:
	scale_list = [cate_scales[cate]['scales'][i][1] for i in range(len(cate_scales[cate]['scales']))]
	scale_mean = np.mean(scale_list) 
	if not np.isnan(scale_mean):
		scale_std = np.std(scale_list)
		cate_scales[cate]['mean']=scale_mean
		cate_scales[cate]['std']=scale_std
		scale_means.append(scale_mean) # is the political scale that is used to make the dotplot. 
		scale_stds.append(scale_std)
		#rand_std = randomized_std(cate_scales[cate], pol_sales_list, relevant_books, 1000)
		w.write(str(cate)+' '+str(scale_mean)+' '+str(scale_std)+' '+str(len(cate_scales[cate]['scales']))+' '+cate_scales[cate]['topic'])
w.close()
##









#Trying to replace and eval on pandas expressions, not finished
import re
import pandas as pd
import numpy as np




class sqlParts:
	myKeywords = []
	myAttributes = []
	myTables = []
	myConditions = []
	myBooleans = []
	myComparisons = []
	myArguments = []
	Attributes = []
	Tables = []

def Main():
	s = sqlParts()
	s.Tables.insert(len(s.Tables),'cars.csv')
	s.Tables.insert(len(s.Tables),'carOwner.csv')
	s.Tables.insert(len(s.Tables),'carColor.csv')

	df = []
	
	chunksize = 1000000
	for table in s.Tables:
		f = pd.read_csv(table, sep=',', iterator = True, chunksize=chunksize, index_col=False)
		f2 = pd.concat(f,ignore_index=True)
		df.insert(len(df),f2)

	df[0].set_index('ID')
	df[1].set_index('carID')
	a=df[0]
	b=df[1]

	
	result = pd.merge(a,b,left_on='ID', right_on='carID')
	print result
	df2=df[0]
	print(df2[(df2.ID > 2) & (df2.ID <= 7)])
	#for f in df:
		#print f

	cars = df[0]
	carOwner = df[1]
	carColor = df[2]
	wherestr = "cars.YEAR > 2007 AND cars.MAKE = 'HONDA'"
	#origstr = 'M.movie_title = A.film AND A.imdb_score < 7'
	#data = {'M.movie_title': 'Star Wars', 'A.film': 'Star Wars', 'A.imdb_score': 8} #change these around to get different true/false returns
	pdstr = pandasify(wherestr)
	#print pdstr
	evalstr = 'cars['+pdstr+']'
	print evalstr
	#print eval(evalstr)
	#print cars[(cars.YEAR > 2007) & (cars.MAKE == 'HONDA')]


#myTables[0] = 

#	df1 = pd.DataFrame({'key': ['A', 'B', 'C', 'D','F'], 'value': np.random.randn(5)})
#	df2 = pd.DataFrame({'key': ['B', 'D', 'D', 'E','A'], 'value': np.random.randn(5)})

#	result = pd.merge(df1, df2, on='key')

#	print result


def pandasify(sqlstr): #converts SQL operators into Python operators
	sqlstr = re.sub(r'\s=\s', r' == ', sqlstr) #equals
	sqlstr = re.sub(r'\bAND\b', r'&', sqlstr) # and
	sqlstr = re.sub(r'\bOR\b', r'!', sqlstr) # or
	sqlstr = re.sub(r'\bNOT\b', r'~', sqlstr) # not
	return sqlstr

def substitute(pystr, data): #put the attribute values in where their names are currently
	for attrib in data:
		if type(data[attrib]) == str:
			pystr = re.sub(r'\b'+attrib+r'\b', '"'+data[attrib]+'"', pystr) #if it's a string put quotes around it
		else:
			pystr = re.sub(r'\b'+attrib+r'\b', str(data[attrib]), pystr)
	return pystr

Main()

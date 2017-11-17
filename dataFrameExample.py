import pandas as pd
import numpy as np
import time
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
	
	#s.Tables.insert(len(s.Tables),'Crimes_-_2001_to_present.csv')
	#s.Tables.insert(len(s.Tables),'Crimes_-_2001_to_present.csv')

	s.Tables.insert(len(s.Tables),'cars.csv')
	s.Tables.insert(len(s.Tables),'carOwner.csv')
	s.Tables.insert(len(s.Tables),'carColor.csv')

	df = []
	
	chunksize = 1000000
	for table in s.Tables:
		f = pd.read_csv(table, sep=',', iterator = True, chunksize=chunksize, index_col=False)
		f2 = pd.concat(f,ignore_index=True)
		df.insert(len(df),f2)

	start = time.time()
	a=df[0]
	b=df[1]
	
	#[(df[0].ID > 1418123) & (df[0].ID <= 1418140)]
	result = pd.merge(a,b,left_on='ID',right_on='carID')
	#result = pd.merge(a,b,on='ID')
	#print(result[(result.ID > 1418123) & (result.ID <= 1418140)])
	c=df[2]
	result2 = pd.merge(result,c,left_on='colorID',right_on='ID')
	#print (result2[(result2==True)])
	args = []
	args.insert(len(args),result2.COLOR == 'YELLOW')
	args.insert(len(args),result2.COLOR == 'RED')
	var = args[0] | args[1]
	print (result2[var])
	#print (result2[(args[0] | args[1])])

	end = time.time()
	print ("Process Time is:", end - start)


Main()

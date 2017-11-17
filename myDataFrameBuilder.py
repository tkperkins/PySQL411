import pandas as pd
import numpy as np
import time
import os
from myDataFrameBuilder import *

#the global dataframe object
perm_df = []

#A function that just delcares global variables
def projectGlobals():
	global perm_df
	

def sendParts(s):

	showTokens(s)
	showFrames(s)
	#print dataframe and name of dataframe	
	return

def showFrames(s):
	df = []
	for i in perm_df:
		#Only select the dataframes we want
		for table in s.Tables:
			if table.Name == i.name:
				df.insert(len(df),i)


	
	d = pd.merge(df[0],df[1],left_on=s.myArguments[0].left, right_on=s.myArguments[0].right)
	print d

	#print local_df[0].name +" " + local_df[1].name + " " + local_df[2].name
	#return	
	
	#e =  pd.merge(d,local_df[2],left_on='movie_title',right_on='Film')
	#print e


#add all files in directory to global dataframe object. 
#Make sure to name each dataframe
def buildDataFrames():
	listOfFiles = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith(".csv")]
	chunksize = 1000000
	for index, g in enumerate(listOfFiles[0:len(listOfFiles)]):
		f = pd.read_csv(g, sep=',', iterator = True, chunksize=chunksize, index_col=False)
		f2 = pd.concat(f,ignore_index=True)
		f2.name = g
		f2['dummy']=0
		perm_df.insert(len(perm_df),f2)

#Prints each of the tokens stored in the sqlParts object
#that was received from mySQLreader.py
def showTokens(s):
	print "********************** KEYWORDS ********************** "
	for keyword in s.myKeywords:
		print '\t' + keyword.replace(",","")
	print "*************** WHERE CLAUSE ARGUMENTS *************** "
	print "TYPE" + '\t\t\t' + "LEFT" + '\t\t' + "OPERATOR" + '\t\t' + "RIGHT"
	for argument in s.myArguments:
		print argument.Type + "\t"+  argument.left + "\t" + argument.operator + "\t" + argument.right + "\t" + argument.boolean	
	print "**************** ATTRIBUTES ARGUMENTS **************** "	
	print "TYPE" + '\t\t\t' + "NAME" + '\t\t' + "ALIAS"
	for obj in s.Attributes:
		print str(obj.Type) + '\t\t' + str(obj.Name) + '\t\t' + str(obj.Alias).replace(",","")
	print "***************** TABLE ARGUMENTS ******************* "	
	print "TYPE" + '\t\t\t' + "NAME" + '\t\t' + "ALIAS"
	for obj in s.Tables:
		print str(obj.Type) + '\t\t' + str(obj.Name) + '\t\t' + str(obj.Alias).replace(",","")

#	print "ATTRIBUTES: "
#	for attribute in s.myAttributes:
#		print '\t' + attribute.replace(",","")
#	print "TABLES: "
#	for table in s.myTables:
#		print '\t' + table.replace(",","")
#	print "CONDITIONS: "
#	for condition in s.myConditions:
#		print '\t' + condition.replace(",","")
	return

#	chunksize = 1000000
#	for table in s.Tables:
#		f = pd.read_csv(table.Name, sep=',', iterator = True, chunksize=chunksize, index_col=False)
#		f2 = pd.concat(f,ignore_index=True)
#		df.insert(len(df),f2)

	#start = time.time()
	#a=(df[0][(df[0].imdb_score < 7)])
	#b=df[1]

	#result = pd.merge(a,b,left_on='movie_title',right_on='Film')

						
#	local_df = []
#	for table in s.Tables:
#		for i in perm_df:
#			if i.name == table.Name:
#				local_df = i.copy()
				#local_df.insert(len(local_df),x)					
#				print (local_df.name)
	#return
	#[(df[0].ID > 1418123) & (df[0].ID <= 1418140)]
		
	#__result = pd.merge(a,b,left_on='ID',right_on='carID')
	#result = pd.merge(a,b,on='ID')
	#print(result[(result.ID > 1418123) & (result.ID <= 1418140)])
		
	#__c=df[2]
	#__result2 = pd.merge(result,c,left_on='colorID',right_on='ID')
	#print (result2[(result2==True)])
	#__args = []
	#__args.insert(len(args),result2.COLOR == 'YELLOW')
	#__args.insert(len(args),result2.COLOR == 'RED')
	#__var = args[0] | args[1]
	#__print (result2[var])
	#print (result2[(args[0] | args[1])])

	#end = time.time()
	#print ("Process Time is:", end - start)



#Main()

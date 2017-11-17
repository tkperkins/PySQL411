import csv
import pandas as pd
import time
## TO DO: Try to select only those records that have values matching some passed parameters for some attributes
#identify all of the attributes of the csv file from the first row of the file
def getAttributes(fileName, reader):
	try:		
		#read just the first row of the file
		attributes = next(reader)		
		return attributes

	except:
		print ("Error getting attributes from {}".format(fileName))

#only loads one line of file into memory at a time
def openFile(fileName):
	try:
		return open(fileName, 'rb')
	except:
		print ("{} does not exist.".format(fileName))

#returns a reader object which will iterate over lines in a given file
def getReader(csvfile):
	return csv.reader(csvfile, delimiter=',', quotechar='"')

#reads all of the values from the csv and prints to console
def getValues(reader):
	try:
		vals = None
		i = 0
		for row in reader:
			if (i>0):
				#print ', '.join(row)
				vals= row[ 0 : len(row) ]
			i = 1
		#print (vals)
		return vals
	except:
		print ("Error getting values")

def getChunk(fName):

	chunksize = 1000000
	count = 0
	df = pd.read_csv(fName, sep=',', iterator = True, chunksize=chunksize)
	#for chunk in df:
		#count = count+1
		#print ("{}{}").format("COUNT IS: ", count)
	#	next
	df2 = pd.concat(df,ignore_index=True)
	start = time.time()
	#print df2.head()
	#print df2.describe()
	#date_group = df2.groupby('Date')
	#y3 = date_group.size()
	#match = df2.query("YEAR > 2001")
	print(df2[(df2.ID > 1418123) & (df2.ID <= 1418140)])	
	#try:
	#print(match)
	#except:	
		#print(match)
	end = time.time()
	print ("Inner getChunk Function Process Time is:", end - start)
#	print df2.memory_usage(index=True).sum()
	#print (df.loc[[10]])	
	#print ("done!")

def getLastRow(reader):
	try:
		return sum(1 for line in reader)
		#for i, l in enumerate(reader):
		#	pass
		#return i	
	except:
		print ("Error detecting last row")

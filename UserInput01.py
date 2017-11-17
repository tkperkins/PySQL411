import pandas as pd
import csv as csv
import time

print "You may specify up to three CSV files to import."\
      "\n   Your response should be: filename.csv \n"\

testingFiles = raw_input ("Do you want to use the testing files - Yes or No? :")
if testingFiles == 'Yes':
      #Placeholder files to avoid typing in responses while testing; will remove or comment out
      print 'Using testing files. \n'
      file1 = 'oscars.csv'
      file2 = 'oscars2.csv'
      file3 = 'movies.csv'
elif testingFiles == "No":
#This section prompts the user to specify their files to ingest.
#This section is commented out for testing; comments remove when ready.
            file1 = raw_input ("What is CSV file 1 that you want to import?\n  :")
            file2 = raw_input ("\nWhat is CSV file 2 that you want to import? (if none, type 2NONE)\n  :")
            file3 = raw_input ("\nWhat is CSV file 3 that you want to import? (if none, type 3NONE)\n  :")
else:
      print 'Probably not working. ERROR.'
      akfjkj
#Start time to measure ingest time taken
start = time.time()

#This reads and relays some summary information about user data
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)
df3 = pd.read_csv(file3)
#End time to measure ingest time taken
end = time.time()
print ("CSV ingest process time is:", end - start)
print "\n"

#Read information on the files and show user basic file information for reference.
def fileInfo():
      rows1 = len(df1.index)
      rows2 = len(df2.index)
      rows3 = len(df2.index)
      df123 = [rows1, rows2, rows3]
      print "Row counts for files ingested are: "
      print df123
      print "\n"
      print "Top 10 rows of each ingested file:"
      print "\n"
      print file1
      print df1.head(10)
      print "\n"
      print file2
      print df2.head(10)
      print "\n"
      print file3
      print df3.head(10)
      print "\n"
fileInfo()

#Get user to identify up to three attributes on each table that should be indexed.
testingAttributes = raw_input ("Do you want to use the testing files - Yes or No? :")
if testingAttributes == 'Yes':
      #Placeholder files to avoid typing in responses while testing; will remove or comment out
      print 'Using testing attributes. \n'
      df1ind1 = 'Year'
      df1ind2 = 'Winner'
      df1ind3 = 'Film'
      df2ind1 = 'Year'
      df2ind2 = 'Winner'
      df2ind3 = 'Film'
      df3ind1 = 'movie_title'
      df3ind2 = 'title_year'
      df3ind3 = 'budget'

elif testingAttributes == "No":
      def ask3UserIndexes():
            df1ind1 = raw_input ("\nFrom file1, identify attribute 1 that you plan to query or join on, for which an index would be useful (or type NONE1)\n  :")
            df1ind2 = raw_input ("\nFrom file1, identify attribute 2 that you plan to query or join on, for which an index would be useful (or type NONE2)\n  :")
            df1ind3 = raw_input ("\nFrom file1, identify attribute 3 that you plan to query or join on, for which an index would be useful (or type NONE3)\n  :")
            df2ind1 = raw_input ("\nFrom file2, identify attribute 1 that you plan to query or join on, for which an index would be useful (or type NONE4)\n  :")
            df2ind2 = raw_input ("\nFrom file2, identify attribute 2 that you plan to query or join on, for which an index would be useful (or type NONE5)\n  :")
            df2ind3 = raw_input ("\nFrom file2, identify attribute 3 that you plan to query or join on, for which an index would be useful (or type NONE6)\n  :")
            df3ind1 = raw_input ("\nFrom file3, identify attribute 1 that you plan to query or join on, for which an index would be useful (or type NONE7)\n  :")
            df3ind2 = raw_input ("\nFrom file3, identify attribute 2 that you plan to query or join on, for which an index would be useful (or type NONE8)\n  :")
            df3ind3 = raw_input ("\nFrom file3, identify attribute 3 that you plan to query or join on, for which an index would be useful (or type NONE9)\n  :")
      ask3UserIndexes()
else:
      print 'Probably not working. ERROR.'
      akfjkj


#Adjust dataframes to add indexes
#Needs to be made smart - don't index if user doesn't specify, or duplicates
def addIndexes():
      df1.set_index([df1ind1, df1ind2, df1ind3], inplace=True)
      df2.set_index([df2ind1, df2ind2, df2ind3], inplace=True)
      df3.set_index([df3ind1, df3ind2, df3ind3], inplace=True)
addIndexes()

def showIndexedDF():
      print "\n"
      print "Top 10 rows of each INDEXED file:"
      print "\n"
      print file1
      print df1.head(10)
      print "\n"
      print file2
      print df2.head(10)
      print "\n"
      print file3
      print df3.head(10)
      print "\n"
showIndexedDF()


#This section prompts the user to specify their SQL query as a string.
def userQueryLoop():
      countqueries = 0
      userSQL = ''
      while userSQL <> "EXIT":
            print ("You've specified this many queries so far: ", countqueries)
            userSQL = raw_input ("\n(type EXIT to exit) What is the SQL SELECT FROM WHERE query that you want to specify? \n  :")
            countqueries = countqueries + 1
userQueryLoop()
print "\n \n \n \n \n Thank you."
print ("You completed ", countqueries," queries.")

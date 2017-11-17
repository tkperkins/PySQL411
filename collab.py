# Overall process:
# Force user to identify files for ingest; load files into dataframes
# Force user to identify key attributes / possibly force identification of indexes
# Allow user to specify SELECT/FROM/WHERE query against the ingested files
# Start timer
# MySQLParser.py used to parse the SQL statement into?
# Take MySQLParser output 'sqlParts' and convert into Pandas compatible clauses?
# ??
# End timer
# Provide timer results; indicate that results are being written to disk; ask if user wants data frame displayed on screen.
# Print data frame results.
# 


import os
import csv
import pandas as pd
import numpy as np
import sys
import time
import re

class gate:
	seen_SELECT = False
	seen_FROM = False
	seen_WHERE = False

class AliasGroup():
	def __init__(self,Name,Alias,Type):
		self.Name = Name
		self.Alias = Alias
		self.Type = Type

class WhereClause():
	def __init__(self,Type,left,operator,right,boolean):
		self.Type = Type		
		self.left = left
		self.operator = operator
		self.right = right
		self.boolean = boolean

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

operators = ["=","!=",">","<",">=","<=","<>",",","LIKE"]

boolean = ["AND","OR","NOT"] 

def Main():
	# examples
	left = pd.DataFrame({'key1': ['foo', 'bar'], 'lval': [1, 2]})
	right = pd.DataFrame({'key2': ['foo', 'bar','hoo'], 'rval': [4, 5, 6]})
	mid = pd.DataFrame({'key2': ['foo', 'bar','hoover'], 'rval': [4, 5, 7]})

	s = sqlParts()
	#print merge
	# examples of where clauses we will handle

	whereex1 ="key1 = foo AND lval >1"
	wherex2="(left.key1 = left.key2) or left.key1 LIKE % fo"
	#add more

	print mid.applymap(lambda x: is_like(x,'%oo%'))
	#dictionary of aliases as keys and values as tables names
	#dictionary maps dataframes to their positions in the list

def sql_to_regex(matchstr):
    matchstr = re.sub(r'\%', '.*', matchstr)
    matchstr = re.sub(r'\_', '.', matchstr)
    matchstr = '^' + matchstr + '$'
    return matchstr

def is_like(df_seq, matchstr): #first argument should be a df.col_name, then the SQL match string
    return df_seq.str.contains(sql_to_regex(matchstr))


def substitute(pystr, data): #put the attribute values in where their names are currently
	for attrib in data:
		if type(data[attrib]) == str:
			pystr = re.sub(r'\b'+attrib+r'\b', '"'+data[attrib]+'"', pystr) #if it's a string put quotes around it
		else:
			pystr = re.sub(r'\b'+attrib+r'\b', str(data[attrib]), pystr)
	return pystr

def pandasify(sqlstr, s): #converts SQL operators into Python operators
	sqlstr = re.sub(r'\s=\s', r' == ', sqlstr) #equals
	sqlstr = re.sub(r'\bAND\b', r'&', sqlstr) # and
	sqlstr = re.sub(r'\bOR\b', r'!', sqlstr) # or
	sqlstr = re.sub(r'\bNOT\b', r'~', sqlstr) # not
	return sub_in_dataframes(sqlstr,s)

def sub_in_dataframes(pystr, s):
	#s.Tables[i].Name is the filename of df[i]... I THINK?
	#dfstr = 'df'
	for i in range(0,len(s.Tables)):
		if s.Tables[i].Alias == '':

		else:
			pystr = re.sub(r'\b'+s.Tables[i].Alias+r'\.(\w+\b)', r'df\['+str(i)+r'\].\1_'+s.Tables[i].Alias, pystr)
			print pystr
	return pystr

#a string cleaner function: standardizes the sql statement
def cleanString(sql):
	
	arg = sql	
	start_pos = 0
	
	for i in range(start_pos,len(sql)):
		for char in operators:
			if (sql[i] == char and char == ","):	
				sql = sql[:i] + ", " + sql[i+1:] 
				i = 0
			if (sql[i] == char):
				if (sql[i-1] != " "): 
					sql = sql[:i] + " " + sql[i:] 
					i = 0								
				elif (sql[i+1] != " "): 
					sql = sql[:i+1]  + " " +  sql[i+2:]
					i = 0
	
	#remove any extra whitespace
	return sql.strip()
			
#Check if a certain Keyword has been passed: did we go through this gate yet?
def Keyword(word, g):
	if (word.upper() == "SELECT"):
		g.seen_SELECT = True
		return g.seen_SELECT
	elif (word.upper() == "FROM"): 
		g.seen_FROM = True
		return g.seen_FROM
	elif (word.upper() == "WHERE"):
		g.seen_WHERE = True		
		return g.seen_WHERE
	else:
		return False

def Comparison(word):
	w = str(word)	
	if (is_Type(w,operators)):
		return "COMPARISON: " + word
	else:
		return "";

def Boolean(word):
	w  = str(word)
	if (w.upper() == "AND" or w.upper() == "OR" or w.upper() == "NOT"):
		return "BOOLEAN: " + w.upper()
	else:
		return ""

def Like(word):
	w = str(word)
	if (w.upper() == "LIKE"):
		return "PATTERN MATCH: " + w.upper()
	else:
		return ""

def Wildcard(word):
	w = str(word)
	if (w == '%'):
		return "WILDCARD: " + word
	else:
		return ""

def SortTokens(tokens, s,g):
	
	for token in tokens:
		#keywords
		if (Keyword(token,g)):
			Keyword(token,g)
			s.myKeywords.insert(len(s.myKeywords),token)
		#SELECT		
		elif (g.seen_SELECT and not g.seen_FROM and not g.seen_WHERE):
			s.myAttributes.insert(len(s.myAttributes),token)		
		#FROM		
		elif (g.seen_FROM and not g.seen_WHERE):
			s.myTables.insert(len(s.myTables),token)
		#WHERE		
		elif (g.seen_WHERE):		
			s.myConditions.insert(len(s.myConditions),token)	
			#=,!=,>,<,>=,<=,<>		
			if (Comparison(token)):
				s.myComparisons.insert(len(s.myComparisons),token)
			if (Boolean(token)):
				s.myBooleans.insert(len(s.myBooleans),token)
	FindArguments(s)
	try:
		FindAttributeAliases(s.myAttributes,s.Attributes,"ATTRIBUTE")
		FindAttributeAliases(s.myTables,s.Tables,"TABLE")
	except AttributeError as e:
		print e

#identifies whether an element is named (i.e. an attribute or table) or if 
#it is an alias
def FindAttributeAliases(listedElements, groupedElements,typeElement):
	count = 0
	tokenCount = 0	
	# An object with two attributes
	Attributes = AliasGroup("","","") 
	newGroup = True
	for token in listedElements:
		count = count + 1
		tokenCount = tokenCount + 1		
		#The first token is always the attribute and never the alias
		#The second token can be the alias or the keyword AS
		if (newGroup == True):
			Attributes.Name = token
			Attributes.Name = token
		#ignore the AS keyword
		elif (count > 1 and token.upper() != "AS" and newGroup == False):
			Attributes.Alias = token
			newGroup = True		
			count = 0
		elif (count > 1):
			raise AttributeError("Incorrect attribute syntax entered on command line: " + attribute)
		#error handle for token.index return		
		try:
			if(token.index(",") > 1):
				newGroup = True
		except:
			newGroup = False
		#append to AliasGroup if starting a newGroup or there are no more tokens to evaluate
		if (tokenCount == len(listedElements) or newGroup):
			Attributes.Type = typeElement
			groupedElements.append(AliasGroup(Attributes.Name.replace(",",""),Attributes.Alias.replace(",",""),Attributes.Type))

def is_Type(o,dtype):
	for token in dtype:
		if (str(o).upper() == token):
			return True
	return False

def FindArguments(s):
	seen_OPERATOR = False
	in_QUOTE = False
	#an object with 4 attributes
	where = WhereClause("","","","","")
	arg = ""
	for token in s.myConditions:
		try:
			if in_QUOTE == False and token.index("'") >= 0:
				in_QUOTE = True
				arg = token
				#print arg
			elif in_QUOTE and token.index("'") >= 0:
				arg = arg + " " + token
				in_QUOTE = False				
				#print arg
		except:
			if in_QUOTE:
				arg = arg + token
				#
				#print arg
		
		if in_QUOTE == False and arg == "":
			if is_Type(token,boolean):
				where.boolean = str(token)
				where.Type = "BOOLEAN"
				s.myArguments.insert(len(s.myArguments),where)
				where = WhereClause("","","","","")
			elif is_Type(token,operators):
				seen_OPERATOR  = True
				where.operator = token
			elif seen_OPERATOR  != True:
				where.left = token
			elif seen_OPERATOR  == True:
				where.right = token
				where.Type = "ARGUMENT"
				s.myArguments.insert(len(s.myArguments),where)
				seen_OPERATOR  = False
				where = WhereClause("","","","","")
		elif in_QUOTE == False and arg != "":
			if seen_OPERATOR  != True:
				where.Type = "LIKE"				
				where.left = "LIKE"
				where.right = arg
				s.myArguments.insert(len(s.myArguments),where)
				arg = ""
				where = WhereClause("","","","","")
			elif seen_OPERATOR  == True:
				where.right = arg
				where.Type = "ARGUMENT"
				s.myArguments.insert(len(s.myArguments),where)
				seen_OPERATOR  = False
				where = WhereClause("","","","","")
			where.Type = "QUOTE"
			where.left = arg
			s.myArguments.insert(len(s.myArguments),where)
			arg = ""
			where = WhereClause("","","","","")
def PrintTokens(s):

#	print "ATTRIBUTES: "
#	for attribute in s.myAttributes:
#		print '\t' + attribute.replace(",","")
#	print "TABLES: "
#	for table in s.myTables:
#		print '\t' + table.replace(",","")
#	print "CONDITIONS: "
#	for condition in s.myConditions:
#		print '\t' + condition.replace(",","")

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


def Test():
	sql = sys.argv[1]
	sql = cleanString(sql)	
	stmt = str(sql)
	tokens = stmt.split()
	in_QUOTE = False
	arg = ""

	for token in tokens:
		try:
			if in_QUOTE == False and token.index("'") >= 0:
				in_QUOTE = True
				arg = token
				print arg
			elif in_QUOTE and token.index("'") >= 0:
				arg = arg + " " + token
				print arg
		except:
			if in_QUOTE:
				arg = arg + token
				in_QUOTE = False
				print arg

Main()
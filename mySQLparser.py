import os
import csv
import pandas as pd
import numpy as np
import sys
import time

#an instance class, used like a struct
class gate:
	seen_SELECT = False
	seen_FROM = False
	seen_WHERE = False

class argGate:
	seen_LEFT = False
	seen_OPERATOR = False
	seen_RIGHT = False

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

operators = ["=","!=",">","<",">=","<=","<>",","]

boolean = ["AND","OR","NOT"] 

#mySQLparser's MAIN function
def Main(args=""):
	start = time.time()
	#allow for testing
	if (len(sys.argv) < 2):
		sql = args
	else:
		sql = sys.argv[1]
	sql = cleanString(sql)	
	stmt = str(sql)
	tokens = stmt.split()
	g = gate()
	s = sqlParts()
	SortTokens(tokens,s,g)
	PrintTokens(s)
	end = time.time()
	return s
	print "Parse Time is: " + str(round(end - start,4) * 1000) + "ms"

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
	# A two dimensional object
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

	a =  argGate()
	where = WhereClause("","","","","")
	noWhere = WhereClause("","","","","")
	for token in s.myConditions:
		if is_Type(token,boolean):
			where.boolean = str(token)
			where.Type = "BOOLEAN"
			print "BOOLEAN" + str(token)
			s.myArguments.insert(len(s.myArguments),where)
			where = WhereClause("","","","","")
		elif is_Type(token,operators):
			a.seen_OPERATOR = True
			where.operator = token
			print "OPERATOR " + token
		elif a.seen_OPERATOR != True:
			print "LEFT " + token
			where.left = token
		elif a.seen_OPERATOR == True:
			where.right = token
			where.Type = "ARGUMENT"
			print "RIGHT " + token
			s.myArguments.insert(len(s.myArguments),where)
			a.seen_OPERATOR = False
			where = WhereClause("","","","","")
				
		#elif(tokenCount == len(s.myConditions)):
		#	argStr = argStr + token
		#	s.myArguments.insert(len(s.myArguments),argStr)	
		#elif (count <= 2 and not is_Type(token,boolean)):
		#	argStr = argStr + token + " "
		#elif (count == 3 and tokenCount == (operator_index + 1) and operator_index != 0  and not is_Type(token,boolean)):
		#	argStr = argStr + token
		#	s.myArguments.insert(len(s.myArguments),argStr)
		#	count = 0 			
		#	argStr = ""
		#	operator_index = 0
		#elif (count == 3  and not is_Type(token,boolean)):
		#	argStr = argStr + token + " "
		#elif (tokenCount == (operator_index + 1) and operator_index != 0):
		#	argStr = argStr + token 			
		#	s.myArguments.insert(len(s.myArguments),argStr)
		#	count = 0			
		#	argStr = ""
		#	operator_index = 0

def PrintTokens(s):

	print "ATTRIBUTES: "
	for attribute in s.myAttributes:
		print '\t' + attribute.replace(",","")
	print "TABLES: "
	for table in s.myTables:
		print '\t' + table.replace(",","")
	print "CONDITIONS: "
	for condition in s.myConditions:
		print '\t' + condition.replace(",","")
	#print "WHERE COMPARISON OPERATORS: "
	#for comparison in s.myComparisons:
	#	print '\t' + comparison.replace(",","")
	#print "WHERE BOOLEAN OPERATORS: "
	#for boolean in s.myBooleans:
	#	print '\t' + boolean.replace(",","")

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


Main()

import os
import csv
import pandas as pd
import numpy as np
import sys
import time
from myDataFrameBuilder import *

#an instance class, used like a struct
class gate:
	seen_SELECT = False
	seen_FROM = False
	seen_WHERE = False

#this class creates attributes to associate the 
#name of an object, like a tuple or a table
#with an aliased name
class AliasGroup():
	def __init__(self,Name,Alias,Type):
		self.Name = Name
		self.Alias = Alias
		self.Type = Type

#this class allows for storing each of the various parts of 
#a WHERE clause as separate attributes
class WhereClause():
	def __init__(self,Type,left,operator,right,boolean):
		self.Type = Type		
		self.left = left
		self.operator = operator
		self.right = right
		self.boolean = boolean

class sqlParts:
	#our main keywords: SELECT, FROM, WHERE
	myKeywords = []
	#a collection of WhereClause objects. Allows us to keep track of the order that conditions were sent
	myArguments = []
	#a collection of AliasGroup objects. 
	Attributes = []
	#another collection of AliasGroup objects.
	Tables = []
######################################################
#These objects I am only using for debugging purposes.
#I am keeping them because they list every token
######################################################
	myAttributes = []
	myTables = []
	myConditions = []
	myBooleans = []
	myComparisons = []

operators = ["=","!=",">","<",">=","<=","<>",",","LIKE"]

boolean = ["AND","OR","NOT"] 

#################################
#mySQLparser's MAIN function	#
#################################
def Main(args=""):
	
	buildDataFrames()
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
	sendParts(s)
	end = time.time()
	print "Parse Time is: " + str(round(end - start,4) * 1000) + "ms"
	return

###########################################################
#a string cleaner function: standardizes the sql statement#
###########################################################
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
					sql = sql[:i+1]  + " " +  sql[i+1:]
					i = 0
			
	#remove any extra whitespace
	return sql.strip()
###############################################################			
#Check if a certain Keyword has been passed: did we go through#
#this gate yet?						      #
###############################################################
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

##########################
#Sorts tokens string	#
########################
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
################################################
#separates and element into an name alias pair#
##############################################
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
			Attributes = AliasGroup("","","") 

##############################################################################
#a function to figure out if a string is of certain type:                   #   
#namely is it an operator ['<','<>','=',etc.] or boolean ['AND','OR','NOT']#
###########################################################################
def is_Type(o,dtype):
	for token in dtype:
		if (str(o).upper() == token):
			return True
	return False

####MONSTER FUNCTION#### 
###TO DO: MODULARIZE####
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
				#raw_input()
			elif in_QUOTE and token.index("'") >= 0:
				arg = arg + " " + token
				in_QUOTE = False				
				#print arg
				#raw_input()
		except:
			if in_QUOTE:
				arg = arg + token
				#
				#print arg
				#raw_input()
		
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
		elif in_QUOTE == True and seen_OPERATOR == True:
				where.right = arg
				where.Type = "ARGUMENT"
				s.myArguments.insert(len(s.myArguments),where)
				seen_OPERATOR  = False
				where = WhereClause("","","","","")

	return
			

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

def Test():
	s = sqlParts()
	#s.myArguments.boolean = sys.argv[1]
	#s.insert(len(s),i)
	operator = negateOperator(sys.argv[1])
	
	print operator
 
#Test()
Main()

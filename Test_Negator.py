import sys

def negateOperator(o):
	#"=","!=",">","<",">=","<=","<>",","
	if o == '=':
		o = '<>'
	elif o == '<':
		o = '>='
	elif o == '>':
		o = '<='
	elif o == '>=':
		o = '<'
	elif o == '<=':
		o = '>'
	elif o == '<>' or o =='!=':
		o = '='
	return o
	

def Test():
	operator = negateOperator(sys.argv[1])
	
	print operator
 
Test()

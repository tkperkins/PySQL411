import sqlparse

def Main():

	sql = 'SELECT M.director_name FROM movies.csv M, oscars.csv O WHERE M.movie_title = O.Film AND O.Award = "Cinematography" AND O.Winner = True;'

	result = sqlparse.parse(sql)

	#print result[0].tokens[-1].tokens
	recurse(result[0].tokens[-1])


def recurse(tok_list, depth=0):
	for tok in tok_list.tokens:
		if isinstance(tok, sqlparse.sql.TokenList):
			recurse(tok, depth+1)
		elif isinstance(tok, sqlparse.sql.Token):
			print '-'*depth + '|' + str(tok.ttype) + ': ' + str(tok.value)
		else:
			raise Error

Main()
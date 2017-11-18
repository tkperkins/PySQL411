SELECT M1.director_name, M1.title_year, M1.movie_title, M2.title_year, M2.movie_title, M3.title_year, M3.movie_title
FROM ./movies.csv M1 JOIN ./movies.csv M2 JOIN ./movies.csv M3 ON (M1.director_name = M2.director_name AND M1.director_name = M3.director_name)
WHERE M1.movie_title <> M2.movie_title AND M2.movie_title <> M3.movie_title AND M1.movie_title <> M3.movie_title AND M1.title_year < M2.title_year-10 AND M2.title_year < M3.title_year-10

## John's code currently doesn't pick up JOIN ON
## Arbitrarily complicated nested
## SQL Parsing library...

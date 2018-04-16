# Note: Queries work with multiple spaces between terms

## Perfectly executing Queries
python engine.py "select * from table1"
python engine.py "select A, B from table1"
python engine.py "select distinct A, A from table1"
python engine.py "select min(A), max(B) from table1"
python engine.py "select C from table1 where A=922"
python engine.py "select A, table1.B, C, D from table1, table2 where table1.B=table2.B"
python engine.py "select A, table1.B, C, D from table1, table2 where ((table1.B=table2.B) and (A<=D and C<=D))"
python engine.py "select distinct avg(A) from table1, table2 where (table1.B=table2.B) and (C>=3000 and C<=8000)"


## Queries giving out errors

# "ERROR: Query does not begin with select <columns>"
python engine.py "selec * from table1"

# "ERROR: Query does not contain from <tables>"
python engine.py "select * fro table1"

# "ERROR: No file named 'metadata.txt' present"
# "ERROR: No .csv file for '<table>' present"
# "ERROR: No table named '<table>' present in 'metadata.txt'"
python engine.py "select * from table3"

# "ERROR: Non existent column name '<col>'"
python engine.py "select D from table1"

# "ERROR: Ambiguous column name '<col>'"
python engine.py "select B from table1, table2"

# "ERROR: Unknown value '<condition>' in where clause"
python engine.py "select * from table1 where A = blah"

# "ERROR: Invalid syntax in where clause"
python engine.py "select * from table1 where A == 100"

# "ERROR: distinct is applied on entire row, not on separate columns"
python engine.py "select distinct(A), distinct(B) from table1"

# "ERROR: Unbalanced brackets in list of columns"
python engine.py "select A,B) from table1"

# "ERROR: Invalid value '<col>' in list of columns"
python engine.py "select table1.D from table1"

# "ERROR: Cannot apply multiple aggregations on the same column"
python engine.py "select min(max(A)) from table1"

# "ERROR: Either All or None of the columns should have aggregates"
python engine.py "select max(A), B from table1"


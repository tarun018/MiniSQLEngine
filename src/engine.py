# Written by Tarun Gupta (201403002)
import sys
import csv
import re

def preprocess(query):
    q = query.strip()
    # remove multiple spaces between terms
    q = re.sub(" +", " ", q)
    # ensure relational operators, commas, brackets have spaces around them
    q = re.sub(" ?(<(?!=)|<=|>(?!=)|>=|!=|=|,|\\(|\\)) ?", " \\1 ", q)
    return q

# check if select and from exist in query
def check_valid(query):
    if "select" not in query[:6]:
        print "ERROR: Query does not begin with select <columns>"
        sys.exit(1)
    if "from" not in query:
        print "ERROR: Query does not contain from <tables>"
        sys.exit(1)

# get column names of respective tables
def get_metadata():
    metadata = {}
    try:
        with open("metadata.txt") as f:
            while True:
                s = f.readline().rstrip("\r\n")
                if s == "":
                    break
                if s == "<begin_table>":
                    table_name = f.readline().rstrip("\r\n")
                    metadata[table_name] = []
                    while True:
                        s = f.readline().rstrip("\r\n")
                        if s == "<end_table>":
                            break
                        metadata[table_name].append(table_name + "." + s)
    except IOError:
        print "ERROR: No file named 'metadata.txt' present"
        sys.exit(1)
    return metadata

# return the cross product of two tables
def join_tables(table1, table2):
    table = []
    for row1 in table1:
        for row2 in table2:
            l = row1[:]
            l.extend(row2)
            table.append(l)
    return table

def get_table(table_names):
    col_names = get_metadata()
    col = []
    try:
        for table_name in table_names:
            col.extend(col_names[table_name])
    except KeyError:
        print "ERROR: No table named '" + table_name + "' present in 'metadata.txt'"
        sys.exit(1)
    # load all table data from .csv files
    all_tables = []
    for table_name in table_names:
        table = []
        try:
            with open(table_name + ".csv") as f:
                rows = csv.reader(f)
                for r in rows:
                    table.append(map(int, r))
        except IOError:
            print "ERROR: No .csv file for '" + table_name + "' present"
            sys.exit(1)
        all_tables.append(table)
    # get merged table as 2D array
    res = all_tables[0]
    for i in xrange(1, len(all_tables)):
        res = join_tables(res, all_tables[i])
    table = [col]
    table.extend(res)
    return table

# resolve ambiguity of column name, if it is not clear
def resolve_ambiguity(col, names):
    if "." in col:
        return col
    cnt = 0
    for i in names:
        j = i.split(".")[1]
        if col == j:
            cnt += 1
    if cnt == 0:
        print "ERROR: Non existent column name '" + col + "'"
        sys.exit(1)
    elif cnt > 1:
        print "ERROR: Ambiguous column name '" + col + "'"
        sys.exit(1)
    for i in names:
        j = i.split(".")[1]
        if col == j:
            return i

def apply_where(conds, table):
    # ignore if there are no where conditions
    if len(conds) == 0:
        return table
    ops = ["<", "<=", ">", ">=", "!=", "=", "(", ")", "and", "or", ""]
    for i in xrange(len(conds)):
        if conds[i] not in ops and not conds[i].isdigit():
            try:
                conds[i] = resolve_ambiguity(conds[i], table[0])
                conds[i] = "row[" + str(table[0].index(conds[i])) + "]"
            except ValueError:
                print "ERROR: Unknown value '" + conds[i] + "' in where clause"
                sys.exit(1)
    c = " ".join(conds).strip()
    c = c.replace(" = ", " == ")
    res = [table[0]]
    for row in table[1:]:
        # apply where conditions on each row
        try:
            if eval(c):
                res.append(row)
        except SyntaxError:
            print "ERROR: Invalid syntax in where clause"
            sys.exit(1)
    return res

# perform the corresponding function as mentioned in params
def aggfunc(a, b, func):
    if func == "max":
        return max(a, b)
    elif func == "min":
        return min(a, b)
    elif func == "sum" or func == "avg":
        return a + b

def balanced_brackets(l):
    s = []
    for i in l:
        if i == "(":
            s.append(i)
        elif i == ")":
            if len(s) == 0:
                return False
            s.pop()
    if len(s) > 0:
        return False
    return True

def print_cols(col_names, table):
    agg = ["min", "max", "sum", "avg"]
    brac = ["(", ")", ""]
    distinct = False
    agglist = []
    # distinct keyword can only be at the beginning
    if col_names[0] == "distinct":
        if col_names[1] in brac:
            print "ERROR: Distinct is applied on entire row, not on separate columns"
            sys.exit(1)
        col_names.remove("distinct")
        distinct = True
    if not balanced_brackets(col_names):
        print "ERROR: Unbalanced brackets in list of columns"
        sys.exit(1)
    # either * or mention column names
    if len(col_names) == 1 and col_names[0] == "*":
        ind = [i for i in xrange(len(table[0]))]
    else:
        ind = []
        aggcnt = 0
        for col in col_names:
            if col in agg:
                aggcnt += 1
            if col not in agg and col not in brac:
                try:
                    col = resolve_ambiguity(col, table[0])
                    ind.append(table[0].index(col))
                except ValueError:
                    print "ERROR: Invalid value '" + col + "' in list of columns"
                    sys.exit(1)
        if aggcnt > len(ind):
            print "ERROR: Cannot apply multiple aggregations on the same column"
            sys.exit(1)
        # get the aggregate functions corresponding to the columns
        i = 0
        while i < len(col_names):
            if col_names[i] in agg and col_names[i+1] == "(" and col_names[i+3] == ")":
                agglist.append(col_names[i])
                i += 5
            else:
                i += 1
        if len(agglist) != 0 and len(agglist) != len(ind):
            print "ERROR: Either All or None of the columns should have aggregates"
            sys.exit(1)
    # create table with only the selected columns
    res = []
    for row in table:
        l = []
        for i in ind:
            l.append(row[i])
        res.append(l)
    # compute aggregate functions of the columns if any
    if len(agglist) != 0:
        for j in xrange(len(agglist)):
            res[0][j] = agglist[j] + "(" + res[0][j] + ")"
        try:
            m = res[1]
            for row in res[2:]:
                for j in xrange(len(row)):
                    m[j] = aggfunc(m[j], row[j], agglist[j])
            for j in xrange(len(agglist)):
                if agglist[j] == "avg":
                    m[j] /= (len(res) - 1)*1.0
            res = [res[0]]
            res.append(m)
        except IndexError:
            res = [res[0]]
    # remove duplicate rows if distinct keyword
    if distinct == True:
        uniq = []
        for i in res:
            if i not in uniq:
                uniq.append(i)
        res = uniq
    # print selected columns of table
    for i in res:
        print ", ".join(map(str, i))
    print

# extract column names, table names and where clause conditions from query
def extract_fields(query):
    q = re.split(" , | ", query)
    f = q.index("from")
    try:
        w = q.index("where")
    except ValueError:
        w = len(q)
    col_names = q[1:f]
    table_names = q[f+1:w]
    conds = q[w+1:]
    return col_names, table_names, conds

if __name__ == "__main__":
    query = sys.argv[1]
    query = preprocess(query)
    check_valid(query)
    col_names, table_names, conds = extract_fields(query)
    table = get_table(table_names)
    table = apply_where(conds, table)
    print_cols(col_names, table)

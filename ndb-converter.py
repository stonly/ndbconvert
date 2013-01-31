#!/usr/bin/python
import MySQLdb
import getpass

def get_tables(db):
    """
	Returns a list of all the table names in the database.
	Requires the database connection 'db'
    """
    cur = db.cursor() 
    cur.execute("SHOW TABLE STATUS WHERE Engine != 'ndbcluster'")
    table_list = []
    for row in cur.fetchall() :
        table_list.append(row[0])
    cur.close()
    return table_list

def convert_table(db, tbl):
    """
	Converts the table 'tbl' to NDB.
	Returns a dictionary with the key result and the 
	value 'success' if successful or a value with the failure code if not
    """
    cur = db.cursor() 
    try:
        cur.execute("ALTER TABLE %s ENGINE = NDB " % tbl)
        cur.fetchall()
        cur.close()
        return dict(result='success')
    except Exception, detail:
        return dict(result=str(detail))

def process_tables(db, tbl_list, tbl_dict={}):
    """ 
	Attempts to convert the tables in 'tbl_list' to NDB
	Recursivly until 3 attempts are made for every table
    """
    failed_tables = []
    recur = None
    for tbl in tbl_list:
        if tbl not in tbl_dict:
	    tbl_dict[tbl] = 0
        if tbl_dict[tbl] < 3:
	    print("attempt %s converting %s" % (tbl_dict[tbl]+1, tbl))
            recur = 1 
	    status = convert_table(db, tbl)
	    if status['result'] == 'success' and tbl in tbl_dict:
	        del tbl_dict[tbl]
		print("success converting %s" % tbl)
            elif status['result'] != 'success':
	        print "could not convert", tbl
	        print "error", status['result']
	        process_error(db, tbl, status['result'])
		tbl_dict[tbl] += 1
	        failed_tables.append(tbl)
    if recur:
        process_tables(db, failed_tables, tbl_dict)
    elif len(failed_tables) >= 1:
	return ";".join(failed_tables)
    else:
	return "successfuly upload"

def process_error(db, tbl, detail):
    """
	Attempts to remedy the error 'details' for the 'tbl'
    """
    cur = db.cursor() 
    if '1214' in detail :
        print("droping FULLTEXT keys from %s" % tbl)
	cur.execute("show index from %s where Index_type='FULLTEXT'" % tbl)
	fulltext_keys = set()
	for row in cur.fetchall():
	    fulltext_keys.add(row[2])
	for ftk in fulltext_keys:
	    cur.execute("ALTER TABLE %s DROP index %s" % (tbl, ftk))
    elif '1073' in detail :
        field = str(detail).split("'")[1]
        print("trying to update %s to varchar" % field)
        cur.execute("ALTER TABLE %s MODIFY %s VARCHAR(255)" % (tbl, field))
    return cur.close()

if __name__ == '__main__':
    host = raw_input("Host : ")
    user = raw_input("User : ")
    pwd  = getpass.getpass("Password : ")
    db_name   = raw_input("Database : ")
    db = None
    try :
	db = MySQLdb.connect(host=host,
                         user=user,
			 passwd=pwd,
                         db=db_name)
    except Exception, details:
	print("unable to connect to %s " % db_name)
    if db:
        print process_tables(db, get_tables(db))

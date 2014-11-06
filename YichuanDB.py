# all database related functions will be placed here
# 25 Feb 2013 Yichuan Shi
# version 0.1

import os, sys, psycopg2, time

class ConnectionParameter:
    def __init__(self, host = 'localhost',
                 db = 'whs',
                 port = '5432',
                 user = 'postgres',
                 password = 'gisintern'):
        self.host = host
        self.db = db
        self.port = port
        self.user = user
        self.password = password

##    def toCommandString(self):
##        return PSQL + ' -h ' + self.host + ' -d ' + self.db + ' -p ' + self.port + ' -U ' + self.user

    def getConn(self):
        conn = psycopg2.connect(host = self.host,
                              database = self.db,
                              port = self.port,
                              user = self.user,
                              password = self.password)
        return conn

class handle_connection():
    def __init__(self, f):
        """function pass to the constructor"""
        self.f = f

    def __call__(self, *args):
        conn = args[-1]
        if not isinstance(conn, psycopg2._psycopg.connection):
            raise TypeError('The last argument is not a connection type')

        result = None
        # call function
        try:
            result = self.f(*args)

        except Exception as e:
            print(e)
            print('roll back')
            conn.rollback()

        else:
            print('commit transaction')
            conn.commit()

        return result

def getConn84(dbname='world_heritage_sites'):
    conn_parameter = ConnectionParameter(db = dbname, port='5433')
    return conn_parameter.getConn()

def getConn91(dbname='spatial_db'):
    conn_parameter = ConnectionParameter(db = dbname, port='5434')
    return conn_parameter.getConn()

def getCurrentWH():
    """
    A quick way to get a connection to the current WH database
    postgres9.1/postgis2.0 at localhost
    port 5432

    return
    ---
    psycopg2 connection object

    """
    conn_parameter = ConnectionParameter(db='whs', port='5432')
    return conn_parameter.getConn()

@handle_connection
def getDictFromDBTable(schema, table, keyfield, valuefield, conn):
    """
    Get look up dictionaries of key-value
    It returns only the first encounter if many to many cardinality

    return
    ---
    dict[key]= [value]
    """
    lookup_dict = dict()


    with conn.cursor() as cur:
    # sql

        sql = """
        SELECT %s, %s
        FROM %s"""%(keyfield, valuefield, schema + '.' + table)
        print(sql)
        cur.execute(sql)

        for each in cur.fetchall():
            if not lookup_dict.has_key(each[0]):
                lookup_dict[each[0]] = each[1]
            else:
                print('warning: maybe one to many relationships')


    return lookup_dict



@handle_connection
def get_sql_result(sql, conn):
    result = None

    print(sql)

    # run sql
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchall()

    return result

# a thin wrapper
def get_sql_result_as_list(sql, conn):
    """
    Get a list of a return sql statement
    It returns only the first encounter if many to many cardinality

    return
    ---
    dict[key]= [value]
    """
    result = get_sql_result(sql, conn)


    return [each[0] for each in result]

def get_all_fields(table, conn):
    """
    Qualified table name
    """

    schema = table.split('.')[0]
    name = table.split('.')[1]

    sql = """select column_name from
            information_schema.columns where
            table_schema = '%s' and table_name = '%s'"""%(schema, name)

    return get_sql_result_as_list(sql, conn)


@handle_connection
def process_sql(sql, conn):
    result = None

    print(sql)
    # run sql
    with conn.cursor() as cur:
        cur.execute(sql)



def clean_view(schema_to_clean, conn):
    """
    this function is used to clear ALL views in the given schema

    """
    sql = """
    SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'
    """%(schema_to_clean, )


    views = get_sql_result_as_list(sql, conn)
    for view in views:
        sql = """
        DROP VIEW IF EXISTS %s CASCADE
        """%(schema_to_clean + '.' + view,)
        process_sql(sql, conn)



@handle_connection
def _test_be_decor(table, field, conn):
    cur = conn.cursor()

    sql = """SELECT %s FROM %s """%(field, table)

    cur.execute(sql)
    result = cur.fetchall()
    cur.close()

    return result


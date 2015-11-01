#from yapgt import Model
from utils import connect

class Query_Seq_Idx_Cache(object):
    def __init__(self):
        pass 

class Query_Seq_Idx(object):
    def __init__(self, unique_id=0):
        self.query="""
        SELECT
            relid,
            relname,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            n_tup_ins,
            n_tup_upd,
            n_tup_del
        FROM
            pg_stat_all_tables
        """
        # We need the cursor
        self.db_connection_cursor = self.create_db_connection()
        # Retrieve initial data
        self.columns, self.rows = self.get_query_data(self.db_connection_cursor, self.query)
        print self.columns
        mygenerator = self.get_row()

        for i in mygenerator:
            print i

    def get_columns(self):
        return self.columns

    def get_row(self):
        for row in self.rows:
            return row

    def show_query(self):
        return self.query

    def create_db_connection(self):
        return connect.pg_connect("localhost", "5432", "postgres", None, "postgres")

    def get_query_data(self, db_connection_cursor, query):
        return connect.pg_get_data(db_connection_cursor, query)

class Query_Table_Idx(object):
    def __init__(self):
        pass
        """
        SELECT
            indexrelid,
            idx_scan,
            idx_tup_read,
            idx_tup_fetch,
            relname,
            indexrelname
        FROM
            pg_stat_all_indexes
        """

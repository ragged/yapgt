import psycopg2


def pg_connect(
        host,
        port,
        user,
        password,
        database):
    """
    Small connection class
    If no password is suplied we try the default postgres user
    and expect a setted pg_ident or something similar
    """
    #if DEBUG: keep("Model().pg_connect()")
    try:
        if password:
            conn = psycopg2.connect(
                database=database,
                user=user,
                port=port,
                password=str(password),
                host=host
                #connection_factory = psycopg2.extras.DictConnection
                )
        else:
            conn = psycopg2.connect(
                database=database,
                user=user,
                port=port,
                password="",
                host=None
                #connection_factory = psycopg2.extras.DictConnection
                )

    except psycopg2.Error, psy_err:
        print "The connection is not possible!"
        print psy_err
        print psycopg2.Error
        if host is None:
            raise psy_err

    conn.set_isolation_level(0)
    return conn


def pg_get_data(connection, query):
    ''' Just a general method to fetch the date for different queries '''
    #if DEBUG: keep("Model().pg_get_data()")
    
    cur = connection.cursor()
    cur.execute(query)
    data = cur.fetchall()
    column_headers = [desc[0] for desc in cur.description]
    
    return column_headers, data

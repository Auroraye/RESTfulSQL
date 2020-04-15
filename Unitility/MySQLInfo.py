localhost = "localhost"
localport = 3306
localuser = "4440user"
localpass = "4440password"
localbase = "4440db"
sharedhost = "db4free.net"
sharedport = 3306
shareduser = "mxkezffynken"
sharedpass = "XUWNG3gdFw82"
sharedbase = ""
host = localhost
port = localport
user = localuser
password = localpass


def db_query(mysql, query, args):
    """
    A handler method for calling database procedures.
    :param query: the name of the query to be executed
    :type query: str
    :param args: arguments to pass in to the procedure
    :type args: tuple
    :return: a 2D tuple for result (becomes () if there is error), an error message (None if no error)
    :rtype: (tuple, str)
    """

    cur = mysql.connection.cursor()
    result, error = (), None
    try:
        cur.execute(query, args)
        result = cur.fetchall()
    except:
        error = mysql.connection.error()
    finally:
        cur.close()
        mysql.connection.commit()
    return result, error

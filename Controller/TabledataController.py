from util.QueryHelper import db_query
from Controller.PredictableExeption import *

def update_tabledata(table, column, value, condition, mysql):
    # Try to parse the table variable in order to detect exception.
    tables = table.split(",")
    if len(tables) == 0:
        raise PredictableInvalidArgumentException("1")
    elif len(tables) > 1:
        raise PredictableInvalidArgumentException("4")

    # Parse the list of columns from string into array.
    columns = column.split(",")
    # Now, check the duplication in columns.
    for elem in columns:
        if columns.count(elem) > 1:
            raise PredictableDuplicateColumnException(elem)

    # Parse the list of values from string into array.
    values = value.split(",")
    # Now, check the relation between columns and values.
    if len(values) != len(columns):
        raise PredictableColumnNumberMismatchException(elem)

    # Try to parse the table variable in order to detect exception.
    condition = condition.split(",")

    # The next step is to communicate with the database.

    # The first thing to do is to turn of the autocommit variable,
    # and start a new transaction.
    con = mysql.connection
    cur = con.cursor()
    con.autocommit = False

    # Now, we can start to communicate with the database.
    command = "UPDATE `" + table + "` "
    command = command + "SET "
    x = 0
    for elem in columns:
        command = command + "`" + elem + "` = `" + value[x] + "`, "
        x+=1
    if len(condition) != 0:
        command = command + "Where `" + condition + "`;"
    try:
        cur.execute(command)
    except Exception as e:
        con.rollback()
        cur.close()
        raise e

    con.commit()
    con.autocommit = True
    cur.close()
    status = 200
    message = "Table " + table + " is updated."
    return status, message, None, None

def delete_tabledata(table, column, mysql):
    command = "Alter TABLE `" + table +"` "
    commnad = command + "DROP COLUMN `" + column + "`;"
    cur = mysql.connection.cursor()
    try:
        cur.execute(command)
    except Exception as e:
        cur.close()
        raise e
    cur.close()
    
    status = 200
    data = ""
    message = "Column '" + column + "' is deleted."
    error = ""

    return status, message, data, error


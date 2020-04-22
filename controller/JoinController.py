from util.LFUHelper import *
from util.QueryHelper import db_query
from controller.PredictableExeption import *

def get_join(mysql, tables, columns, jointype, match, returned_view_name):
    
    table = tables.split(",")
    if len(tables) <= 1:
        raise PredictableJoinTableNotEnoughException()

    column = columns.split(",")
    if (jointype != "full"):
        if (len(match) != (len(table) - 1)):
            message = "Number of column mismatched with number of table"
            return 400, message, None, None
        else:
            matches = match.spilt(",")

    con = mysql.connection
    cur = con.cursor()
    con.autocommit = False

    if (returned_view_name != ""):
        command = "CREATE VIEW " + returned_view_name + " AS SELECT "
    else:
        command = "CREATE VIEW " + table[0] + "view AS SELECT "
    if (len(column) == 0):
            command += "* from "
    else:
        for col in column:
            command += col + ", "
        command = command[:-2] + " from "
        command += table[0] + " "

    if (jointype == "inner"):              
        for count in range (1, len(table)):
            command += "INNER JOIN " + table[count] + " ON " + match[count - 1]

    elif (jointype == "partial"):
        for count in range (1, len(table)):
            command += "LEFT JOIN " + table[count] + " ON " + match[count - 1]

    elif (jointype == "full"):
        if (len(table) != 2):
            message = "Only two tables can be full join at one time"
            return 400, message, None, None
        command += "CROSS JOIN " + table[1]

    else:
        message = "Incorrect join type"
        return 400, message, None, None

    command += ";"
    try:
        cur.execute(command)
    except Exception as e:
        print(command)
        con.rollback()
        cur.close()
        raise e

    con.commit()
    con.autocommit = True
    cur.close()
    status = 200
    if returned_view_name != "":
        message = "Join between tables is created successfully. New view \'{}\' is saved.".format(
                returned_view_name)
    else:
        message = "Join between tables is created successfully. New view" + table[0] +"view is saved."
    return status, message, None, None
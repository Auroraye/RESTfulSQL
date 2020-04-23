from util.LFUHelper import *
from util.QueryHelper import db_query
from controller.PredictableExeption import *


def get_join(mysql, tables, columns, renames, jointype, match, returned_view_name):
    table = [x.strip() for x in tables.split(',')]
    if len(tables) <= 1:
        raise PredictableJoinTableNotEnoughException()

    if jointype != "full":
        matches = [x.strip() for x in match.split(',')]
        if (matches[0] == "") or (len(matches) != (len(table) - 1)):
            message = "Number of column mismatched with number of table"
            return 412, message, None, None

    if returned_view_name != "":
        command = "CREATE VIEW " + returned_view_name + " AS SELECT "
    else:
        message = "View name missed"
        return 412, message, None, None
    if columns == "":
        command += "* from "
    else:
        column = [x.strip() for x in columns.split(',')]
        names = [x.strip() for x in renames.split(',')]
        if len(names) != len(column):
            message = "Number of names and columns mismatched"
            return 412, message, None, None
        for count in range(0, len(column)):
            command += column[count] + " AS " + names[count] + ", "
        command = command[:-2] + " from "
    
    

    command += table[0] + " "

    if jointype == "inner":
        for count in range(1, len(table)):
            command += "INNER JOIN " + table[count] + " ON " + matches[count - 1] + " "

    elif jointype == "partial":
        for count in range(1, len(table)):
            command += "LEFT JOIN " + table[count] + " ON " + matches[count - 1] + " "

    elif jointype == "full":
        if len(table) != 2:
            message = "Only two tables can be full join at one time"
            return 412, message, None, None
        command += "CROSS JOIN " + table[1] + " "

    else:
        message = "Incorrect join type"
        return 412, message, None, None

    command = command[:-1] + ";"
    data, error = db_query(mysql, command, None)
    if error is not None:
        message = "Input incorrect! " + error
        return 412, message, None, error
    message = "Join between tables is created successfully. New view \'{}\' is saved.".format(
        returned_view_name)
    LFU_increment(returned_view_name, mysql)
    return 201, message, data, None

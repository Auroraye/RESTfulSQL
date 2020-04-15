from Controller.PredictableExeption import *
from util.QueryHelper import db_query


def get_metadata(table_name, mysql, current_database):
    resultlist = []
    error = ""
    if table_name == 'TABLE':
        result, error = db_query(
            mysql, 'SHOW FULL TABLES IN {};'.format(current_database), None)
        for item in result:
            temp = {"Tables": item[0],
                    "Table_type": item[1]
                    }
            resultlist.append(temp)
        message = "Success! Get all table info from " + current_database + "."
    elif table_name == 'VIEW':
        result, error = db_query(
            mysql, 'SHOW FULL TABLES IN {} WHERE TABLE_TYPE LIKE \'VIEW\';'.format(current_database), None)
        for item in result:
            temp = {"Views": item[0],
                    "Table_type": item[1]
                    }
            resultlist.append(temp)
        message = "Success! Get all view info from " + current_database + "."
    else:
        result, error = db_query(
            mysql, 'DESCRIBE {};'.format(table_name), None)
        for item in result:
            # temp = jsonify(Field=item[0], Type=item[1], Null=item[2], Key=item[3])
            temp = {"Field": item[0],
                    "Type": item[1],
                    "Null": item[2],
                    "Key": item[3]
                    }
            resultlist.append(temp)
        message = "Success! Get all column info from " + current_database + "."
    data = resultlist
    status = 200
    if error is not None:
        status = 400
        message = "Failed. Error: " + error
    return status, message, data, error


def update_column(table, column, operation, value, mysql):
    # First, check if the number of element in table is correct.
    tables = table.split(",")
    if len(tables) == 0:
        raise PredictableInvalidArgumentException("1")
    elif len(tables) > 1:
        raise PredictableInvalidArgumentException("2")
    # Then, check if the table is in the database.
    con = mysql.connection
    cur = con.cursor()
    command = "SELECT * FROM `" + table + "` LIMIT 1;"
    try:
        cur.execute(command)
    except Exception as e:
        raise PredictableTableNotFoundException(table)

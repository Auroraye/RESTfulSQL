from util.QueryHelper import db_query

def get_metadata(table_name, mysql, current_database):
    resultlist = []
    if table_name == 'TABLE':
        result, error = db_query(
            mysql, 'SHOW FULL TABLES IN {};'.format(current_database), None)
        for item in result:
            temp = {"Tables": item[0],
                    "Table_type": item[1]
                    }
            resultlist.append(temp)
    elif table_name == 'VIEW':
        result, error = db_query(
            mysql, 'SHOW FULL TABLES IN {} WHERE TABLE_TYPE LIKE \'VIEW\';'.format(current_database), None)
        for item in result:
            temp = {"Views": item[0],
                    "Table_type": item[1]
                    }
            resultlist.append(temp)
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
    message = resultlist
    status = 200
    data = ""
    error = ""
    return status, message, data, error


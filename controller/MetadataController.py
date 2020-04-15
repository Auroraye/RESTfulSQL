from controller.PredictableExeption import *
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

    # Then parse the other parameters.
    columns = column.split(",")
    operations = operation.split(",")
    values = value.split(",")
    if not len(columns) == len(operations) == len(values):
        raise PredictableNumberOfParameterNotMatchException("columns,types,values")

    # Reformat the parameters while checking three kinds of potential error:
    # 1. One kind of operation apply to the same column more than one times;
    # 2. If there are invalid or undefined operation;
    # 3. If the values of the corresponding operation defined.
    by_col = {}
    i = 0
    defined_type = ["default", "type", "nullable"]
    defined_type_value = ["int", "float", "double", "decimal", "date", "string", "char", "varchar"]
    defined_nullable_value = ["yes", "no", "true", "false", "1", "0"]
    while i < len(columns):
        col = columns[i]
        typ = operations[i]
        val = values[i]
        if typ not in defined_type:
            raise PredictableInvalidArgumentException("4")
        if typ == "type":
            if val not in defined_type_value:
                if val.startwith("char") or val.startwith("varchar") or val.startwith("int"):
                    try:
                        b = val.index("(")
                        e = val.index(")")
                        if e < b:
                            raise Exception()
                        num = int(val[b+1:e])
                    except Exception as e:
                        raise PredictableInvalidArgumentException("5")
                else:
                    raise PredictableInvalidArgumentException("5")
            elif val == "float" or val == "double":
                val = "decimal"
            elif val == "string" or val == "varchar":
                val = "varchar(200)"
            elif val == "char":
                val = "char(200)"
        elif typ == "nullable":
            val = val.lower()
            if val not in defined_nullable_value:
                raise PredictableInvalidArgumentException("5")
            elif val == "true" or val == "yes" or val == "1":
                val = "null"
            else:
                val = "not null"
        if col not in by_col:
            by_typ = {typ: val}
            by_col[col] = by_typ
        else:
            if typ in by_col[col]:
                raise PredictableConflictOperationException("columns,"+col)
            else:
                by_col[col][typ] = val
        i += 1

    # Here start to change the columns.
    con.autocommit = False
    for col in by_col:
        command = "ALTER TABLE `" + table + "`"
        typ = ""
        if "type" in by_col[col]:
            typ = by_col[col]["type"]
        else:
            tem_com = "SHOW COLUMNS FROM `" + table + "` WHERE `Field` = \'" + col + "\';"
            try:
                cur.execute(tem_com)
                result = cur.fetchall()
                row = result[0]
                typ = row[1]
            except Exception as e:
                raise e
        if "type" in by_col[col] or "nullable" in by_col[col]:
            command += " MODIFY `" + col + "` " + typ
            if "nullable" in by_col[col]:
                command += " " + by_col[col]["nullable"]
            try:
                cur.execute(command)
            except Exception as e:
                con.abort()
                cur.close()
                raise e
        if "default" in by_col[col]:
            val = by_col[col]["default"]
            try:
                if typ == "int":
                    val = int(val)
                elif typ == "decimal":
                    val = float(val)
                elif typ == "date":
                    val = val
                else:
                    val = "\"" + val + "\""
            except Exception as e:
                con.abort()
                cur.close()
                raise PredictableTypeNotMatchException(typ + "," + val)
            # ALTER City SET DEFAULT 'Sandnes';
            command = "ALTER TABLE `" + table + "` ALTER `" + col + "` SET DEFAULT " + val + ";"
            try:
                cur.execute(command)
            except Exception as e:
                con.abort()
                cur.close()
                raise e
    status = 200
    message = "Table " + table + "'s metadata has been changed accordingly."
    return status, message, None, None
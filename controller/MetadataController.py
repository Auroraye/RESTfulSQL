import os

from controller.PredictableExeption import *
from util.ExtractSpecialArray import *
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
            mysql, 'DESCRIBE `{}`;'.format(table_name), None)
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


def post_unique_key(table, key, name, mysql):
    uniques = extract_unique_key(key)
    names = name.split(",")

    # The first potential error is that not all the keys have a corresponding name.
    if len(uniques) != len(names):
        raise PredictableNumberOfParameterNotMatchException("keys,key_names")

    status, message, table_structure, error = get_metadata(table, mysql, os.getenv("MYSQL_DB"))

    # Check the validation of each key.
    for k in uniques:
        ks = k.split(",")
        for a_k in ks:
            if check_exist_from_json(a_k, table_structure, "Field") is False:
                raise PredictableUnknownKeyException(a_k)

    # Check the duplication of key and key name
    tem = []
    for k in uniques:
        if k in tem:
            raise PredictableDuplicateKeyException(k)
        else:
            tem.append(k)
    tem = []
    for n in names:
        if n in tem:
            raise PredictableDuplicateConstraintNameException(n)
        else:
            tem.append(n)

    con = mysql.connection
    cur = con.cursor()
    con.autocommit = False

    i = 0
    while i < len(uniques):
        command = "ALTER TABLE `" + table + "` ADD CONSTRAINT `" + names[i] + "` "
        command += "UNIQUE (" + uniques[i] + ");"
        try:
            cur.execute(command)
        except Exception as e:
            con.rollback()
            cur.close()
            raise e
        i += 1
    con.commit()
    cur.close()
    status = 200
    message = "Table " + table + " has been added the specified keys."
    return status, message, None, None


def get_unique_key(table, mysql):
    check_table_field(table)

    con = mysql.connection
    cur = con.cursor()

    # Check if the table is in the database
    command = "SELECT * FROM `" + table + "`;"
    try:
        cur.execute(command)
    except Exception as e:
        cur.close()
        raise PredictableTableNotFoundException

    command = "SHOW CREATE TABLE `" + table + "`;"
    result = ""
    try:
        cur.execute(command)
        result = cur.fetchall()
        print(result)
        result = result[0][1]
        cur.close()
    except Exception as e:
        raise e
    data = []
    stop = False
    while not stop:
        try:
            ind = -1
            # Check if there is more unique key.
            try:
                ind = result.index('UNIQUE KEY')
            except Exception as e:
                stop = True
            if ind == -1:
                continue

            # Get the key name.
            b = result.index("`", ind + 1)
            e = result.index("`", b + 1)
            key_name = result[b+1: e]

            # Get what columns are in this key.
            b = result.index("(", e + 1)
            e = result.index(")", b + 1)
            cols = result[b+1: e]
            columns = cols.split(",")
            i = 0
            while i < len(columns):
                columns[i] = columns[i].strip('`')
                i += 1

            # Pack up this key information.
            final = {"key_name": key_name, "columns": columns}
            data.append(final)

            # Cut off the rest of the result
            result = result[e+1:]
        except Exception as e:
            raise e
    status = 200
    return status, None, data, None


def delete_unique_key(table, name, mysql):
    names = name.split(",")
    if len(names) == 0:
        raise PredictableInvalidArgumentException("7")

    # Check if duplication in name field
    tem = []
    for n in names:
        if n in tem:
            raise PredictableDuplicateConstraintNameException(n)
        else:
            tem.append(n)

    # Check if the keys are in the table.
    status, message, key, error = get_unique_key(table, mysql)
    for n in names:
        if check_exist_from_json(n, key, "key_name") is False:
            raise PredictableUnknownKeyException(n)

    # Start to communicate with the database.
    con = mysql.connection
    cur = con.cursor()
    con.autocommit = False

    for n in names:
        command = "ALTER TABLE `" + table + "` DROP INDEX `" + n + "`;"
        try:
            cur.execute(command)
        except Exception as e:
            con.rollback()
            cur.close()
            raise e

    status = 200
    data = ""
    message = "Keys are deleted."
    error = ""

    return status, message, data, error


def post_foreign_key(table, key, target, name, mysql):
    check_table_field(table)

    keys = key.split(",")
    targets = target.split(",")
    names = name.split(",")

    # Check the length of there three fields.
    if len(keys) != len(targets):
        raise PredictableNumberOfParameterNotMatchException("keys,targets")
    if len(keys) != len(names):
        raise PredictableNumberOfParameterNotMatchException("keys,key_names")
    if len(targets) != len(names):
        raise PredictableNumberOfParameterNotMatchException("targets,key_names")

    # Check the duplication of any of the three fields.
    tem = []
    for k in keys:
        if k in tem:
            raise PredictableDuplicateKeyException(k)
        else:
            tem.append(k)
    tem = []
    for k in targets:
        if k in tem:
            raise PredictableDuplicateKeyException(k)
        else:
            tem.append(k)
    tem = []
    for n in names:
        if n in tem:
            raise PredictableDuplicateConstraintNameException(n)
        else:
            tem.append(n)
    pass


def delete_foreign_key(table, name, mysql):
    pass


def get_foreign_key(table, mysql):
    check_table_field(table)

    con = mysql.connection
    cur = con.cursor()

    # Check if the table is in the database
    command = "SELECT * FROM `" + table + "`;"
    try:
        cur.execute(command)
    except Exception as e:
        cur.close()
        raise PredictableTableNotFoundException

    command = "SHOW CREATE TABLE `" + table + "`;"
    result = ""
    try:
        cur.execute(command)
        result = cur.fetchall()
        print(result)
        result = result[0][1]
        cur.close()
    except Exception as e:
        raise e
    data = []
    stop = False
    while not stop:
        try:
            ind = -1
            # Check if there is more unique key.
            try:
                ind = result.index('CONSTRAINT')
            except Exception as e:
                stop = True
            if ind == -1:
                continue

            # Get the key name.
            b = result.index("`", ind + 1)
            e = result.index("`", b + 1)
            key_name = result[b + 1: e]

            # Get what column is in this key.
            b = result.index("(", e + 1)
            e = result.index(")", b + 1)
            col = result[b + 2: e-1]

            # Get the target.
            b = result.index("REFERENCES", e + 1)
            b = result.index("`", b + 1)
            e = result.index("`", b + 1)
            t_table = result[b + 1: e]
            b = result.index("`", e + 1)
            e = result.index("`", b + 1)
            t_column = result[b + 1: e]

            # Pack up this key information.
            final = {"key_name": key_name, "column": col, "target_table": t_table, "target_column": t_column}
            data.append(final)

            # Cut off the rest of the result
            result = result[e + 1:]
        except Exception as e:
            raise e
    status = 200
    return status, None, data, None

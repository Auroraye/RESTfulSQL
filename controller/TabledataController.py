import os
import json
from controller.MetadataController import get_foreign_key, get_metadata, get_metadata_
from util.ExtractSpecialArray import check_table_field, extract_unique_key, check_exist_from_json
from util.QueryHelper import db_query
from controller.PredictableExeption import *


def get_tabledata(table, column, page, filter, sort_by, mysql):
    command = "SELECT "
    if column:
        columns = column.split(",")
        for c in columns:
            command += c + ","
        command = command[:-1] + " "
    else: 
        command += "* "
    
    command += "FROM " + table

    if (filter):
        command += " WHERE " + filter

    if (sort_by):
        command += " ORDER BY "
        sort_by = sort_by.split(",")
        for sort in sort_by:
            command += sort + ","
        command = command[:-1]

    command += " LIMIT 250 OFFSET " + str((int(page) - 1) * 250)

    columns, error = db_query(mysql, "SHOW columns FROM " + table)
    data, error = db_query(mysql, command)
    if column is None:
        row_headers=[x[0] for x in columns]
    else:
        row_headers = column.split(",")

    json_data=[]
    for row in data:
        json_data.append(dict(zip(row_headers, row)))
    # print(json.dumps(json_data))

    if (error == "FAILED_TO_CONNECT"):
        return 401, None, None, "Please connect to a database using the /connect endpoint."
    elif (error):
        return 400, None, None, error
    else:
        return 200, "Success", json_data, None


def update_tabledata(table, column, value, condition, mysql):
    # Try to parse the table variable in order to detect exception.
    tables = table.split(",")
    if len(tables) == 0:
        raise PredictableInvalidArgumentException("1")
    elif len(tables) > 1:
        raise PredictableInvalidArgumentException("6")
    
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

    # Now, we can start to communicate with the database.
    command = "UPDATE " + table + " "
    command = command + "SET "
    x = 0
    for elem in columns:
        command = command + elem + " = " + values[x] + ", "
        x+=1
    command = command[:-2] + " "
    if len(condition) != 0:
        command = command + "Where " + condition + ";"
    else:
        command = command + ";"
    print(command)
    data, error = db_query(mysql, command)
    if error != None:
        status = 412
        message = "Incorrect Input! " + error
        return status, message, None, error
    else:
        status = 201
        message = "Table " + table + " is updated."
        return status, message, None, None


def delete_tabledata(table, condition, mysql):
    # Try to parse the table variable in order to detect exception.
    tables = table.split(",")
    if len(tables) == 0:
        raise PredictableInvalidArgumentException("1")
    elif len(tables) > 1:
        raise PredictableInvalidArgumentException("6")
    
    command = "DELETE FROM " + table +" "
    command = command + "WHERE " + condition + ";"
    
    print(command)

    data, error = db_query(mysql, command)
    if error != None:
        status = 412
        message = "Incorrect Input! " + error
        return status, message, None, error
    else:
        status = 201
        message = "Row is deleted."
        return status, message, None, None


def vanilla_post_tabledata(table, column, value, mysql):
    check_table_field(table)
    columns = column.split(",")
    values = value.split(",")

    if len(values) != len(columns):
        raise PredictableNumberOfParameterNotMatchException("column,value")

    command = "INSERT INTO `" + table + "` ("
    for c in columns:
        command += "`" + c + "`, "
    command = command[0: -2] + ") VALUE("
    for v in values:
        command += typed_value(v) + ", "
    command = command[0:-2] + ");"

    con, cur = None, None
    try:
        con = mysql.connection
        cur = con.cursor()
    except Exception as e:
        return 401, None, None, e
    try:
        cur.execute(command)
        con.commit()
    except Exception as e:
        cur.close()
        raise e
    cur.close()
    status = 200
    message = "Data is inserted into table " + table + "."
    return status, message, None, None


def typed_value(value):
    try:
        int_type = int(value)
        dec_type = float(value)
        return value
    except Exception as e:
        return "\"" + value + "\""


def insert_upper_table(table, unknown, known, mysql):
    t1, t2, array, t3 = get_foreign_key(table, mysql)
    referenced = {}

    # Get the information about the referenced table
    for info in array:
        if info["target_table"] not in referenced:
            table_info = {"name": info["target_table"]}
            key_tar_list = []
            vale = ""
            for c in known:
                if c["column"] == info["column"]:
                    vale = c["value"]
                    break
            if vale == "":
                continue
            key_tar_list.append({"column": info["target_column"], "value": vale})
            table_info["key"] = key_tar_list
            referenced[info["target_table"]] = table_info
        else:
            key_tar_list = []
            vale = ""
            for c in known:
                if c["column"] == info["column"]:
                    vale = c["value"]
                    break
            if vale == "":
                continue
            key_tar_list.append({info["target_column"]: vale})
            referenced[info["target_table"]]["key"].append(key_tar_list)
    if len(referenced) == 0:
        return unknown, []

    # Organize all the columns in all the referenced table, and put them in to each table.
    all_columns = {}
    for t in referenced:
        t1, t2, data, t3 = get_metadata_(t, mysql)
        for r in data:
            col = r["Field"]
            if col not in referenced[t]["key"]:
                if col in all_columns:
                    all_columns[col] += 1
                else:
                    all_columns[col] = 1
                if "column" in referenced[t]:
                    referenced[t]["column"].append(col)
                else:
                    referenced[t]["column"] = [col]

    # Check if there is any columns appear in more than one table.
    new_know = {}
    still_unknown = {}
    for colu in unknown:
        all_column_key = [k for k in all_columns]
        col = colu["column"]
        val = colu["value"]
        if col in all_column_key:
            if all_columns[col] == 1:
                new_know[col] = val
            else:
                raise PredictableAmbiguousColumnNameException("a," + col)
        else:
            still_unknown[col] = val
    if len(new_know) == 0:
        return unknown, []

    # Create command
    array_command = []
    for info in referenced:
        table_name = referenced[info]["name"]
        cols = {}
        for col in referenced[info]["column"]:
            if col in new_know:
                cols[col] = new_know[col]
        if len(cols) > 0:
            keys = referenced[info]["key"]
            print(type(keys))
            columns = [k for k in cols]
            command = "INSERT INTO `" + table_name + "` (" + referenced[info]["key"][0]["column"]
            j = 1
            while j < len(keys):
                command += ", " + referenced[info]["key"][j]["column"]
                j += 1
            command += ", " + columns[0]
            j = 1
            while j < len(columns):
                command += ", " + columns[j]
                j += 1
            command += ") VALUE(" + typed_value(referenced[info]["key"][0]["value"])
            j = 1
            while j < len(keys):
                command += ", " + typed_value(referenced[info]["key"][j]["value"])
                j += 1
            command += ", " + typed_value(cols[columns[0]])
            j = 1
            while j < len(columns):
                command += ", " + typed_value(cols[columns[j]])
                j += 1
            command += ");"
            print(command)
            array_command.append(command)
    new_unknown = []
    unknown_key = [k for k in still_unknown]
    for k in unknown_key:
        new_unknown.append({"column": k, "value": still_unknown[k]})
    return new_unknown, array_command


def insert_lower_table(table, unknown, known, mysql, db):
    array_command = []

    # Get the list of tables that referencing to this table.
    table_list = []
    command = "SELECT FOR_NAME FROM information_schema.INNODB_SYS_FOREIGN WHERE REF_NAME LIKE \"" + db
    command += "/" + table + "\";"
    print(command)
    cur = mysql.connection.cursor()
    try:
        cur.execute(command)
        result = cur.fetchall()
        for i in result:
            table_list.append(i[0][len(db) + 1:])
        cur.close()
    except Exception as e:
        cur.close()
        raise PredictableHaveNoRightException()

    print(table_list)
    print(unknown)
    # Check margin case:
    if len(table_list) == 0:
        return unknown, []

    referenced = {}
    for t in table_list:
        key = t
        table_info = {"name": t}
        ti, t2, data, t3 = get_foreign_key(t, mysql)
        print(data)
        list_of_key = []
        for row in data:
            if row["target_table"].lower() == table.lower():
                column_name = row["column"]
                referenced_column = row["target_column"]
                column_value = ""
                for x in known:
                    if x["column"] == referenced_column:
                        column_value = x["value"]
                        break
                list_of_key.append({"column": column_name, "value": column_value})
        if len(list_of_key) < 1:
            continue
        table_info["key"] = list_of_key
        referenced[t] = table_info
    if len(referenced) == 0:
        return unknown, []

    # Organize all the columns in all the referenced table, and put them in to each table.
    all_columns = {}
    for t in referenced:
        t1, t2, data, t3 = get_metadata_(t, mysql)
        for r in data:
            col = r["Field"]
            if col not in referenced[t]["key"]:
                if col in all_columns:
                    all_columns[col] += 1
                else:
                    all_columns[col] = 1
                if "column" in referenced[t]:
                    referenced[t]["column"].append(col)
                else:
                    referenced[t]["column"] = [col]

    # Check if there is any columns appear in more than one table.
    new_know = {}
    still_unknown = {}
    for colu in unknown:
        all_column_key = [k for k in all_columns]
        col = colu["column"]
        val = colu["value"]
        if col in all_column_key:
            if all_columns[col] == 1:
                new_know[col] = val
            else:
                raise PredictableAmbiguousColumnNameException("a," + col)
        else:
            still_unknown[col] = val
    if len(new_know) == 0:
        return unknown, []

    # Create command
    array_command = []
    for info in referenced:
        table_name = referenced[info]["name"]
        cols = {}
        for col in referenced[info]["column"]:
            if col in new_know:
                cols[col] = new_know[col]
        if len(cols) > 0:
            keys = referenced[info]["key"]
            print(type(keys))
            columns = [k for k in cols]
            command = "INSERT INTO `" + table_name + "` (" + referenced[info]["key"][0]["column"]
            j = 1
            while j < len(keys):
                command += ", " + referenced[info]["key"][j]["column"]
                j += 1
            command += ", " + columns[0]
            j = 1
            while j < len(columns):
                command += ", " + columns[j]
                j += 1
            command += ") VALUE(" + typed_value(referenced[info]["key"][0]["value"])
            j = 1
            while j < len(keys):
                command += ", " + typed_value(referenced[info]["key"][j]["value"])
                j += 1
            command += ", " + typed_value(cols[columns[0]])
            j = 1
            while j < len(columns):
                command += ", " + typed_value(cols[columns[j]])
                j += 1
            command += ");"
            print(command)
            array_command.append(command)
    new_unknown = []
    print(still_unknown)
    unknown_key = [k for k in still_unknown]
    print(unknown_key)
    for k in unknown_key:
        new_unknown.append({"column": k, "value": still_unknown[k]})
    return new_unknown, array_command


def insert_into_table(command, cur):
    try:
        cur.execute(command)
    except Exception as e:
        print(command)
        raise e


def insert_multiple_tables(table, column, value, mysql, db):
    check_table_field(table)
    columns = column.split(",")
    values = value.split(",")

    if len(columns) != len(values):
        raise PredictableNumberOfParameterNotMatchException("columns,values")

    t1, t2, data, t4 = get_metadata_(table, mysql)
    i = 0
    known = []
    unknown = []
    while i < len(columns):
        if check_exist_from_json(columns[i], data, "Field"):
            known.append({"column": columns[i], "value": values[i]})
        else:
            unknown.append({"column": columns[i], "value": values[i]})
        i += 1

    if len(known) == 0:
        raise PredictableException("Please have at least one valid column to fill in the value.")

    con, cur = None, None
    try:
        con = mysql.connection
        cur = con.cursor()
    except Exception as e:
        return 401, None, None, e
    cur.close()

    command_array = []
    if len(unknown) != 0:
        unknown, more_commad = insert_upper_table(table, unknown, known, mysql)
        command_array.extend(more_commad)

    command = "INSERT INTO `" + table + "` (" + known[0]["column"]
    i = 1
    while i < len(known):
        command += ", " + known[i]["column"]
        i += 1
    command += ") VALUE(" + typed_value(known[0]["value"])
    i = 1
    while i < len(known):
        command += ", " + typed_value(known[i]["value"])
        i += 1
    command += ");"
    command_array.append(command)

    if len(unknown) != 0:
        unknown, more_commad = insert_lower_table(table, unknown, known, mysql, db)
        command_array.extend(more_commad)

    if len(unknown) != 0:
        print(unknown)
        raise PredictableException("There is at least one unknown columns in the field \"columns\"")

    con, cur = None, None
    try:
        con = mysql.connection
        cur = con.cursor()
        con.autocommit = False
    except Exception as e:
        return 401, None, None, e

    print(command_array)
    for c in command_array:
        insert_into_table(c, cur)
    con.commit()
    cur.close()
    status = 200
    message = "Data is inserted into table " + table + "."
    return status, message, None, None


import os
import json

from controller.MetadataController import get_foreign_key, get_metadata
from util.ExtractSpecialArray import check_table_field, extract_unique_key, check_exist_from_json, database
from util.QueryHelper import db_query
from controller.PredictableExeption import *


def get_tabledata(table, columns, page, filter, sort_by, mysql):
    command = "SELECT "
    if columns:
        columns = columns.split(",")
        for column in columns:
            command += column + ","
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
    row_headers=[x[0] for x in columns]

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
    command = "UPDATE `" + table + "` "
    command = command + "SET "
    x = 0
    for elem in columns:
        command = command + "`" + elem + "` = " + value[x] + ", "
        x+=1
    command = command[:-2] + " "
    if len(condition) != 0:
        command = command + "Where " + condition + ";"
    else:
        command = command + ";"
    
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

    command = "DELETE FROM `" + table +"` "
    commnad = command + "WHERE `" + condition + "`;"
   
    data, error = db_query(mysql, command)
    if error != None:
        status = 412
        message = "Incorrect Input! " + error
        return status, message, None, error
    else:
        status = 201
        message = "Row is deleted."
        return status, message, None, None


def post_tabledata(table, column, value, mysql):
    con, cur = None, None
    try:
        con = mysql.connection
        cur = con.cursor()
    except Exception as e:
        return 401, None, None, e
    con.close()
    cur.close()

    check_table_field(table)

    columns = column.split(",")
    values = extract_unique_key(value)

    if len(columns) != len(values):
        raise PredictableNumberOfParameterNotMatchException("column,value")

    # Reformat the parameter for next process.
    col_val = {}
    i = 0
    while i < len(columns):
        col_val[columns[i]] = values[i]
        i += 1
    known = {}
    unknown = {}

    # Check if there are any unknown column.
    t1, t2, data, t3 = get_metadata(table, mysql, os.getenv("MYSQL_DB"))
    for col in columns:
        if check_exist_from_json(col, data, "Field") is True:
            known[col] = col_val[col]
        else:
            unknown[col] = col_val[col]

    # con = mysql.connection
    # cur = con.cursor()
    # con.autocommit = False
    # Check if this table has reference to other table.
    t1, t2, data, t3 = get_foreign_key(table, mysql)


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


def insert_referenced(table, col_val, uniques, mysql):
    # This is the base case:
    # If the columns and values are empty, then do nothing.
    if len(col_val) == 0:
        return col_val, []

    array_command = []

    t1, t2, data, t3 = get_foreign_key(table, mysql)

    # Check how many tables are referenced.
    referenced = []
    for info in data:
        if info["target_table"] not in referenced:
            referenced.append(info["target_table"])

    if len(referenced) == 0:
        # If there is no table referenced, then do nothing.
        return col_val, []
    elif len(referenced) == 1:
        # If there is one table referenced.
        t1, t2, array, t3 = get_metadata(referenced[0], mysql, os.getenv("MYSQL_DB"))

        # Reformat the target columns.
        t_col = {}
        for info in data:
            col_s = info["target_column"]
            cols = col_s.split(",")
            real_c = info["column"].split(",")
            j = 0
            while j < len(cols):
                if cols[j] not in t_col:
                    t_col[cols[j]] = real_c[j]

        # Check if all the unknown columns are in this table.
        columns = col_val.keys()
        known = {}
        unknown = {}
        for col in columns:
            if check_exist_from_json(col, array, "Field") is True:
                known[col] = col_val[col]
            else:
                unknown[col] = col_val[col]

        # Check if there are more unknown column, then recursively call this function to deal with it.
        try:
            unknown, more_command = insert_referenced(table, unknown, known, mysql)
            array_command.append(more_command)
        except Exception as e:
            raise e

        # Now, it is time to insert.
        command = "INSERT INTO `" + referenced[0] + "` ("
        second_half = "VALUE("
        fk_cs = t_col.keys()
        for k in fk_cs:
            command += "`" + k + "`, "
            second_half += typed_value(uniques[fk_cs[k]]) + ", "
        for other_column in array:
            one_column = other_column["Field"]
            if one_column in known:
                command += "`" + one_column + "`, ";
                second_half += typed_value(known[one_column]) + ", "
        second_half = second_half[0:-2] + ");"
        command = command[0: -2] + ") " + second_half
        array_command.append(command)
        return unknown, array_command
    else:
        # If there are more than one referenced table.
        # Reformat the data.
        all_info = {}
        all_column = []
        for ref_t in referenced:
            # Pair off the target column and the referencing column.
            col_to_tar = {}
            for info in data:
                if info["target_table"] == ref_t:
                    p_columns = info["column"]
                    p_targets = info["target_column"]
                    j = 0
                    while j < len(p_columns):
                        if p_targets[j] not in col_to_tar:
                            col_to_tar[p_targets[j]] = p_columns[j]
            # Collect other columns.
            t1, t2, tem_data, t3 = get_metadata(ref_t, mysql)
            this_columns = []
            for meta in tem_data:
                if meta["Field"] not in col_to_tar:
                    this_columns.append(meta["Field"])
                if meta["Field"] not in all_column:
                    all_column.append(meta["Field"])
                elif meta["Field"] in col_val:
                    raise PredictableAmbiguousColumnNameException("a," + meta["Field"])
            this_table_info = {"name": ref_t, "targets": col_to_tar, "columns": this_columns}
            all_info[ref_t] = this_table_info

        # Now, divide the col_val into two parts.
        known = {}
        unknown = {}
        for a_column in col_val:
            if a_column in all_column:
                known[a_column] = col_val[a_column]
            else:
                unknown[a_column] = col_val[a_column]

        # Recursively call this function.
        k = 0
        while len(unknown) > 0 and k < len(referenced):
            unknown, some_command = insert_referenced(referenced[k], unknown, known, mysql)
            array_command.append(some_command)

        # Now, create command.
        for t in all_info:
            command = "INSERT INTO `" + t["name"] + "` ("
            second_half = "VALUE("
            for c_for_t in t["targets"]:
                command += "`" + c_for_t + "`, "
                second_half += typed_value(uniques[t["targets"][c_for_t]]) + ", "
            for c_for_c in t["columns"]:
                command += "`" + c_for_c + "`, "
                second_half += typed_value(known[c_for_c]) + ", "
            second_half = second_half[0:-2] + ");"
            command = command[0: -2] + ") " + second_half
            array_command.append(command)
        return unknown, array_command


def insert_referencing(table, col_val, uniques, mysql):
    # The base case:
    if len(col_val) == 0:
        return col_val, []

    array_command = []

    # Get the list of tables that referencing to this table.
    table_list = []
    command = "SELECT REF_NAME FROM information_schema.INNODB_SYS_FOREIGN WHERE REF_NAME == \"" + database
    command += "/" + table + "\";"
    cur = mysql.connection.cursor()
    try:
        cur.execute(command)
        result = cur.fechall()
        for i in result:
            table_list.append(i[0][len(database) + 1:])
        cur.close()
    except Exception as e:
        cur.close()
        raise PredictableHaveNoRightException()

    # Check margin case:
    if len(table_list) == 0:
        return col_val, []

    # Check if key duplicated and gather more information.
    for t in table_list:
        t1, t2, data, t4 = get_metadata(t, mysql, database)


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
        t1, t2, data, t3 = get_metadata(t, mysql)
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


def insert_lower_table(table, unknown, known, mysql):
    array_command = []

    # Get the list of tables that referencing to this table.
    table_list = []
    command = "SELECT REF_NAME FROM information_schema.INNODB_SYS_FOREIGN WHERE REF_NAME == \"" + database
    command += "/" + table + "\";"
    cur = mysql.connection.cursor()
    try:
        cur.execute(command)
        result = cur.fechall()
        for i in result:
            table_list.append(i[0][len(database) + 1:])
        cur.close()
    except Exception as e:
        cur.close()
        raise PredictableHaveNoRightException()

    # Check margin case:
    if len(table_list) == 0:
        return unknown, []


    pass


def insert_into_table(command, cur):
    try:
        cur.execute(command)
    except Exception as e:
        print(command)
        raise e


def insert_multiple_tables(table, column, value, mysql):
    check_table_field(table)
    columns = column.split(",")
    values = value.split(",")

    if len(columns) != len(values):
        raise PredictableNumberOfParameterNotMatchException("columns,values")

    t1, t2, data, t4 = get_metadata(table, mysql)
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
        nknown, more_commad = insert_lower_table(table, unknown, known, mysql)
        command_array.append(more_commad)

    if len(unknown) != 0:
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


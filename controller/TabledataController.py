import os

from controller.MetadataController import get_foreign_key, get_metadata
from util.ExtractSpecialArray import check_table_field, extract_unique_key, check_exist_from_json
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

    result, error = db_query(mysql, command)
    return 200, "Success", result, None


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

    # Try to parse the table variable in order to detect exception.
    # condition = condition.split(",")

    # Now, we have passed all the preconditions for the table creation.
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
        command = command + "`" + elem + "` = " + value[x] + ", "
        x+=1
    command = command[:-2] + " "
    if len(condition) != 0:
        command = command + "Where " + condition + ";"
    else:
        command = command + ";"
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
    cur = mysql.connection.cursor()
    try:
        cur.execute(command)
    except Exception as e:
        cur.close()
        raise e
    cur.close()
    
    status = 200
    data = ""
    message = "Row is deleted."
    error = ""

    return status, message, data, error


def post_tabledata(table, column, value, mysql):
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

    con = mysql.connection
    cur = con.cursor()
    print(command)
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
        if float(int_type) != dec_type:
            return dec_type
        else:
            return int_type
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
            t1, t2, tem_data, t3 = get_metadata(ref_t, mysql, os.getenv("MYSQL_DB"))
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
    command = "SELECT REF_NAME FROM information_schema.INNODB_SYS_FOREIGN WHERE REF_NAME == \"" + os.getenv("MYSQL_DB")
    command += "/" + table + "\";"
    cur = mysql.connection.cursor()
    try:
        cur.execute(command)
        result = cur.fechall()
        for i in result:
            table_list.append(i[0][len(os.getenv("MYSQL_DB")) + 1:])
        cur.close()
    except Exception as e:
        cur.close()
        raise PredictableHaveNoRightException()

    # Check margin case:
    if len(table_list) == 0:
        return col_val, []

    # Check if key duplicated and gather more information.
    for t in table_list:
        t1, t2, data, t4 = get_metadata(t, mysql, os.getenv("MYSQL_DB"))


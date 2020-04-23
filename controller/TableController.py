from controller.PredictableExeption import *
from util.ExtractSpecialArray import extract_unique_key
from util.QueryHelper import db_query


# This function create a table with unique key(s)
def create_table(table, column, unique, mysql):
    # Try to parse the table variable in order to detect exception.
    tables = table.split(",")
    if len(tables) == 0:
        raise PredictableInvalidArgumentException("1")
    elif len(tables) > 1:
        raise PredictableInvalidArgumentException("2")

    # Parse the list of columns from string into array.
    columns = column.split(",")
    # Now, check the duplication in columns.
    for elem in columns:
        if columns.count(elem) > 1:
            raise PredictableDuplicateColumnException(elem)

    # Parse the list of unique keys from string into array.
    # Because a unique key can be composite key with a form of '(c1,c2)',
    # so it is impossible to use the same technique as what we have done for columns.
    uniques = extract_unique_key(unique)

    # Once all the unique keys are parsed, we need to check if all the key are defined in columns.
    i = 0
    while i < len(uniques):
        key = uniques[i]
        # Remember, for composite key, there are multiple columns in it.
        col_in_key = key.split(",")
        j = 0
        while j < len(col_in_key):
            if not col_in_key[j] in columns:
                raise PredictableUnknownKeyException(col_in_key[j])
            j += 1
        i += 1

    # Now, we have passed all the preconditions for the table creation.
    # The next step is to communicate with the database.

    # The first thing to do is to turn of the autocommit variable,
    # and start a new transaction.
    con, cur = None, None
    try:
        con = mysql.connection
        cur = con.cursor()
    except Exception as e:
        return 401, None, None, e
    con.autocommit = False

    # Now, we can start to communicate with the database.
    command = "CREATE TABLE `" + table + "` ( "
    command = command + "auto_generated_id int not null primary key auto_increment"
    for elem in columns:
        command = command + " , `" + elem + "` varchar(200)"
    command = command + ");"
    try:
        cur.execute(command)
    except Exception as e:
        con.rollback()
        cur.close()
        raise e

    # Now, the table has been created, but the unique keys are not added yet.
    # So, there we are going to add the unique keys
    i = 0
    for elem in uniques:
        i += 1
        command = "alter table `" + table + "` add constraint uniquekey_"
        command += str(i) + " unique (" + elem + ");"
        try:
            cur.execute(command)
        except Exception as e:
            con.rollback()
            cur.close()
            raise e

    con.commit()
    con.autocommit = True
    cur.close()
    status = 201
    message = "Table " + table + " is created."
    return status, message, None, None


def delete_table(table_name, mysql):
    result, error = db_query(mysql, "DROP TABLE " + table_name)
    if (error == "FAILED_TO_CONNECT"):
        return 401, None, None, "Please connect to a database using the /connect endpoint."
    elif (error):
        return 400, None, None, error
    else:
        return 200, "Table {} is deleted.".format(table_name), result, None


def update_table(table, columns, operation, mysql):
    if operation != "insert" and operation != "drop":
        return 400, None, None, "Invalid Operation"
    
    command = "ALTER TABLE " + table
    split_columns = columns.split(",")
    for column_name in split_columns:
        if operation == "insert":
            command += " ADD " + column_name + " VARCHAR(200),"
        else:
            command += " DROP COLUMN " + column_name + ","
    command = command[:-1]
    result, error = db_query(mysql, command)

    message = "Successfully inserted the columns: " + columns + "."
    if operation == "drop":
        message = "Successfully dropped the columns: " + columns + "."

    if (error == "FAILED_TO_CONNECT"):
        return 401, None, None, "Please connect to a database using the /connect endpoint."
    elif (error):
        return 400, None, None, error
    else:
        return 200, message, result, None
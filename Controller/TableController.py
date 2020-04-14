from Controller.PredictableExeption import PredictableUnknownKeyException, PredictableInvalidArgumentException, \
    PredictableDuplicateColumnException


# This function create a table with unique key(s)
def create_table_with_unique(table, column, unique, mysql):
    # Create error variable, after each mysql query, check if it is null.
    # If not null, then return error
    error = None

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
    uniques = []
    while len(unique) > 0:
        a = 0
        # Each time, there are two cases:
        # 1. The next key is a single key;
        # 2. The next key is a composite key.
        # We need to handle these two cases separately.
        comma = -1
        prent = -1
        try:
            comma = unique.index(',')
        except ValueError as v:
            comma = -1
        except Exception as e:
            raise e
        try:
            prent = unique.index('(')
        except ValueError as v:
            prent = -1
        except Exception as e:
            raise e

        # When the next key is a composite key...
        if comma > prent >= 0:
            try:
                end = unique.index(')')
            except ValueError as v:
                raise PredictableInvalidArgumentException("3")
            except Exception as e:
                raise e
            prent += 1
            composite_key = unique[prent: end]
            uniques.append(composite_key)
            unique = uniques[end+1:]
        # When the next key is a single key...
        else:
            if comma == 0:
                # If there is an empty key, then skip it.
                unique = unique[1:]
            else:
                # Cut the single key off from the tring and push it into the array.
                single_key = unique[0:comma]
                uniques.append(single_key)
                if comma == -1:
                    # If this is the last key, then empty the string.
                    unique = ""
                else:
                    unique = unique[comma+1:]
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
    con = mysql.connection
    cur = con.cursor()
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
    return {"success": "Table " + table + " is created."}







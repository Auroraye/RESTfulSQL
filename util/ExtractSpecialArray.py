from controller.PredictableExeption import *


def extract_unique_key(u):
    unique = u
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
            unique = uniques[end + 1:]
        # When the next key is a single key...
        else:
            if comma == 0:
                # If there is an empty key, then skip it.
                unique = unique[1:]
            else:
                # Cut the single key off from the string and push it into the array.

                if comma == -1:
                    single_key = unique
                    uniques.append(single_key)
                    # If this is the last key, then empty the string.
                    unique = ""
                else:
                    single_key = unique[0:comma]
                    uniques.append(single_key)
                    unique = unique[comma + 1:]
    return uniques


def check_exist_from_json(key, data, tag):
    for row in data:
        if row[tag] == key:
            return True
    return False


def check_table_field(table):
    # Try to parse the table variable in order to detect exception.
    tables = table.split(",")
    if len(tables) == 0:
        raise PredictableInvalidArgumentException("1")
    elif len(tables) > 1:
        raise PredictableInvalidArgumentException("2")

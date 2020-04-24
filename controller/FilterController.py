import os

from controller.MetadataController import get_metadata, check_exist_from_json, get_metadata_
from controller.PredictableExeption import *
from util.ExtractSpecialArray import check_table_field, extract_unique_key
from util.LFUHelper import LFU_increment


def post_filter(table, column, operator, condition, atype, view, mysql):
    check_table_field(table)
    check_table_field(view)
    columns = column.split(",")
    operators = operator.split(",")
    conditions = extract_unique_key(condition)
    if len(columns) != len(conditions):
        raise PredictableNumberOfParameterNotMatchException("conditions,columns")
    if len(columns) != len(operators):
        raise PredictableNumberOfParameterNotMatchException("columns,operators")
    if len(operators) != len(conditions):
        raise PredictableNumberOfParameterNotMatchException("operators,conditions")
    if len(columns) == 0:
        raise PredictableInvalidArgumentException("10")

    atype = atype.upper()
    if atype not in ["AND", "OR", "XOR"]:
        raise PredictableInvalidArgumentException("9")
    table = table.lower()
    t1, t2, array, t3 = get_metadata_(table, mysql)
    for c in columns:
        if not check_exist_from_json(c, array, "Field"):
            raise PredictableUnknownKeyException(c)
    try:
        con = mysql.connection
        cur = con.cursor()
    except Exception as e:
        return 401, None, None, e
    con.autocommit = False

    command = "CREATE VIEW `" + view + "` AS SELECT * FROM `" + table + "` WHERE " + columns[0]
    if operators[0].startswith("~"):
        command += " NOT " + operators[0][1:] + " " + conditions[0]
    else:
        command += " " + operators[0] + " " + conditions[0]

    i = 1
    while i < len(columns):
        command += " " + atype + " " + columns[i]
        if operators[i].startswith("~"):
            command += " NOT " + operators[i][1:] + " " + conditions[i]
        else:
            command += " " + operators[i] + " " + conditions[i]
        i += 1

    command += ";"

    try:
        cur.execute(command)
        con.commit()
        cur.close()
        status = 200
        message = "View " + view + " has been created."
        LFU_increment(view, mysql)
        return status, message, None, None
    except Exception as e:
        cur.close()
        con.rollback()
        raise e


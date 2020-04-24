import MySQLdb
from MySQLdb._exceptions import OperationalError

from controller.PredictableExeption import PredictableNumberOfParameterNotMatchException
from util.ExtractSpecialArray import check_table_field
from util.LFUHelper import LFU_increment


def post_group_by(table, function, rename, group, view, mysql):
    check_table_field(table)
    check_table_field(view)
    functions = function.split(",")
    renames = rename.split(",")
    if len(functions) != len(renames):
        raise PredictableNumberOfParameterNotMatchException("functions,renames")

    command = "CREATE VIEW `" + view + "` AS (SELECT *"
    i = 0
    while i < len(functions):
        command += ", " + functions[i] + " AS " + renames[i]
        i += 1
    command += " FROM `" + table + "` GROUP BY `" + group + "`);"
    con, cur = None, None
    try:
        con = mysql.connection
        cur = con.cursor()
    except Exception as e:
        return 401, None, None, e
    try:
        cur.execute(command)
        con.commit()
        cur.close()
        status = 200
        message = "View " + view + " has been created."
        LFU_increment(view, mysql)
        return status, message, None, None
    except MySQLdb._exceptions.OperationalError:
        pass
    except Exception as e:
        try:
            con.rollback()
            cur.close()
            raise e
        except MySQLdb._exceptions.OperationalError:
            raise e

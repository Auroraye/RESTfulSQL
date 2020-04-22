from controller.PredictableExeption import PredictableNumberOfParameterNotMatchException
from util.ExtractSpecialArray import check_table_field


def post_group_by(table, function, rename, group, view, mysql):
    check_table_field(table)
    functions = function.split(",")
    renames = rename.split(",")
    if len(functions) != len(renames):
        raise PredictableNumberOfParameterNotMatchException("functions,renames")

    command = "CREATE VIEW `" + view + "` AS SELECT *"
    i = 0
    while i < len(functions):
        command += ", " + functions[i] + " AS " + renames[i]
    command += " FROM `" + table + "` GROUP BY `" + group + "`;"
    con = mysql.connection
    cur = con.cursor()
    try:
        cur.execute(command)
        con.commit()
        cur.close()
        con.close()
    except Exception as e:
        con.rollback()
        cur.close()
        con.close()
        raise e
    status = 200
    message = "View " + view + " has been created."
    return status, message, None, None
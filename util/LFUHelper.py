# Least Frequently Used (LFU)
from util.QueryHelper import db_query

VIEW_LIMIT = 25         # a constant limiting the max number of views for each user


def LFU_increment(view, mysql):
    if LFU_create_view_table(mysql) == 0:
        db_query(mysql, 'INSERT INTO view_count_reserved (view_name, count) VALUES ("{}", 1);'.format(view), None)
    else:
        result_view_list = LFU_get_view_table(mysql)
        for ele in result_view_list:
            if ele[0] == view:
                db_query(mysql, 'UPDATE view_count_reserved SET count = count + 1 WHERE view_name = \'{}\';'.format(view), None)
                return
        if len(result_view_list) == VIEW_LIMIT:
            LFU_envict(mysql, result_view_list)
        db_query(mysql, 'INSERT INTO view_count_reserved (view_name, count) VALUES ("{}", 1);'.format(view), None)


def LFU_envict(mysql, result_view_list):
    least_key = None
    least_count = float('inf')
    for ele in result_view_list:
        if ele[1] < least_count:
            print('count is    ', ele[1])
            least_key = ele[0]
            least_count = ele[1]
    print(least_key)
    if least_key != None:
        db_query(mysql, 'DELETE FROM view_count_reserved WHERE view_name = \'{}\';'.format(least_key), None)


def LFU_reset(mysql):
    db_query(mysql, 'DROP TABLE view_count_reserved;', None)

    
def LFU_delete(mysql, view_name):
    result_view_list = LFU_get_view_table(mysql)
    for ele in result_view_list:
            if ele[0] == view_name:
                db_query(mysql, 'DROP VIEW \'{}\';'.format(view_name))
                return 1
    return 0


def LFU_create_view_table(mysql):
    exist_result, error = db_query(
        mysql, 'show tables like \'view_count_reserved\';', None)
    if len(exist_result) == 0:
        result, error = db_query(
            mysql, 'CREATE TABLE `view_count_reserved`(`view_name` VARCHAR(100) NOT NULL,`count` INT UNSIGNED,PRIMARY KEY (`view_name`));', None)
        return 0            # empty view table return 0
    else:
        return 1            # view table exists return 1


def LFU_get_view_table(mysql):
    result, error = db_query(
        mysql, 'SELECT * FROM view_count_reserved;', None)
    return result


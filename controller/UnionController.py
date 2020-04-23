from util.LFUHelper import *
from util.QueryHelper import db_query


def get_union(mysql, table_name_A, columns_A, table_name_B, columns_B, returned_view_name):
    # check whether table contains in the db
    table_name_A = table_name_A.strip()
    table_name_B = table_name_B.strip()
    A_exist, error = db_query(mysql, 'SHOW TABLES LIKE \'{}\';'.format(table_name_A))
    B_exist, error = db_query(mysql, 'SHOW TABLES LIKE \'{}\';'.format(table_name_B))
    if len(A_exist) == 0 or len(B_exist) == 0:
        message = "Table you entered does not exist. Please check again."
        return 400, message, None, None

    data = []
    # if lists are both empty, select all
    if columns_A == None and columns_B == None:
        data, error = db_query(mysql, 'SELECT * FROM {} UNION SELECT * FROM {};'.format(table_name_A, table_name_B))
        if error != None:
            message = "Some error occurs " + error
            return 400, message, None, error
        if returned_view_name != None:
            message = "Union between two tables is created successfully. New view \'{}\' is saved.".format(
                returned_view_name)
            db_query(mysql, 'CREATE VIEW {} AS SELECT * FROM (SELECT * FROM {} UNION SELECT * FROM {}) AS temp;'.format(returned_view_name, table_name_A, table_name_B))
            LFU_increment(returned_view_name, mysql)
        else:
            message = "Union between two tables is created successfully. No view is saved."
    elif columns_A == None or columns_B == None:
        message = "Number of columns does not match. One of them is None."
        return 402, message, None, None
    else:
        # check the number of column match or not
        col_list_A = columns_A.split(',')
        col_list_B = columns_B.split(',')
        if len(col_list_A) != len(col_list_B):
            message = "Number of columns does not match. A has {} columns and B has {} columns.".format(len(col_list_A),len(col_list_B))
            return 402, message, None, None
        print("jindao else")
        # check whether table contains those cols
        col_str_A = ""
        for index in range(len(col_list_A)):
            current = col_list_A[index].strip()
            if check_exist(current, table_name_A, mysql) == False:
                message = "One of columns you entered does not exist in table A. Please check again."
                return 401, message, None, None
            if index == len(col_list_A) - 1:
                col_str_A = col_str_A + current
            else:
                col_str_A = col_str_A + current + ","

        col_str_B = ""
        for index in range(len(col_list_B)):
            current = col_list_B[index].strip()
            if check_exist(current, table_name_B, mysql) == False:
                message = "One of columns you entered does not exist in table A. Please check again."
                return 401, message, None, None
            if index == len(col_list_B) - 1:
                col_str_B = col_str_B + current
            else:
                col_str_B = col_str_B + current + ","

        data, error = db_query(mysql, 'SELECT {} FROM {} UNION SELECT {} FROM {};'.format(col_str_A, table_name_A, col_str_B, table_name_B))
        if error != None:
            message = "Some error occurs " + error
            return 400, message, None, error
        if returned_view_name != None:
            result, error = db_query(mysql, 'CREATE VIEW {} AS SELECT * FROM (SELECT {} FROM {} UNION SELECT {} FROM {}) AS temp;'.format(
                returned_view_name, col_str_A, table_name_A, col_str_B, table_name_B))
            LFU_increment(returned_view_name, mysql)
            if error != None:
                message = "Some error occurs " + error
            else:
                message = "Union between two tables is created successfully. New view \'{}\' is saved.".format(
                    returned_view_name)
        else:
            message = "Union between two tables is created successfully. No view is saved."

    return 200, message, data, None


def check_exist(col_name, table, mysql):
    is_exist, error = db_query(mysql, 'SHOW COLUMNS FROM `{}` LIKE \'{}\';'.format(table, col_name))
    if len(is_exist) == 0:
        return False
    else:
        return True

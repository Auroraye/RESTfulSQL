
from util.QueryHelper import db_query

def get_union(mysql, table_name_A, col_list_A, table_name_B, col_list_B, returned_view_name):
    # check whether table contains in the db
    table_name_A = table_name_A.strip()
    table_name_B = table_name_B.strip()
    A_exist, error = db_query(mysql, 'SHOW TABLES LIKE \'{}\';'.format(table_name_A), None)
    B_exist, error = db_query(mysql, 'SHOW TABLES LIKE \'{}\';'.format(table_name_B), None)
    if len(A_exist) == 0 or len(B_exist) == 0:
        message = "Table you entered does not exist. Please check again."
        return 400, message, None, None

    data = []
    # if lists are both empty, select all
    if col_list_A == [""] and col_list_B == [""]:
        data, error = db_query(mysql, 'SELECT * FROM {} UNION SELECT * FROM {};'.format(table_name_A, table_name_B), None)
        if error != None:
            message = "Some error occurs " + error
            return 400, message, None, error
        if returned_view_name != "":
            message = "Union between two tables is created successfully. New view \'{}\' is saved.".format(
                returned_view_name)
            db_query(mysql, 'CREATE VIEW {} AS SELECT * FROM (SELECT * FROM {} UNION SELECT * FROM {}) AS temp;'.format(returned_view_name, table_name_A, table_name_B), None)
        else:
            message = "Union between two tables is created successfully. No view is saved."
    else:
        print("jindao else")
        # check whether table contains those cols
        col_str_A = ""
        for index in range(len(col_list_A)):
            current = col_list_A[index].strip()
            if check_exist(current, table_name_A, mysql) == False:
                message = "One of columns you entered does not exist in table A. Please check again."
                return 400, message, None, None
            if index == len(col_list_A) - 1:
                col_str_A = col_str_A + current
            else:
                col_str_A = col_str_A + current + ","

        col_str_B = ""
        for index in range(len(col_list_B)):
            current = col_list_B[index].strip()
            if check_exist(current, table_name_B, mysql) == False:
                message = "One of columns you entered does not exist in table A. Please check again."
                return 400, message, None, None
            if index == len(col_list_B) - 1:
                col_str_B = col_str_B + current
            else:
                col_str_B = col_str_B + current + ","

        data, error = db_query(mysql, 'SELECT {} FROM {} UNION SELECT {} FROM {};'.format(col_str_A, table_name_A, col_str_B, table_name_B), None)
        if error != None:
            message = "Some error occurs " + error
            return 400, message, None, error
        if returned_view_name != "":
            result, error = db_query(mysql, 'CREATE VIEW {} AS SELECT * FROM (SELECT {} FROM {} UNION SELECT {} FROM {}) AS temp;'.format(
                returned_view_name, col_str_A, table_name_A, col_str_B, table_name_B), None)
            if error != None:
                message = "Some error occurs " + error
            else:
                message = "Union between two tables is created successfully. New view \'{}\' is saved.".format(
                    returned_view_name)
        else:
            message = "Union between two tables is created successfully. No view is saved."

    return 200, message, data, None

def check_exist(col_name, table, mysql):
    is_exist, error = db_query(mysql, 'SHOW COLUMNS FROM `{}` LIKE \'{}\';'.format(table, col_name), None)
    if len(is_exist) == 0:
        return False
    else:
        return True

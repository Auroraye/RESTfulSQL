import json
from flask_mysqldb import MySQL
from flask import Flask, request, jsonify
from flask_restplus import Api, Resource, fields, reqparse

from util.Result import *
from util.QueryHelper import *
from util.LFUHelper import *
from controller.MetadataController import *
from controller.PredictableExeption import PredictableException
from controller.TableController import *
from controller.UnionController import *
from controller.MetaController import *
from controller.TabledataController import *
from controller.JoinController import *
from controller.GroupController import post_group_by

flask_app = Flask(__name__)
api = Api(app=flask_app,
          version="1.0",
          title="RESTfulSQL API",
          description="A Restful API Wrapper for MYSQL")

mysql = MySQL(flask_app)

connect_space = api.namespace("connect", description="Connect to a database")
table_space = api.namespace("table", description="Manage tables")
tabledata_space = api.namespace("table/data", description="Manage data records")
metadata_space = api.namespace("metadata", description="Manage metadata")
uniquekey_space = api.namespace("metadata/uniquekey", description="Manage unique key")
foreignkey_space = api.namespace("metadata/foreignkey", description="Manage foreign key")
union_space = api.namespace("union", description="Get a union of two table")
groupby_space = api.namespace("groupby", description="Apply grouping and statistic functions to a table")
join_space = api.namespace("join", description="Get a join of tables")

connect_model = api.model("Connection Model",
                          {"host": fields.String(description="The server name", example="localhost", required=True),
                           "port": fields.Integer(description="The database port", example=3306, required=True),
                           "username": fields.String(description="Username", example="root", required=True),
                           "password": fields.String(description="Password", example="password", required=True),
                           "database": fields.String(description="The database name", example="database",
                                                     required=True)})


# Here starts the connect module.
@connect_space.route("")
class Connect(Resource):
    @api.doc(description="<b>Connect to a database.</b>"
                         + "<br/> <br/> Explanation: <br/> Connect to a local or remote database by passing in all "
                           "the required information. A successful connection is required to use any of the API "
                           "endpoints. "
                         + "<br/> <br/> Assumption: <br/> The user have created a database before using the API. "
                         + "<br/> <br/> Limitation: <br/> Create database is not supported currently.",
             responses={200: "OK", 401: "Failed to connect to the database"})
    @api.expect(connect_model)
    def post(self):
        flask_app.config["MYSQL_HOST"] = request.json["host"]
        flask_app.config["MYSQL_PORT"] = int(request.json["port"])
        flask_app.config["MYSQL_USER"] = request.json["username"]
        flask_app.config["MYSQL_PASSWORD"] = request.json["password"]
        flask_app.config["MYSQL_DB"] = request.json["database"]

        result, error = db_query(mysql, "SHOW STATUS")
        if (error):
            table_space.abort(401, result[8:-2])
        else:
            return return_response(200, "Successfully connected to the database!")


# Here ends the connect module


# Here starts the table module.
table_model = api.model("Table Model",
                        {"name": fields.String(required=True,
                                               description="The name of the table to be created",
                                               example="NewTable"),
                         "columns": fields.String(required=True,
                                                  description="A list of columns in this new table, separate by comma",
                                                  example="col1,col2,col3,col4"),
                         "uniques": fields.String(description="A list of columns that have unique key on it/them, "
                                                              "separate by comma, and composite key is grouped by "
                                                              "parentheses",
                                                  example="col1,(col2,col4)")})

update_table_model = api.model("Table Model - Update", {
    "name": fields.String(description="An existing table name", example="Table1", required=True),
    "columns": fields.String(description="A list of column names", example="Column1, Column2, Column3",
                             required=True),
    "operation": fields.String(description="Operation mode: insert, drop. If the mode is insert, the columns will be "
                                           "added to the table. If the mode is drop, the columns will be removed from "
                                           "the table", enum=['insert', 'drop'], required=True)})


@table_space.route("")
class TableList(Resource):
    @api.doc(description="<b> Create a new table in the database </b> </br> </br> Explanation: </br> Create a new "
                         "table with specified name and list of columns and unique keys(indexes). As default, "
                         "all the columns will be set to varchar(200) as their default type. The list of column names "
                         "is separated by comma, and there <em>SHOULD NOT</em> have space at any point in this list. "
                         "This is the same for the list of indexes, each indexes is separated from others by comma, "
                         "and for composite indexes, parentheses should be used to group all the elements for a "
                         "composite indexes. </br> </br> Assumption: </br> There are some pre-conditions when to use "
                         "this function. <ul> <li> The table name must not exist in the database before, to check "
                         "this assumption, please go to GET Metadata function and query by \'TABLE\' to make sure the "
                         "new table name is not in the result. <li> There should not be any duplicate columns in the "
                         "columns field. <li> All elements appear in uniques field must also appear in columns field. "
                         "</li> </ul> </br> </br> Limitation: </br> For this function, whatever errors occur during the"
                         " executing time, the whole process would be aborted. Hence, a very small mistake on input "
                         "can cause the whole function to fail. This can make sure the schema fits the users' need, "
                         "but it causes some inconvenience.",
             responses={201: "Created", 400: "Bad Request", 401: "Unauthorized access", 412: "Invalid arguments"})
    @api.expect(table_model)
    @api.param("name",
               description="The name of the new table, and this name must not exist in database before this operation.",
               type="string")
    @api.param("columns",
               description="A list of columns to be created in the new table, and each columns need to be separated "
                           "by comma. There should not be duplicate columns nor space between columns.",
               type="string")
    @api.param("uniques",
               description="A list of indexes to be added to this new table, and each index is separated by comma. "
                           "For composite index, the elements of the same key should be grouped by parentheses. This "
                           "is an optional parameter.",
               type="string")
    def post(self):
        try:
            table = request.json["name"]
            column = request.json["columns"]
            unique = request.json["uniques"]
            status, message, data, error = create_table(table, column, unique, mysql)
            if status == 401:
                table_space.abort(status, error)
            return {"message": message}, status
        except PredictableException as e:
            table_space.abort(e.get_status(), e.handle_me())
        except Exception as e:
            table_space.abort(400, e)

    @api.doc(description="<b>Insert or remove columns from an existing table.</b>"
                         + "<br/> <br/> Explanation: <br/> Insert or remove table columns by specifying the column "
                           "names in a comma separated list. The data type of the new insert column is VARCHAR(200) "
                           "by default. "
                         + "<br/> <br/> Assumption: <br/> The table must exist in the database. To insert an column, "
                           "the column name does not exist in the table. To remove an column, the column name exist "
                           "in the table. "
                         + "<br/> <br/> Limitation: <br/> The default data type is VARCHAR(200), but the data type "
                           "can be changed using the UPDATE /metadata endpoint.",
             responses={200: "OK", 400: "Invalid Operation", 401: "Unauthorized access"})
    @api.expect(update_table_model)
    def put(self):
        table = request.json["name"]
        columns = request.json["columns"]
        operation = request.json["operation"].lower()
        status, message, data, error = update_table(table, columns, operation, mysql)
        if error:
            table_space.abort(status, error)
        return return_response(status, message)


@table_space.route("/<string:table_name>")
class Table(Resource):
    @api.doc(description="<b>Delete an existing table from the database.</b>"
                         + "<br/> <br/> Explanation: <br/> Delete all the data inside of an existing table and remove "
                           "the table itself. "
                         + "<br/> <br/> Assumption: <br/> The table is already exist in the database.",
             params={"table_name": "An existing table name."},
             responses={200: "OK", 400: "The table does not exist in the database", 401: "Unauthorized access"})
    def delete(self, table_name):
        status, message, data, error = delete_table(table_name, mysql)
        if (error):
            table_space.abort(status, error)
        return return_response(status, message)


# Here ends the table module


# Here starts the table data module
tabledata_model = api.model("Tabledata Model",
                            {"name": fields.String(required=True),
                             "columns": fields.String(required=True),
                             "values": fields.String(required=True),
                             "conditions": fields.String()})
insertdata_model = api.model("Insert Data Model",
                             {"name": fields.String(required=True,
                                                    description="The table to insert data",
                                                    example="Table1"),
                              "columns": fields.String(required=True,
                                                       description="The columns to insert data, and can also specify "
                                                                   "the order of values; each column is separated by "
                                                                   "comma",
                                                       example="col1,col2,col3,col4"),
                              "values": fields.String(required=True,
                                                      description="A list of values, and each value is corresponding "
                                                                  "to a column in the columns field in sequential "
                                                                  "order; each value is separated by comma",
                                                      example="val1,val2,val3,val4")})


@tabledata_space.route("")
class TabledataList(Resource):
    @api.doc(description="<b>Get the data from an exisiting table. All the parameters are deatiled below.</b>"
                         + "<br/> <br/> Explanation: <br/> Get the data from an exisiting table in the database. "
                         + "<br/> <br/> Assumption: <br/> The table exists in the database."
                         + "<br/> <br/> Limitation: <br/> This operation doesn't support complex aggregation such as "
                           "sort, avg, min, and max. Please check POST /groupby for advanced aggregation.")
    @api.param('sort_by',
               description='Sort the result set in ascending or descending order. The sort_by keyword sorts the '
                           'records in ascending order by default. To sort the records in descending order, '
                           'use the DESC keyword. An example: column1 ASEC, column2 DESC',
               type='string')
    @api.param('filter',
               description='Extract only those records that fulfill the filter condition. It supports opeators: =, >, '
                           '<, >=, <=, !=, BETWEEN, LIKE, and IN. It can be combined with AND, OR, and NOT operators. '
                           'An example: column1 = 1 OR column2 = 2',
               type='string')
    @api.param('page',
               description='Each page returns 250 rows. Setting the page number can retrieve more data and the '
                           'default page is 1.',
               type='integer')
    @api.param('columns', description='Specify the column to retrieve. All columns is returned by default.',
               type='string')
    @api.param('name', description='An exisiting table name.', type='string', required=True)
    @api.doc(responses={200: "OK", 400: "Table does not exist in the database", 401: "Unauthorized access"})
    def get(self):
        name = request.args["name"]
        columns = request.args["columns"] if "columns" in request.args else None
        page = request.args["page"] if "page" in request.args else 1
        filter = request.args["filter"] if "filter" in request.args else None
        sort_by = request.args["sort_by"] if "sort_by" in request.args else None
        status, message, data, error = get_tabledata(name, columns, page, filter, sort_by, mysql)
        if (error):
            table_space.abort(400, error)
            return error, 400
        else:
            return return_response(status, message, data)

    @api.doc(responses={200: "OK", 400: "Invalid Argument"})
    @api.expect(tabledata_model)
    def put(self):
        try:
            table = request.json["name"]
            column = request.json["columns"]
            value = request.json["values"]
            conditions = request.json["conditions"]
            status, message, data, error = update_tabledata(table, column, value, conditions, mysql)
            return {"message": message}, status
        except PredictableException as e:
            table_space.abort(
                500, e.__doc__, status=e.hangdle_me(), statusCode="300")
        except Exception as e:
            raise e

    @api.doc(description="</b> The vanilla version of this method supports insert one record into a single table. "
                         "</b> </br> </br> Explanation: </br> This function adds a new record(row) into a specified "
                         "table. </br> </br> Assumption: </br> There are some pre-condition of this function. The "
                         "first requirement is that the name must exist in the database. Moreover, all the specified "
                         "columns must in that table, and there should not be any duplicate columns in the parameter. "
                         "The length of the columns and the length of the values must match. </br> </br> Limitation: "
                         "</br> The advanced version has not yet completed.",
             responses={201: "Created", 400: "Bad Request", 401: "Unauthorized access", 412: "Invalid arguments"})
    @api.param("name",
               description="The name of table to insert new data",
               type="string")
    @api.param("columns",
               description="A list of columns to add new data, this also specify the order of the values",
               type="string")
    @api.param("values",
               description="A list of values to be inserted into the table, and it has one-to-one correspondence with "
                           "columns.",
               type="string")
    @api.expect(insertdata_model)
    def post(self):
        try:
            table = request.json["name"]
            column = request.json["columns"]
            value = request.json["values"]
            status, message, data, error = vanilla_post_tabledata(table, column, value, mysql)
            if status == 401:
                table_space.abort(status, error)
            return {"message": message}, status
        except PredictableException as e:
            table_space.abort(e.get_status(), e.handle_me())
        except Exception as e:
            table_space.abort(400, e)


@tabledata_space.route("/<string:table_name>")
class Tabledata(Resource):
    @api.doc(responses={200: 'OK'})
    def delete(self, table_name):
        condition = request.json["condition"]
        status, message, data, error = delete_tabledata(table_name, condition, mysql)
        return {"message": message}, status


# Here ends the table data module


# Here starts the meta data module.
column_model = api.model("Column Model",
                         {"name": fields.String(required=True,
                                                description="The name of the table to change the metadata",
                                                example="Table1"),
                          "columns": fields.String(required=True,
                                                   description="A list of columns in the table to change the "
                                                               "metadata(setting), and this parameter allow "
                                                               "duplication as long as there are same number but "
                                                               "different type of operations apply on that column",
                                                   example="col1,col2,col3,col3"),
                          "types": fields.String(required=True,
                                                 description="A list of operation to apply the column specified "
                                                             "above, and this is an enum, the valid values are: "
                                                             "default-to change default value of the column; type-to "
                                                             "change the data type of the column; nullable-to specify "
                                                             "if the column allow null value",
                                                 example="default, type, type, nullable"),
                          "values": fields.String(required=True,
                                                  description="This is the list about what to do with each operation, "
                                                              "for default, there is not requirement of value; for "
                                                              "type operation, the valid values are int, float, "
                                                              "double, decimal, date, string, char, and varchar; for "
                                                              "nullable type, value yes, true and 1 are for allowing "
                                                              "null value, and value no, false and 0 are for the "
                                                              "opposite",
                                                  example="something,int,varchar(100),yes")})


@metadata_space.route("")
class MetadataList(Resource):
    @api.doc(description="<b> Change the setting of columns in a table </b> </br> </br> Explanation: </br> This "
                         "function can change the metadata of a table, in which the settings of the columns in that "
                         "table. These settings include default value, data type, and nullability. To change the "
                         "default value, the operation is \'default\', and the valid value for this operation has no "
                         "requirement as long as the database does not send error. To change the data type, "
                         "the operation is \'type\', and the valid values are int(int), float, double, "
                         "decimal(decimal), date(date), string, varchar(varchar), and char(char). To change the "
                         "nullability, the operation is \'nullable\', and the valid values are yes, true, "
                         "and 1 for nullable, and no, false, and 0 for not nullable. As there are three possible "
                         "operations on a single column, therefore, in the columns parameter, the same columns can "
                         "appear as many as three times, and the requirement is that this column must have the same "
                         "number of different operation, otherwise, there would be error. </br> </br> Assumption: "
                         "</br> Most importantly, the table must exists, and all the columns must be defined in the "
                         "table. Moreover, the length of columns, operations, and values must be all equal. The "
                         "arguments for the operation and value parameter must be a valid argument. </br> </br> "
                         "Limitation: </br> This function only support very limited data type, namely, int, decimal, "
                         "date, char(), and varchar().",
             responses={201: "Created", 400: "Bad Request", 401: "Unauthorized access", 412: "Invalid arguments"})
    @api.param("name",
               description="The table to change its metadata.",
               type="string")
    @api.param("columns",
               description="A list of columns in the table to change the  metadata(setting), and this parameter allow "
                           "duplication as long as there are same number but different type of operations apply on "
                           "that column.",
               type="string")
    @api.param("operations",
               description="A list of operation to apply the column specified above, and this is an enum, the valid "
                           "values are: default-to change default value of the column; type-to change the data type "
                           "of the column; nullable-to specify if the column allow null value.",
               type="string")
    @api.param("values",
               description="This is the list about what to do with each operation,  for default, there is not "
                           "requirement of value; for type operation, the valid values are int, float, double, "
                           "decimal, date, string, char, and varchar; for nullable type, value yes, true and 1 are "
                           "for allowing null value, and value no, false and 0 are for the opposite.",
               type="string")
    @api.expect(column_model)
    def post(self):
        name = request.json["name"]
        column = request.json['columns']
        kind = request.json['types']
        value = request.json['values']
        try:
            status, message, data, error = update_column(name, column, kind, value, mysql)
            if status == 401:
                table_space.abort(status, error)
            return organize_return(status, message, data, error)
        except PredictableException as e:
            table_space.abort(e.get_status(), e.handle_me())
        except Exception as e:
            table_space.abort(400, e)


@metadata_space.route("")
class Metadata(Resource):
    @api.doc(description="<b>Get the metadata.</b>"
                         + "<br/> <br/> Explanation: <br/> Get the metadata of the database or metadata of certain "
                           "table. "
                         + "<br/> <br/> Assumption: <br/> The table exists in the database.")
    @api.param('table_name',
               description='Enter \'TABLE\' to get a list of tables in database; Enter \'VIEW\' to get a list of '
                           'views in the database; Enter an existing table name to get columns\' information for that '
                           'table.',
               type='string')
    @api.doc(responses={200: "OK", 400: "Table does not exist in the database", 401: "Unauthorized access"})
    def get(self):
        table_name = request.args["table_name"]
        status, message, data, error = get_metadata(table_name, mysql, flask_app.config['MYSQL_DB'])
        return return_response(status, message, data, error)


uniquekey_model = api.model("Unique Key Model - Post",
                            {"name": fields.String(required=True,
                                                   description="The name of table to modify",
                                                   example="Table1"),
                             "keys": fields.String(required=True,
                                                   description="A list of columns to add unique keys(indexes), "
                                                               "and comma is used to separate each column, "
                                                               "and parentheses is used to group composite key",
                                                   example="col1,(col3,col2)"),
                             "key_names": fields.String(required=True,
                                                        description="A list of names for the new keys",
                                                        example="index1,index2")})
key_delete = api.model("Key Model - Delete",
                       {"name": fields.String(required=True,
                                              description="The name of table to modify",
                                              example="Table1"),
                        "key_names": fields.String(required=True,
                                                   description="A list of key names to drop from the table",
                                                   example="key1,key2")})


@uniquekey_space.route("")
class UniqueKey(Resource):
    @api.doc(description="<b> Add a new index to the table </b> </br> </br> Explanation: </br> This function add new "
                         "unique keys(indexes) to the specified table. The key field is a list of columns to add new "
                         "index, and the field key_name is a list of the names to these new indexes. </br> </br> "
                         "Assumption: </br> The table must exist; the columns must be defined; the key names must be "
                         "new; the length of keys and key_names must match. Any one of the requirements fails will "
                         "cause the fail of the whole function. For composite key, all elements in a parentheses are "
                         "count for one in term of matching the length between the keys parameter and the key_names "
                         "parameter. </br> </br> Limitation: </br> ^_^",
             responses={201: "Created", 400: "Bad Request", 401: "Unauthorized access", 412: "Invalid arguments"})
    @api.param("name",
               description="The name of table to modify.",
               type="string")
    @api.param("keys",
               description="A list of columns to add unique keys(indexes), and comma is used to separate each column, "
                           "and parentheses is used to group composite key",
               type="string")
    @api.param("key_names",
               description="A list of names for the new keys.",
               type="string")
    @api.expect(uniquekey_model)
    def post(self):
        table = request.json["name"]
        key = request.json["keys"]
        name = request.json["key_names"]
        try:
            status, message, data, error = post_unique_key(table, key, name, mysql)
            if status == 401:
                table_space.abort(status, error)
            return organize_return(status, message, data, error)
        except PredictableException as e:
            table_space.abort(e.get_status(), e.handle_me())
        except Exception as e:
            table_space.abort(400, e)

    @api.doc(description="<b> Delete unique keys(indexes) from a table </b> </br> </br> Explanation: </br> This "
                         "function drops indexes from a specified table. </br> </br> Assumption: </br> The table must "
                         "exist, and the key names must be defined in that table. </br> </br> Limitation: </br> ^_^",
             responses={201: "Created", 400: "Bad Request", 401: "Unauthorized access", 412: "Invalid arguments"})
    @api.param("name",
               description="The name of table to modify.",
               type="string")
    @api.param("key_names",
               description="A list of unique key(index) names to drop from the specified table.",
               type="string")
    @api.expect(key_delete)
    def delete(self):
        table = request.json["name"]
        name = request.json["key_names"]
        try:
            status, message, data, error = delete_unique_key(table, name, mysql)
            if status == 401:
                table_space.abort(status, error)
            return organize_return(status, message, data, error)
        except PredictableException as e:
            table_space.abort(e.get_status(), e.handle_me())
        except Exception as e:
            table_space.abort(400, e)


@api.doc(description="<b> Get a list of unique keys(indexes) of a table </b> </br> </br> Explanation: </br> This "
                     "function returns a list of unique keys(indexes) that is defined in the specified table. If "
                     "there is no index on that table, then it returns null value. </br> </br> Assumption: </br> The "
                     "table must exist in the database. </br> </br> Limitation: </br> ^_^",
         responses={201: "Created", 400: "Bad Request", 401: "Unauthorized access", 412: "Invalid arguments"})
@api.param("table_name",
           description="The table to be queried.",
           type="string")
@uniquekey_space.route("/<string:table_name>")
class UniqueKeyList(Resource):
    def get(self, table_name):
        try:
            status, message, data, error = get_unique_key(table_name, mysql)
            if status == 401:
                table_space.abort(status, error)
            return organize_return_with_data(status, message, data, error)
        except PredictableException as e:
            table_space.abort(e.get_status(), e.handle_me())
        except Exception as e:
            table_space.abort(400, e)


foreignkey_model = api.model("Unique Key Model",
                             {"name": fields.String(required=True,
                                                    description="The name of table to modify",
                                                    example="Table1"),
                              "keys": fields.String(required=True,
                                                    description="A list of columns to add foreign keys(references), "
                                                                "and comma is used to separate each column",
                                                    example="col1,col2"),
                              "targets": fields.String(required=True,
                                                       description="A list of columns that the keys reference to, "
                                                                   "and the format is \'tablename.columnname\'",
                                                       example="Table2.col1,Table3.col2"),
                              "key_names": fields.String(required=True,
                                                         description="A list of names for the new keys",
                                                         example="reference1,reference2")})


@foreignkey_space.route("")
class ForeignKey(Resource):
    @api.doc(description="<b> Add a new index to the table </b> </br> </br> Explanation: </br> This function add new "
                         "foreign keys(references) to the specified table. The key field is a list of columns to add "
                         "new ireference, and the field key_name is a list of the names to these new indexes. The "
                         "field targets is the list of columns that the keys reference to. </br> </br> "
                         "Assumption: </br> The table must exist; the columns must be defined; the key names must be "
                         "new; the length of keys, targets and key_names must match; the targets value must in the "
                         "correct format and both the table name and the column name must be defined. Any one of the "
                         "requirements fails will cause the fail of the whole function.</br> </br> Limitation: </br> "
                         "This function does not support add composit freign key(reference) now.",
             responses={201: "Created", 400: "Bad Request", 401: "Unauthorized access", 412: "Invalid arguments"})
    @api.param("name",
               description="The name of table to modify.",
               type="string")
    @api.param("keys",
               description="A list of columns to add unique keys(indexes), and comma is used to separate each column",
               type="string")
    @api.param("targets",
               description="A list of columns that the keys reference to, and the format is \'tablename.columnname\'",
               type="string")
    @api.param("key_names",
               description="A list of names for the new keys.",
               type="string")
    @api.expect(foreignkey_model)
    def post(self):
        table = request.json["name"]
        key = request.json["keys"]
        target = request.json["targets"]
        name = request.json["key_names"]
        try:
            status, message, data, error = post_foreign_key(table, key, target, name, mysql)
            if status == 401:
                table_space.abort(status, error)
            return organize_return(status, message, data, error)
        except PredictableException as e:
            table_space.abort(e.get_status(), e.handle_me())
        except Exception as e:
            table_space.abort(400, e)

    @api.expect(key_delete)
    def delete(self):
        table = request.json["name"]
        name = request.json["key_names"]
        status, message, data, error = delete_foreign_key(table, name, mysql)
        return organize_return(status, message, data, error)


@foreignkey_space.route("/<string:table_name>")
class UniqueKeyList(Resource):
    def get(self, table_name):
        status, message, data, error = get_foreign_key(table_name, mysql)
        return organize_return_with_data(status, message, data, error)


# Here ends the metadata module


@union_space.route("")
class Union(Resource):
    @api.doc(description="<b>Union two existing tables from the database.</b>"
                         + "<br/> <br/> Explanation: <br/> Check whether input tables and columns are valid and then "
                           "union selected columns. "
                         + "<br/> <br/> Assumption: <br/> If leave 'columns_A' and 'columns_B' blank, it will "
                           "automatically select ALL from two tables and union. The number of columns in these two "
                           "field mush match.")
    @api.param('returned_view_name', description='Name the view if you want to save the result as a view.',
               type='string')
    @api.param('columns_B',
               description='Specify the column to retrieve from table B and separate each column name by comma.  '
                           'Select ALL if leave it blank',
               type='string')
    @api.param('table_name_B', description='An exisiting table name.', type='string', required=True)
    @api.param('columns_A',
               description='Specify the column to retrieve from table A and separate each column name by comma. '
                           'Select ALL if leave it blank',
               type='string')
    @api.param('table_name_A', description='An existing table name.', type='string', required=True)
    @api.doc(
        responses={200: "OK", 400: "Table does not exist in the database", 401: "Column does not exist in the table",
                   402: "Number of columns does not match"})
    def get(self):
        table_name_A = request.args["table_name_A"]
        columns_A = request.args["columns_A"] if "columns_A" in request.args else None
        table_name_B = request.args["table_name_B"]
        columns_B = request.args["columns_B"] if "columns_B" in request.args else None
        returned_view_name = request.args["returned_view_name"] if "returned_view_name" in request.args else None
        status, message, data, error = get_union(mysql, table_name_A, columns_A, table_name_B, columns_B,
                                                 returned_view_name)
        return return_response(status, message, data, error)


join_model = api.model("Join Model",
                       {"tables": fields.String(required=True),
                        "columns": fields.String(required=True),
                        "joinType": fields.String(required=True),
                        "match": fields.String(required=True),
                        "returned_view_name": fields.String})

group_model = api.model("Group Model",
                        {"name": fields.String(required=True),
                         "functions": fields.String,
                         "rename": fields.String(required=True),
                         "group_by": fields.String,
                         "view_name": fields.String})


@groupby_space.route("")
class GroupBy(Resource):
    @api.expect(group_model)
    def post(self):
        table = request.json["name"]
        function = request.json["functions"]
        new_name = request.json["rename"]
        groupby = request.json["group_by"]
        view = request.json["view_name"]

        try:
            status, message, data, error = post_group_by(table, function, new_name, groupby, view, mysql)
            return organize_return(status, message, data, error)
        except PredictableException as e:
            return e.handle_me()
        except Exception as e:
            raise e


@join_space.route("")
class Join(Resource):
    @api.expect(join_model)
    def post(self):
        tables = request.json["tables"]
        columns = request.json["columns"]
        jointype = request.json["joinType"]
        match = request.json["match"]
        returned_view_name = request.json["returned_view_name"]

        status, message, data, error = get_join(mysql, tables, columns, jointype, match, returned_view_name)
        return organize_return(status, message, data, error)

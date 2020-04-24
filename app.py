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

# flask_app = Flask(__name__)
# api = Api(app=flask_app,
#           version="1.0",
#           title="RESTfulSQL API",
#           description="A Restful API Wrapper for MYSQL")
#
# mysql = MySQL(flask_app)

# Import env variable
import os
from dotenv import load_dotenv

load_dotenv()

flask_app = Flask(__name__)
api = Api(app=flask_app,
          version="1.0",
          title="RESTfulSQL API",
          description="A Restful API Wrapper for MYSQL")

flask_app.config["MYSQL_HOST"] = os.getenv("MYSQL_HOST")
flask_app.config["MYSQL_PORT"] = 3306
flask_app.config["MYSQL_USER"] = os.getenv("MYSQL_USER")
flask_app.config["MYSQL_PASSWORD"] = os.getenv("MYSQL_PASSWORD")
flask_app.config["MYSQL_DB"] = os.getenv("MYSQL_DB")

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
                         "database": fields.String(description="The database name", example="database", required=True)})

# Here starts the connect module.
@connect_space.route("")
class Connect(Resource):
    @api.doc(description="<b>Connect to a database.</b>"
        + "<br/> <br/> Explanation: <br/> Connect to a local or remote database by passing in all the required information. A successful connection is required to use any of the API endpoints."
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
                        {"name": fields.String(required=True),
                         "columns": fields.String(required=True),
                         "uniques": fields.String()})

update_table_model = api.model("Table Model - Update", {
    "name": fields.String(description="An exisiting table name", example="Table1", required=True),
    "columns": fields.String(description="A list of column names", example="Column1, Column2, Column3",
                             required=True),
    "operation": fields.String(description="Operation mode: insert, drop. If the mode is insert, the columns will be added to the table. If the mode is drop, the columns will be removed from the table", enum=['insert', 'drop'], required=True)})


@table_space.route("")
class TableList(Resource):
    @api.doc(responses={200: "OK", 400: "Invalid Argument", 500: "Mapping Key Error"})
    @api.expect(table_model)
    def post(self):
        try:
            table = request.json["name"]
            column = request.json["columns"]
            unique = request.json["uniques"]
            status, message, data, error = create_table(table, column, unique, mysql)
            return {"message": message}, status
        except PredictableException as e:
            table_space.abort(
                500, e.__doc__, status=e.handle_me(), statusCode="300")
        except Exception as e:
            raise e

    @api.doc(description="<b>Insert or remove columns from an exisiting table.</b>"
        + "<br/> <br/> Explanation: <br/> Insert or remove table columns by specifying the column names in a comma separated list. The data type of the new insert column is VARCHAR(200) by default."
        + "<br/> <br/> Assumption: <br/> The table must exist in the database. To insert an column, the column name does not exist in the table. To remove an column, the column name exist in the table."
        + "<br/> <br/> Limitation: <br/> The default data type is VARCHAR(200), but the data type can be changed using the UPDATE /metadata endpoint.",
        responses={200: "OK", 400: "Invalid Operation", 401: "Unauthorized access"})
    @api.expect(update_table_model)
    def put(self):
        table = request.json["name"]
        columns = request.json["columns"]
        operation = request.json["operation"].lower()
        status, message, data, error = update_table(table, columns, operation, mysql)
        if (error):
            table_space.abort(status, error)
        return return_response(status, message)


@table_space.route("/<string:table_name>")
class Table(Resource):
    @api.doc(description="<b>Delete an existing table from the database.</b>"
        + "<br/> <br/> Explanation: <br/> Delete all the data inside of an existing table and remove the table itself."
        + "<br/> <br/> Assumption: <br/> The table is already exist in the database.",
        params={"table_name": "An existing table name."},
        responses={200: "OK", 400: "The table does not exist in the database", 401: "Unauthorized access"})
    def delete(self, table_name):
        status, message, data, error = delete_table(table_name, mysql)
        if error:
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
                             {"name": fields.String(required=True),
                              "columns": fields.String(required=True),
                              "values": fields.String(required=True)})


@tabledata_space.route("")
class TabledataList(Resource):
    @api.doc(description="<b>Get the data from an exisiting table. All the parameters are deatiled below.</b>"
        + "<br/> <br/> Explanation: <br/> Get the data from an exisiting table in the database. "
        + "<br/> <br/> Assumption: <br/> The table exists in the database."
        + "<br/> <br/> Limitation: <br/> This operation doesn't support complex aggregation such as sort, avg, min, and max. Please check POST /groupby for advanced aggregation.")
    @api.param('sort_by', description='Sort the result set in ascending or descending order. The sort_by keyword sorts the records in ascending order by default. To sort the records in descending order, use the DESC keyword. An example: column1 ASEC, column2 DESC', type='string')
    @api.param('filter', description='Extract only those records that fulfill the filter condition. It supports opeators: =, >, <, >=, <=, !=, BETWEEN, LIKE, and IN. It can be combined with AND, OR, and NOT operators. An example: column1 = 1 OR column2 = 2', type='string')
    @api.param('page', description='Each page returns 250 rows. Setting the page number can retrieve more data and the default page is 1.', type='integer')
    @api.param('columns', description='Specify the column to retrieve. All columns is returned by default.', type='string')
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

    @api.expect(insertdata_model)
    def post(self):
        try:
            table = request.json["name"]
            column = request.json["columns"]
            value = request.json["values"]
            status, message, data, error = vanilla_post_tabledata(table, column, value, mysql)
            return {"message": message}, status
        except PredictableException as e:
            table_space.abort(
                500, e.__doc__, status=e.hangdle_me(), statusCode="300")
        except Exception as e:
            raise e


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
                         {"name": fields.String(required=True),
                          "columns": fields.String(required=True),
                          "types": fields.String(required=True),
                          "values": fields.String(required=True)})


@metadata_space.route("")
class MetadataList(Resource):
    @api.expect(column_model)
    def post(self):
        name = request.json["name"]
        column = request.json['columns']
        kind = request.json['types']
        value = request.json['values']
        status, message, data, error = update_column(name, column, kind, value, mysql)
        return organize_return(status, message, data, error)


@metadata_space.route("")
class Metadata(Resource):
    @api.doc(description="<b>Get the metadata.</b>"
                         + "<br/> <br/> Explanation: <br/> Get the metadata of the database or metadata of certain table."
                         + "<br/> <br/> Assumption: <br/> The table exists in the database.")
    @api.param('table_name', description='Enter \'TABLE\' to get a list of tables in database; Enter \'VIEW\' to get a list of views in the database; Enter an existing table name to get columns\' information for that table.',
               type='string')
    @api.doc(responses={200: "OK", 400: "Table does not exist in the database", 401: "Unauthorized access"})
    def get(self):
        table_name = request.args["table_name"]
        status, message, data, error = get_metadata(table_name, mysql, flask_app.config['MYSQL_DB'])
        return return_response(status, message, data, error)


uniquekey_model = api.model("Unique Key Model - Post",
                            {"name": fields.String(required=True),
                             "keys": fields.String(required=True),
                             "key_names": fields.String(required=True)})
key_delete = api.model("Unique Key Model - Delete",
                       {"name": fields.String(required=True),
                        "key_names": fields.String(required=True)})


@uniquekey_space.route("")
class UniqueKey(Resource):
    @api.expect(uniquekey_model)
    def post(self):
        table = request.json["name"]
        key = request.json["keys"]
        name = request.json["key_names"]
        status, message, data, error = post_unique_key(table, key, name, mysql)
        return organize_return(status, message, data, error)

    @api.expect(key_delete)
    def delete(self):
        table = request.json["name"]
        name = request.json["key_names"]
        status, message, data, error = delete_unique_key(table, name, mysql)
        return organize_return(status, message, data, error)


@uniquekey_space.route("/<string:table_name>")
class UniqueKeyList(Resource):
    def get(self, table_name):
        status, message, data, error = get_unique_key(table_name, mysql)
        return organize_return_with_data(status, message, data, error)


foreignkey_model = api.model("Unique Key Model",
                             {"name": fields.String(required=True),
                              "keys": fields.String(required=True),
                              "targets": fields.String(required=True, description="Format is 'TableName.ColumnName'."),
                              "key_names": fields.String(required=True)})


@foreignkey_space.route("")
class ForeignKey(Resource):
    @api.expect(foreignkey_model)
    def post(self):
        table = request.json["name"]
        key = request.json["keys"]
        target = request.json["targets"]
        name = request.json["key_names"]
        status, message, data, error = post_foreign_key(table, key, target, name, mysql)
        return organize_return(status, message, data, error)

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
                         + "<br/> <br/> Explanation: <br/> Check whether input tables and columns are valid and then union selected columns."
                         + "<br/> <br/> Assumption: <br/> If leave 'columns_A' and 'columns_B' blank, it will automatically select ALL from two tables and union. The number of columns in these two field mush match.")
    @api.param('returned_view_name', description='Name the view if you want to save the result as a view.', type='string')
    @api.param('columns_B',
               description='Specify the column to retrieve from table B and separate each column name by comma.  Select ALL if leave it blank',
               type='string')
    @api.param('table_name_B', description='An exisiting table name.', type='string', required=True)
    @api.param('columns_A', description='Specify the column to retrieve from table A and separate each column name by comma. Select ALL if leave it blank', type='string')
    @api.param('table_name_A', description='An existing table name.', type='string', required=True)
    @api.doc(responses={200: "OK", 400: "Table does not exist in the database", 401: "Column does not exist in the table", 402: "Number of columns does not match"})
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

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

table_space = api.namespace("table", description="Manage tables")
tabledata_space = api.namespace("table/data", description="Manage data records")
metadata_space = api.namespace("metadata", description="Manage metadata")
uniquekey_space = api.namespace("metadata/uniquekey", description="Manage unique key")
foreignkey_space = api.namespace("metadata/foreignkey", description="Manage foreign key")
union_space = api.namespace("union", description="get a union of two table")
join_space = api.namespace("join", description="get a join of tables")


# Here starts the table module.
table_model = api.model("Table Model",
                        {"name": fields.String(required=True),
                         "columns": fields.String(required=True),
                         "uniques": fields.String()})

update_table_model = api.model("Table Model - Update",{
                        "name": fields.String(description="Table name", example="Table1", required=True),
                        "columns": fields.String(description="Column name in comma separated list", example="Column1, Column2, Column3",required=True),
                        "operation": fields.String(description="Operation on the columns", enum=['insert', 'drop'], required=True)})

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
    
    @api.doc(description="Alter table columns", responses={200: "OK", 400: "Invalid Operation"})
    @api.expect(update_table_model)
    def put(self):
        table = request.json["name"]
        columns = request.json["columns"]
        operation = request.json["operation"]
        status, message, data, error = update_table(table, columns, operation, mysql)
        if (error):
            table_space.abort(status, error)
        return return_response(status, message)


@table_space.route("/<string:table_name>")
class Table(Resource):
    @api.doc(params={"table_name": "Table name"}, description="Delete table", responses={200: "OK"})
    def delete(self, table_name):
        status, message, data, error = delete_table(table_name, mysql)
        if (error):
            table_space.abort(500, error)
        return organize_return(status, message, data, error)
# Here ends the table module


# Here starts the table data module
tabledata_model = api.model("Tabledata Model",
                            {"name": fields.String(required=True),
                             "columns": fields.String(required=True),
                             "values": fields.String(required=True),
                             "conditions": fields.String()})


@tabledata_space.route("")
class TabledataList(Resource):
    @api.doc(description="Get the data from table")
    @api.param('sort_by', description='Sort by', type='string')
    @api.param('filter', description='Apply a filter', type='string')
    @api.param('page', description='Page to retrieve (each page contains 250 rows)', type='integer')
    @api.param('columns', description='Columns to retrieve', type='string')
    @api.param('name', description='Table name', type='string', required=True)
    @api.doc(responses={200: "OK"})
    def get(self):
        name = request.args["name"]
        columns = request.args["columns"] if "columns" in request.args else None
        page = request.args["page"] if "page" in request.args else 1
        filter = request.args["filter"] if "filter" in request.args else None
        sort_by = request.args["sort_by"] if "sort_by" in request.args else None
        status, message, data, error = get_tabledata(name, columns, page, filter, sort_by, mysql)

        return return_response(status, message, data)

    @api.doc(responses={200: "OK", 400: "Invalid Argument"})
    @api.expect(tabledata_model)
    def post(self):
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


@metadata_space.route("/<string:table_name>")
class Metadata(Resource):
    """
    if input is 'TABLE', output all tables in the db
    if input is 'VIEW', output all views in the db
    if input is <table_name>, output metadata for that table
    """

    def get(self, table_name):
        status, message, data, error = get_metadata(table_name, mysql, flask_app.config['MYSQL_DB'])
        return organize_return_with_data(status, message, data, error)


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


'''
If columns_A and columns_B are empty, SELECT ALL from both tables.
(以后可以支持UNION ALL)
'''
union_model = api.model("Union Model",
                            {"table_name_A": fields.String(required=True),
                             "columns_A": fields.String,
                             "table_name_B": fields.String(required=True),
                             "columns_B": fields.String,
                             "returned_view_name": fields.String})

@union_space.route("")
class Union(Resource):
    @api.expect(union_model)
    def post(self):
        table_name_A = request.json["table_name_A"]
        columns_A = request.json["columns_A"]
        table_name_B = request.json["table_name_B"]
        columns_B = request.json["columns_B"]
        returned_view_name = request.json["returned_view_name"]

        col_list_A = columns_A.split(',')
        col_list_B = columns_B.split(',')

        if len(col_list_A) != len(col_list_B):
            raise ValueError("Number of columns in both tables are not equal.")

        status, message, data, error = get_union(mysql, table_name_A, col_list_A, table_name_B, col_list_B, returned_view_name)
        return organize_return_with_data(status, message, data, error)




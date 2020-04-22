import json
from flask_mysqldb import MySQL

from Controller.MetadataController import get_metadata
from util.QueryHelper import *
<<<<<<< Updated upstream
from flask import Flask, request, jsonify
from flask_restplus import Api, Resource, fields, reqparse
from Controller.MetadataController import *
from Controller.PredictableExeption import PredictableException
from Controller.TableController import create_table, delete_table
from Controller.TabledataController import *
=======
from util.LFUHelper import *
from controller.MetadataController import *
from controller.PredictableExeption import PredictableException
from controller.TableController import create_table, delete_table
from controller.UnionController import *
from controller.MetaController import *
from controller.TabledataController import *
from controller.JoinController import *
>>>>>>> Stashed changes

# Import env variable
import os
from dotenv import load_dotenv
load_dotenv()

flask_app = Flask(__name__)
api = Api(app=flask_app,
          version="1.0",
          title="Name Recorder",
          description="Manage names of various users of the application")

flask_app.config["MYSQL_HOST"] = "localhost"
flask_app.config["MYSQL_PORT"] = 3306
flask_app.config["MYSQL_USER"] = "4440user"
flask_app.config["MYSQL_PASSWORD"] = "4440password"
flask_app.config["MYSQL_DB"] = "4440db"

mysql = MySQL(flask_app)

table_space = api.namespace("table", description="Manage tables")
metadata_space = api.namespace("metadata", description="Manage metadata")
<<<<<<< Updated upstream
tabledata_space = api.namespace("table/data", description="Manage data records")
=======
uniquekey_space = api.namespace("metadata/uniquekey", description="Manage unique key")
foreignkey_space = api.namespace("metadata/foreignkey", description="Manage foreign key")
union_space = api.namespace("union", description="get a union of two table")
join_space = api.namespace("join", description="get a join of multiple tables")
>>>>>>> Stashed changes


table_model = api.model("Table Model",
                        {"columns": fields.String(required=True),
                         "uniques": fields.String()})

tabledata_model = api.model("Tabledata Model",
                        {"columns": fields.String(required=True),
                         "values": fields.String(required=True),
                         "conditions": fields.String()})

column_model = api.model("Column Model",
                          {"columns": fields.String(required=True),
                           "types": fields.String(required=True),
                           "values": fields.String(required=True)})

@table_space.route("/<string:table_name>")
class TableList(Resource):
    @api.doc(responses={200: "OK", 400: "Invalid Argument", 500: "Mapping Key Error"})
    @api.expect(table_model)
    def post(self, table_name):
        try:
            table = table_name
            column = request.json["columns"]
            unique = request.json["uniques"]
            status, message, data, error = create_table(table, column, unique, mysql)
            return {"message": message}, status
        except KeyError as e:
            table_space.abort(
                500, e.__doc__, status="Could not save information", statusCode="500")
        except PredictableException as e:
            table_space.abort(
                500, e.__doc__, status=e.handle_me(), statusCode="300")
        except Exception as e:
            table_space.abort(
                400, e.__doc__, status="Could not save information", statusCode="400")
    
    def delete(self, table_name):
<<<<<<< Updated upstream
        status, message, data, error = delete_table(table_name, mysql)
        return {"message": message}, status


@metadata_space.route("/<table_name>")
=======
        status, message, error = delete_table(table_name, mysql)
        return organize_return(status, message, data, error)

@metadata_space.route("/<string:table_name>")
>>>>>>> Stashed changes
class Metadata(Resource):
    '''
    if input is 'TABLE', output all tables in the db
    if input is 'VIEW', output all views in the db
    if input is <table_name>, output metadata for that table
    '''
    def get(self, table_name):
        status, message, data, error = get_metadata(table_name, mysql, flask_app.config['MYSQL_DB'])
        return {"message": message, "data": data}, status

@tabledata_space.route("/<table_name>")
class Tabledata(Resource):
    @api.doc(responses={200: "OK", 400: "Invalid Argument"})
    @api.expect(tabledata_model)
    def post(self, table_name):
        try:
            table = table_name
            column = request.json["columns"]
            value = request.json["value"]
            conditions = request.json["conditions"]
            status, message, data, error = update_tabledata(table, column, value, conditions, mysql)
            return organize_return(status, message, data, error)
        except PredictableException as e:
            table_space.abort(
                500, e.__doc__, status=e.handle_me(), statusCode="300")
        except Exception as e:
<<<<<<< Updated upstream
            table_space.abort(
                400, e.__doc__, status="Could not update information", statusCode="400")

    @api.doc(responses={200: 'OK'},
    params={'column': 'Specify the Column need to be deleted'})
    def delete(self, table_name, column):
        status, message, data, error = delete_tabledata(table_name, column, mysql)
<<<<<<< Updated upstream
        return {"message": message}, status
=======
        return organize_return(status, message, data, error)
>>>>>>> Stashed changes
=======
            raise e

tabledata_delete_model = api.model("Tabledata Model",
                            {"conditions": fields.String(required=True)})

@tabledata_space.route("/<string:table_name>")
class Tabledata(Resource):
    @api.doc(responses={200: 'OK'})
    @api.expect(tabledata_delete_model)
    def delete(self, table_name):
        condition = request.json["conditions"]
        status, message, data, error = delete_tabledata(table_name, condition, mysql)
        return organize_return(status, message, data, error)
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

join_model = api.model("Join Model",
                            {"tables": fields.String(required=True),
                             "columns": fields.String(required=True),
                             "joinType": fields.String(required=True),
                             "match": fields.String(required=True),
                             "returned_view_name": fields.String})

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
        return organize_return_with_data(status, message, data, error)



>>>>>>> Stashed changes

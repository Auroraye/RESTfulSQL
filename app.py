import json
from flask_mysqldb import MySQL

from controller.MetaController import organize_return
from controller.MetadataController import get_metadata
from util.QueryHelper import *
from flask import Flask, request, jsonify
from flask_restplus import Api, Resource, fields, reqparse
from controller.MetadataController import *
from controller.PredictableExeption import PredictableException
from controller.TableController import create_table, delete_table
from controller.TabledataController import *

# Import env variable
import os
from dotenv import load_dotenv
load_dotenv()

flask_app = Flask(__name__)
api = Api(app=flask_app,
          version="1.0",
          title="Name Recorder",
          description="Manage names of various users of the application")

flask_app.config["MYSQL_HOST"] = os.getenv("MYSQL_HOST")
flask_app.config["MYSQL_PORT"] = 3306
flask_app.config["MYSQL_USER"] = os.getenv("MYSQL_USER")
flask_app.config["MYSQL_PASSWORD"] = os.getenv("MYSQL_PASSWORD")
flask_app.config["MYSQL_DB"] = os.getenv("MYSQL_DB")

mysql = MySQL(flask_app)

table_space = api.namespace("table", description="Manage tables")
metadata_space = api.namespace("metadata", description="Manage metadata")
tabledata_space = api.namespace("table/data", description="Manage data records")


table_model = api.model("Table Model",
                        {"columns": fields.String(required=True),
                         "uniques": fields.String()})

tabledata_model = api.model("Tabledata Model",
                        {"columns": fields.String(required=True),
                         "values": fields.String(required=True),
                         "conditions": fields.String()})

tabledata_delete_model = api.model("Tabledata Delete Model",
                        {"condition": fields.String(required=True)})


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
        status, message, error = delete_table(table_name, mysql)
        return organize_return(status, message, data, error)


column_model = api.model("Column Model",
                          {"columns": fields.String(required=True),
                           "types": fields.String(required=True),
                           "values": fields.String(required=True)})


@metadata_space.route("/<string:table_name>")
class Metadata(Resource):
    '''
    if input is 'TABLE', output all tables in the db
    if input is 'VIEW', output all views in the db
    if input is <table_name>, output metadata for that table
    '''
    def get(self, table_name):
        status, message, data, error = get_metadata(table_name, mysql, flask_app.config['MYSQL_DB'])
        return organize_return(status, message, data, error)

    @api.expect(column_model)
    def update(self, table_name):
        name = table_name
        column = request['columns']
        kind = request['types']
        value = request['values']
        status, message, data, error = update_column(name, column, kind, value, mysql)
        return organize_return(status, message, data, error)

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
            return {"message": message}, status
        except PredictableException as e:
            table_space.abort(
                500, e.__doc__, status=e.hangdle_me(), statusCode="300")
        except Exception as e:
            table_space.abort(
                400, e.__doc__, status="Could not update information", statusCode="400")

    @api.doc(responses={200: 'OK'})
    @api.expect(tabledata_delete_model)
    def delete(self, table_name):
        condition = request.json["condition"]
        status, message, data, error = delete_tabledata(table_name, condition, mysql)
        return {"message": message}, status
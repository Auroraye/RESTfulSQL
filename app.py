from Controller.PredictableExeption import PredictableException
from Controller.TableController import create_table, delete_table
from Controller.MetadataController import *
from flask_mysqldb import MySQL
from util.QueryHelper import *
from flask import Flask, request, jsonify
from flask_restplus import Api, Resource, fields, reqparse
import json

flask_app = Flask(__name__)
api = Api(app=flask_app,
          version="1.0",
          title="Name Recorder",
          description="Manage names of various users of the application")

# flask_app.config['MYSQL_HOST'] = "db4free.net"
# flask_app.config['MYSQL_PORT'] = 3306
# flask_app.config['MYSQL_USER'] = "mxkezffynken"
# flask_app.config['MYSQL_PASSWORD'] = "XUWNG3gdFw82"
# flask_app.config['MYSQL_DB'] = database


# change accordingly
flask_app.config['MYSQL_HOST'] = 'localhost'
flask_app.config['MYSQL_USER'] = 'root'
flask_app.config['MYSQL_PASSWORD'] =
flask_app.config['MYSQL_DB'] = 'company'


mysql = MySQL(flask_app)
current_database = 'company'

table_space = api.namespace('table', description='Manage tables')
metadata_space = api.namespace('metadata', description='Manage metadata')
tabledata_space = api.namespace(
    'table/data', description='Manage data records')


table_model = api.model('Table Model',
                        {'name': fields.String(required=True,
                                               description="Name of the person",
                                               help="Name cannot be blank."),
                         'columns': fields.String(required=True),
                         'uniques': fields.String()})


@table_space.route("/")
class TableList(Resource):
    @api.doc(responses={200: 'OK', 400: 'Invalid Argument', 500: 'Mapping Key Error'})
    @api.expect(table_model)
    def post(self):
        try:
            table = request.json['name']
            column = request.json['columns']
            unique = request.json['uniques']
            return create_table(table, column, unique, mysql)
        except KeyError as e:
            table_space.abort(
                500, e.__doc__, status="Could not save information", statusCode="500")
        except PredictableException as e:
            table_space.abort(
                500, e.__doc__, status=e.handle_me(), statusCode="300")
        except Exception as e:
            table_space.abort(
                400, e.__doc__, status="Could not save information", statusCode="400")
        return {'result': 'Language added'}, 201


@table_space.route('/<string:table_name>')
class Table(Resource):
    def delete(self, table_name):
        status, message, data, error = delete_table(table_name, "")
        return {"message": message}, status


metadata_model = api.model('Metadata Model',
                        {'columns': fields.String(required=True,
                                                  description="Name of the columns"),
                         'type': fields.String(required=True,
                                               description="Type to be modified")
                         })

@metadata_space.route("/<table_name>")
class Metadata(Resource):
    '''
    if input is 'TABLE', output all tables in the db
    if input is 'VIEW', output all views in the db
    if input is <table_name>, output metadata for that table
    '''
    def get(self, table_name):
        status, message, data, error = get_metadata(table_name, mysql, current_database)
        return {"message": message, "data": data}, status


    @api.expect(metadata_model)
    def post(self, table_name):
        op_type = request.json['type']
        column_list = request.json['columns']
        status, message, data, error = post_metadata(table_name, mysql, column_list, op_type)
        return {"message": message, "data": data}, status

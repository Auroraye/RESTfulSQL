from Controller.PredictableExeption import PredictableException
from Controller.TableController import create_table
from flask_mysqldb import MySQL
from util.MySQLInfo import *
from flask import Flask, request, jsonify
from flask_restplus import Api, Resource, fields, reqparse
import json


flask_app = Flask(__name__)
app = Api(app=flask_app,
          version="1.0",
          title="Name Recorder",
          description="Manage names of various users of the application")

# flask_app.config['MYSQL_HOST'] = host
# flask_app.config['MYSQL_PORT'] = port
# flask_app.config['MYSQL_USER'] = user
# flask_app.config['MYSQL_PASSWORD'] = password
# flask_app.config['MYSQL_DB'] = database

# change accordingly
flask_app.config['MYSQL_HOST'] = 'localhost'
flask_app.config['MYSQL_USER'] = 'root'
flask_app.config['MYSQL_PASSWORD'] =
flask_app.config['MYSQL_DB'] =


mysql = MySQL(flask_app)

name_space = app.namespace('names', description='Manage names')
table_space = app.namespace('Table', description='Manage tables')
metadata_space = app.namespace('Metadata', description='Manage metadata')
tabledata_space = app.namespace('Table/Data', description='Manage data records')
metadata_space = app.namespace('Metadata', description='Manage metadata')


model = app.model('Name Model',
                  {'name': fields.String(required=True,
                                         description="Name of the person",
                                         help="Name cannot be blank.")})

list_of_names = {}


@name_space.route("/<int:id>")
class MainClass(Resource):

    @app.doc(responses={200: 'OK', 400: 'Invalid Argument', 500: 'Mapping Key Error'},
             params={'id': 'Specify the Id associated with the person'})
    def get(self, id):
        try:
            name = list_of_names[id]
            return {
                "status": "Person retrieved",
                "name": list_of_names[id]
            }
        except KeyError as e:
            name_space.abort(
                500, e.__doc__, status="Could not retrieve information", statusCode="500")
        except Exception as e:
            name_space.abort(
                400, e.__doc__, status="Could not retrieve information", statusCode="400")

    @app.doc(responses={200: 'OK', 400: 'Invalid Argument', 500: 'Mapping Key Error'},
             params={'id': 'Specify the Id associated with the person'})
    @app.expect(model)
    def post(self, id):
        try:
            list_of_names[id] = request.json['name']
            return {
                "status": "New person added",
                "name": list_of_names[id]
            }
        except KeyError as e:
            name_space.abort(
                500, e.__doc__, status="Could not save information", statusCode="500")
        except Exception as e:
            name_space.abort(
                400, e.__doc__, status="Could not save information", statusCode="400")


table_model = app.model('Table Model',
                        {'name': fields.String(required=True,
                                                description="Name of the person",
                                                help="Name cannot be blank."),
                         'columns': fields.String(required=True),
                         'uniques': fields.String()})


@table_space.route("/")
class TableClass(Resource):
    @app.doc(responses={200: 'OK', 400: 'Invalid Argument', 500: 'Mapping Key Error'})
    @app.expect(table_model)
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
        return {'result' : 'Language added'}, 201


@metadata_space.route("/<table_name>")
class MainClass(Resource):
    '''
    if input is 'TABLE', output all tables in the db
    if input is 'VIEW', output all views in the db
    if input is <table_name>, output metadata for that table
    '''
    def get(self, table_name):
        resultlist = []
        if table_name == 'TABLE':
            result, error = db_query(mysql, 'SHOW FULL TABLES IN company;', None)
            for item in result:
                temp = {"Tables": item[0],
                        "Table_type": item[1]
                        }
                resultlist.append(temp)
        elif table_name == 'VIEW':
            result, error = db_query(mysql, 'SHOW FULL TABLES IN company WHERE TABLE_TYPE LIKE \'VIEW\';', None)
            for item in result:
                temp = {"Views": item[0],
                        "Table_type": item[1]
                        }
                resultlist.append(temp)
        else:
            result, error = db_query(mysql, 'DESCRIBE {};'.format(table_name), None)
            for item in result:
                # temp = jsonify(Field=item[0], Type=item[1], Null=item[2], Key=item[3])
                temp = {"Field": item[0],
                        "Type": item[1],
                        "Null": item[2],
                        "Key": item[3]
                        }
                resultlist.append(temp)
        resultTuple = tuple(resultlist)
        return jsonify(resultTuple)
        # return jsonify([user for user in result])

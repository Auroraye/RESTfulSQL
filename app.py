from flask_mysqldb import MySQL
from flask import Flask, request
from flask_restplus import Api, Resource, fields


flask_app = Flask(__name__)
app = Api(app=flask_app,
          version="1.0",
          title="Name Recorder",
          description="Manage names of various users of the application")

flask_app.config['MYSQL_HOST'] = 'localhost'
flask_app.config['MYSQL_USER'] = 'root'
flask_app.config['MYSQL_PASSWORD'] = 'Tyx19980910!'
flask_app.config['MYSQL_DB'] = 'company'


# flask_app.config['MYSQL_HOST'] = 'db4free.net'
# flask_app.config['MYSQL_PORT'] = 3306
# flask_app.config['MYSQL_USER'] = 'mxkezffynken'
# flask_app.config['MYSQL_PASSWORD'] = 'XUWNG3gdFw82'

# Intialize MySQL
mysql = MySQL(flask_app)

name_space = app.namespace('names', description='Manage names')

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


@app.route('/test')
class Test(Resource):
    def get(self):
        cur = mysql.connection.cursor()
        cur.execute("SELECT * FROM company.department;")
        result = cur.fetchall()
        print(result)
        return {
                "status": "Person retrieved",
                "name": "lalalall"
        }

a_language = app.model('Language', {'language' : fields.String('The language.')})
languages = []
item1 = {'language' : 'Python'}
languages.append(item1)

languages_space = app.namespace('Language', description='test for languages')
@languages_space.route('/language')
class Language(Resource):
    def get(self):
        return languages

    @app.expect(a_language)
    def post(self):
        languages.append(app.payload)
        return {'result' : 'Language added'}, 201

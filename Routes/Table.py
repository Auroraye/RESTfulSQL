from flask import request
from flask_restplus import Resource

import app


@app.table_space.route("/<int:id>")
class TableClass(Resource):
    @app.doc(responses={200: 'OK', 400: 'Invalid Argument', 500: 'Mapping Key Error'},
             params={'name': 'Specify table name you want to create',
                     'columns': 'Specify what columns you want to have in the new table',
                     'uniques': 'Specify any unique keys you want to have in the new table'})
    @app.expect(app.model)
    def post(self):
        try:
            table = request.json['name']
            column = request.json['columns']
            unique = request.json['uniques']

        except KeyError as e:
            app.table_space.abort(
                500, e.__doc__, status="Could not save information", statusCode="500")
        except Exception as e:
            app.table_space.abort(
                400, e.__doc__, status="Could not save information", statusCode="400")

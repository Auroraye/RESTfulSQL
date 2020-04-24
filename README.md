# RESTful SQL
![Application screenshoot](/images/app.png =250x)

A REST API wrapper to support CRUD operations in a MYSQL database system.It allows to effortlessly build and deploy highly customizable RESTful Web Services that supports most of the underlying SQL statements such as SELECT, INSERT, UPDATE, and DELETE operators.

## Data Preparation and Setup

### Setup
Supported database: MySQL, version 8.0.19. To install MySQL, please follow the official [MySQL Installer Guide](https://dev.mysql.com/doc/mysql-installer/en/).

### Data
You are free to create your own table and add data using our API endpoints. We also have provided an example schema and different operations in the ```/example``` folder.

## Application and Code
Required Python version: Python 3.7 or higher. To install Python, please visit the  [Python Download Guideline](https://www.python.org/downloads/)

To install all the libraries needed to execute the code:
```sh
$ pip install flask
$ pip install flask-restplus
$ pip install Werkzeug==0.16.1 
$ pip install flask-mysqldb
$ pip install request
```

## Running the App
To run the application in production mode:
```
$ FLASK_APP=app.py flask run
```
To run the application in debug mode: 
```
$ FLASK_APP=app.py FLASK_ENV=development flask run
```
The url for the namespace is http://127.0.0.1:5000/main

## Code Documentation and References
All the code is written by the team following the [Flask-RESTful 0.3.8 documentation](https://flask-restful.readthedocs.io/en/latest/).

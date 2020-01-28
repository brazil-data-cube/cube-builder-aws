import os

from bdc_db import BDCDatabase
from flask import Flask
from flask_script import Manager

HOST = os.environ.get('RDS_HOST')
DBNAME = os.environ.get('RDS_DBNAME')
USER = os.environ.get('RDS_USER')
PASSWORD = os.environ.get('RDS_PASSWORD')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://{}:{}@{}:5432/{}'.format(
   USER, PASSWORD, HOST, DBNAME
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

manager = Manager(app)
BDCDatabase(app)

from bdc_db.models import db
from flask_migrate import MigrateCommand
manager.add_command('db', MigrateCommand)

@manager.command
def run():
    HOST = os.environ.get('SERVER_HOST', '0.0.0.0')
    try:
        PORT = int(os.environ.get('PORT', '5000'))
    except ValueError:
        PORT = 5000

    app.run(HOST, PORT)

@manager.command
def create_extension():
    import psycopg2

    conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(
        HOST, DBNAME, USER, PASSWORD
    )
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("CREATE EXTENSION postgis;")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    manager.run()

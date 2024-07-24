from pymongo import MongoClient
import os
from flask import current_app, g
from flask_pymongo import PyMongo
from werkzeug.local import LocalProxy


# client = MongoClient(os.environ.get('DB_URI_LOCAL'))
#
#
# db = client['informes_legais']


def get_db():
    """
    Configuration method to return db instance
    """
    db = getattr(g, "_database", None)

    if db is None:
        db = g._database = PyMongo(current_app).db

    return db


db = LocalProxy(get_db)



from flask import Blueprint
from flask import jsonify
from biodb.mysql import MYSQL

bp = Blueprint("internal", __name__, url_prefix="/internal")


@bp.route("/tables")
def list_tables():

    with MYSQL.cursor('reader') as cursor:
        query = "SHOW TABLES;"
        cursor.execute(query)
        results = cursor.fetchall()
        return jsonify(results)


@bp.route("/version")
def print_version():
    with open('version.txt') as f:
        return f.read().strip()

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


@bp.route("/snapshot_id")
def print_snapshot_id():
    with open('snapshot.txt') as f:
        return f.read().strip()


@bp.route("/rp")
def print_mysql_root_password():
    with open('password.txt') as f:
        return f.read().strip()

from flask import request
from flask import Blueprint
from flask import jsonify
from biodb.mysql import MYSQL

bp = Blueprint("tables", __name__, url_prefix="/tables")

@bp.route("")
def list_tables():
    
    with MYSQL.cursor('reader') as cursor:
        query = "SHOW TABLES;"
        cursor.execute(query)
        results = cursor.fetchall()
        return jsonify(results)
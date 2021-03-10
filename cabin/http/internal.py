from flask import Blueprint
from flask import jsonify
from biodb import settings
from biodb.data.registry import load_table_registry
from biodb.mysql import READER

bp = Blueprint("internal", __name__, url_prefix="/internal")


@bp.route("/tables")
def print_type_to_tablename():
    return jsonify({
        'user': READER,
        'password': settings.SGX_MYSQL_READER_PASSWORD,
        'database': settings.SGX_MYSQL_DB,
        'tables': {hd.type: hd.name for hd in load_table_registry(latest_only=True)}
    })


@bp.route("/rc_id")
def print_version():
    with open('rc_id.txt') as f:
        return f.read().strip()


@bp.route("/snapshot_id")
def print_snapshot_id():
    with open('/sgx/biodb/snapshot_id.txt') as f:
        return f.read().strip()

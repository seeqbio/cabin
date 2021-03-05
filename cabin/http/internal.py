from flask import Blueprint
from flask import jsonify
from biodb import settings
from biodb.data.registry import load_table_registry

bp = Blueprint("internal", __name__, url_prefix="/internal")


@bp.route("/tables")
def print_type_to_tablename():
    tables = {}
    for _, hdataset in sorted(load_table_registry().items()):
        if hdataset.is_latest():
            tables[hdataset.type] = hdataset.name

    return jsonify({
        'user': 'reader',
        'password': settings.SGX_MYSQL_READER_PASSWORD,
        'database': settings.SGX_MYSQL_DB,
        'tables': tables
    })


@bp.route("/rc_id")
def print_version():
    with open('rc_id.txt') as f:
        return f.read().strip()


@bp.route("/snapshot_id")
def print_snapshot_id():
    with open('/sgx/biodb/snapshot_id.txt') as f:
        return f.read().strip()

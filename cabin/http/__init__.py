from flask import Flask


def create_app():
    app = Flask('biodb.http') # name does not affect discoverability of FLASK_APP
    from . import internal
    app.register_blueprint(internal.bp)
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    return app

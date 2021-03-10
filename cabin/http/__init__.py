from flask import Flask


def create_app():

    app = Flask('biodb')
    from . import internal
    app.register_blueprint(internal.bp)
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    return app

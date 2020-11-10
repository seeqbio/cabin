from flask import Flask

def create_app():

    app = Flask('biodb')
    from . import tables
    app.register_blueprint(tables.bp)

    return app
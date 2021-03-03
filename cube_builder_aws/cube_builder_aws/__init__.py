#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

import json
from datetime import date
from json import JSONEncoder

from bdc_catalog import BDCCatalog
from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

from .config import DBNAME, HOST, PASSWORD, PORT, USER
from .logger import logger


def create_app():
    """Create Brazil Data Cube application from config object.
    Returns:
        Flask Application with config instance scope
    """
    app = Flask(__name__)

    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://{}:{}@{}:{}/{}'.format(
        USER, PASSWORD, HOST, PORT,  DBNAME
    )
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

    with app.app_context():
        # Initialize Flask SQLAlchemy
        BDCCatalog(app)

        setup_app(app)

    return app


def setup_error_handlers(app: Flask):
    """Configure Cube Builder Error Handlers on Flask Application."""
    
    @app.errorhandler(HTTPException)
    def handle_exception(e):
        """Return JSON instead of HTML for HTTP errors."""
        # start with the correct headers and status code from the error
        logger.error(str(e), exc_info=True)
        response = e.get_response()
        # Set Error description as body data.
        response.data = json.dumps(e.description)
        response.content_type = "application/json"
        return response

    @app.errorhandler(Exception)
    def handle_exception(e):
        """Return JSON instead of HTML for HTTP errors."""
        # start with the correct headers and status code from the error
        logger.error(str(e), exc_info=True)
        return jsonify(str(e)), 500


def setup_app(app):
    """Configure internal middleware for Flask app."""

    @app.after_request
    def after_request(response):
        """Enable CORS."""
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Methods', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, X-Api-Key')
        return response

    class ImprovedJSONEncoder(JSONEncoder):
        def default(self, o):
            from datetime import datetime
            
            if isinstance(o, set):
                return list(o)
            if isinstance(o, datetime):
                return o.isoformat()
            return super(ImprovedJSONEncoder, self).default(o)

    app.json_encoder = ImprovedJSONEncoder

    setup_error_handlers(app)

    from .views import bp
    app.register_blueprint(bp)
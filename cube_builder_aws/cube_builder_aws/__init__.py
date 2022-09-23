#
# This file is part of Cube Builder AWS.
# Copyright (C) 2022 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

import json
from datetime import date
from json import JSONEncoder

from bdc_catalog import BDCCatalog
from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException

from .config import SQLALCHEMY_DATABASE_URI
from .logger import logger


def create_app():
    """Create Brazil Data Cube application from config object.
    Returns:
        Flask Application with config instance scope
    """
    app = Flask(__name__)

    app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
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
import jwt
import os
from flask import request, jsonify
from functools import wraps
from config import ENABLE_OBT_OAUTH, AUTH_CLIENT_SECRET_KEY, \
    AUTH_CLIENT_AUDIENCE

def get_token():
    try:
        bearer, authorization = request.headers['Authorization'].split()
        if 'bearer' not in bearer.lower():
            return jsonify('Invalid token. Please login!'), 403
        return authorization

    except Exception:
        return jsonify('Token is required. Please login!'), 403


def validate_scope(scope_required, scope_token):
    if scope_required:
        service, function, actions = scope_required.split(':')
        if (service != scope_token['type'] and scope_token['type'] != '*') or \
            (function != scope_token['name'] and scope_token['name'] != '*') or \
            (actions not in scope_token['actions'] and '*' not in scope_token['actions']):
            return jsonify('Scope not allowed!'), 401


def require_oauth_scopes(scope):
    def jwt_required(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # auth disabled
            if not ENABLE_OBT_OAUTH or int(ENABLE_OBT_OAUTH) == 0:
                return func(*args, **kwargs)
            
            # auth enabled
            if not AUTH_CLIENT_SECRET_KEY:
                return jsonify('Set CLIENT_SECRET_KEY in environment variable'), 500
            if not AUTH_CLIENT_AUDIENCE:
                return jsonify('Set CLIENT_AUDIENCE in environment variable'), 500

            try:
                token = get_token()
                payload = jwt.decode(token, AUTH_CLIENT_SECRET_KEY, verify=True,
                    algorithms=['HS512'], audience=AUTH_CLIENT_AUDIENCE)

                if payload.get('user_id'):
                    request.user_id = payload['user_id']
                    validate_scope(scope, payload['access'][0])
                    return func(*args, **kwargs)
                else:
                    return jsonify('Incomplete token. Please login!'), 403 

            except jwt.ExpiredSignatureError:
                return jsonify('This token has expired. Please login!'), 403 
            except jwt.InvalidTokenError:
                return jsonify('Invalid token. Please login!'), 403 
        return wrapper
    return jwt_required
#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""Config module."""

import os

# Prefix to be set on Item and in the bucket alias.
ITEM_PREFIX = os.getenv('ITEM_PREFIX')

AWS_KEY_ID = os.environ.get('KEY_ID', '')
AWS_SECRET_KEY = os.environ.get('SECRET_KEY', '')

SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI', 'postgresql://postgres:postgres@localhost:5432/bdc_catalog')

LAMBDA_FUNCTION_NAME = os.environ.get('LAMBDA_FUNCTION_NAME', '')
QUEUE_NAME = os.environ.get('QUEUE_NAME', '')
KINESIS_NAME = os.environ.get('KINESIS_NAME', '')
TABLE_NAME = os.environ.get('TABLE_NAME', '')

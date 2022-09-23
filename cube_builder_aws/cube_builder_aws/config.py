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

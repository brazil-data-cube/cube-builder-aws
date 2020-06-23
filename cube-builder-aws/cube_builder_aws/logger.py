#
# This file is part of Cube Builder AWS.
# Copyright (C) 2019-2020 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Define a base logger module for AWS lambdas."""

import logging

logger = logging.getLogger('cube_builder_aws')
logger.setLevel(logging.INFO)

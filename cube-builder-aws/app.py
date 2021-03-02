#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

try:
    import unzip_requirements
except ImportError:
    pass

from cube_builder_aws import create_app

create_app()

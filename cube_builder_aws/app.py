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

import base64
import json

from cube_builder_aws import create_app
from cube_builder_aws.controller import CubeController

app = create_app()

controller = CubeController()

#########################################
# REQUEST -> from SQS trigger or Kinesis
#########################################
def continue_process(event, context):
    with app.app_context():
        params_list = []
        if 'Records' in event:
            for record in event['Records']:
                if 'kinesis' in record:
                    payload=base64.b64decode(record["kinesis"]["data"])
                    params = json.loads(payload)
                    params_list.append(params)
                else:
                    params = json.loads(record['body'])
                    params_list.append(params)
        else:
            params = event
            params_list.append(params)

        message = controller.continue_process_stream(params_list)
        return message

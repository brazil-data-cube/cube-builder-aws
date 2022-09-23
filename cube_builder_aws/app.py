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

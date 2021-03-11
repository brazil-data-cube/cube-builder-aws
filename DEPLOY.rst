..
    This file is part of Python Module for Cube Builder AWS.
    Copyright (C) 2019-2021 INPE.

    Cube Builder AWS is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Deploying
=========


Create infrastructure
---------------------

.. code-block:: shell

        $ cd deploy/step_1/
        $ sh start.sh

1. access https://console.aws.amazon.com/rds/home by browser

2. select region used to create RDS

3. select databases

4. Wait until the created database has a status of 'Available' (~10min)

5. click on database


Create database structure
-------------------------

Create initial database structure to catalog the cubes to be generated

.. code-block:: shell

        $ cd ../../deploy/step_2/
        $ sh start.sh


Deploy Lambda service
---------------------

** create file *.env* based on *example.env* in cube-builder-aws folder. Then set the environment variables with your information in *.env*

then:

.. code-block:: shell

        $ cd ../../deploy/step_3/
        $ sh deploy.sh


Get service status
---------------------

.. code-block:: shell

        $ curl {your-lambda-endpoint}/


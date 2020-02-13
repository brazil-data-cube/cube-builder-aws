..
    This file is part of Python Module for Cube Builder.
    Copyright (C) 2019 INPE.

    Cube Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Deploy
======

.. code-block:: shell

        $ cd deploy/step_1/
        $ sh start.sh

** access https://console.aws.amazon.com/rds/home by browser
** select region used to create RDS
** select databases
** Wait until the created database has a status of 'Available' (~10min)
** click on database

.. code-block:: shell

        $ cd ../../deploy/step_2/
        $ sh start.sh

** set environment with your information in *../../bdc-scripts/serverless.yml*

.. code-block:: shell

        $ cd ../../deploy/step_3/
        $ sh deploy.sh

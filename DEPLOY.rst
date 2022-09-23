..
    This file is part of Cube Builder AWS.
    Copyright (C) 2022 INPE.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.


Deploy
======

This section explains how to get the ``Cube-Builder-AWS`` up and running on `Amazon Web Services <https://aws.amazon.com/>`_.
If you do not read yet the :doc:`installation`, take a look at this tutorial on how to install it in your system in devmode
and be familiar with Python module.


.. warning::

    Make sure to identify which region the dataset is available.
    For example, most of `GEO Earth datasets <https://aws.amazon.com/earth/>`_ like ``Sentinel-2``, ``Landsat-8`` are 
    stored in ``Oregon`` (``us-west-2``). In this tutorial, we are going to use ``us-west-2``.

    If you generate data cubes on regions different from where the BDC services or dataset are,
    you may face high cost charges in the billing.



.. requirements:

Requirements
------------

- `RDS PostgreSQL <https://aws.amazon.com/rds/postgresql/>`_: A minimal instance of PostgreSQL database with PostGIS support.
  The ``instance_type`` depends essentially on how many parallel processing ``Lambdas`` are running. For this example,
  we can use the minimal instance ``db.t2.micro``. For a Brazil territory, considerer more robust instances like ``db.t2.large`` 
  which supports aroung ``600`` concurrent connections.

  After the instance up and running, you must initialize `BDC-Catalog <https://github.com/brazil-data-cube/bdc-catalog>`_. 
  Please, refer to ``Compatibility Table`` in :doc:`installation` for supported versions.

- `S3 - Simple Storage Service <https://aws.amazon.com/s3/>`_: A bucket to store ``Lambda codes`` and another bucket for ``data storage``.

- `Kinesis <https://aws.amazon.com/kinesis/>`_: a Kinesis instance to streaming data cube step metadata be transfered along ``Lambdas`` and ``DynamoDB``.
  For this example, minimal instance to support ``1000`` records (Default lambda parallel executions) is enough.

- `DynamoDB <https://aws.amazon.com/dynamodb/>`_: a set of dynamo tables to store data cube metadata.


Prepare environment
-------------------

The ``Cube-Builder-AWS`` command utilities uses `NodeJS <https://nodejs.org/en/>`_ module named `serverless <https://www.serverless.com/>`_
to deploy the stack of data cubes on Amazon Web Services.
First you need to install ``NodeJS``. We recommend you to use `nvm <https://github.com/nvm-sh/nvm>`_ which can be easily installed with
single command line and its supports to have multiple versions of nodejs installed. You can install it with command::

    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash


Set the following entry into ``~/.bashrc``::

    export NVM_DIR="$([ -z "${XDG_CONFIG_HOME-}" ] && printf %s "${HOME}/.nvm" || printf %s "${XDG_CONFIG_HOME}/nvm")"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" # This loads nvm


Install ``NodeJS 12+``::

    nvm install 12
    nvm use 12 # Activate the version as current


After that, use the following command to install ``serverless`` and their dependencies::

    npm install -g serverless


The second part is to have `AWS Identity and Access Management (IAM) <https://aws.amazon.com/iam/>`_ credentials with right access to deploy
the `requirements`_ section.


Prepare the infrastructure
--------------------------

We have prepared a script to set up a RDS PostgreSQL instance up and running. Use the following script::

    cd deploy/step_1/
    sh start.sh


The AWS RDS database set up takes aroung 10 minutes to launch. You can monitore the status following
https://console.aws.amazon.com/rds/home.

.. note::

    Make sure you are in region ``us-west-2 (Oregon)``.



Create database structure
-------------------------

Once RDS database is up and running, we need to create the ``BDC-Catalog`` model::

    cd ../../deploy/step_2/
    sh start.sh


Deploy Lambda service
---------------------

Before to proceed in ``Cube-Builder`` service, we need to create a ``cube-builder-aws/.env``.
We have prepared a minimal example ``cube-builder-aws/example.env`` and the following variables are available:

- ``PROJECT_NAME``: A name for the given project set up. This name will be set as ``prefix`` in Lambdas.
- ``STAGE``: A type of service environment context. Use ``dev`` or ``prod``.
- ``REGION``: AWS region to launch services.
- ``KEY_ID``: AWS Access Key.
- ``SECRET_KEY``: AWS Access Secret Key.
- ``SQLALCHEMY_DATABASE_URI``: URI for PostgreSQL instance. It have the following structure: ``postgresql://USER:PASSWD@HOST/DB_NAME``

Once ``cube-builder-aws/.env`` is set, you can run then following script to launch Lambda into AWS::

    cd ../../deploy/step_3/
    sh deploy.sh


The script helper will generate an URI for the Lambda location.
You can access this resource and check if everything is running.


Next steps
----------

After ``Cube-Builder-AWS`` backend is up and running, we recommend you to install the `Data Cube Manager GUI <https://github.com/brazil-data-cube/dc-manager>`_

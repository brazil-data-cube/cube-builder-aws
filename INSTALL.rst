..
    This file is part of Python Module for Cube Builder.
    Copyright (C) 2019-2020 INPE.

    Cube Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Prepare environment
===================

Prepare your AWS account and HOST to deploy application.

1) in AWS Console
-----------------

    - create AWS account
    - Loggin with AWS account created
    - create IAM user
    - set full permissions (fullAccess) to IAM user created
    - generate credentals to IAM user


2) in your HOST (command line):
-------------------------------

    - install *AWS CLI*
    - configure credentials
        -  e.g: aws configure --profile *iam-user-name*
    - install *Node.js* (global)
    - install *serverless*

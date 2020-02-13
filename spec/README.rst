..
    This file is part of Python Module for Cube Builder AWS.
    Copyright (C) 2019 INPE.

    Cube Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


API SPEC
========

API SPEC based in OPENAPI 3.0.

Requirements
------------

- [NodeJS 8+](https://nodejs.org/en/)
- [ReDoc](https://github.com/Redocly/redoc)

Execute the following command to install `node modules` dependencies:

.. code-block:: shell

        $ npm install

After that, generate Cube builder AWS documentation:

.. code-block:: shell

        $ npm run build

It will create folder `dist` with a bundled file `index.html`. You may serve this file with HTTP Server.
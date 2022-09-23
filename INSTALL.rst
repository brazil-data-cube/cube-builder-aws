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


Installation
============

The ``Cube Builder AWS`` depends essentially on:

- `Python Client Library for STAC (stac.py) <https://github.com/brazil-data-cube/stac.py>`_

- `Flask <https://palletsprojects.com/p/flask/>`_

- `Psycopg2 Binary <https://pypi.org/project/psycopg2-binary/>`_

- `rasterio <https://rasterio.readthedocs.io/en/latest/>`_

- `NumPy <https://numpy.org/>`_

- `Boto3 <https://boto3.amazonaws.com/v1/documentation/api/latest/index.html>`_

- `Flask-SQLAlchemy <https://pypi.org/project/Flask-SQLAlchemy/>`_

- `marshmallow-SQLAlchemy <https://marshmallow-sqlalchemy.readthedocs.io/en/latest/>`_

- `Brazil Data Cube Catalog Module <https://github.com/brazil-data-cube/bdc-catalog.git>`_

- `Rio-cogeo <https://pypi.org/project/rio-cogeo/>`_



Compatibility
+++++++++++++

+------------------+-------------+
| Cube-Builder-AWS | BDC-Catalog |
+==================+=============+
| 0.8.2            | 0.8.2       |
+------------------+-------------+
| 0.8.0 ~ 0.8.1    | 0.8.1       |
+------------------+-------------+
| 0.6.x            | 0.8.1       |
+------------------+-------------+
| 0.4.x            | 0.8.1       |
+------------------+-------------+


Clone the software repository
+++++++++++++++++++++++++++++

Use ``git`` to clone the software repository::

    git clone https://github.com/brazil-data-cube/cube-builder-aws.git


Install Cube-Builder-AWS in Development Mode
+++++++++++++++++++++++++++++++++++++++++++++


Go to the source code folder::

        $ cd cube-builder-aws


Install in development mode::

        $ pip3 install -e .[docs,tests]


.. note::

    If you want to create a new *Python Virtual Environment*, please, follow this instruction:

    *1.* Create a new virtual environment linked to Python 3.8::

        python3.8 -m venv venv


    **2.** Activate the new environment::

        source venv/bin/activate


    **3.** Update pip and setuptools::

        pip3 install --upgrade pip

        pip3 install --upgrade setuptools


Build the Documentation
+++++++++++++++++++++++


You can generate the documentation based on Sphinx with the following command::

    $ python setup.py build_sphinx


The above command will generate the documentation in HTML and it will place it under::

    docs/sphinx/_build/html/


You can open the above documentation in your favorite browser, as::

    firefox docs/sphinx/_build/html/index.html



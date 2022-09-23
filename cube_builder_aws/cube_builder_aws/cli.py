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

"""Create a python click context and inject it to the global flask commands."""

import click
from bdc_catalog.models import Application, CompositeFunction, db
from flask.cli import FlaskGroup, with_appcontext

from . import create_app
from .utils.package import package_info
from .version import __version__


@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    """Command line for data cube builder."""


@cli.command('load-data')
@with_appcontext
def load_data():
    """Create Cube Builder composite functions supported."""
    from .utils.processing import get_or_create_model

    with db.session.begin_nested():
        _, _ = get_or_create_model(
            CompositeFunction,
            defaults=dict(name='Median', alias='MED', description='Median by pixels'),
            alias='MED'
        )

        _, _ = get_or_create_model(
            CompositeFunction,
            defaults=dict(name='Least CC First', alias='LCF', description='Least Cloud Cover First'),
            alias='LCF'
        )

        _, _ = get_or_create_model(
            CompositeFunction,
            defaults=dict(name='Identity', description=''),
            alias='IDT'
        )

        where = dict(
            name=__package__,
            version=__version__
        )

        # Cube-Builder application
        application, _ = get_or_create_model(
            Application,
            defaults=dict(),
            **where
        )

    db.session.commit()


def main(as_module=False):
    """Load cube-builder as package in python module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m cube_builder_aws" if as_module else None)
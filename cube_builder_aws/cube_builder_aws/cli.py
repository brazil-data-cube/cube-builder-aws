#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
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
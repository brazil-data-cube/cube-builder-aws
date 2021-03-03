#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Create a python click context and inject it to the global flask commands."""

import click
from flask.cli import FlaskGroup, with_appcontext

from . import create_app
from .utils.package import package_info


@click.group(cls=FlaskGroup, create_app=create_app)
def cli():
    """Command line for data cube builder."""


def main(as_module=False):
    """Load cube-builder as package in python module."""
    import sys
    cli.main(args=sys.argv[1:], prog_name="python -m cube_builder" if as_module else None)
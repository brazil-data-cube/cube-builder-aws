#
# This file is part of Python Module for Cube Builder AWS.
# Copyright (C) 2019-2021 INPE.
#
# Cube Builder AWS is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#


"""Package information for Cube-Builder."""

import distutils.dist
import io

import pkg_resources


def package_info() -> distutils.dist.DistributionMetadata:
    """Retrieve the Cube Builder setup package information."""
    distribution = pkg_resources.get_distribution(__package__)
    metadata_str = distribution.get_metadata(distribution.PKG_INFO)
    metadata_obj = distutils.dist.DistributionMetadata()
    metadata_obj.read_pkg_file(io.StringIO(metadata_str))

    return metadata_obj
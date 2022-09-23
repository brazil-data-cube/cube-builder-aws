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
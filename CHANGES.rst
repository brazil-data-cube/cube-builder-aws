..
    This file is part of Cube Builder AWS.
    Copyright (C) 2019-2021 INPE.

    Cube Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Changes
=======

Version 0.8.0 (2021-06-29)
--------------------------

- Allow creating grid with others SRID
- generate file with full nodata when not scenes in tile/step (`#141 <https://github.com/brazil-data-cube/cube-builder-aws/issues/141>`_) 
- Add support to hamonization between landsat collections, using (`sensor-harm <https://github.com/brazil-data-cube/sensor-harm>`_) package
- Update get_qa_statistics function to use with landsat Collection 2 (qa_pixel band with bitwise)
- Accept nodata value parameter
- Create datasource band


Version 0.6.0 (2021-04-23)
--------------------------

- Changed the structure of the control table in dynamodb
- Fix endpoint to get cube status (`#137 <https://github.com/brazil-data-cube/cube-builder-aws/issues/137>`_)
- Fix reprocess tiles with force parameter (`#128 <https://github.com/brazil-data-cube/cube-builder-aws/issues/128>`_)


Version 0.4.0 (2021-04-13)
--------------------------

- Possibility to choose which types of cubes to generate (`#12 <https://github.com/brazil-data-cube/cube-builder-aws/issues/12>`_)
- Isolate the function to create the type of cubes (`#13 <https://github.com/brazil-data-cube/cube-builder-aws/issues/13>`_)
- Update endpoints responsible for managing grid ref sys (`#68 <https://github.com/brazil-data-cube/cube-builder-aws/issues/68>`_, `#107 <https://github.com/brazil-data-cube/cube-builder-aws/issues/107>`_)
- Update list cube infos endpoint with new bdc-catalog model (0.8.0) (`#80 <https://github.com/brazil-data-cube/cube-builder-aws/issues/80>`_, `#101 <https://github.com/brazil-data-cube/cube-builder-aws/issues/101>`_)
- Compute checksum of images (`#82 <https://github.com/brazil-data-cube/cube-builder-aws/issues/80>`_)
- Compute bbox (geom wsg84) of images (`#83 <https://github.com/brazil-data-cube/cube-builder-aws/issues/83>`_)
- Generate index based on an expression (`#86 <https://github.com/brazil-data-cube/cube-builder-aws/issues/86>`_)
- Fix code to generate cog using DEFLATE compression method (`#88 <https://github.com/brazil-data-cube/cube-builder-aws/issues/88>`_)
- Update file structure based on BDC packages (`#91 <https://github.com/brazil-data-cube/cube-builder-aws/issues/91>`_)
- Add route to edit cube metadata (`#93 <https://github.com/brazil-data-cube/cube-builder-aws/issues/93>`_)
- Add shape params if not is BDC grid (`#95 <https://github.com/brazil-data-cube/cube-builder-aws/issues/95>`_)
- Update get_mask function to use with fMask or sen2cor (Sentinel 2) (`#96 <https://github.com/brazil-data-cube/cube-builder-aws/issues/96>`_)
- Update function to create timeline (`#97 <https://github.com/brazil-data-cube/cube-builder-aws/issues/97>`_)
- Review data cube process id (`#111 <https://github.com/brazil-data-cube/cube-builder-aws/issues/111>`_)
- Add drone integration to running tests (`#120 <https://github.com/brazil-data-cube/cube-builder-aws/issues/120>`_)
- Add param to disable creating indices in irregular datacube (`#129 <https://github.com/brazil-data-cube/cube-builder-aws/issues/129>`_)


Version 0.2.0 (2020-08-26)
--------------------------

- First experimental version.
- Support of AWS lambdas to generate data cubes.

  - `AWS SQS <https://aws.amazon.com/sqs/>`_.
  - `AWS Kinesis <https://aws.amazon.com/kinesis/>`_.
  - `AWS DynamoDB <https://aws.amazon.com/dynamodb/>`_.
  - `AWS S3 <https://aws.amazon.com/s3/>`_.
- Deploy with `Serverless <https://www.serverless.com/>`_. and script to prepare enviroment.
- Package support through Setuptools.
- Installation, deployment and running instructions.
- Source code versioning based on `Semantic Versioning 2.0.0 <https://semver.org/>`_.
- License: `MIT <https://github.com/brazil-data-cube/cube-builder-aws/blob/master/LICENSE>`_.

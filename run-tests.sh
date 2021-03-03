# pydocstyle cube_builder_aws/cube_builder_aws && \
isort cube_builder_aws/cube_builder_aws tests setup.py --check-only --diff && \
sphinx-build -qnW --color -b doctest docs/sphinx/ docs/sphinx/_build/doctest  && \
pytest

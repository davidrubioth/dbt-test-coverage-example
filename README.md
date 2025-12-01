# dbt-test-coverage

This repository contains an example on how to generate test coverage reports for dbt projects using dbt unit tests as reference. 

I have used the models implemented in this repo https://github.com/portovep/dbt-testing-examples. For making the example more interesting, I have added some aggregations and operations to the models. Also I removed the dependency with dbt-unit-testing package and translated the tests to native dbt unit tests.

The solution is based in a python script that parses the dbt project and generates a report with the coverage for each model. The script has been implemented by some iterations using AugmentCode (https://www.augmentcode.com/). The prompt used can be found in the prompts folder.

The report covers 4 categories:
1. Columns (model columns tested in unit test expectations)
2. Aggregations (SUM, COUNT, AVG, etc.)
3. Operations (math, string, date functions, etc.)
4. Joins (INNER, LEFT, RIGHT, FULL)

## Features

- Dbt unit tests examples
- Test coverage report generator

## Local setup

You can follow the instructions in the [dbt-testing-examples](https://github.com/portovep/dbt-testing-examples) repository to setup the database and run the tests.

This repository uses poetry as package manager. You can install the dependencies with:

```
poetry install
```

## Generating the test coverage report

```
python scripts/test_coverage_report.py
```

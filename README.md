# SQL Field Report
Author: [Will James](https://github.com/wct-james)

[![Downloads](https://static.pepy.tech/badge/sql-field-report)](https://pepy.tech/project/sql-field-report)
## Installation
To install use pip:
`pip install sql-field-report`

## Usage

SQL Field Report is a [Typer](https://github.com/tiangolo/typer) CLI tool to see the available functions run:

`sql-field-report --help`

```
MSSQL Database Report

Generate an excel report summarising the data in an MSSQL Database

Args:
    server (str): The SQL server name
    port (int): The SQL server port
    user (str): Your SQL Server username
    password (str): Your SQL Server password
    database_name (str): The name of the database to analyse
    schema (str): The schema for the database tables to analyse
    output_file_name (str): The output file name of the report
```
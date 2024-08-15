## Introduction

This project allows you to automate SQL queries by leveraging Python's capabilities. The core functionality includes executing SQL commands, handling query results, and performing common database operations like CRUD (Create, Read, Update, Delete) actions.

## Requirements

Before you start, ensure you have the following installed:

- Python 3.6 or higher
- Required Python packages listed in `requirements.txt`. Install Required Packages: pip install -r requirements.txt

## Configure Database Connection:
Edit the okta.json file to include your database connection details.
{
"host": "presto-default.dev.twilio.com",
"port": 8443,
"user": "Enter your okta user_name here ",
"password": "Enter your okta password here ",
"catalog": "hive",
"schema": "applied_ds"
}

To run the SQL query automation script, use the following command:

python3 sql.py


## Introduction

This project allows you to automate SQL queries by leveraging Python's capabilities. The core functionality includes executing SQL commands, handling query results, and performing common database operations like CRUD (Create, Read, Update, Delete) actions.

## Requirements

Before you start, ensure you have the following installed:

- Python 3.6 or higher
- Create a python virtual env : python3 -m venv myenv
- Activate the Virtual Environment: source myenv/bin/activate
- Install Required Python packages listed in `requirements.txt`. Install Required Packages: pip install -r requirements.txt

## Configure Database Connection:

Edit the okta.json file to include your database connection details.
```{
"host": "presto-default.dev.twilio.com",
"port": 8443,
"user": "Enter your okta user_name here ",
"password": "Enter your okta password here ",
"catalog": "hive",
"schema": "applied_ds"
}
```
To run the SQL query automation script, use the following command:

For "TRAILING" mode  use the below format for data.json:
```
{
"values": [3, 4, 5],
"choice": "TRAILING",
"combinations": [[34, 4], [34, 23], [23,34]]
}
```

```
For "DATARANGE" use the below format for data.json:
{
"values": [3, 4, 5],
"choice": "DATERANGE",
"combinations": [["2023-11-20","2023-12-01"]]
}
```

 ### Run  the python script 
 ` python3 sql_final.py`


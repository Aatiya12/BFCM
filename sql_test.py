import json
import prestodb
import csv
def load_credentials(file_path):
    """Load credentials from a JSON file."""
    with open(file_path, 'r') as file:
        return json.load(file)
def get_block_1_parameters():
    """
    Prompts the user to enter a list of integers for the first parameter block.
    Returns the formatted string of integers wrapped in parentheses.
    """

    user_input = input("Enter a list of integers separated by commas (e.g. 3,50,3000): ")
    try:
        # Convert the user input into a list of integers
        int_list = [int(item) for item in user_input.split(",")]

        # Create the formatted string for the SQL query
        formatted_parameters = ",".join(f"({num})" for num in int_list)

        # Print or return the formatted string
        print("Formatted SQL parameters:")
        print(formatted_parameters)

        return formatted_parameters
    except ValueError:
        print("Invalid input. Please enter a list of integers separated by commas.")
    print(user_input)

    return user_input

def get_block_2_parameters():
    """
    Prompts the user to choose between TRAILING and DATERANGE modes for the second parameter block.
    Collects the necessary inputs based on the chosen mode.
    Returns the formatted SQL condition string for the chosen mode.
    """
    mode = input("Enter the mode for block 2 (TRAILING or DATERANGE): ").strip().upper()

    if mode == "TRAILING":
        start = int(input("Enter the START integer for TRAILING mode: ").strip())
        end = int(input("Enter the END integer for TRAILING mode: ").strip())
        formatted_parameters = f"""
(DATE_TRUNC('DAY', sms.data_load_date) >= DATE_TRUNC('DAY', CURRENT_DATE - interval '{start}' day)
AND DATE_TRUNC('DAY', sms.data_load_date) <= DATE_TRUNC('DAY', CURRENT_DATE - interval '{end - 3}' day))
AND DATE_TRUNC('DAY', sms.date_created) >= DATE_TRUNC('DAY', CURRENT_DATE - interval '{start}' day)
AND DATE_TRUNC('DAY', sms.date_created) < DATE_TRUNC('DAY', CURRENT_DATE - interval '{end}' day)
"""
    elif mode == "DATERANGE":
        start_date = input("Enter the START date for DATERANGE mode (YYYY-MM-DD): ").strip()
        end_date = input("Enter the END date for DATERANGE mode (YYYY-MM-DD): ").strip()
        formatted_parameters = f"""
((DATE_TRUNC('DAY', sms.data_load_date) >= DATE('{start_date}') 
AND DATE_TRUNC('DAY', sms.data_load_date) <= DATE('{end_date}') + interval '3' day))
AND DATE_TRUNC('DAY', sms.date_created) >= DATE('{start_date}')
AND DATE_TRUNC('DAY', sms.date_created) <= DATE('{end_date}')
"""
    else:
        raise ValueError("Invalid mode. Please enter either TRAILING or DATERANGE.")
    print(formatted_parameters)
    return formatted_parameters

# Get the formatted SQL parameter blocks from the user input

def connect_db(credentials):
    """Connect to PrestoDB and show tables using provided credentials."""
    conn = prestodb.dbapi.connect(
            host= credentials['host'],
            port= credentials['port'],
            user= credentials['user'],
            catalog='hive',
            schema='applied_ds',
            http_scheme='https',
            auth=prestodb.auth.BasicAuthentication(credentials['user'], credentials['password'])
    )
    cur = conn.cursor()
    print("connection successful")
    sql_query = cur.execute('select * from test   limit 2')


    print("Generated SQL Query:")
    print(sql_query)
    results = cur.fetchall()
    # Get the count of rows returned by the query
    row_count = cur.rowcount
    for row in results:
        print(row)


    print("Results have been written to result.csv")
    cur.close()
    conn.close()
    print("Connection successful")

# Load credentials from the JSON file
credentials = load_credentials('credentials/okta.json')

# Show tables in the specified schema using loaded credentials
print("running main function")
connect_db(credentials)


import json
import prestodb
import csv
import ast
import pandas as pd
def load_credentials(file_path):
    """Load credentials from a JSON file."""
    with open(file_path, 'r') as file:
        return json.load(file)


def get_block_1_parameters(input_1):
    """
    Prompts the user to enter a list of integers for the first parameter block.
    Returns the formatted string of integers wrapped in parentheses.
    """

    user_input = print(f"Enter a list of integers separated by commas (e.g. 3,50,3000):{input_1} ")
    try:
        # Convert the user input into a list of integers
        int_list = [int(val) for val in input_1]

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

def get_block_2_parameters(mode,start_date,end_date):
    """
    Prompts the user to choose between TRAILING and DATERANGE modes for the second parameter block.
    Collects the necessary inputs based on the chosen mode.
    Returns the formatted SQL condition string for the chosen mode.
    """
    mode = input_2
    print (mode)
    if mode == "TRAILING":
        print ("mode 1")
        formatted_parameters = f"""
(DATE_TRUNC('DAY', sms.data_load_date) >= DATE_TRUNC('DAY', CURRENT_DATE - interval '{start_date}' day)
AND DATE_TRUNC('DAY', sms.data_load_date) <= DATE_TRUNC('DAY', CURRENT_DATE - interval '{end_date - 3}' day))
AND DATE_TRUNC('DAY', sms.date_created) >= DATE_TRUNC('DAY', CURRENT_DATE - interval '{start_date}' day)
AND DATE_TRUNC('DAY', sms.date_created) < DATE_TRUNC('DAY', CURRENT_DATE - interval '{end_date}' day)
"""
    elif mode == "DATERANGE":
        print("mode 2")
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

def read_csv():
    # Read the CSV file into a DataFrame
    df = pd.read_csv('sms_data.csv')

    # Assuming the column with IDs is named 'id'
    id_list = df['id'].astype(str).tolist()
    if not id_list:
        # Return an empty query or a query without the id condition
        print("empty list")
        return ""

    # Format the list into a string for SQL
    id_list_str = ','.join(f"'{id}'" for id in id_list)
    print(id_list_str)
    # SQL query string with placeholder for IDs
    sql_query_template = "AND account_sid IN({ids})"

    # Format the SQL query with the IDs
    sql_query = sql_query_template.format(ids=id_list_str)
    print("csv_data")
    print(sql_query)
    return sql_query


# Get the formatted SQL parameter blocks from the user input
#Function to get date combinations from the user
def get_date_combinations(mode, input_3):
    if mode == "TRAILING":
        print(input_3)

        # combinations = processed_data.replace("(", "").replace(")", "").split(",")
        # [(int(combinations[i]), int(combinations[i+1])) for i in range(0, len(combinations), 2)]
        return input_3
    elif mode == "DATERANGE":
        combinations = input("Enter date combinations (e.g., (2023-11-20,2023-12-01),(2023-12-05,2023-12-10)): ").strip()
        combinations = combinations.replace("(", "").replace(")", "").split(",")
        return [(combinations[i].strip(), combinations[i+1].strip()) for i in range(0, len(combinations), 2)]

def connect_db(credentials, input_1, input_2, input_3):

    """Connect to PrestoDB and show tables using provided credentials."""
    conn = prestodb.dbapi.connect(
        host=credentials['host'],
        port=credentials['port'],
        user=credentials['user'],
        catalog='hive',
        schema='public',
        http_scheme='https',
        auth=prestodb.auth.BasicAuthentication(credentials['user'], credentials['password'])
    )
    cur = conn.cursor()
    parameter_block_1 = get_block_1_parameters(input_1)
    # Define the SQL query template with placeholders for the parameter blocks
    sql_query_template = """
    WITH windows AS (
    SELECT * FROM (
        VALUES
        {parameter_block_1}
    ) AS t(m_window)
),
full_mps_ingress AS (SELECT
CASE
    WHEN sms.called LIKE '%:%' THEN split(sms.called, ':')[1]
    WHEN bitwise_and(sms.flags , 512) != 0 THEN 'MMS'
    ELSE 'SMS'
END AS channel_name
,CASE
    WHEN sms.called LIKE '%:%'
        THEN 'Channel Number'
    WHEN bitwise_and(sms.flags,2048) != 0
        THEN 'Alpha Sender'
    WHEN bitwise_and(sms.flags,4096) != 0
        THEN 'Short Code'
    WHEN from_type='LC' AND from_category='TF'
        THEN 'Toll Free'
    ELSE 'Long Code'
END as phone_number_type
,'ingress' AS ingress_egress
,w.m_window AS m_window
,sms.provider_id as provider
,DATE_TRUNC('DAY',sms.date_created) AS date_day
,date_add('second',floor((hour(sms.date_created)*3600+minute(sms.date_created)*60+second(sms.date_created))/w.m_window)*w.m_window,DATE_TRUNC('DAY',sms.date_created)) as w_minute
,CASE WHEN bitwise_and(sms.flags,1) < 1
  THEN 'outbound' ELSE 'inbound' END AS in_out
,COALESCE(SUM(sms.num_segments), 0) AS segments
  ,COUNT(*) AS messages
  ,SUM(CASE WHEN sms.messaging_app_sid IS NULL THEN 0 ELSE sms.num_segments END) AS messaging_service_segments
,SUM(CASE WHEN sms.messaging_app_sid IS NULL THEN 0 ELSE 1 END) AS messaging_service_messages
FROM
public.rawpii_sms_kafka sms
CROSS JOIN windows w
WHERE
{parameter_block_2}
AND sms.to_cc IN ('US','CA')
{read_csv}

GROUP BY
1,2,3,4,5,6,7,8
),
fulll AS (
SELECT
channel_name
,phone_number_type
,date_day
,ingress_egress
,m_window
,w_minute
  ,in_out
,provider
,CAST(segments AS real)/CAST(m_window AS real) AS tps
,CAST(messages AS real)/CAST(m_window AS real) AS mps
,CAST(messaging_service_segments AS real)/CAST(m_window AS real) AS messaging_service_tps
,CAST(messaging_service_messages AS real)/CAST(m_window AS real) AS messaging_service_mps
FROM
full_mps_ingress
),
allchannel AS (
SELECT
'ALL' AS channel_name
,phone_number_type
,date_day
,ingress_egress
,m_window
,w_minute
    ,in_out
     ,provider
,COALESCE(SUM(tps), 0) AS tps
,COALESCE(SUM(mps), 0) AS mps
,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
allnumber AS (
SELECT
channel_name
,'ALL' AS phone_number_type
,date_day
,ingress_egress
,m_window
,w_minute
    ,in_out
     ,provider
,COALESCE(SUM(tps), 0) AS tps
,COALESCE(SUM(mps), 0) AS mps
,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
allnumberchannel AS (
SELECT
'ALL' AS channel_name
,'ALL' AS phone_number_type
,date_day
,ingress_egress
,m_window
,w_minute
    ,in_out
     ,provider
,COALESCE(SUM(tps), 0) AS tps
,COALESCE(SUM(mps), 0) AS mps
,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
allchannelprov AS (
SELECT
'ALL' AS channel_name
,phone_number_type
,date_day
,ingress_egress
,m_window
,w_minute
    ,in_out
     ,-99 AS provider
,COALESCE(SUM(tps), 0) AS tps
,COALESCE(SUM(mps), 0) AS mps
,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
allnumberprov AS (
SELECT
channel_name
,'ALL' AS phone_number_type
,date_day
,ingress_egress
,m_window
,w_minute
    ,in_out
     ,-99 AS provider
,COALESCE(SUM(tps), 0) AS tps
,COALESCE(SUM(mps), 0) AS mps
,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
allprov AS (
SELECT
channel_name
,phone_number_type
,date_day
,ingress_egress
,m_window
,w_minute
    ,in_out
     ,-99 AS provider
,COALESCE(SUM(tps), 0) AS tps
,COALESCE(SUM(mps), 0) AS mps
,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    ),
allall AS (
SELECT
'ALL' AS channel_name
,'ALL' AS phone_number_type
,date_day
,ingress_egress
,m_window
,w_minute
    ,in_out
     ,-99 AS provider
,COALESCE(SUM(tps), 0) AS tps
,COALESCE(SUM(mps), 0) AS mps
,COALESCE(SUM(messaging_service_tps), 0) AS messaging_service_tps
,COALESCE(SUM(messaging_service_mps), 0) AS messaging_service_mps
FROM
fulll
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
    )

SELECT
channel_name
,phone_number_type
,provider
,ingress_egress
,date_day AS "date"
,m_window
,in_out
,MAX(tps) AS peak_tps
,MAX(mps) AS peak_mps
,MAX(messaging_service_tps) AS messaging_service_peak_tps
,MAX(messaging_service_mps) AS messaging_service_peak_mps
FROM
( SELECT * FROM fulll
    UNION ALL
 SELECT * FROM allchannel
    UNION ALL
SELECT * FROM allnumber
    UNION ALL
SELECT * FROM allnumberchannel
    UNION ALL
 SELECT * FROM allchannelprov
    UNION ALL
SELECT * FROM allnumberprov
    UNION ALL
SELECT * FROM allprov
    UNION ALL
SELECT * FROM allall)
GROUP BY
1,2,3,4,5,6,7

"""
    mode = input_2
    date_combinations = input_3
    print(date_combinations)
    all_data = pd.DataFrame()  # Initialize an empty DataFrame to store all results
    count =0
    for start_date, end_date in date_combinations:
        parameter_block_2 = get_block_2_parameters(mode, start_date, end_date)
        get_csv_data = read_csv()
        sql_query = sql_query_template.format(parameter_block_1=parameter_block_1,parameter_block_2 = parameter_block_2, read_csv =get_csv_data)
        print(f"Generated SQL Query:{count}")
        print(sql_query)
        cur.execute(sql_query)
        column_names = [desc[0] for desc in cur.description]
        results = cur.fetchall()

        # Convert results to DataFrame and append to all_data
        result_df = pd.DataFrame(results, columns=column_names)
        all_data = pd.concat([all_data, result_df], ignore_index=True)
        count = count+1
    conn.close()

    print(column_names)
    # Save all data to a CSV file
    print("all_data")
    print(all_data)
    all_data.to_csv('output.csv', index=False)

    cur.close()
    conn.close()
    print("Connection successful")

# Load credentials from the JSON file
credentials = load_credentials('credentials/okta.json')

# Show tables in the specified schema using loaded credentials
print("running main function")
def load_and_use_json():
    # Read the JSON file
    with open('data.json', 'r') as file:
        data = json.load(file)

    # Extract data
    values = data['values']
    choice = data['choice']
    combinations = data['combinations']

    # Convert combinations to list of tuples
    combinations = [tuple(comb) for comb in combinations]

    # Use the data
    print("Values:", values)
    print("Choice:", choice)
    print("Combinations:", combinations)
    return values, choice, combinations
input_1, input_2, input_3 = load_and_use_json()
connect_db(credentials,input_1, input_2, input_3 )

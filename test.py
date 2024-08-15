import prestodb
import csv

def execute_query_and_save_to_csv(host, port, user, catalog, schema, sql_query_template, parameter_block_1, parameter_block_2, csv_file_path):
    # Connect to the Presto database
    conn = prestodb.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
    )
    cur = conn.cursor()

    try:
        # Generate the SQL query
        sql_query = sql_query_template.format(parameter_block_1=parameter_block_1, parameter_block_2=parameter_block_2)

        # Print the generated SQL query for debugging
        print("Generated SQL Query:")
        print(sql_query)

        # Execute the SQL query
        cur.execute(sql_query)

        # Fetch all results from the executed query
        results = cur.fetchall()

        # Print the results in the console for debugging
        if results:
            for row in results:
                print(row)
        else:
            print("No results found.")

        # Get column names from the cursor description
        column_names = [desc[0] for desc in cur.description]

        # Write results to a CSV file
        with open(csv_file_path, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)

            # Write the column names as the first row
            csv_writer.writerow(column_names)

            # Write the data rows
            csv_writer.writerows(results)

        print(f"Results have been written to {csv_file_path}")

    except prestodb.exceptions.PrestoUserError as e:
        print(f"Presto user error: {e.message}")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

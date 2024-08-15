# Get the column names from the cursor
column_names = [desc[0] for desc in cur.description]
columns_definition = ', '.join([f'{col} VARCHAR' for col in column_names])  # Assuming all columns are VARCHAR for simplicity

# Drop the destination table if it exists
drop_table_query = f'DROP TABLE IF EXISTS {destination_table}'
print("Executing drop table query:")
print(drop_table_query)
cur.execute(drop_table_query)

# Create the new table
create_table_query = f'CREATE TABLE {destination_table} ({columns_definition})'
print("Executing create table query:")
print(create_table_query)
cur.execute(create_table_query)

# Insert the data into the new table
insert_values = ', '.join(['%s' for _ in column_names])
insert_query = f'INSERT INTO {destination_table} ({", ".join(column_names)}) VALUES ({insert_values})'

print("Inserting data into the new table:")
for row in results:
    cur.execute(insert_query, row)

cur.close()
conn.close()
print(f"Data successfully written to table '{destination_table}'")

if __name__ == "__main__":
    # Load credentials from the JSON file
    with open('credentials.json', 'r') as file:
        credentials = json.load(file)

    # Connect to the database and execute the query
    connect_db(credentials)
import os

import duckdb
import pandas as pd

"""
Task 1: Create a table in DuckDB
Now I simply create the table using the DDL, but in the real world we can use this setup for DB migrations.
* data-analysis.ipynb to analyze the data and create the DDL.
* No primary key needed for this test assignment, but IRL complex queries might need it.
"""
def create_table(conn):
    with open(os.path.join(os.path.dirname(__file__), "db/V1_create_electric_vehicles_table.sql"), "r") as f:
        conn.execute(f.read())

"""
Task 2: Insert data into the table without using DuckDB's built-in functions.
* I assume with no built-in functions, I can't use Duckdb's read_csv function.
* Legislative District is categorical and nullable, so I set it as a string.
"""
def load_data(conn, csv_file_path):
    # Read and sanitize the data
    df = pd.read_csv(
        csv_file_path,
        dtype={"Legislative District": "string"}
    ).rename(
        columns={
            'VIN (1-10)': 'vin',
            'County': 'county',
            'City': 'city',
            'State': 'state',
            'Postal Code': 'postal_code',
            'Model Year': 'model_year',
            'Make': 'make',
            'Model': 'model',
            'Electric Vehicle Type': 'electric_vehicle_type',
            'Clean Alternative Fuel Vehicle (CAFV) Eligibility': 'cafv_eligibility',
            'Electric Range': 'electric_range',
            'Base MSRP': 'base_msrp',
            'Legislative District': 'legislative_district',
            'DOL Vehicle ID': 'dol_vehicle_id',
            'Vehicle Location': 'vehicle_location',
            'Electric Utility': 'electric_utility',
            '2020 Census Tract': 'census_tract'
        }
    )
    df = df.where(pd.notnull(df), None)

    # Prepare insert statement
    data_tuples = list(df.itertuples(index=False, name=None))
    placeholders = ', '.join(['?'] * len(df.columns))
    insert_sql = f"INSERT INTO electric_vehicles ({', '.join(df.columns)}) VALUES ({placeholders})"

    conn.executemany(insert_sql, data_tuples)


# Task 3a: Count the number of electric cars per city.
def get_vehicle_count_per_city(conn):
    query = """
    SELECT city, COUNT(*) as vehicle_count
    FROM electric_vehicles
    GROUP BY city
    ORDER BY vehicle_count DESC
    LIMIT 10
    """
    query_result = conn.sql(query)
    query_result.show()
    return query_result.fetchdf()

# Task 3b: Find the top 3 most popular electric vehicles.
def get_top_3_vehicles(conn):
    query = """
    SELECT make, model, COUNT(*) as vehicle_count
    FROM electric_vehicles
    GROUP BY make, model
    ORDER BY vehicle_count DESC
    LIMIT 3
    """
    query_result = conn.sql(query)
    query_result.show()
    return query_result.fetchdf()

# Task 3c: Find the most popular electric vehicle in each postal code.
def get_popular_vehicle_per_postal_code(conn):
    query = """
    SELECT postal_code, make, model, COUNT(*) as vehicle_count
    FROM electric_vehicles
    GROUP BY postal_code, make, model
    ORDER BY vehicle_count DESC
    LIMIT 20
    """
    query_result = conn.sql(query)
    query_result.show()
    return query_result.fetchdf()

# Task 3d: Count the number of electric cars by model year. Write out the answer as parquet files partitioned by year.
def get_vehicle_count_per_model_year(conn):
    query = """
    SELECT model_year, COUNT(*) as vehicle_count
    FROM electric_vehicles
    GROUP BY model_year
    ORDER BY model_year
    """
    query_result = conn.sql(query)
    query_result.show()
    df = query_result.fetchdf()
    df.to_parquet(os.path.join(os.path.dirname(__file__), "output/vehicle_count_per_model_year.parquet"), partition_cols=["model_year"])
    return df

def main():
    conn = duckdb.connect(':memory:')
    create_table(conn)

    csv_file_path = os.path.join(os.path.dirname(__file__), "data/Electric_Vehicle_Population_Data.csv")
    load_data(conn, csv_file_path)

    print("Task 3a: Count the number of electric cars per city")
    get_vehicle_count_per_city(conn)

    print("Task 3b: Get the top 3 vehicles")
    get_top_3_vehicles(conn)

    print("Task 3c: Get the most popular vehicle per postal code")
    get_popular_vehicle_per_postal_code(conn)

    print("Task 3d: Get the vehicle count per model year")
    get_vehicle_count_per_model_year(conn)

    conn.close()

if __name__ == "__main__":
    main()

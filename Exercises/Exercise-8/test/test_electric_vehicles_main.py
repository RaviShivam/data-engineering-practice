import os
import sys
from unittest import mock

import duckdb
import pandas as pd
import pytest

# Not a proper python package, so I need to append the path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from main import (create_table, load_data, get_vehicle_count_per_city,
                  get_top_3_vehicles, get_popular_vehicle_per_postal_code,
                  get_vehicle_count_per_model_year)


@pytest.fixture(scope='session')
def sample_csv_path():
    return os.path.join(os.path.dirname(__file__), "sample-data/electric-vehicles.csv")


@pytest.fixture(scope='session')
def conn():
    connection = duckdb.connect(':memory:')
    yield connection
    connection.close()


@pytest.fixture(scope='session')
def setup_database(conn, sample_csv_path):
    create_table(conn)
    load_data(conn, sample_csv_path)


@pytest.fixture(scope='session')
def expected_data(sample_csv_path):
    sample_data = pd.read_csv(
        sample_csv_path,
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
    expected_df = sample_data.where(pd.notnull(sample_data), None)
    return expected_df


class TestElectricVehicles:
    def test_create_table(self, conn):
        create_table(conn)

        result = conn.sql("PRAGMA show_tables").fetchall()
        assert ('electric_vehicles',) in result

        result_df = conn.sql("DESCRIBE electric_vehicles").fetchdf()
        expected_columns = [
            'vin', 'county', 'city', 'state', 'postal_code', 'model_year', 'make', 'model',
            'electric_vehicle_type', 'cafv_eligibility', 'electric_range', 'base_msrp',
            'legislative_district', 'dol_vehicle_id', 'vehicle_location', 'electric_utility', 'census_tract'
        ]
        assert list(result_df['column_name']) == expected_columns

    def test_load_data(self, conn, setup_database, expected_data):
        result_df = conn.execute("SELECT * FROM electric_vehicles").fetchdf()

        result_df = result_df.astype(expected_data.dtypes.to_dict())

        pd.testing.assert_frame_equal(
            result_df.sort_values('dol_vehicle_id').reset_index(drop=True),
            expected_data.sort_values('dol_vehicle_id').reset_index(drop=True)
        )

    def test_get_vehicle_count_per_city(self, conn, setup_database):
        result_df = get_vehicle_count_per_city(conn)

        expected_df = pd.DataFrame({
            'city': ['Eugene', 'San Diego', 'Yakima'],
            'vehicle_count': [1, 1, 1]
        })

        result_df = result_df.astype(expected_df.dtypes.to_dict())

        pd.testing.assert_frame_equal(
            result_df.sort_values('city').reset_index(drop=True),
            expected_df.sort_values('city').reset_index(drop=True)
        )

    def test_get_top_3_vehicles(self, conn, setup_database):
        result_df = get_top_3_vehicles(conn)

        expected_df = pd.DataFrame({
            'make': ['TESLA', 'VOLVO'],
            'model': ['MODEL 3', 'S60'],
            'vehicle_count': [2, 1]
        })

        result_df = result_df.astype(expected_df.dtypes.to_dict())

        pd.testing.assert_frame_equal(
            result_df.sort_values('make').reset_index(drop=True),
            expected_df.sort_values('make').reset_index(drop=True)
        )

    def test_get_popular_vehicle_per_postal_code(self, conn, setup_database):
        result_df = get_popular_vehicle_per_postal_code(conn)

        expected_df = pd.DataFrame({
            'postal_code': ['92101', '97404', '98908'],
            'make': ['TESLA', 'VOLVO', 'TESLA'],
            'model': ['MODEL 3', 'S60', 'MODEL 3'],
            'vehicle_count': [1, 1, 1]
        })

        result_df = result_df.astype(expected_df.dtypes.to_dict())

        pd.testing.assert_frame_equal(
            result_df.sort_values('postal_code').reset_index(drop=True),
            expected_df.sort_values('postal_code').reset_index(drop=True)
        )

    def test_get_vehicle_count_per_model_year(self, conn, setup_database):
        # Mock to prevent actual file writing
        with mock.patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            result_df = get_vehicle_count_per_model_year(conn)
            assert mock_to_parquet.called

        expected_df = pd.DataFrame({
            'model_year': [2019, 2020],
            'vehicle_count': [1, 2]
        })

        result_df = result_df.astype(expected_df.dtypes.to_dict())

        pd.testing.assert_frame_equal(
            result_df.sort_values('model_year').reset_index(drop=True),
            expected_df.sort_values('model_year').reset_index(drop=True)
        )

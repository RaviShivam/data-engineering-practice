CREATE TABLE IF NOT EXISTS electric_vehicles (
    vin VARCHAR,
    county VARCHAR,
    city VARCHAR,
    state CHAR(2),
    postal_code VARCHAR,
    model_year INTEGER,
    make VARCHAR,
    model VARCHAR,
    electric_vehicle_type VARCHAR,
    cafv_eligibility VARCHAR,
    electric_range INTEGER,
    base_msrp INTEGER,
    legislative_district VARCHAR,
    dol_vehicle_id VARCHAR,
    vehicle_location VARCHAR,
    electric_utility VARCHAR,
    census_tract VARCHAR
);

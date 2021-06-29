
# Create tables
create_date_dim = """
CREATE TABLE dateDim (
    dateId Varchar PRIMARY KEY,
    "date" Date NOT NULL,
    "year" Int NOT NULL,
    "month" Int NOT NULL,
    "day" Int NOT NULL,
    "dayOfWeek" Int NOT NULL,
    "weekOfYear" Int NOT NULL
)
"""
        
create_location_dim = """
CREATE TABLE locationDim (
    locationId Varchar PRIMARY KEY,
    city Varchar NOT NULL,
    state Varchar NOT NULL,
    country Varchar NOT NULL,
    latitude Varchar NOT NULL,
    longitude Varchar NOT NULL,
    totalPopulation Varchar NOT NULL
)
"""

create_temperature_fact = """
CREATE TABLE temperatureFact (
    temperatureId BigInt PRIMARY KEY,
    dateId Varchar NOT NULL,
    locationId Varchar NOT NULL,
    avgTemp FLOAT NOT NULL,
    avgTempUncert FLOAT NOT NULL
)
"""

create_tables = [create_date_dim, create_location_dim, create_temperature_fact]

# Drop tables
drop_table = "DROP TABLE IF EXISTS {table}"

drop_date_dim = drop_table.format(table="dateDim")

drop_location_dim = drop_table.format(table="locationDim")

drop_temperature_fact = drop_table.format(table="temperatureFact")

drop_tables = [drop_date_dim, drop_location_dim, drop_temperature_fact]


# Copy from s3 to Redshift
copy_to_table = """
    COPY {table}
    FROM '{{aws_bucket}}/{{aws_key}}/{source}'
    CREDENTIALS
    'aws_access_key_id={{access_key_id}};aws_secret_access_key={{secret_access_key}}'
    FORMAT AS PARQUET;
"""
copy_to_date_dim = copy_to_table.format(table="dateDim", source="dateDim")
copy_to_location_dim = copy_to_table.format(table="locationDim", source="locationDim")
copy_to_temperature_fact = copy_to_table.format(table="temperatureFact", source="temperatureFact")

copy_tables = [copy_to_date_dim, copy_to_location_dim, copy_to_temperature_fact]

# Quality Checks
data_exists = "SELECT COUNT(*) FROM {table}"

null_constraint_check = "Select {column} FROM {table} Where {column} is Null"
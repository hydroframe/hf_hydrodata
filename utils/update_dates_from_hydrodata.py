"""
Program to update the date in the data catalog entry table and dataset table using
The latest dates available by looking at files in /hydrodata.

This is called by a jenkins job after the data acquisition jobs run to look at the most
recent acquired files and get the latest dates available to store in the data catalog.
"""

# pylint: disable=C0301,E0401,W0212,R0914
import os
import datetime
import re
import xarray as xr
import hf_hydrodata as hf
import public_release

SCHEMA = os.getenv("DC_SCHEMA", "public")


def main():
    """Main routine to collect dates and update catalog"""

    connection = public_release._get_connection(SCHEMA)
    _update_soil_moisture_dates(connection)
    _update_anomoly_dates(connection)


def _update_soil_moisture_dates(connection):
    """
    Update entries for CONUS2 current conditions dates in the SQL db of the connection.
    """

    start_date = datetime.datetime.strptime("2023-08-12", "%Y-%m-%d")
    end_date = start_date
    # We need to update the dates because applicable entries are in the result dict
    wy = 2024
    while True:
        # Find the latest files in the water year folder
        soil_moisture_wy_path = f"/hydrodata/HydroGEN/current_conditions/CONUS2/WY{wy}"
        if not os.path.exists(soil_moisture_wy_path):
            # if no water year folder exists then we already found the latest date
            break
        # Look at all the file names in the folder to find latest soil moisture date
        for file_name in os.listdir(soil_moisture_wy_path):
            if file_name.startswith("soil_moisture."):
                date_str = file_name[
                    len("soil_moisture.") : len("soil_moisture.mmddYYYY")
                ]
                dt = datetime.datetime.strptime(date_str, "%m%d%Y")
                end_date = max(end_date, dt)

        wy = wy + 1

    end_date_str = end_date.strftime("%Y-%m-%d")
    sm_entry_id = "213"
    sql = f"UPDATE {SCHEMA}.data_catalog_entry SET entry_end_date='{end_date_str}' where id='{sm_entry_id}'"
    public_release._execute_sql(connection, sql)
    print(
        f"Updated {SCHEMA}.data_catalog_entry id '{sm_entry_id}' entry_end_date='{end_date_str}'"
    )
    dataset_id = "conus2_current_conditions"
    sql = f"UPDATE {SCHEMA}.dataset SET dataset_end_date='{end_date_str}' where id='{dataset_id}'"
    public_release._execute_sql(connection, sql)
    print(
        f"Updated {SCHEMA}.dataset id '{dataset_id}' dataset_end_date='{end_date_str}'"
    )


def _update_anomoly_dates(connection):
    """
    Update the data_catalog_entry rows in the data catalog for dataset obs_amomalies.
    Update the entry_end_date columns with the latest end dates for the respective anomolies."
    """

    dataset = "obs_anomalies"
    variables = [
        "anomaly",
        "anomaly_daily_week_of_values",
        "streamflow",
        "swe",
        "water_table_depth",
    ]
    periods = ["daily", "weekly", "monthly"]
    dates_memo = {}

    for variable in variables:
        for period in periods:
            data_entries = hf.get_catalog_entries(
                None, dataset=dataset, variable=variable, period=period
            )
            for entry in data_entries:
                data_entry_id = entry["id"]
                site_type = entry["site_type"]
                dataset_var = entry["dataset_var"]

                path = entry["path"]
                data_files = os.listdir(path.replace("/WY{wy}.nc", ""))
                last_year = None
                for file in data_files:
                    # Skip the file if it is not an anomaly file
                    if not re.match(r"(WY)(\d)+(.nc)", file):
                        continue
                    # Get the water year from the file name
                    water_year = re.sub(r"\D", "", file)
                    if last_year is None or int(water_year) > last_year:
                        last_year = int(water_year)
                last_year_dataset = xr.open_dataset(path.format(wy=last_year))

                # Get the last non NaN value from the dataset
                last_year_data_frame = last_year_dataset.to_dataframe()

                # Need to make the dataframe singley indexed and sorted by date
                current_entry = (
                    last_year_data_frame[dataset_var]
                    .reset_index()
                    .set_index("date")
                    .drop(columns="site")
                    .sort_values("date")
                    .last_valid_index()
                )
                # If no valid data check the previous water year
                if not current_entry:
                    last_year -= 1
                    last_year_dataset = xr.open_dataset(path.format(wy=last_year))
                    current_entry = last_year_dataset.to_dataframe().last_valid_index()

                if current_entry and isinstance(current_entry, str):
                    end_date = datetime.datetime.strptime(current_entry, "%Y-%m-%d")
                    end_date_str = end_date.strftime("%Y-%m-%d")

                    # Update the date of the period for the site type in the dates_memo map
                    if not dates_memo.get(site_type):
                        dates_memo[site_type] = {}
                    dates_memo[site_type][period] = end_date.strftime("%m/%d/%Y")

                    # Update the data catalog rows
                    sql = f"UPDATE {SCHEMA}.data_catalog_entry SET entry_end_date='{end_date_str}' where id = '{data_entry_id}'"
                    public_release._execute_sql(connection, sql)
                    print(
                        f"Updated {SCHEMA}.data_catalog_entry id '{data_entry_id}' entry_end_date='{end_date_str}'"
                    )


if __name__ == "__main__":
    main()

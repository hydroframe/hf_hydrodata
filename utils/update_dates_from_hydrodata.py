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
from glob import glob
import calendar
import xarray as xr
import pandas as pd
import hf_hydrodata as hf
import public_release

SCHEMA = os.getenv("DC_SCHEMA", "public")


def main():
    """Main routine to collect dates and update catalog"""

    connection = public_release._get_connection(SCHEMA)
    _update_soil_moisture_dates(connection)
    _update_anomoly_dates(connection)
    _update_cw3e_dates(connection)
    _update_conus21_baseline_dates(connection)


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


def _update_cw3e_dates(connection):
    """
    Update the entry_end_date column in the data_catalog_entry table for dataset CW3E, version 1.0.
    Update the dataset_end_date in the dataset table. If different variables have different end dates,
    choose the earliest one."
    """

    dataset = "CW3E"
    variables = [
        "air_temp",
        "east_windspeed",
        "north_windspeed",
        "precipitation",
        "specific_humidity",
        "atmospheric_pressure",
        "downward_longwave",
        "downward_shortwave",
    ]

    periods = ["hourly", "daily"]

    # Keep track of all end dates (if they end up being different) for
    # setting the overall dataset end date as the minimum date available
    all_dates = {}

    for variable in variables:
        for period in periods:
            dataset_entries = hf.get_catalog_entries(
                dataset=dataset, variable=variable, period=period, dataset_version="1.0"
            )

            for entry in dataset_entries:
                data_entry_id = entry["id"]
                dataset_var = entry["dataset_var"]
                path = entry["path"]

                # Start from 1/1/2025 to prevent the need to search through all 40+ water year directories
                # Based on when this script was developed, we know the end date will be at least 1/1/2025.
                # This start date may be adjusted to a later date if the script becomes slow from searching too
                # many water year directories
                start_date = datetime.datetime.strptime("2025-01-01", "%Y-%m-%d")
                end_date = start_date
                wy = 2025

                if period == "hourly":
                    path = path.replace(
                        "/CW3E.{dataset_var}.{wy_start_24hr:06d}_to_{wy_end_24hr:06d}.pfb",
                        "",
                    )

                    while True:
                        # Find the latest files in the water year folder
                        wy_path = path.replace("{wy}", f"{wy}")

                        if not os.path.exists(wy_path):
                            # If no water year folder exists then we already found the latest date
                            break

                        # Look at all the file names in the folder to find latest date
                        data_files = sorted(glob(f"{wy_path}/CW3E.{dataset_var}.*"))
                        latest_file = data_files[-1]
                        latest_timestep = latest_file.split("_to_")[-1].split(".")[0]

                        # Read in file for converting date to water year timestep
                        if calendar.isleap(wy):
                            date_xwalk = pd.read_csv(
                                "/hydrodata/forcing/raw_data/CW3E/reference/to_water_year_leap_year.csv",
                                dtype=str,
                            )
                        else:
                            date_xwalk = pd.read_csv(
                                "/hydrodata/forcing/raw_data/CW3E/reference/to_water_year.csv",
                                dtype=str,
                            )
                        xwalk_row = date_xwalk.loc[
                            date_xwalk["timestep_end"] == latest_timestep
                        ]
                        latest_date = xwalk_row["date"].iloc[0]

                        latest_day = int(latest_date[2:])
                        latest_month = int(latest_date[:2])

                        if xwalk_row["water_year"].iloc[0] == "same":
                            latest_year = wy
                        else:
                            latest_year = wy - 1

                        dt = datetime.datetime(latest_year, latest_month, latest_day)
                        end_date = max(end_date, dt)

                        wy = wy + 1

                elif period == "daily":
                    if variable in ["air_temp", "atmospheric_pressure"]:
                        dataset_var = dataset_var.split("_")[0]
                    path = "/".join(path.split("/")[:-1])

                    while True:
                        # Find the latest files in the water year folder
                        wy_path = path.replace("{wy}", f"{wy}")

                        if not os.path.exists(wy_path):
                            # If no water year folder exists then we already found the latest date
                            break

                        # Look at all the file names in the folder to find latest date
                        data_files = sorted(glob(f"{wy_path}/CW3E.{dataset_var}.*"))
                        latest_file = data_files[-1]
                        day_of_wy = str(int(latest_file.split(".")[-2]))

                        # Read in file for converting date to water year timestep
                        if calendar.isleap(wy):
                            date_xwalk = pd.read_csv(
                                "/hydrodata/forcing/raw_data/CW3E/reference/to_water_year_leap_year.csv",
                                dtype=str,
                            )
                        else:
                            date_xwalk = pd.read_csv(
                                "/hydrodata/forcing/raw_data/CW3E/reference/to_water_year.csv",
                                dtype=str,
                            )
                        xwalk_row = date_xwalk.loc[
                            date_xwalk["day_of_water_year"] == day_of_wy
                        ]
                        latest_date = xwalk_row["date"].iloc[0]

                        latest_day = int(latest_date[2:])
                        latest_month = int(latest_date[:2])

                        if xwalk_row["water_year"].iloc[0] == "same":
                            latest_year = wy
                        else:
                            latest_year = wy - 1

                        dt = datetime.datetime(latest_year, latest_month, latest_day)
                        end_date = max(end_date, dt)

                        wy = wy + 1

                # Update values in data catalog entry table
                end_date_str = end_date.strftime("%Y-%m-%d")

                sql = f"UPDATE {SCHEMA}.data_catalog_entry SET entry_end_date='{end_date_str}' where id = '{data_entry_id}'"
                public_release._execute_sql(connection, sql)
                print(
                    f"Updated {SCHEMA}.data_catalog_entry id '{data_entry_id}' entry_end_date='{end_date_str}'"
                )

                # Update the date of the variable in the all_dates dict
                if not all_dates.get(variable):
                    all_dates[variable] = end_date
                else:
                    all_dates[variable] = min(all_dates[variable], end_date)

    # Update value in dataset table
    dataset_end_date = min(all_dates.values())
    dataset_end_date_str = dataset_end_date.strftime("%Y-%m-%d")

    dataset_id = "CW3E"
    sql = f"UPDATE {SCHEMA}.dataset SET dataset_end_date='{dataset_end_date_str}' where id='{dataset_id}'"
    public_release._execute_sql(connection, sql)
    print(
        f"Updated {SCHEMA}.dataset id '{dataset_id}' dataset_end_date='{dataset_end_date_str}'"
    )

def _update_conus21_baseline_dates(connection):
    """
    Update the entry_end_date column in the data_catalog_entry table 
    for dataset conus2_baseline, dataset_version 2.1.
    Update the dataset_end_date in the dataset table. If different
    variables have different end dates, choose the earliest one.
    """

    dataset = "conus2_baseline"
    variables = {
        "pressure_head": ["hourly"],
        "saturation": ["hourly"],
        "streamflow": ["daily"],
        "soil_moisture": ["daily"],
        "subsurface_storage": ["daily"],
        "surface_water_storage": ["daily"],
        "water_table_depth": ["daily"],
        "evapotranspiration": ["hourly", "daily"],
        "ground_evap": ["hourly", "daily"],
        "ground_evap_heat": ["hourly"],
        "ground_heat": ["hourly"],
        "ground_temp": ["hourly", "daily"],
        "infiltration": ["hourly"],
        "irrigation": ["hourly"],
        "latent_heat": ["hourly", "daily"],
        "outward_longwave_radiation": ["hourly"],
        "sensible_heat": ["hourly", "daily"],
        "soil_temp": ["hourly", "daily"],
        "swe": ["hourly", "daily"],
        "transpiration": ["hourly", "daily"],
        "transpiration_leaves": ["hourly"],
    }

    all_dates = {}

    for variable, periods in variables.items():
        for period in periods:
            entry = hf.get_catalog_entry(
                dataset=dataset, variable=variable, period=period, dataset_version="2.1"
            )

            data_entry_id = entry["id"]
            dataset_var = entry["dataset_var"]
            path = entry["path"]
            dir_path = "/".join(path.split("/")[:-1])

            # Start from 1/1/2025 to prevent the need to search through all 40+ water year directories
            # Based on when this script was developed, we know the end date will be at least 1/1/2025.
            # This start date may be adjusted to a later date if the script becomes slow from searching too
            # many water year directories
            start_date = datetime.datetime.strptime("2024-01-01", "%Y-%m-%d")
            end_date = start_date
            wy = 2024

            if period == "hourly":
                if variable not in ["pressure_head", "saturation"]:
                    dataset_var = "clm_output"

                while True:
                    # Find the latest files in the water year folder
                    wy_path = dir_path.replace("{wy}", f"{wy}")

                    if not os.path.exists(wy_path):
                        # If no water year folder exists then we already found the latest date
                        break

                    # Look at all the file names in the folder to find latest date
                    data_files = sorted(glob(f"{wy_path}/conus21.wy{wy}.out.{dataset_var}.*"))
                    latest_file = data_files[-1]
                    file_name = latest_file.split('/')[-1]
                    pattern = re.compile(r'\.(\d{5})\.?(?:C\.)?pfb(?:\.dist)?$')
                    latest_timestep = int(pattern.search(file_name).group(1))

                    dt = datetime.datetime(wy-1, 10, 1) + datetime.timedelta(hours=latest_timestep-1)
                    end_date = max(end_date, dt)

                    wy = wy + 1

            elif period == "daily":
                while True:
                    # Find the latest files in the water year folder
                    wy_path = dir_path.replace("{wy}", f"{wy}")

                    if not os.path.exists(wy_path):
                        # If no water year folder exists then we already found the latest date
                        break

                    # Look at all the file names in the folder to find latest date
                    data_files = sorted(glob(f"{wy_path}/{dataset_var}.{wy}.daily.*.pfb"))
                    latest_file = data_files[-1]
                    file_name = latest_file.split('/')[-1]
                    pattern = re.compile(r'daily\.(\d{3})\.pfb$')
                    latest_timestep = int(pattern.search(file_name).group(1))

                    dt = datetime.datetime(wy-1, 10, 1) + datetime.timedelta(days=latest_timestep-1)
                    end_date = max(end_date, dt)

                    wy = wy + 1

            # Update values in data catalog entry table
            end_date_str = end_date.strftime("%Y-%m-%d")

            sql = f"UPDATE {SCHEMA}.data_catalog_entry SET entry_end_date='{end_date_str}' where id = '{data_entry_id}'"
            public_release._execute_sql(connection, sql)
            print(
                f"Updated {SCHEMA}.data_catalog_entry id '{data_entry_id}' entry_end_date='{end_date_str}'"
            )

            # Update the date of the variable in the all_dates dict
            if not all_dates.get(variable):
                all_dates[variable] = end_date
            else:
                all_dates[variable] = min(all_dates[variable], end_date)

    # Update value in dataset table
    dataset_end_date = min(all_dates.values())
    dataset_end_date_str = dataset_end_date.strftime("%Y-%m-%d")

    dataset_id = "conus2_baseline"
    sql = f"UPDATE {SCHEMA}.dataset SET dataset_end_date='{dataset_end_date_str}' where id='{dataset_id}'"
    public_release._execute_sql(connection, sql)
    print(
        f"Updated {SCHEMA}.dataset id '{dataset_id}' dataset_end_date='{dataset_end_date_str}'"
    )

if __name__ == "__main__":
    main()

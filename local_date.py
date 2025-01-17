'''
Reads 3 Veraset files on sequential days, which are split based on a universal UTC timestamp, and creates a new "visits" file based on the local timestamp of each user.
Need to configure 'month', 'day', and 'year' variables, as well as the list for the sequential days.
'''

import pandas as pd
import os
from datetime import datetime
import sys

month_conv = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9,
              "oct": 10, "nov": 11, "dec": 12}

# Create a new pandas column that contains the date based on the local time of each user
def get_local_date(path, date, subset):
    df = pd.read_csv(path, usecols = subset)
    df['local_date'] = pd.to_datetime(df['local_timestamp'], unit='s').dt.date
    df['local_date'] = pd.to_datetime(df['local_date'])
    df = df[df["local_date"] == date]
    return df

if __name__ == "__main__":

    month = "feb"
    day = "10"
    year = "2020"
    path = f"/home/george/data/Veraset/Visits/{year}/{month}".format()

    subset = ["utc_timestamp", "local_timestamp", "caid", "location_name", "top_category", "sub_category",
               "city", "state", "street_address", "zip_code", "census_block_group", "minimum_dwell"]

    output = pd.DataFrame()
    date = datetime(int(year), month_conv[month], int(day))

    counter = 0
    for temp_day in ["09", "10", "11"]:
        files = os.listdir(os.path.join(path, temp_day)) # Veraset provides 10 visit files for each day
        files = [os.path.join(path, temp_day, file) for file in files if file[0] != "_"]
        files.sort()
        for file in files:
            res = get_local_date(file, date, subset)
            output = pd.concat([output, res])
            counter += 1
            print(counter)

    output.reset_index(inplace=True, drop=True)
    output.to_parquet(f"/home/george/data/Veraset/Visits/local_dataset/{year}/{month}/{day}.parquet")

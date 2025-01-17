'''
Chooses the days based on whether someone can be considered an exerciser.
Chooses the days for which to make the comparison of visits for the two groups.
Saves metadata for exercisers and non_exercisers, on their visits to different categories for each day.
Also saves total number of users in each group for each day, to be able to calculate conf. intervals.
'''

import os
import pandas as pd
import pickle
import plotly.graph_objects as go
import statsmodels.api as sm

def conf_interval(total_true, total_population, a = 0.01, method = 'normal'):
    return sm.stats.proportion_confint(total_true, total_population, a, method)

year = "2019"
month = "dec"
days_exercisers = ["02", "03", "04", "05", "06", "07", "08"]
base_folder = "/home/george/data/Veraset/Visits/local_dataset"

determine_exercisers = []
for day in days_exercisers:
    read_exercisers = f"/home/george/code/Fitness/metadata/exercisers/{year}/{month}/{day}.pkl"
    with open(read_exercisers, "rb") as f:
        determine_exercisers.extend(pickle.load(f))
determine_exercisers = set(determine_exercisers) # At least 1 exercise session considers an exerciser

days_comparison = ["02", "03", "04", "05", "06", "07", "08"]

# Categories for which the comparisons were made. Only the most significant were shown in the manuscript.
categories = ["Full-Service Restaurants", "Pharmacies and Drug Stores", "Snack and Nonalcoholic Beverage Bars",
              "Gasoline Stations with Convenience Stores", "All Other General Merchandise Stores",
              "Nature Parks and Other Similar Institutions", "Department Stores", "Hardware Stores",
              "Food (Health) Supplement Stores", "Musical Instrument and Supplies Stores", "Diet and Weight Reducing Centers",
              "Correctional Institutions", "Limited-Service Restaurants"]

restaurants_fit = []
pharmacies_fit = []
beverage_fit = []
total_fit = []
restaurants_non_fit = []
pharmacies_non_fit = []
beverage_non_fit = []
total_non_fit = []
gas_fit = []
gas_non_fit = []
merchandise_fit = []
merchandise_non_fit = []
park_fit = []
park_non_fit = []
department_fit = []
department_non_fit = []
hardware_fit = []
limited_fit = []
hardware_non_fit = []
food_fit = []
food_non_fit = []
music_fit = []
music_non_fit = []
diet_fit = []
diet_non_fit = []
correctional_fit = []
correctional_non_fit = []
limited_non_fit = []

fits = [restaurants_fit,pharmacies_fit,beverage_fit,gas_fit,merchandise_fit,park_fit,
        department_fit,hardware_fit,food_fit,music_fit,diet_fit,correctional_fit,limited_fit]
non_fits = [restaurants_non_fit,pharmacies_non_fit,beverage_non_fit,gas_non_fit,
            merchandise_non_fit,park_non_fit,department_non_fit,hardware_non_fit,
            food_non_fit,music_non_fit,diet_non_fit,correctional_non_fit, limited_non_fit]

for day in days_comparison:
    print(day)
    temp_df = pd.read_parquet(os.path.join(base_folder, f"{year}/{month}/{day}.parquet"))
    temp_df = temp_df.dropna(subset=["sub_category"])
    active_df = temp_df[temp_df["caid"].isin(determine_exercisers)]
    todays_exercisers = active_df[active_df["sub_category"] == "Fitness and Recreational Sports Centers"]["caid"].unique()
    active_df = active_df[~active_df["caid"].isin(todays_exercisers)]
    for i, cat in enumerate(categories):
        fits[i].append(len(active_df[active_df["sub_category"] == cat]))
    total_fit.append(len(active_df))
    non_active_df = temp_df[~temp_df["caid"].isin(determine_exercisers)]
    for i, cat in enumerate(categories):
        non_fits[i].append(len(non_active_df[non_active_df["sub_category"] == cat]))
    total_non_fit.append(len(non_active_df))

### Save the results here. To be loaded in another file for processing
with open('feb-03-09-visits.pkl', 'wb') as f:
    pickle.dump([fits, non_fits, total_fit,total_non_fit], f)

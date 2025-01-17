'''
Choose three days for which exercisers will be chosen. Choose the non-exercise day to find substitutions.
Saves the metadata for all subcategories, to be opened in a separate file an analyze.
'''


import pandas as pd
import os
import pickle
from collections import Counter, defaultdict
import matplotlib.pyplot as plt

year = "2019"
month = "dec"
exercise_days = ["02", "03", "04"]
non_exercise_days = ["05"]
base_folder = "/home/george/data/Veraset/Visits/local_dataset"

questioned_sub_categories = ["Full-Service Restaurants", "Limited-Service Restaurants", "Nature Parks and Other Similar Institutions", "Snack and Nonalcoholic Beverage Bars"]

exercisers_at_least_1_day  = []
exercisers_at_least_2_days = []
exercisers_at_least_3_days = []

# Previous day
path = f"/home/george/code/Fitness/metadata/exercisers/{year}/{month}/{exercise_days[-1]}.pkl"
with open(path, "rb") as f:
    exercisers_at_least_1_day = pickle.load(f)
# -2 day
path = f"/home/george/code/Fitness/metadata/exercisers/{year}/{month}/{exercise_days[-2]}.pkl"
with open(path, "rb") as f:
    exercisers_at_least_2_days = pickle.load(f)
    exercisers_at_least_2_days = list(set(exercisers_at_least_1_day).intersection(exercisers_at_least_2_days))
# -3 day
path = f"/home/george/code/Fitness/metadata/exercisers/{year}/{month}/{exercise_days[-3]}.pkl"
with open(path, "rb") as f:
    exercisers_at_least_3_days = pickle.load(f)
    exercisers_at_least_3_days = list(set(exercisers_at_least_2_days).intersection(exercisers_at_least_3_days))
print(len(exercisers_at_least_1_day))
print(len(exercisers_at_least_2_days))
print(len(exercisers_at_least_3_days))

# Need to remove from each the exercisers of the "test" day
path = f"/home/george/code/Fitness/metadata/exercisers/{year}/{month}/{non_exercise_days[0]}.pkl"
with open(path, "rb") as f:
    test_exercisers = pickle.load(f)

exercisers_at_least_1_day = list(set(exercisers_at_least_1_day).difference(test_exercisers))
exercisers_at_least_2_days = list(set(exercisers_at_least_2_days).difference(test_exercisers))
exercisers_at_least_3_days = list(set(exercisers_at_least_3_days).difference(test_exercisers))

### Remove from previous day the exercisers of -2
exercisers_at_least_1_day = list(set(exercisers_at_least_1_day)-set(exercisers_at_least_2_days))
print(len(exercisers_at_least_1_day))
### Remove from -2 day the exercisers of -3
exercisers_at_least_2_days = list(set(exercisers_at_least_2_days) - set(exercisers_at_least_3_days))

# Counter for each subcategory
group_1_day_visits  = defaultdict(list)
group_1_day_total   = []
group_2_days_visits = defaultdict(list)
group_2_days_total  = []
group_3_days_visits = defaultdict(list)
group_3_days_total  = []

# Read oldest day
df = pd.read_parquet(os.path.join(base_folder, f"{year}/{month}/{exercise_days[0]}.parquet"))
df = df.dropna(subset=['sub_category']) # remove home and work
df = df[df["caid"].isin(exercisers_at_least_3_days)]
df = df[df["sub_category"] != "Fitness and Recreational Sports Centers"]
group_3_days_total.append(len(df))
for cat in questioned_sub_categories:
    group_3_days_visits[cat].append(len(df[df["sub_category"] == cat]))
print("Finished first day")

# Read 2nd day
df = pd.read_parquet(os.path.join(base_folder, f"{year}/{month}/{exercise_days[1]}.parquet"))
df = df.dropna(subset=['sub_category']) # remove home and work
df = df[df["sub_category"] != "Fitness and Recreational Sports Centers"]
temp_df = df[df["caid"].isin(exercisers_at_least_3_days)]
group_3_days_total.append(len(temp_df))
for cat in questioned_sub_categories:
    group_3_days_visits[cat].append(len(temp_df[temp_df["sub_category"] == cat]))
temp_df = df[df["caid"].isin(exercisers_at_least_2_days)]
group_2_days_total.append(len(temp_df))
for cat in questioned_sub_categories:
    group_2_days_visits[cat].append(len(temp_df[temp_df["sub_category"] == cat]))
print("Finished second day")

# Read 3rd day
df = pd.read_parquet(os.path.join(base_folder, f"{year}/{month}/{exercise_days[2]}.parquet"))
df = df.dropna(subset=['sub_category']) # remove home and work
df = df[df["sub_category"] != "Fitness and Recreational Sports Centers"]
temp_df = df[df["caid"].isin(exercisers_at_least_3_days)]
group_3_days_total.append(len(temp_df))
for cat in questioned_sub_categories:
    group_3_days_visits[cat].append(len(temp_df[temp_df["sub_category"] == cat]))
temp_df = df[df["caid"].isin(exercisers_at_least_2_days)]
group_2_days_total.append(len(temp_df))
for cat in questioned_sub_categories:
    group_2_days_visits[cat].append(len(temp_df[temp_df["sub_category"] == cat]))
temp_df = df[df["caid"].isin(exercisers_at_least_1_day)]
group_1_day_total.append(len(temp_df))
for cat in questioned_sub_categories:
    group_1_day_visits[cat].append(len(temp_df[temp_df["sub_category"] == cat]))

# Read final day
df = pd.read_parquet(os.path.join(base_folder, f"{year}/{month}/{non_exercise_days[0]}.parquet"))
df = df.dropna(subset=['sub_category']) # remove home and work
df = df[df["sub_category"] != "Fitness and Recreational Sports Centers"]
temp_df = df[df["caid"].isin(exercisers_at_least_3_days)]
group_3_days_total.append(len(temp_df))
for cat in questioned_sub_categories:
    group_3_days_visits[cat].append(len(temp_df[temp_df["sub_category"] == cat]))
temp_df = df[df["caid"].isin(exercisers_at_least_2_days)]
group_2_days_total.append(len(temp_df))
for cat in questioned_sub_categories:
    group_2_days_visits[cat].append(len(temp_df[temp_df["sub_category"] == cat]))
temp_df = df[df["caid"].isin(exercisers_at_least_1_day)]
group_1_day_total.append(len(temp_df))
for cat in questioned_sub_categories:
    group_1_day_visits[cat].append(len(temp_df[temp_df["sub_category"] == cat]))

# Save metadata to be opened later
with open('dec-02-05-final-nonoverlapping.pkl', 'wb') as f:
    pickle.dump([group_1_day_visits, group_1_day_total, group_2_days_visits,
                 group_2_days_total, group_3_days_visits, group_3_days_total], f)

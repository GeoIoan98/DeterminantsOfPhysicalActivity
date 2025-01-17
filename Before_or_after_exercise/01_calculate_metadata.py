'''
For each subcategory, finds the number of visits before and after visiting a fitness center.
Saves metadata to be loaded and analyzed in another file.
'''

import os
import pandas as pd
from tqdm import tqdm
import csv
tqdm.pandas()

def before_or_after(x, other_sub):
    idx_1 = x.loc[x["sub_category"] == "Fitness and Recreational Sports Centers"].index[0]
    idx_2 = x.loc[x["sub_category"] == other_sub].index[0]

    if idx_1 < idx_2:
        return "Fitness_First"
    else:
        return "Fitness_Later"

year = "2019"
month = "dec"
days_exercisers = ["02"]

base_folder = "/home/george/data/Veraset/Visits/local_dataset"

for day in days_exercisers:
    df = pd.read_parquet(os.path.join(base_folder, f"{year}/{month}/{day}.parquet"))

sub_categories = df["sub_category"].unique()
sub_categories = list(sub_categories)
sub_categories = [x for x in sub_categories if pd.isnull(x) == False]

temp_df = df[df["sub_category"] == "Fitness and Recreational Sports Centers"]
fitness_users = temp_df["caid"].unique()
just_fitness = df[df["caid"].isin(fitness_users)]



all_counters = []
for sub in tqdm(sub_categories):
    print(sub)
    if sub == "Fitness and Recreational Sports Centers":
        continue
    temp_df = just_fitness[just_fitness["sub_category"] == sub]
    unique_users = temp_df["caid"].unique()
    final_df = just_fitness[just_fitness["caid"].isin(unique_users)]
    final_df = final_df.groupby('caid').agg(before_or_after, sub) # agg instead of progress_apply
    all_counters += [final_df["sub_category"].value_counts()]
both = {i:j for i,j in zip(sub_categories,all_counters)}
to_append = [] #["Sub_Category", "Fitness_First", "Fitness_Later"]

for sub in sub_categories:
    try:
        fit_first = [both[sub]["Fitness_First"]]
    except: # If no covisits with fitness
        fit_first = [0]
    try:
        fit_later = [both[sub]["Fitness_Later"]]
    except:
        fit_later = [0]
    to_append.append([sub] + fit_first + fit_later)

with open("2019-dec-02.csv", "w") as f:
    wr = csv.writer(f)
    wr.writerows(to_append)

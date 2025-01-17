import os
import pandas as pd
from tqdm import tqdm
import csv
tqdm.pandas()

def before_or_after(x, other_sub):
    idx_1 = x.loc[x["sub_category"] == "Fitness and Recreational Sports Centers"].index[0]
    idx_2 = x.loc[x["sub_category"] == other_sub].index[0]

    # idx_1 = x.index[x=="Fitness and Recreational Sports Centers"].tolist()[0]
    # idx_2 = x.index[x==other_sub].tolist()[0]

    if idx_1 < idx_2:
        return "Fitness_First"
    else:
        return "Fitness_Later"

year = "2020"
month = "jan"
days_exercisers = ["12"]

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
    except:
        fit_first = [0]
    try:
        fit_later = [both[sub]["Fitness_Later"]]
    except:
        fit_later = [0]
    to_append.append([sub] + fit_first + fit_later)

with open("2020-jan-12.csv", "w") as f:
    wr = csv.writer(f)
    wr.writerows(to_append)

# before_after_df = pd.read_csv("2019-dec-09.csv", header=None, names = ["sub_category", "Fitness_First", "Fitness_Later"])
# before_after_df["Fitness_First_Percentage"] = before_after_df["Fitness_First"] / (before_after_df["Fitness_First"]
#                                                                                 + before_after_df["Fitness_Later"])
# before_after_df["Fitness_Later_Percentage"] = before_after_df["Fitness_Later"] / (before_after_df["Fitness_First"]
#                                                                                 + before_after_df["Fitness_Later"])
# before_after_df_removed_smaller = before_after_df[before_after_df["Fitness_First"] > 200]
# print(before_after_df_removed_smaller.sort_values(by = ["Fitness_First_Percentage"], ascending=False)[:10])
# print(before_after_df_removed_smaller.sort_values(by = ["Fitness_First_Percentage"], ascending=True)[:10])

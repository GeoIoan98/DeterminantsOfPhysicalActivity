'''
Applies all necessary data transformations to conduct OLS, solving the equation as provided in the manuscript, for the given days.
The initial version contained a visit for each row.
The final version contains a row for each user, for each day.
Each column is a subcategory, for the 20 most visited, having 1 or 0, depending on whether the user visited the subcategory or not.
Extra columns are: the day of the week, and the U.S. state of each user (most visits to that state on that day)
'''

import os
import pandas as pd
from collections import Counter

def create_dictionary_of_visits(visits, day):
    d = visits.apply(lambda row: Counter(row["visits"]), axis=1).to_dict()
    d = {k: dict(v) for k,v in d.items()}
    df_final = pd.DataFrame.from_dict(d, orient = 'index')
    df_final.reset_index(inplace=True)
    df_final.rename(columns = {"index": "caid"}, inplace=True)
    df_final["day"] = day
    return df_final

def append_state(df_initial, df_final):
    state = df_initial.groupby("caid").agg(true_state=pd.NamedAgg(column="state", aggfunc=lambda x: Counter(x).most_common()[0][0]))
    df_final_state = df_final.merge(state, right_index=True, left_on="caid", how = 'left')
    df_final_state.reset_index(inplace=True,drop=True)
    return df_final_state

def subsample_with_chosen_subcategories(list_of_dfs, subcategories_to_keep):
    for i, df in enumerate(list_of_dfs):
        list_of_dfs[i] = df[df["sub_category"].isin(subcategories_to_keep)]
        list_of_dfs[i].reset_index(inplace=True, drop=True)
    return list_of_dfs

year = "2019"
month = "dec"
base_folder = f"/home/george/data/Veraset/Visits/local_dataset/{year}/{month}"
days = ["02", "03", "04", "05", "06", "07", "08"]
chosen_cols = ["local_timestamp", "caid", "location_name", "sub_category", "state", "minimum_dwell"]

df_mon = pd.read_parquet(os.path.join(base_folder, days[0] + ".parquet"), columns=chosen_cols)
print("Finished Monday")
df_tue = pd.read_parquet(os.path.join(base_folder, days[1] + ".parquet"), columns=chosen_cols)
print("Finished Tuesday")
df_wed = pd.read_parquet(os.path.join(base_folder, days[2] + ".parquet"), columns=chosen_cols)
df_thu = pd.read_parquet(os.path.join(base_folder, days[3] + ".parquet"), columns=chosen_cols)
df_fri = pd.read_parquet(os.path.join(base_folder, days[4] + ".parquet"), columns=chosen_cols)
df_sat = pd.read_parquet(os.path.join(base_folder, days[5] + ".parquet"), columns=chosen_cols)
df_sun = pd.read_parquet(os.path.join(base_folder, days[6] + ".parquet"), columns=chosen_cols)
print("Finished Sunday")

counters = Counter(df_tue["sub_category"]).most_common()
most_visited_subcategories = [ sub[0] for i, sub in enumerate(counters) if (i < 21) and ( sub[0] is not None) ]

df_list = [df_mon, df_tue, df_wed, df_thu, df_fri, df_sat, df_sun]
df_list = subsample_with_chosen_subcategories(df_list, most_visited_subcategories)
df_mon = df_list[0]
df_tue = df_list[1]
df_wed = df_list[2]
df_thu = df_list[3]
df_fri = df_list[4]
df_sat = df_list[5]
df_sun = df_list[6]
print("Kept only top 20 subcategories")
# Create the binary visits (either visited a location or not)
visits_mon = df_mon.groupby("caid").agg(visits=pd.NamedAgg(column="sub_category", aggfunc=lambda x: set(x)))
visits_tue = df_tue.groupby("caid").agg(visits=pd.NamedAgg(column="sub_category", aggfunc=lambda x: set(x)))
visits_wed = df_wed.groupby("caid").agg(visits=pd.NamedAgg(column="sub_category", aggfunc=lambda x: set(x)))
visits_thu = df_thu.groupby("caid").agg(visits=pd.NamedAgg(column="sub_category", aggfunc=lambda x: set(x)))
visits_fri = df_fri.groupby("caid").agg(visits=pd.NamedAgg(column="sub_category", aggfunc=lambda x: set(x)))
visits_sat = df_sat.groupby("caid").agg(visits=pd.NamedAgg(column="sub_category", aggfunc=lambda x: set(x)))
visits_sun = df_sun.groupby("caid").agg(visits=pd.NamedAgg(column="sub_category", aggfunc=lambda x: set(x)))
print("Finished grouped by user")
# Create the dictionary and the df
df_final_mon = create_dictionary_of_visits(visits_mon, "Mon")
print("Finished the co_visited dictionary for Monday")
df_final_tue = create_dictionary_of_visits(visits_tue, "Tue")
print("Finished the co_visited dictionary for Tuesday")
df_final_wed = create_dictionary_of_visits(visits_wed, "Wed")
df_final_thu = create_dictionary_of_visits(visits_thu, "Thu")
df_final_fri = create_dictionary_of_visits(visits_fri, "Fri")
df_final_sat = create_dictionary_of_visits(visits_sat, "Sat")
df_final_sun = create_dictionary_of_visits(visits_sun, "Sun")
print("Finished the co_visited dictionary for all")
# Append the state
df_final_mon_state = append_state(df_mon, df_final_mon)
print("Appended the state for Monday")
df_final_tue_state = append_state(df_tue, df_final_tue)
df_final_wed_state = append_state(df_wed, df_final_wed)
df_final_thu_state = append_state(df_thu, df_final_thu)
df_final_fri_state = append_state(df_fri, df_final_fri)
df_final_sat_state = append_state(df_sat, df_final_sat)
df_final_sun_state = append_state(df_sun, df_final_sun)
print("Appended the state for all")

df_final_mon_state.to_parquet("02-08-Dec-2019-daily-binary-mon.parquet")
df_final_tue_state.to_parquet("02-08-Dec-2019-daily-binary-tue.parquet")
df_final_wed_state.to_parquet("02-08-Dec-2019-daily-binary-wed.parquet")
df_final_thu_state.to_parquet("02-08-Dec-2019-daily-binary-thu.parquet")
df_final_fri_state.to_parquet("02-08-Dec-2019-daily-binary-fri.parquet")
df_final_sat_state.to_parquet("02-08-Dec-2019-daily-binary-sat.parquet")
df_final_sun_state.to_parquet("02-08-Dec-2019-daily-binary-sun.parquet")

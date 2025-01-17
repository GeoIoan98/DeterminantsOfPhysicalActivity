'''
Select the day to work on.
Calculate the total number of co-visits between all pairwise subcategories.
For example, if a user visited A,B,C on a day, then we add a co-visit for A+B, for B+C, and for A+C.
Save the metadata, to be loaded and analyzed in a different file.
'''


import pandas as pd
import os
from tqdm import tqdm
from collections import defaultdict
from itertools import combinations
import pickle

# If a user visited only a single subcategory on a specific day, convert it to a 0 to be removed later.
def remove_single(x):
    if len(x) == 1:
        return 0
    else:
        return x


year = "2918"
month = "dec"
day = "02"
df = pd.read_parquet(f"/home/george/data/Veraset/Visits/local_dataset/{year}/{month}/{day}.parquet")
print("Finished Reading")

co_visits = df.dropna(subset=['sub_category'])
co_visits = co_visits.groupby("caid").agg(
    created=pd.NamedAgg(column="sub_category", aggfunc=lambda x: set(x)))

co_visits = co_visits["created"].apply(remove_single)
co_visits = co_visits.to_frame()
co_visits = co_visits[co_visits.created != 0]

co_visits_list = list(co_visits["created"])
co_visits_dict = defaultdict(int)

# Create a dictionary with all possible combinations
for s in tqdm(co_visits_list):
    combs = (combinations(s,2))
    for comb in combs:
        co_visits_dict[comb[0]+"+"+comb[1]] += 1
        co_visits_dict[comb[1]+"+"+comb[0]] += 1


f = open(f"/home/george/code/Fitness/metadata/covisits/{year}_{month}_{day}.pkl","wb")
pickle.dump(co_visits_dict,f)

'''
For each specified day, it identifies and saves the ID's of the users that visited a fitness center.
'''

import os
import pandas as pd
import pickle

year = "2020"
month = "feb"
base = f"/home/george/data/Veraset/Visits/local_dataset/{year}/{month}"
days = ["03", "04", "05", "06", "07", "08", "09"]

for day in days:
    print(day)
    df = pd.read_parquet(os.path.join(base, day + ".parquet"))
    fitness = df[df["sub_category"] == "Fitness and Recreational Sports Centers"]
    all_fitness_users = fitness.caid.unique()

    save_folder = f"/home/george/code/Fitness/metadata/exercisers/{year}/{month}/{day}.pkl"

    with open(save_folder, 'wb') as f:
        pickle.dump(all_fitness_users, f)

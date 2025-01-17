'''
Loads the selected pre-processed data and conducts OLS. Saves the results.
'''

import os
import pandas as pd
import statsmodels.api as sm
from collections import defaultdict
from tqdm import tqdm

base = "/home/george/code/Fitness/metadata/linear_regression"
files = os.listdir(base)
files = [os.path.join(base, file) for file in files]

file_0 = pd.read_parquet("/home/george/code/Fitness/metadata/linear_regression/02-08-Dec-2019-daily-binary-fri.parquet")
file_1 = pd.read_parquet("/home/george/code/Fitness/metadata/linear_regression/02-08-Dec-2019-daily-binary-sat.parquet")
file_2 = pd.read_parquet("/home/george/code/Fitness/metadata/linear_regression/02-08-Dec-2019-daily-binary-sun.parquet")

df = pd.concat([file_0, file_1, file_2])
df = df.fillna(0)
df = df.set_index(["caid"])
df = pd.get_dummies(df, columns=['day', 'true_state'], drop_first=True)

X = df.loc[:, df.columns != "Fitness and Recreational Sports Centers"]
Y = df['Fitness and Recreational Sports Centers']
X = sm.add_constant(X)

model = sm.OLS(Y.astype('float32'), X.astype('float32')).fit()
model.save("./metadata/linear_regression/fri-sat-sun-dec.pickle")

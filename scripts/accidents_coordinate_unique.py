import numpy as np
import pandas as pd

in_csv = '/tmp/lo_2011_2024.csv'
out_csv = '/tmp/lo_unique_coordinates.csv'

df = pd.read_csv(in_csv)[['X coordinate', 'Y coordinate']].dropna()
df['X coordinate'] = np.round(df['X coordinate'], 2)
df['Y coordinate'] = np.round(df['Y coordinate'], 2)

df.drop_duplicates().to_csv(out_csv, index=False, header=False)

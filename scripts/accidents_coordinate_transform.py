import pandas as pd

from pyproj import Transformer

in_csv = '/tmp/lo_2011_2024.csv'
out_csv = '/tmp/lo_2011_2024_coords.csv'

original_crs_epsg = 3301
target_crs_epsg = 4326 
transformer = Transformer.from_crs(original_crs_epsg, target_crs_epsg)

df = pd.read_csv(in_csv)

x, y = transformer.transform(df['X coordinate'], df['Y coordinate'])
df.to_csv(out_csv, index=False)

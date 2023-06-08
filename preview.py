# %% 
import pandas as pd
import glob
import time 
import duckdb

# %%
conn = duckdb.connect('mydb.db')

# %%
current_time = time.time()

data_dir = './data'
file_paths = glob.glob(f"{data_dir}/*.json")

dataframes = [pd.read_json(file_path) for file_path in file_paths]
combined_dataframe = pd.concat(dataframes, ignore_index=True)
print(f"time: {time.time() - current_time}")
# %% Select all data into dataframe
current_time = time.time()
df = conn.execute("""
    SELECT *
    FROM 'data/*.json'
""").df() 
print(f"time: {time.time() - current_time}")
print(df)
# MUCH FASTER THAN PANDAS!
# %% Describe df
conn.register('df_view', df)
conn.execute("DESCRIBE df_view").df()

#%% 
conn.execute("SELECT COUNT(*) FROM df_view").df()

#%% View null values with pandas
df.isnull().sum()

## % We can select directly df
conn.execute("SELECT COUNT(*) FROM df").df()

## % For SQL queries use "" instead of ''
conn.execute("""
    SELECT * FROM df WHERE "brand" ='Ypê'
""").df()

# %% Write as parquet
conn.execute("COPY (FROM df_view) TO './data/all_products.parquet' (FORMAT 'parquet')")

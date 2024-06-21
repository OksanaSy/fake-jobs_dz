import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

output_file = 'result.parquet'
df = pd.read_parquet(output_file)


print(df)

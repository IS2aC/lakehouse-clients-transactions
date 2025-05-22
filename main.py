import pandas as pd 

df = pd.read_csv('data-source/transactions/trx_2025-05-16.csv')

print(df['type_of_transaction'].unique())
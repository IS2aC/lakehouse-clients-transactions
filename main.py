import pandas as pd 
import hashlib
import os


df =  pd.read_csv('/home/isaac/lakehouse-clients-transactions/data-source/source/trx_2025-05-16.csv')

columns =  ['cpm_payment_date','pays_de_transaction', 
            'id_clt', 'cpm_site_id', 'sector', 'cpm_currency'
            'cpm_error_message', 'log_dopayment', 'montant']
print(df[columns])

# def hash_value(value):
#     """
#     Hash a value using SHA-256.
#     """
#     return hashlib.sha256(str(value).encode()).hexdigest()

# columns_to_hash = ['client_idenfication', 'service_identification']

# for file in  os.listdir('data-source/transactions'):
#     if file.endswith('.csv'):
#         df = pd.read_csv(os.path.join('data-source/transactions', file))
#         print(f"DataFrame from {file}:")
#         for col in columns_to_hash:
#             df[col] = df[col].apply(hash_value)
#         df.to_csv('data-source/transactions/' + file, index = False)
#         print('---------------------------------------------------------------------')


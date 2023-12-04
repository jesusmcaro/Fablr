from faker import Faker
import pandas as pd
from pandas import DataFrame
#from .extended_providers import 
import numpy as np
import hashlib

class Fablr(Faker):
    
    def __init__(self):
        self.fake = Faker()
        #self.fake.add_provider(foo)
    def set_seed(self, seed):
        Faker.seed(seed)
    

    def generate_data(self,rows: int, column_providers: dict) -> list:
        data = []
        for _ in range(rows):
            row_data = {}
            for column, provider_kwargs in column_providers.items():
                provider = provider_kwargs['provider']
                kwargs = provider_kwargs.get('kwargs', {})
                row_data[column] = getattr(self.fake, provider)(**kwargs)
            data.append(row_data)
        return data
    
    def generate_dataframe(self, rows: int, column_providers: dict, primary_keys: list = None) -> DataFrame:
        df = self.generate_data(rows, column_providers)
        df = pd.DataFrame(df)
        if primary_keys is not None:
            df = hash_columns(df, primary_keys)
            df = df.drop_duplicates(subset = "hash", keep = "first")
            re_calc = rows - df.shape[0]
            while re_calc > 0:
                df = df.append(self.generate_dataframe(re_calc, column_providers, primary_keys), ignore_index = True, verify_integrity = True)
                df = df.drop_duplicates(subset = "hash", keep = "first")
                re_calc = rows - df.shape[0]
        return df

        
def hash_map(row_subset: pd.Series) -> str:
   pre_hash = ':'.join(map(str, row_subset.to_list()))
   hashed_values = hashlib.sha256(pre_hash.encode("utf-8")).hexdigest()
   return hashed_values

def hash_columns(df, cols: list) -> str:
    df['hash'] = df[cols].apply(hash_map, axis=1)
    return df

import hashlib
import pandas as pd
from faker import Faker
from pandas import DataFrame
from .extended_providers import (
    sample_dataframe_provider,
    sample_list_provider,
    random_float_provider,
)


class Fablr(Faker):
    def __init__(self):
        self.fake = Faker()
        self.fake.add_provider(sample_dataframe_provider(self.fake))
        self.fake.add_provider(sample_list_provider(self.fake))
        self.fake.add_provider(random_float_provider(self.fake))

    def set_seed(self, seed):
        Faker.seed(seed)

    def clear(self):
        self.fake.unique.clear()
        self.fake.to_one_provider_clear()  # jank but works
        self.fake.sample_list_provider_clear()

    def generate_data(self, rows: int, column_providers: dict) -> list:
        data = []
        for _ in range(rows):
            row_data = {}
            for column, provider_kwargs in column_providers.items():
                provider = provider_kwargs["provider"]
                kwargs = provider_kwargs.get("kwargs", {})
                unique = provider_kwargs.get("unique")
                if unique:
                    faker = self.fake.unique
                else:
                    faker = self.fake
                row_data[column] = getattr(faker, provider)(**kwargs)
            data.append(row_data)
        return data

    def build_dataframe(self, rows: int, column_providers: dict) -> DataFrame:
        df_dict = self.generate_data(rows, column_providers)
        df = pd.DataFrame(df_dict)
        return df

    def generate_dataframe(
        self, rows: int, column_providers: dict, primary_keys: list = None
    ) -> DataFrame:
        df = self.build_dataframe(rows, column_providers)
        print(df)
        if primary_keys is not None:
            df = hash_columns(df, primary_keys)
            df = df.drop_duplicates(subset="hash", keep="first")
            while df.shape[0] < rows:
                additional_rows_df = self.build_dataframe(
                    rows - df.shape[0], column_providers
                )
                additional_rows_df = hash_columns(additional_rows_df, primary_keys)
                additional_rows_df = additional_rows_df.drop_duplicates(
                    subset="hash", keep="first"
                )
                df = df.append(
                    additional_rows_df, ignore_index=True, verify_integrity=True
                )
                df = df.drop_duplicates(subset="hash", keep="first")
                print(df)
            df = df.drop(columns="hash")
        return df


def hash_map(row_subset: pd.Series) -> str:
    pre_hash = ":".join(map(str, row_subset.to_list()))
    hashed_values = hashlib.sha256(pre_hash.encode("utf-8")).hexdigest()
    return hashed_values


def hash_columns(df, cols: list) -> str:
    df["hash"] = df[cols].apply(hash_map, axis=1)
    return df

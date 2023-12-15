import pandas as pd
from datetime import datetime as dt
from datetime import date as date
from fablr import dataframes as d


class TestDataFrame:
    gen = d.Fablr()
    gen.set_seed(123)
    rows = 10
    date_format = "%Y-%m-%d"
    test_dict = {
        "col_1": {
            "provider": "sample_list",
            "kwargs": {"list": [1, 2, 3, 4]},
            "unique": False,
        },
        "col_2": {
            "provider": "sample_list",
            "kwargs": {"list": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]},
        },
        "col_3": {
            "provider": "date_between_dates",
            "kwargs": {
                "date_start": dt.strptime("2019-01-01", date_format),
                "date_end": dt.strptime("2023-12-01", date_format),
            },
        },
        "col_4": {
            "provider": "sample_list",
            "kwargs": {
                "list": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
                "unique": True,
            },
        },
    }
    test_df = gen.generate_dataframe(rows, test_dict)

    def test_is_instance(self):
        test_df = self.test_df
        assert isinstance(test_df, pd.DataFrame)

    def test_shape(self):
        test_df = self.test_df
        assert test_df.shape == (10, 4)

    def test_non_unique(self):
        test_df = self.test_df
        assert test_df["col_1"].unique().shape[0] < test_df["col_1"].shape[0]

    def test_unique(self):
        test_df = self.test_df
        assert test_df["col_4"].unique().shape[0] == self.rows

    def test_datetype(self):
        test_df = self.test_df
        assert isinstance(test_df["col_3"].iloc[0], date)


class TestDataFramePrimaryKey:
    gen = d.Fablr()
    gen.set_seed(123)
    rows = 10000
    test_dict = {
        "id": {"provider": "random_int", "kwargs": {"min": 1, "max": 30000}},
        "first_name": {"provider": "first_name"},
        "last_name": {"provider": "last_name"},
        "email": {"provider": "email"},
    }

    def test_primary_key(self):
        test_df = self.gen.generate_dataframe(
            self.rows, self.test_dict, primary_keys=["id"]
        )
        deduped_df = test_df.drop_duplicates(subset="id", keep="first")
        assert deduped_df.shape[0] == test_df.shape[0]

    def test_primary_key_recalc(self):
        keys = ["first_name", "last_name", "email"]
        test_df = self.gen.generate_dataframe(
            self.rows, self.test_dict, primary_keys=keys
        )
        deduped_df = test_df.drop_duplicates(subset=keys, keep="first")
        assert deduped_df.shape[0] == test_df.shape[0]


class TestDataModeling:
    gen = d.Fablr()
    gen.set_seed(123)
    user_rows = 3000
    login_rows = 10000
    attr_rows = 3000
    date_format = "%Y-%m-%d"
    user_dict = {
        "id": {"provider": "random_int", "kwargs": {"min": 1, "max": 30000}},
        "first_name": {"provider": "first_name"},
        "last_name": {"provider": "last_name"},
        "email": {"provider": "email"},
    }
    user_df = gen.generate_dataframe(user_rows, user_dict, primary_keys=["id"])
    logins_dict = {
        "user_id": {
            "provider": "sample_dataframe",
            "kwargs": {"df": user_df, "column": "id"},
        },
        "login_date": {
            "provider": "date_between_dates",
            "kwargs": {
                "date_start": dt.strptime("2019-01-01", date_format),
                "date_end": dt.strptime("2023-12-01", date_format),
            },
        },
    }
    logins_df = gen.generate_dataframe(login_rows, logins_dict)
    attr_dict = {
        "user_id": {
            "provider": "sample_dataframe",
            "kwargs": {"df": user_df, "column": "id", "unique": True},
        },
    }
    attr_df = gen.generate_dataframe(attr_rows, attr_dict)

    def test_left_join(self):
        left_df = self.user_df
        right_df = self.logins_df
        joined_df = left_df.merge(
            right_df, how="left", left_on="id", right_on="user_id"
        )
        match_df = joined_df[(~joined_df["user_id"].isna())]
        assert sum(match_df["user_id"] == match_df["id"]) == match_df.shape[0]

    def test_left_unique_join(self):
        left_df = self.user_df
        right_df = self.attr_df
        joined_df = left_df.merge(
            right_df, how="inner", left_on="id", right_on="user_id"
        )
        assert joined_df.shape[0] == self.attr_rows == self.user_rows

    def test_uniuque_keys(self):
        left_df = self.user_df
        right_df = self.attr_df
        joined_df = left_df.merge(
            right_df, how="inner", left_on="id", right_on="user_id"
        )
        assert (
            joined_df["user_id"].drop_duplicates().count()
            == joined_df["id"].drop_duplicates().count()
        )

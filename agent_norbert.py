from typing import List, Union
import uuid
import random
import pandas as pd
import datetime as dt

CHANCE_TO_HAVE = 0.6

# 1,5edf63c9-a4de-4592-b70d-8fa137c00f88,2019-02-01,2.158469060601665


class User:
    def __init__(self, account_id: str, date_key: str, revenue: float) -> None:

        self._validate_input(account_id, date_key, revenue)

        self.account_id = account_id
        self.date_key = date_key
        self.revenue = revenue

    @staticmethod
    def _validate_input(account_id: str, date_key: str, revenue: float) -> None:
        if not isinstance(account_id, str):
            raise ValueError(
                f"Wrong type for {account_id} - expected string, got {type(account_id)}!"
            )

        if not isinstance(date_key, str):
            raise ValueError(
                f"Wrong type for {date_key} - expected string, got {type(date_key)}!"
            )

        if not isinstance(revenue, float):
            raise ValueError(
                f"Wrong type for {revenue} - expected float, got {type(revenue)}!"
            )

    def get_info(self) -> List[Union[str, str, float]]:
        return [self.account_id, self.date_key, self.revenue]


class RecordsCreator:
    def __init__(self, no_accounts, start_date, end_date) -> None:
        self.no_accounts = no_accounts
        self.start_date = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        self.end_date = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        self.users = []
        self.output = []
        self.df = None

    def fullfill(self) -> None:

        date_range = (
            pd.date_range(self.start_date, self.end_date, freq="MS")
            .strftime("%Y-%m-%d")
            .tolist()
        )

        for idx, _ in enumerate(range(self.no_accounts)):
            account_id = str(uuid.uuid4())

            for date in date_range:
                revenue = float(random.random() * 10)

                if idx == 0:
                    user = User(account_id, date, revenue)
                    self.users.append(user)
                    self.output.append([account_id, date, revenue])
                else:
                    if self.coin_flip(random.random()):
                        user = User(account_id, date, revenue)
                        self.users.append(user)
                        self.output.append([account_id, date, revenue])

    def coin_flip(self, guess: float) -> bool:
        return guess < CHANCE_TO_HAVE

    def generate(self) -> None:
        self.df = pd.DataFrame(
            self.output, columns=["account_id", "date_key", "revenue"]
        )
        self.df.to_csv(path_or_buf=f"./output_norbert.csv", index=True)


if __name__ == "__main__":

    users_records = RecordsCreator(
        no_accounts=1000, start_date="2019-01-01", end_date="2022-05-01"
    )
    users_records.fullfill()
    users_records.generate()
    print("xd")

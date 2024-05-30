import os
import unittest

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

USERNAME = os.environ["USERNAME"]
PASSWORD = os.environ["PASSWORD"]
ENDPOINT = os.environ["ENDPOINT"]
PORT = os.environ["PORT"]
DATABASE = os.environ["DATABASE"]

engine = create_engine(
    f"postgresql://{USERNAME}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
)

users = pd.read_sql_query("SELECT * FROM users", con=engine)
trades = pd.read_sql_query("SELECT * FROM trades", con=engine)


class TestUserColumns(unittest.TestCase):
    def test_user_column(self):
        self.assertEqual(len(users.columns), 5)

    def test_user_has_one_currency(self):
        """Check if a user has only one currency."""
        self.assertTrue(
            all(
                users[["login_hash", "currency"]]
                .groupby("login_hash")["currency"]
                .nunique()
                == 1
            )
        )


class TestTradesColumns(unittest.TestCase):
    def test_trades_column(self):
        """Check if trades table has 11 columns."""
        self.assertEqual(len(trades.columns), 11)

    def test_ticket_hash_uniqueness(self):
        """Check if ticket hash is unique."""
        self.assertEqual(len(trades["ticket_hash"].unique()), len(trades))

    def test_negative_open_price(self):
        """Check if open price is positive."""
        self.assertEqual(len(trades[trades["open_price"] < 0]), 0)

    def test_open_time_before_close_time(self):
        """Check if open time is before close time."""
        self.assertEqual(len(trades[trades["open_time"] > trades["close_time"]]), 0)

    def test_symbol_codes_alphanumeric(self):
        """Check if symbol codes are alphanumeric."""
        s = trades[~trades["symbol"].str.isalnum()]["symbol"]
        print()
        print(s)
        self.assertEqual(len(s), 0)

    def test_close_time_in_past(self):
        """Check if close time is in the past."""
        self.assertEqual(len(trades[trades["close_time"] >= pd.Timestamp.now()]), 0)

    def test_volume_positive(self):
        """Check if volume is positive."""
        s = trades[trades["volume"] <= 0]["volume"]
        print()
        print(s)
        self.assertEqual(len(s), 0)

    def test_user_has_one_server(self):
        """Check if a user has only one server."""
        self.assertTrue(
            all(
                trades[["login_hash", "server_hash"]]
                .groupby("login_hash")["server_hash"]
                .nunique()
                == 1
            )
        )


class TestJoinedTable(unittest.TestCase):
    def test_trades_has_user(self):
        """Check if trades have user."""
        self.assertTrue(all(trades["login_hash"].isin(users["login_hash"])))

    def test_trades_have_active_user(self):
        """Check if trades have inactive user."""
        self.assertEqual(
            len(users[users["enable"] == 0]["login_hash"].isin(trades["login_hash"])), 0
        )


if __name__ == "__main__":
    unittest.main()

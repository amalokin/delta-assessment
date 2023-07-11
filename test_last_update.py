import unittest
import pandas as pd
from pandas._testing import assert_frame_equal
import last_update as lu


class TestFlightDataProcessing(unittest.TestCase):
    def test_handle_24_hour_format(self):
        self.assertEqual(lu.handle_24_hour_format("24:00:00"), "00:00:00")
        self.assertEqual(lu.handle_24_hour_format("12:00:00"), "12:00:00")

    def test_find_last_update(self):
        df = pd.DataFrame(
            {
                "flightkey": ["A", "B", "A", "B"],
                "lastupdt_timestamp": pd.to_datetime(
                    [
                        "2023-07-11 12:00:00",
                        "2023-07-11 13:00:00",
                        "2023-07-11 14:00:00",
                        "2023-07-11 15:00:00",
                    ]
                ),
                "other_column": ["x", "y", "z", "w"],
            }
        )
        expected_output = pd.DataFrame(
            {
                "flightkey": ["A", "B"],
                "lastupdt_timestamp": pd.to_datetime(
                    ["2023-07-11 14:00:00", "2023-07-11 15:00:00"]
                ),
                "other_column": ["z", "w"],
            }
        ).set_index("flightkey")
        output = lu.find_last_update(df, "flightkey").set_index("flightkey")
        assert_frame_equal(output, expected_output)


if __name__ == "__main__":
    unittest.main()

import pandas as pd
from io import StringIO
import argparse


def handle_24_hour_format(time_str):
    """Converts 24:00 time to 00:00 time, which is supported by pandas"""

    if time_str == "24:00:00":
        return "00:00:00"
    else:
        return time_str


def read_data(path: str) -> pd.DataFrame:
    """Reads the data from the given path and returns a dataframe"""

    with open(path, "r") as f:
        raw_data = f.read().split(",,,,,,,\n")[
            -2
        ]  # based on an observation that data is enclosed within 7 commas

    df = pd.read_csv(
        StringIO(raw_data),
        header=0,
    )  # using parse_dates={"lastupdt_timestamp": ["flight_dt", "lastupdt"]} would be optimal, but it drops the original columns

    # Convert the flight_dt and lastupdt columns to datetime
    df["flight_dt"] = pd.to_datetime(df["flight_dt"], format="%m/%d/%y").astype(str)
    df["lastupdt"] = df["lastupdt"].apply(
        handle_24_hour_format
    )  # handle 24:00 time, this is slow
    df["lastupdt"] = pd.to_datetime(df["lastupdt"], format="mixed").astype(
        str
    )  # mixed format required
    df["lastupdt_timestamp"] = pd.to_datetime(
        df["flight_dt"] + " " + df["lastupdt"]
    ).dt.tz_localize(
        None
    )  # drop the timezone info

    return df


def find_last_update(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Finds the last update for each key in the dataframe"""

    # Perform the ranking
    df["rn"] = (
        df.sort_values(by="lastupdt_timestamp", ascending=False).groupby(key).cumcount()
        + 1
    )

    # Filter the rows with rank 1
    df = df[df["rn"] == 1]

    # Drop the rank column
    df = df.drop(columns="rn")

    return df


def write_data(df: pd.DataFrame, path: str) -> None:
    """Writes the dataframe to the given path"""

    # Reorder the columns
    df = df[
        [
            "flightkey",
            "flightnum",
            "flight_dt",
            "orig_arpt",
            "dest_arpt",
            "flightstatus",
            "lastupdt_timestamp",
        ]
    ]
    df.to_csv(path, index=False)


if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="Process I/O paths and filtering key.")

    # Add the arguments
    parser.add_argument(
        "ReadPath", metavar="readpath", type=str, help="the path to read data from"
    )
    parser.add_argument(
        "WritePath", metavar="writepath", type=str, help="the path to write data to"
    )
    parser.add_argument(
        "Key", metavar="key", type=str, help="the key to filter data by"
    )

    # Main routine
    args = parser.parse_args()

    dataframe = read_data(args.ReadPath)
    dataframe = find_last_update(dataframe, args.Key)
    write_data(dataframe, args.WritePath)

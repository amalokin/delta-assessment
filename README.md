# Flight Data Processing Assessment
This Python module processes flight data. It reads data from a specified CSV file, finds the last update for each key in the data, and writes the resulting data to a specified output CSV file.

## SQL

### Requirements

Data is stored in a PostgreSQL database (to ensure 100% SQL dialect compatibility).

### Usage

Run `last_update.sql` in a PostgreSQL database. The script assumes that the data is stored 
in a table called `flights` with the schema provided in the assessment instructions. 
Modify the table name on line 7 to reflect the table name in your environment. 

Since `flightkey` is a primary key, 
the query will produce the same results as `SELECT * FROM flights;`. However, if it's used with a non-unique key,
like `flightnum`, it will produce the last updates for the given aggregation key value. To do that, 
modify the query line 13 `PARTITION BY` field.

### Potential Improvements

The query performance could be improved by creating an index on the `flightkey` column at the expense of storage space 
and extra maintenance on update operations. If data is updated infrequently, CDC (Change Data Capture) could be used 
to create an extra column which would indicate the current row for each collection of keys. This would allow for
rapid retrieval of the latest update for each key.



## Python

### Requirements
This module requires Python 3.6 or later and depends on the following Python libraries:
```
pandas
```

You can install these dependencies using pip:

```bash
pip install pandas
```

### Usage
Run the script from the command line, providing three arguments: the path to the input file, the path to the output file, and the key to filter the data by.

```bash
python last_update.py /path/to/input.csv /path/to/output.csv flightkey
```
#### Arguments:
`readpath`: The path to the CSV file to read the data from.

`writepath`: The path to the CSV file to write the processed data to.

`key`: The key to filter the data by.

### Functions
`handle_24_hour_format(time_str)`: Converts "24:00:00" time to "00:00:00" which is only representation supported by pandas.

`read_data(path: str) -> pd.DataFrame`: Reads the data from the given path and returns a dataframe.

`find_last_update(df: pd.DataFrame, key: str) -> pd.DataFrame`: Finds the last update for each key in the dataframe.

`write_data(df: pd.DataFrame, path: str) -> None`: Writes the dataframe to the given path.

### Notes
The data is expected to be enclosed within 7 commas and to be at the second to the last enclosure.
The script handles 24:00 time, which can be slow. Upstream data formatting is recommended.

### Potential Improvements

The script could be simplified if data quality checks and resulting conversions are shifted upstream. 
For exceptionally large batch data processing, a data warehousing solution like Apache Spark, Snowflake, AWS Redshift, or
GCP Bigquery could be used. On the other hand, if the data comes in a stream, a key-value store like Redis could be used
to store the last update for each key.

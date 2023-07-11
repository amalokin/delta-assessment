-- Since flightkey is primary key, this query will return the same result as SELECT * FROM flights;
WITH timestamped_flights AS (
    SELECT
        *,
        (flight_dt + lastupdt)::TIMESTAMP AS lastupdt_timestamp
    FROM
        flights  -- replace with your table name
),
ranked_flights AS (
    SELECT
        *,
        -- replace flightkey with any potentially non-unique aggregation key, like flightnum
        ROW_NUMBER() OVER(PARTITION BY flightkey ORDER BY lastupdt_timestamp DESC) as rn
    FROM
        timestamped_flights
)
SELECT 
    flightkey,
    flightnum,
    flight_dt,
    orig_arpt,
    dest_arpt,
    flightstatus,
    lastupdt_timestamp as "lastupdt"
FROM 
    ranked_flights
WHERE 
    rn = 1
;

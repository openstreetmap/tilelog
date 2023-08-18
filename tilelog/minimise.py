import tilelog.constants

GROUP_COLUMNS = """
    z,
    version,
    referer,
    useragent,
    secchua,
    requestedwith,
    accept,
    acceptlang,
    asn,
    country,
    datacenter,
    region,
    status,
    cachehit,
    tls,
    render,
    year,
    month,
    day,
    hour"""


def create_minimised(curs, date):
    # Start by checking if any rows are in the table to avoid running twice.
    # This won't catch multiple parallel runs, but will catch running it twice in a row.
    check_query = f"""
SELECT * FROM {tilelog.constants.FASTLY_MINIMISED_LOGS}
WHERE year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
LIMIT 1;
    """
    curs.execute(check_query,
                 {"year": date.year, "month": date.month, "day": date.day})
    # curs.rowcount doesn't work for Athena, so iterate through the row. This does nothing
    # if there are no rows in the table for this day, otherwise, it throws an exception.
    for line in curs:
        raise RuntimeError("minimisation queries have already been run for this day")

    insert_query = f"""
INSERT INTO {tilelog.constants.FASTLY_MINIMISED_LOGS}
SELECT
    COUNT(*) AS requests,
    COUNT(DISTINCT ip) AS distinct_ip,
    SUM(size) AS size,
    SUM(duration) AS duration,
    {GROUP_COLUMNS}
FROM {tilelog.constants.FASTLY_PARQET_LOGS}
WHERE year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
    AND hour = %(hour)d
GROUP BY {GROUP_COLUMNS}
    """
    for hour in range(0, 24):
        curs.execute(insert_query,
                     {"year": date.year, "month": date.month, "day": date.day, "hour": hour})

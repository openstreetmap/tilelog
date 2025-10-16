import tilelog.constants

TILE_REGEX = r"""'^GET /(\d{1,2})/(\d{1,6})/(\d{1,6})\.png'"""

regex = TILE_REGEX
SELECT_COLUMNS = f"""
    time,
    ip,
    CAST(regexp_extract(request, {regex}, 1) AS integer) AS z,
    CAST(regexp_extract(request, {regex}, 2) AS integer) AS x,
    CAST(regexp_extract(request, {regex}, 3) AS integer) AS y,
    regexp_extract(request, ' HTTP/(.+?)$', 1) AS version,
    host,
    referer,
    useragent,
    secchua,
    fetchsite,
    origin,
    requestedwith,
    accept,
    acceptlang,
    asn,
    country,
    datacenter,
    region,
    status,
    CAST(NULLIF(size,'"-"') AS integer) AS size,
    duration,
    cachehit,
    tls,
    render,
    year,
    month,
    day,
    hour"""


def create_parquet(curs, date):

    # Start by checking if any rows are in the table to avoid running twice.
    # This won't catch multiple parallel runs, but will catch running it twice in a row.
    check_query = f"""
SELECT * FROM {tilelog.constants.FASTLY_PARQET_LOGS}
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
        raise RuntimeError("aggregation queries have already been run for this day")

    insert_query = f"""
INSERT INTO {tilelog.constants.FASTLY_PARQET_LOGS}
SELECT {SELECT_COLUMNS}
FROM {tilelog.constants.FASTLY_LOG_TABLE}
WHERE status IN (200, 206, 304)
    AND regexp_like(request, {TILE_REGEX})
    AND year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
    """
    curs.execute(insert_query,
                 {"year": date.year, "month": date.month, "day": date.day})

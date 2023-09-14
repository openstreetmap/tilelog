import csv
import tilelog.constants


def country_logs(curs, date, dest):
    query = f"""
SELECT
country,
COUNT(DISTINCT ip) AS ips,
cast(count(*) as double)/86400 AS tps,
cast(count(*) filter (WHERE cachehit = 'MISS') as double)/86400 AS tps_miss
    FROM {tilelog.constants.FASTLY_PARQET_LOGS}
WHERE year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
GROUP BY country
HAVING COUNT(DISTINCT ip) > %(min_distinct)d AND COUNT(*) > %(min_requests)d
ORDER BY COUNT(*) DESC
    """

    curs.execute(query,
                 {"year": date.year, "month": date.month, "day": date.day,
                  "min_distinct": tilelog.constants.MIN_DISTINCT_TILE_REQUESTS,
                  "min_requests": tilelog.constants.MIN_TILE_REQUESTS})
    csvwriter = csv.writer(dest, dialect=csv.unix_dialect,
                           quoting=csv.QUOTE_NONNUMERIC)
    # Write the header row
    csvwriter.writerow(["country", "ips", "tps", "tps_miss"])
    # Write the rows
    csvwriter.writerows(curs)

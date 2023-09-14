import click
import csv

import tilelog.constants


def app_logs(curs, date, dest):
    click.echo("Querying for app usage")
    query = fr"""
SELECT
app,
cast(COALESCE(SUM(requests), 0) as double)/86400 AS tps,
cast(COALESCE(SUM(requests) FILTER (WHERE cachehit = 'MISS'), 0) as double)/86400 AS tps_miss
FROM (
    SELECT
    CASE
    WHEN requestedwith != '' THEN requestedwith
    WHEN useragent LIKE 'Mozilla/5.0 QGIS/%%' THEN 'Mozilla/5.0 QGIS/*'
    WHEN useragent LIKE 'qfield|%%|%%|%%| QGIS/%%' THEN 'qfield|*|*|*| QGIS/*'
    WHEN useragent LIKE 'Mozilla/5.0 (compatible; Marble/%%)' THEN 'Mozilla/5.0 (compatible; Marble/*)'
    WHEN useragent LIKE 'JOSM/1.5 %%' THEN 'JOSM/1.5 *'
    WHEN useragent LIKE 'OruxMaps %%' THEN 'OruxMaps *'
    WHEN useragent LIKE 'MapProxy-%%' THEN 'MapProxy-*'
    WHEN useragent LIKE 'Dart/%% (dart:io)' THEN 'Dart/* (dart:io)'
    WHEN useragent LIKE 'Wikiloc %%' THEN 'Wikiloc *'
    WHEN useragent LIKE 'Mapbox iOS SDK (%%)' THEN 'Mapbox iOS SDK (*)'
    WHEN useragent LIKE 'flutter_map (%%)' THEN 'flutter_map (*)'
    WHEN useragent LIKE 'Graph Messenger T%% - P%%' THEN 'Graph Messenger T* - P*'

    -- Extract app name from foo.bar/123.456
    WHEN regexp_like(useragent, '^([^./]+(\.[^./]+)*)/\d+(\.\d+)*$') THEN regexp_extract(useragent, '^([^./]+(\.[^./]+)*)/\d+(\.\d+)*$', 1) || '/*'
    -- Some apps have extra stuff after the name/version
    WHEN regexp_like(useragent, '^([^/ ]+)(/[^/ ]+) CFNetwork/[^/ ]+ Darwin/[^/ ]+$') THEN regexp_extract(useragent, '^([^/ ]+)(/[^/ ]+) CFNetwork/[^/ ]+ Darwin/[^/ ]+$', 1) || '/* CFNetwork/* Darwin/*'
    -- Mapbox apps follow the pattern Bikemap/22.0.2 Mapbox/5.13.0-pre.1 MapboxGL/0.0.0 (c6fb3581) iOS/15.5.0 (arm64)
    WHEN regexp_like(useragent, '^([^/ ]+)(/[^/ ]+) Mapbox/') THEN regexp_extract(useragent, '^([^/ ]+)(/[^/ ]+) Mapbox/', 1) || '/* Mapbox/*'
    WHEN regexp_like(useragent, '^([^/ ]+)(/[^/ ]+) GLMap/') THEN regexp_extract(useragent, '^([^/ ]+)(/[^/ ]+) GLMap/', 1) || '/* GLMap/*'

    WHEN useragent LIKE 'Mozilla/%%' AND referer != '' THEN referer -- This will only show referers that are not https ones, e.g. flash apps
    ELSE useragent END AS app,
    requests,
    cachehit
    FROM {tilelog.constants.FASTLY_MINIMISED_LOGS}
WHERE year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
    AND (
        requestedwith != '' -- Leaflet-based apps
        OR referer = '' -- Non-websites
        OR (referer NOT LIKE 'https://%%/%%' AND referer NOT LIKE 'http://%%/%%') -- Referer set, but not a website
    )
) AS combined_requests
GROUP BY app
HAVING SUM(requests) > %(tps)d*86400
ORDER BY SUM(requests) DESC
    """  # noqa: E501

    curs.execute(query, {"year": date.year, "month": date.month,
                         "day": date.day, "tps": tilelog.constants.MIN_TPS})
    click.echo("Writing host usage to file")
    csvwriter = csv.writer(dest, dialect=csv.unix_dialect,
                           quoting=csv.QUOTE_NONNUMERIC)
    # Write the header row
    csvwriter.writerow(["app", "tps", "tps_miss"])
    # Write the rows
    csvwriter.writerows(curs)

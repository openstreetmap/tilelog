import click
from publicsuffixlist import PublicSuffixList
import pyathena
import csv
import datetime
import lzma
import re

MIN_TILE_REQUESTS = 10
MIN_DISTINCT_TILE_REQUESTS = 3

MIN_TPS = 5.0


@click.command()
@click.option('--date', type=click.DateTime(["%Y-%m-%d"]),
              default=(datetime.datetime.utcnow() -
                       datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
              help="Date to generate logs for. Defaults to yesterday.")
@click.option('--staging',
              default="s3://openstreetmap-fastly-processed-logs/tilelogs/",
              help="AWS s3 location for Athena results")
@click.option('--region', default="eu-west-1", help="Region for Athena")
@click.option('--tile', type=click.File('wb'),
              help="File to output tile usage logs to")
@click.option('--host', type=click.File('w', encoding='utf-8'),
              help="File to output host usage logs to")
def cli(date, staging, region, tile, host):
    click.echo("Generating files for {}".format(date.strftime("%Y-%m-%d")))
    with pyathena.connect(s3_staging_dir=staging,
                          region_name=region).cursor() as curs:
        if tile is not None:
            tile_logs(curs, date, tile)
        if host is not None:
            host_logs(curs, date, host)


def tile_logs(curs, date, dest):
    click.echo("Querying for tile usage")
    query = """
SELECT regexp_extract(request, '^GET /(\\d+/\\d+/\\d+).png', 1),
        CAST(COUNT(*) AS varchar)
FROM logs.fastly_logs_v18
WHERE regexp_like(request, '^GET /1?\\d/\\d+/\\d+.png')
    AND status IN (200, 206, 304)
    AND year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
GROUP BY regexp_extract(request, '^GET /(\\d+/\\d+/\\d+).png', 1)
HAVING COUNT(DISTINCT ip) >= %(min_distinct)d
    AND COUNT(*) >= %(min_requests)d
ORDER BY regexp_extract(request, '^GET /(\\d+/\\d+/\\d+).png', 1)
    """
    curs.execute(query, {"year": date.year, "month": date.month,
                         "day": date.day,
                         "min_distinct": MIN_DISTINCT_TILE_REQUESTS,
                         "min_requests": MIN_TILE_REQUESTS})
    click.echo("Writing tile usage to file")
    with lzma.open(dest, "w") as file:
        for tile in curs:
            file.write("{} {}\n".format(tile[0], tile[1]).encode('ascii'))

psl = PublicSuffixList()
def normalize_host(host):
    if host is None:
        return ""
    # IPs don't have a public/private suffix
    if re.match("^(\d+\.){3}\d+$", host):
        return host

    suffix = psl.privatesuffix(host)

    # Something like "localhost", or an invalid host like ".example.com" may return None,
    # so don't try to give just the suffix in those cases
    if suffix is None:
        return host
    return suffix

def host_logs(curs, date, dest):
    click.echo("Querying for host usage")
    query = """
SELECT
host,
cast(count(*) as double)/86400 AS tps,
cast(count(*) FILTER (WHERE cachehit = 'MISS') as double)/86400 AS tps_miss
FROM (
    SELECT regexp_extract(referer,
                          'https?://([^/]+?)(:[0-9]+)?/.*', 1) AS host,
    cachehit
FROM logs.fastly_logs_v18
WHERE status IN (200, 206, 304)
    AND year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
    AND referer != ''
    AND requestedwith = ''
    ) AS stripped_referers
GROUP BY host
ORDER BY COUNT(*) DESC;
    """
    curs.execute(query, {"year": date.year, "month": date.month,
                         "day": date.day})

    # Create a dict of lists of TPS
    grouped_hosts = {}
    for host in curs:
        stripped_hostname = normalize_host(host[0])
        if stripped_hostname not in grouped_hosts:
            grouped_hosts[stripped_hostname] = [host[1], host[2]]
        else:
            # TPS is host[1] and grouped_hosts[][0], so the indexes don't match up
            grouped_hosts[stripped_hostname] = [grouped_hosts[stripped_hostname][0] + host[1],
                                                grouped_hosts[stripped_hostname][1] + host[2]]

    sorted_hosts = sorted([[host, metrics[0], metrics[1]] for (host,metrics) in grouped_hosts.items() if metrics[0] >= MIN_TPS],
           key=lambda host: host[1], reverse=True) # Sort by TPS
    click.echo("Writing host usage to file")
    csvwriter = csv.writer(dest, dialect=csv.unix_dialect,
                           quoting=csv.QUOTE_NONNUMERIC)
    csvwriter.writerows(sorted_hosts)

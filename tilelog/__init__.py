import click
import pyathena
from pyathena.arrow.cursor import ArrowCursor
import datetime
import lzma

import tilelog.constants
import tilelog.aggregate
import tilelog.minimise
import tilelog.location
import tilelog.country
import tilelog.app
import tilelog.host


@click.command()
@click.option('--date', type=click.DateTime(["%Y-%m-%d"]),
              default=(datetime.datetime.utcnow() -
                       datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
              help="Date to generate logs for. Defaults to yesterday.")
@click.option('--staging',
              default="s3://openstreetmap-fastly-processed-logs/tilelogs/",
              help="AWS s3 location for Athena results")
@click.option('--generate-success', is_flag=True, default=False,
              help="Create logs of successful requests in Parquet")
@click.option('--generate-minimise', is_flag=True, default=False,
              help="Create minimised request logs in Parquet")
@click.option('--generate-location', is_flag=True, default=False,
              help="Create location request logs in Parquet")
@click.option('--region', default="eu-west-1", help="Region for Athena")
@click.option('--tile', type=click.File('wb'),
              help="File to output tile usage logs to")
@click.option('--host', type=click.File('w', encoding='utf-8'),
              help="File to output host usage logs to")
@click.option('--app', type=click.File('w', encoding='utf-8'),
              help="File to output app usage logs to")
@click.option('--country', type=click.File('w', encoding='utf-8'),
              help="File with country-level statistics")
def cli(date, staging, generate_success, generate_minimise, generate_location,
        region, tile, host, app, country):
    click.echo(f"Generating files for {date.strftime('%Y-%m-%d')}")
    with pyathena.connect(s3_staging_dir=staging, region_name=region,
                          cursor_class=ArrowCursor).cursor() as curs:

        # Aggregation must be run first, because the other tasks depend on it
        if generate_success:
            tilelog.aggregate.create_parquet(curs, date)
        if generate_minimise:
            tilelog.minimise.create_minimised(curs, date)
        if generate_location:
            tilelog.location.create_location(curs, date)
        if tile is not None:
            tile_logs(curs, date, tile)
        if host is not None:
            tilelog.host.host_logs(curs, date, host)
        if app is not None:
            tilelog.app.app_logs(curs, date, app)
        if country is not None:
            tilelog.country.country_logs(curs, date, country)


def tile_logs(curs, date, dest):
    click.echo("Querying for tile usage")
    query = f"""
SELECT z, x, y,
        COUNT(*)
FROM {tilelog.constants.FASTLY_PARQET_LOGS}
WHERE year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
GROUP BY z, x, y
HAVING COUNT(DISTINCT ip) >= %(min_distinct)d
    AND COUNT(*) >= %(min_requests)d
ORDER BY z, x, y
    """
    curs.execute(query, {"year": date.year, "month": date.month,
                         "day": date.day,
                         "min_distinct": tilelog.constants.MIN_DISTINCT_TILE_REQUESTS,
                         "min_requests": tilelog.constants.MIN_TILE_REQUESTS})
    click.echo("Writing tile usage to file")
    with lzma.open(dest, "w") as file:
        for tile in curs:
            file.write(f"{tile[0]}/{tile[1]}/{tile[2]} {tile[3]}\n".encode('ascii'))

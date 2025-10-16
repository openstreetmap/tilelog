import click
import pyathena
from pyathena.pandas.cursor import PandasCursor
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
              default="s3://openstreetmap-athena-results/",
              help="AWS s3 location for Athena results")
@click.option('--raster-generate-success', is_flag=True, default=False,
              help="Create logs of successful requests in Parquet")
@click.option('--raster-generate-minimise', is_flag=True, default=False,
              help="Create minimised request logs in Parquet")
@click.option('--raster-generate-location', is_flag=True, default=False,
              help="Create location request logs in Parquet")
@click.option('--region', default="eu-north-1", help="Region for Athena")
@click.option('--raster-tile', type=click.File('wb'),
              help="File to output tile usage logs to")
@click.option('--raster-host', type=click.File('w', encoding='utf-8'),
              help="File to output host usage logs to")
@click.option('--raster-app', type=click.File('w', encoding='utf-8'),
              help="File to output app usage logs to")
@click.option('--raster-country', type=click.File('w', encoding='utf-8'),
              help="File with country-level statistics")
def cli(date, staging, raster_generate_success, raster_generate_minimise, raster_generate_location,
        region, raster_tile, raster_host, raster_app, raster_country):
    click.echo(f"Generating files for {date.strftime('%Y-%m-%d')}")
    with pyathena.connect(s3_staging_dir=staging, region_name=region,
                          cursor_class=PandasCursor).cursor() as curs:

        # Aggregation must be run first, because the other tasks depend on it
        if raster_generate_success:
            tilelog.aggregate.create_parquet(curs, date)
        if raster_generate_minimise:
            tilelog.minimise.create_minimised(curs, date)
        if raster_generate_location:
            tilelog.location.create_location(curs, date)
        if raster_tile is not None:
            tile_logs(curs, date, raster_tile)
        if raster_host is not None:
            tilelog.host.host_logs(curs, date, raster_host)
        if raster_app is not None:
            tilelog.app.app_logs(curs, date, raster_app)
        if raster_country is not None:
            tilelog.country.country_logs(curs, date, raster_country)


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

import click
import pyathena
import datetime
import lzma


@click.command()
@click.option('--date', type=click.DateTime(["%Y-%m-%d"]),
              default=(datetime.datetime.utcnow() -
                       datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
              help="Date to generate logs for. Defaults to yesterday.")
@click.option('--staging',
              default="s3://openstreetmap-fastly-processed-logs/tilelogs/",
              help="AWS s3 location for Athena results")
@click.option('--region', default="eu-west-1", help="Region for Athena")
def cli(date, staging, region):
    click.echo("Generating files for {}".format(date.strftime("%Y-%m-%d")))
    with pyathena.connect(s3_staging_dir=staging,
                          region_name=region).cursor() as curs:
        tile_logs(curs, date)


def tile_logs(curs, date):
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
HAVING COUNT(DISTINCT ip) >= 3
    AND COUNT(*) >= 10
ORDER BY regexp_extract(request, '^GET /(\\d+/\\d+/\\d+).png', 1)
    """
    curs.execute(query, {"year": date.year, "month": date.month,
                         "day": date.day})
    filename = "tiles-{}.txt.xz".format(date.strftime("%Y-%m-%d"))
    with lzma.open(filename, "w") as file:
        for tile in curs:
            file.write("{} {}\n".format(tile[0], tile[1]).encode('ascii'))

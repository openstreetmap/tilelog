import click
from publicsuffixlist import PublicSuffixList
import csv
import re

import tilelog.constants


psl = PublicSuffixList()


def normalize_host(host):
    if host is None:
        return ""
    # IPs don't have a public/private suffix
    if re.match(r"^(\d+\.){3}\d+$", host):
        return host

    suffix = psl.privatesuffix(host)

    # Something like "localhost", or an invalid host like ".example.com" may return None,
    # so don't try to give just the suffix in those cases
    if suffix is None:
        return host
    return suffix


def host_logs(curs, date, dest):
    click.echo("Querying for host usage")
    query = f"""
SELECT
host,
cast(COALESCE(SUM(requests), 0) as double)/86400 AS tps,
cast(COALESCE(SUM(requests) FILTER (WHERE cachehit = 'MISS'), 0) as double)/86400 AS tps_miss
FROM (

    SELECT regexp_extract(referer,
                          'https?://([^/]+?)(:[0-9]+)?/.*', 1) AS host,
        requests,
        cachehit
FROM {tilelog.constants.FASTLY_MINIMISED_LOGS}
WHERE year = %(year)d
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

    sorted_hosts = sorted([[host, metrics[0], metrics[1]]
                           for (host, metrics) in grouped_hosts.items()
                           if metrics[0] >= tilelog.constants.MIN_TPS],
                          key=lambda host: host[1], reverse=True)  # Sort by TPS
    click.echo("Writing host usage to file")
    csvwriter = csv.writer(dest, dialect=csv.unix_dialect,
                           quoting=csv.QUOTE_NONNUMERIC)
    # Write the header row
    csvwriter.writerow(["host", "tps", "tps_miss"])
    # Write the rows
    csvwriter.writerows(sorted_hosts)

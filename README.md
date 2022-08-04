# Tilelog

Tilelog is used to generate [tile logs](https://planet.openstreetmap.org/tile_logs/) for the OSMF Standard map layer.

## Requirements

- Access to Athena on the OSMF AWS account with the logs.
- Python 3.6+

## Install

For local development

```sh
python3 -m venv venv
. venv/bin/activate
pip install --editable .
```

## Usage

```
Usage: tilelog [OPTIONS]

Options:
  --date [%Y-%m-%d]   Date to generate logs for. Defaults to yesterday.
  --staging TEXT      AWS s3 location for Athena results
  --generate-success  Create logs of successful requests in Parquet
  --region TEXT       Region for Athena
  --tile FILENAME     File to output tile usage logs to
  --host FILENAME     File to output host usage logs to
  --app FILENAME      File to output app usage logs to
  --help              Show this message and exit.
```

e.g.
```sh
DATE=$(date -u -d "1 day ago" "+%Y-%m-%d")
tilelog --date ${DATE} --tile tiles-${DATE}.txt.xz --host hosts-${DATE}.csv --app apps-${DATE}.csv
```

`--generate-success` can only be run once for each day, so if doing development, should not generally be run as it will interfere with production.

## Format documentation

### Tile logs
Tile logs contain the number of requests per tile in a given 24 hour UTC day. Only tiles where at least 10 requests were made and requests came from at least 3 unique IPs are included for privacy reasons. Requests that were blocked, invalid, or unable to be served due to server load are not included (4xx and 5xx errors).

The format is one tile per line, in the format `z/x/y N` where `z/x/y` is the conventional tile coordinate and `N` is the number of requests.

No particular sorting order of lines is guaranteed.

### Host logs
Host logs contain the website host of sites using tile.openstreetmap.org, their average requests/second, and their average requests/second that were cache misses in a given 24 hour UTC day. For privacy reasons, only sites with at least 432000 requests per day (5 requests/second average) coming from the site are included. Requests that were blocked, invalid, or unable to be served due to server load are not included (4xx and 5xx errors).

The host will normally be a valid domain name, but as this data comes from user requests it may contain other text.

The format is one host per line, in the CSV format `"HOST",N,M` where HOST is the host name, with special characters escaped, N is the requests/second, and M is the requests/second that were cache misses. Hosts are ordered by requests/second

The following may change in the future
- Additional fields added at the end
- The definition of "cache miss"
- The theshold for requests/day to be included
- Handling of invalid domains and empty referers

### App logs
App logs contain the referer of non-website usage, primarily from stand-alone mobile and desktop programs. App name is derived from a combination of User-Agent, X-Requested-With, and non-website Referer.

Multiple app versions are combined and indicated with *.

The format is app host per line, in the CSV format `"APP",N,M` where APP is the app name, with special characters escaped, N is the requests/second, and M is the requests/second that were cache misses. Apps are ordered by requests/second

The following may change in the future
- Additional fields added at the end
- The definition of "cache miss"
- The theshold for requests/day to be included
- Combining of app versions

## Contributing

Unfortunately, testing tilelogs requires access to private logs, making it difficult to test.

Style is `flake8`

## Licence

Copyright (C) 2021-2022 Paul Norman

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

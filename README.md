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

## Licence

Copyright (C) 2021 Paul Norman

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

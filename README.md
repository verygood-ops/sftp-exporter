sftp-exporter
=============

Prometheus metrics exporter for SFTP.


How to run locally
------------------

```bash
python sftp-exporter.py <path_to_config>
```

`path_to_config` should be a path to a
configuration YAML file at the same host.

By default, exporter listens on `127.0.0.1:9339`.

Pass `--host` or `--port` parameters to change
listen interface or port respectively.

How to run in docker
--------------------

```bash
docker run -v <path_to_config>:/config.yml \
 -it quay.io/verygoodsecurity/sftp-exporter:dev-1.0.0
```

How to configure
----------------
Configuration file should contain list of SFTP
check configurations under the single key `checks`

```yaml
checks:
  - host: sftp1.my.org
    port: 4822
    ...
  - host: sftp2.my.org
    port: 4822
```

Available properties for SFTP check configuration:
- `host` A host where SFTP service is listening
- `port` Port of SFTP service (defaults to 22 if not passed)
- `username` Username to authenticate with (defaults to `sftp`)
- `password` Optional password to authenticate with
- `client_key_file` Optional path to local file containing private SSH key to authenticate with
- `timeout` Timeout between checking SFTP (`900` by default)
- `folders` List of folders to check files in (defaults to single item `/`)
- `patterns` List of patterns to check files in (defaults to single pattern `*`)
- `validate_known_hosts` Whether to validate known hosts or not

Smart date handling in file path
--------------------------------
Sometimes either folder name or file name in SFTP can contain
date inside, so check folders or patterns should change over time.
In this case, following parameters could be set to "true" to trigger smart date handling:
- `smart_folder_date` -- enables smart date for folders
- `smart_pattern_date` -- enables smart date for patterns
Then, python `strftime` modifiers can be put into `folders` or `patterns`
  to be replaced with today's date and time values.

For example,

```yaml
checks:
  - smart_pattern_date: true
    patterns:
      - "*%y%m%d*"
```
when called on 31 May, 2021 will match all files that have
`20210531` in the file name.

If you need base date to be set to other values than today,
use
```
- base_pattern_date: yesterday
- base_folder_date: '2 days ago'
```
to set base date to other day when templating.
Base date values are parsed with `dateparser`.


Exported metrics
----------------

`sftp_last_seen_timestamp` -- when the file was spotted on SFTP server last time

Dimensions
 - `folder` Folder name where file was spotted
 - `file` Name of the file have been spotted
 - `host` Host of SFTP server that housed a file

License
-------
MIT.

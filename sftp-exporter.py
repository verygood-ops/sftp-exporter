"""Export statistics on files in a remote SFTP server."""
import argparse
import asyncio
import datetime
import filecmp
import fnmatch
import functools
import logging
import os
import socket
import string
import tempfile
import time
import uuid

from aiohttp import web
import asyncssh
import dateparser
from prometheus_async import aio
from prometheus_client import Gauge
import yaml

# Don't handle dates when loading YAML,
# do it later !
import yaml.constructor
yaml.constructor.SafeConstructor.yaml_constructors[u'tag:yaml.org,2002:timestamp'] = \
    yaml.constructor.SafeConstructor.yaml_constructors[u'tag:yaml.org,2002:str']


logger = logging.getLogger(__name__)
arg_parser = argparse.ArgumentParser('SFTP Exporter for Prometheus.')
arg_parser.add_argument('config_file', nargs='?', default='config.yml',
                        help='Location of a config file.')
arg_parser.add_argument('-H', '--host', default='127.0.0.1',
                        help='Listen on this host.')
arg_parser.add_argument('-P', '--port', type=str, default=9339,
                        help='Listen on this port.')
arg_parser.add_argument('--skip-wrong-config', action='store_true',
                        help='Dont exit on wrong config.')
# "noop" (listdir) timestamp
sftp_file_seen_timestamp = Gauge(name='sftp_last_seen_timestamp',
                                 documentation='Last timestamp when '
                                               'a file have been seen over SFTP.',
                                 labelnames=['folder', 'file', 'host'])
# sftp host health check metrics
sftp_host_up = Gauge(name='sftp_host_up',
                     documentation='Signalizes SFTP host being UP',
                     labelnames=['host', 'username', 'state'])
sftp_put_file_up = Gauge(name='sftp_put_file_up',
                         documentation='Signalizes SFTP folder write-able',
                         labelnames=['host', 'username', 'folder', 'state'])
sftp_get_file_up = Gauge(name='sftp_get_file_up',
                         documentation='Signalizes SFTP folder read-able',
                         labelnames=['host', 'username', 'folder', 'state'])
sftp_del_file_up = Gauge(name='sftp_del_file_up',
                         documentation='Signalizes SFTP folder delete-able',
                         labelnames=['host', 'username', 'folder', 'state'])
# "attributes" exported metrics
sftp_file_modified_timestamp = Gauge(name='sftp_file_modified_timestamp',
                                     documentation='Signalizes SFTP mtime attribute',
                                     labelnames=['folder', 'file', 'host'])
sftp_file_access_timestamp = Gauge(name='sftp_file_access_timestamp',
                                   documentation='Signalizes SFTP mtime attribute',
                                   labelnames=['folder', 'file', 'host'])
sftp_file_size = Gauge(name='sftp_file_size',
                       documentation='Signalizes SFTP mtime attribute',
                       labelnames=['folder', 'file', 'host'])


def file_matcher(smart_date_pattern, base_pattern_date, patterns, f):
    """Determines whether remote file should participate in export or no.

    :param smart_date_pattern: Whether to use smart date pattern matching.
    :param base_pattern_date: A base value to set date to.
    :param patterns: List of patterns for which reporting is enabled.
    :param f: A file name being tested.
    """
    if f in ['.', '..']:
        ret = False
    else:
        if smart_date_pattern:
            now_date = dateparser.parse(base_pattern_date)
            patterns = [
                datetime.datetime.strftime(now_date, p)
                for p in patterns
            ]
        ret = any([fnmatch.fnmatch(f, p) for p in patterns])
    return ret


async def noop_checker(client, folder, now, matcher, **sftp_details):
    """This checker lists every folder and does no operation then.

    Exports sftp_file_seen_timestamp for each seen file that matches pattern.

    :param client: Asyncssh's SFTP client.
    :type client: asyncssh.SFTPClient
    :param folder: A folder to list when connecting.
    :type folder: str
    :param now: Current UNIX timestamp
    :type now: int
    :param matcher: A matcher callback to invoke on each file found.
    :type matcher: function
    :param sftp_details: SFTP details config
    :type sftp_details: dict
    """
    files = await client.listdir(folder)
    matched_files = [f for f in files if matcher(f)]
    for m_f in matched_files:
        sftp_file_seen_timestamp.labels(folder, m_f, sftp_details['host']).set(now)
    return matched_files


async def attributes_checker(client, folder, now, matcher, **sftp_details):
    """This checker uses `readdir()` to get context of a folder,
        with attributes of each file exported.

    :param client: Asyncssh's SFTP client.
    :type client: asyncssh.SFTPClient
    :param folder: A folder to list when connecting.
    :type folder: str
    :param now: Current UNIX timestamp
    :type now: int
    :param matcher: A matcher callback to invoke on each file found.
    :type matcher: function
    :param sftp_details: SFTP details config
    :type sftp_details: dict
    :return:
    """
    files = await client.readdir(folder)
    matched_files = [f for f in files if matcher(f.filename)]
    for m_f in matched_files:
        sftp_file_modified_timestamp\
            .labels(folder, m_f.filename, sftp_details['host']).set(m_f.attrs.mtime)
        sftp_file_access_timestamp \
            .labels(folder, m_f.filename, sftp_details['host']).set(m_f.attrs.atime)
        sftp_file_size \
            .labels(folder, m_f.filename, sftp_details['host']).set(m_f.attrs.size)
    return matched_files


async def put_get_del_checker(client, folder, now, matcher, **sftp_details):
    """This checker uploads .healthcheck file to given folder, gets it,
     compares value and then removes.

    :param client: Asyncssh's SFTP client.
    :type client: asyncssh.SFTPClient
    :param folder: A folder to list when connecting.
    :type folder: str
    :param now: Current UNIX timestamp
    :type now: int
    :param matcher: A matcher callback to invoke on each file found.
    :type matcher: function
    :param sftp_details: SFTP details config
    :type sftp_details: dict
     """
    await noop_checker(client, folder,  now, matcher, **sftp_details)
    check_file_name = sftp_details.get('check_file_name', '.sftp-exporter-health-check')
    if 'check_file_contents' in sftp_details:
        check_file_contents = sftp_details.get('check_file_contents')
    else:
        check_file_contents = uuid.uuid4().hex

    host = sftp_details['host']
    username = sftp_details.get('username', 'sftp')

    def expose(_metric, _state):
        """Expose given metric of put_get_del set."""
        _metric.labels(host, username, folder, _state).set(now)

    remote_file_name = os.path.join(folder, check_file_name)
    fdw, pathw = tempfile.mkstemp()
    try:
        with os.fdopen(fdw, 'w') as tmp:
            tmp.write(check_file_contents)
        await client.put(pathw, remote_file_name)
        expose(sftp_put_file_up, 'Ok')
        fdr, pathr = tempfile.mkstemp()
        try:
            await client.get(remote_file_name, pathr)
            if filecmp.cmp(pathw, pathr):
                expose(sftp_get_file_up, 'Ok')
            else:
                expose(sftp_get_file_up, 'Corrupted')
            try:
                await client.remove(remote_file_name)
                expose(sftp_del_file_up, 'Ok')
                logger.info('put_get_del check finished for {}'.format(sftp_details['host']))
            except asyncssh.SFTPError:
                logger.exception('Failed to perform DEL on {}'.format(sftp_details['host']))
                expose(sftp_del_file_up, 'Error')

        except asyncssh.SFTPError:
            logger.exception('Failed to perform GET on {}'.format(host))
            expose(sftp_get_file_up, 'Error')
        finally:
            os.remove(pathr)
    except asyncssh.SFTPError:
        logger.exception('Failed to perform PUT on {}'.format(host))
        expose(sftp_put_file_up, 'Error')
    finally:
        os.remove(pathw)


def check(callback, **sftp_details):
    """Configure and start a single SFTP check.

    :param callback: Callback to invoke on each file found.
    :param sftp_details: SFTP server folders and patterns detailes.
    """
    host = sftp_details.get('host')
    port = sftp_details.get('port', 22)
    username = sftp_details.get('username', 'sftp')
    password = sftp_details.get('password')
    client_key_file = sftp_details.get('client_key_file')
    folders = sftp_details.get('folders', ['www'])
    patterns = sftp_details.get('patterns', ['*'])
    timeout = sftp_details.get('timeout', 900)
    validate_known_hosts = sftp_details.get('validate_known_hosts', False)

    assert host, 'Host not specified'
    assert isinstance(port, int), 'Invalid port value'
    assert not all([password, client_key_file]), 'Either password ' \
                                                 'or client key should be used, ' \
                                                 'but not both'
    assert any([password, client_key_file]), 'At least one of "password", "client_key_file" ' \
                                             'should be specified.'

    fn = '{}_checker'.format(callback)
    if fn not in globals():
        raise AssertionError('Invalid checker function: {}'.format(callback))

    check_callback = globals()[fn]

    match_callback = functools.partial(
        file_matcher,
        sftp_details.get('smart_pattern_date'),
        sftp_details.get('base_pattern_date', 'today'),
        patterns,
    )

    smart_folder_date = sftp_details.get('smart_folder_date')
    base_folder_date = sftp_details.get('base_folder_date', 'today')

    def prepare_folder(_folder):
        """Prepare folder with smart dates."""
        if smart_folder_date:
            now_date = dateparser.parse(base_folder_date)
            _folder = datetime.datetime.strftime(now_date, _folder)
        return _folder

    async def checker():
        """Invoke actual check logic."""
        now = int(time.time())
        kw = {
            'host': host,
            'port': port,
            'username': username,
        }
        if password:
            kw['password'] = password
        if client_key_file:
            kw['client_keys'] = [client_key_file, ]
        if not validate_known_hosts:
            kw['client_factory'] = _trusting_client
        conn = None
        try:
            conn = await asyncssh.connect(**kw)
            try:
                client = await conn.start_sftp_client()
            except asyncssh.SFTPError:
                logger.exception('Failed to contact host {}'.format(host))
                sftp_host_up.labels(host, username, 'SFTPError').set(now)
            else:
                sftp_host_up.labels(host, username, 'Ok').set(now)
                await asyncio.gather(
                    *[
                        check_callback(client, prepare_folder(folder), now,
                                       match_callback, **sftp_details)
                        for folder in folders
                    ]
                )
        except asyncssh.Error:
            logger.exception('Failed to establish connection to {}'.format(host))
            sftp_host_up.labels(host, username, 'ConnectError').set(now)
        except socket.gaierror:
            logger.exception('Not found host name {}'.format(host))
            sftp_host_up.labels(host, username, 'DNSError').set(now)
        finally:
            if conn:
                conn.close()
                await conn.wait_closed()

    async def checker_loop():
        """Infinite loop that spawns checker tasks."""
        while True:
            asyncio.ensure_future(checker())
            await asyncio.sleep(timeout)

    asyncio.ensure_future(checker_loop())
    logger.info('Started loop for host %s', host)


def _trusting_client():

    class SSHTrustingClient(asyncssh.SSHClient):
        def validate_host_public_key(self, host, addr, port, key):
            return True

    return SSHTrustingClient()


def main():
    args = arg_parser.parse_args()
    config_file_path = os.path.expanduser(args.config_file)
    exit_code = 0
    checks = []
    if not os.path.exists(config_file_path):
        logger.error('Failed to retrieve config file location from parameter!')
        exit_code = 1
    else:
        with open(config_file_path) as fl:
            data = string.Template(fl.read()).substitute(os.environ)
            try:
                config_data = yaml.load(data, Loader=yaml.SafeLoader)
            except yaml.YAMLError:
                logger.exception('Failed to load YAML from config')
                exit_code = 2
            else:
                for check_def in config_data.get('checks', []):
                    checks += [check_def]

    if not exit_code:
        for check_def in checks:
            try:
                check(check_def.get('checker', 'noop'), **check_def)
            except AssertionError as e:
                logger.exception('Failed to load config data: {}'.format(e.args[0]))
                if args.skip_wrong_config:
                    continue
                else:
                    exit_code = 3
                    break
        else:
            app = web.Application()
            app.router.add_get("/metrics", aio.web.server_stats)
            web.run_app(app, host=args.host, port=args.port)

    exit(exit_code)


if __name__ == '__main__':
    logging.basicConfig(
        level=getattr(
            logging,
            os.environ.get('SFTP_EXPORTER_LOG_LEVEL', 'INFO')
        ),
    )
    main()

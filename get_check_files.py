#!/usr/bin/env python3

"""
A script to replace the old get_check_files.pl.
"""

import argparse
import configparser
import csv
from dataclasses import dataclass
from datetime import datetime
import io
import logging
import os.path
import shutil
import sys

from pssh.clients import SSHClient
import afl.dbconnections


@dataclass
class RemoteSpec:
    """ The first three items give what we need to connect to the remote
    host, the last is where we expect to find the files. """
    hostname: str
    user: str
    keyname: str
    directory: str


@dataclass
class LocalSpec:
    """ Owing to the one-time flakiness of network shares, we make a practice
    of writing check files initially to a local filesystem, then copying them
    to a share from which Accounting can make use of them."""

    temp_directory: str
    destination_directory: str


@dataclass
class Company:
    """ Poliops sends files in named cr-{company}.csv: the contents, if any
    we write to a pathname ending /COMPANY/{datetime}.tsv """

    abbreviation: str
    directory_name: str


@dataclass
class FileSpec:
    """ An earlier version of the script had a lot of naming and looking up
    based on company abbreviations, company names, etc. Rather than fool
    further with this, we supply a set of pathnames for each company, and
    pass that as needed. """

    remote_path: str
    remote_path_old: str
    local_path: str
    share_path: str


#
# keys for our configuration.
#

LOCAL = 'local'
REMOTE = 'remote'
DIRECTORY = 'directory'
HOST = 'host'
USER = 'user'
KEYNAME = 'keyname'
TEMP_DIRECTORY = 'temp_directory'
DESTINATION_DIRECTORY = 'destination_directory'
DONE_DIRECTORY = 'done_directory'

KEYS_WANTED = {REMOTE: set([HOST, USER, KEYNAME, DIRECTORY, DONE_DIRECTORY]),
               LOCAL: set([TEMP_DIRECTORY, DESTINATION_DIRECTORY])}


COMPANIES = [Company('afl', 'AFLCIO'),
             Company('cope', 'COPE'),
             Company('wpr', 'WPR')]


FIELD_MAPPINGS = {'FCC': 'LM2',
                  'Project Code': 'PROJECTS',
                  'State Code': 'STATE',
                  'Staffer ID': 'EMPLOYEES',
                  'CommitID': 'COMMITID',
                  'RequestID': 'REQUESTID',
                  'PP Code': 'PROGRAM'}


class RunRecorder:
    """ Leave a trace behind us. """

    def __init__(self) -> None:

        conn = afl.dbconnections.connect('ga_helper')
        self._cursor = conn.cursor()

    def record_found_not_copied(self,
                                pathname: str):
        """ No need to pass in data records. """

        self._cursor.execute("""INSERT INTO ga.poliops_transfers
        (pathname, data_records, status, when_processed)
        VALUES
        (:pathname, 0, 'found but not copied', Sysdate)""",
                             [pathname])

    def record_copy(self,
                    pathname: str,
                    data_records: int):
        """ One of few. """

        self._cursor.execute("""INSERT INTO ga.poliops_transfers
        (pathname, data_records, status, when_processed)
        VALUES
        (:pathname, :data_records, 'copied', Sysdate)""",
                             [pathname, data_records])


def copy_check_files(config_path: str,
                     debugging: bool):
    """ Look for the check request files from Poliops, which will be on
    trfr.aflcio.org. If there is anything but a header, copy the file
    across to this machine.
    """

    def get_configuration(pathname_in: str) -> dict[str, dict[str, str]]:
        """ get the details for remote and local host. """

        retval = {}
        try:
            config_parser = configparser.ConfigParser()
            config_parser.read(pathname_in)
            for group in KEYS_WANTED:
                retval[group] = {}
                for name in KEYS_WANTED[group]:
                    retval[group][name] = config_parser.get(group, name)
        except configparser.Error:
            logging.error('malformed .ini file provided, quitting.')
            exit()

        return retval

    def make_ssh_client(spec: dict[str, str]) -> SSHClient:
        """ setup only. """

        return SSHClient(host=spec[HOST],
                         user=spec[USER],
                         pkey=spec[KEYNAME])

    def name_prospective_files(specs: dict[str, dict[str, str]],
                               companies: list[Company]) -> list[FileSpec]:
        """ return a list of FileSpecs, giving paths at the remote and
        local ends. """

        retval = []
        remote_directory = specs[REMOTE][DIRECTORY]
        temp_directory = specs[LOCAL][TEMP_DIRECTORY]
        share_directory = specs[LOCAL][DESTINATION_DIRECTORY]
        done_directory = specs[REMOTE][DONE_DIRECTORY]
        filename = datetime.today().strftime(
                '%Y%m%d-%H%M') + '.txt'
        for company in companies:
            remote_path = os.path.join(remote_directory,
                                       f'cr-{company.abbreviation}.csv')
            old_path = os.path.join(done_directory,
                                    f'cr-{company.abbreviation}.csv')
            local_path = os.path.join(temp_directory, company.directory_name,
                                      filename)
            share_path = os.path.join(share_directory, company.directory_name,
                                      filename)
            retval.append(FileSpec(remote_path=remote_path,
                                   remote_path_old=old_path,
                                   local_path=local_path,
                                   share_path=share_path))

        return retval

    def available_non_empty_files(client: SSHClient,
                                  prospective_files: list[FileSpec],
                                  recorder: RunRecorder,
                                  logger: logging.Logger) -> list[str]:
        temp = []
        for spec in prospective_files:
            logger.debug('checking for', spec.remote_path)
            host_out = client.run_command(f'ls {spec.remote_path}')
            for line in host_out.stdout:
                temp.append(spec)
        retval = []
        for spec in temp:
            logger.debug(f'checking for {spec.remote_path}')
            host_out = client.run_command(f'wc -l {spec.remote_path}')
            line = host_out.stdout.__next__()
            parts = line.split()
            if len(parts) == 2 and parts[0] != '1':
                retval.append(spec)
                logger.debug(
                    f'found {spec.remote_path} with at least one record')
            else:
                recorder.record_found_not_copied(spec.remote_path)
                logger.debug(f'{spec.remote_path} found but empty')

        return retval

    def rewrite_csv(ifh: io.TextIOBase,
                    ofh: io.TextIOBase) -> int:
        """ ifh is the output stream from ssh, which is giving us essentially
        a CSV text stream.
        ofh is the output stream, to write a tab-delimited file.
        2025-06-02: make sure to strip out tabs from fields--a tab that slipped
          in blew up the 2:15 PM  integration.
        """

        reader = csv.DictReader(ifh)
        fieldnames_out = []
        for name in reader.fieldnames:
            if name in FIELD_MAPPINGS:
                fieldnames_out.append(FIELD_MAPPINGS[name])
            else:
                fieldnames_out.append(name)
        writer = csv.DictWriter(ofh, fieldnames_out, delimiter='\t',
                                quotechar='|', escapechar='\\',
                                lineterminator='\n')
        writer.writeheader()
        for i, row in enumerate(reader):
            row_out = {}
            for name in row:
                if name in FIELD_MAPPINGS:
                    row_out[FIELD_MAPPINGS[name]] = row[name]
                else:
                    row_out[name] = row[name].replace('\t', ' ')
            writer.writerow(row_out)

        return i + 1

    def move_remote_files_aside(client: SSHClient,
                                specs: list[FileSpec]) -> None:
        """ Put it in ~/cr/old, to be out of the way. """

        for spec in specs:
            command_line = f'mv {spec.remote_path} {spec.remote_path_old}'
            print(f'running "{command_line}"')
            host_out = client.run_command(command_line)
            for line in host_out.stdout:
                pass
            exit_code = host_out.exit_code
            if exit_code != 0:
                logging.warning(f'received code of {exit_code}'
                                + f' on {command_line}')

    def copy_files_to_host(client: SSHClient,
                           file_specs: list[FileSpec],
                           recorder: RunRecorder) -> list[str]:
        """
         Copy the non-empty files, rewriting them from CSV to tab-delimited.
        """

        retval = []
        for spec in file_specs:
            with open(spec.local_path, 'w') as ofh:
                host_out = client.run_command(f'cat {spec.remote_path}')
                records = rewrite_csv(host_out.stdout, ofh)
                retval.append(spec)
                recorder.record_copy(spec.remote_path, records)
        return retval

    def copy_files_to_share(specs: list[FileSpec]) -> None:
        for spec in specs:
            shutil.copyfile(spec.local_path, spec.share_path)

    def setup_logger(debug: bool):
        logger = logging.getLogger(__name__)
        handler = logging.StreamHandler(sys.stderr)
        logger.addHandler(handler)
        if debugging:
            logger.setLevel(logging.DEBUG)

        return logger

    recorder = RunRecorder()
    logger = setup_logger(debugging)
    config = get_configuration(config_path)
    client = make_ssh_client(config[REMOTE])
    prospective_files = name_prospective_files(config, COMPANIES)
    files_wanted = available_non_empty_files(client,
                                             prospective_files,
                                             recorder,
                                             logger)
    logging.debug(f'we want {len(files_wanted)} files')
    if len(files_wanted) > 0:
        copied_files = copy_files_to_host(client, files_wanted,
                                          recorder)
        copy_files_to_share(copied_files)
    move_remote_files_aside(client, prospective_files)


parser = argparse.ArgumentParser()
parser.add_argument('config_path')
parser.add_argument('--debugging',
                    action='store_true',
                    default=False)
args = parser.parse_args()
copy_check_files(args.config_path, args.debugging)

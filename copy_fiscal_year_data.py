#!/usr/bin/env python3

"""
This script replaces the old get_fiscal_year_data.pl, which is being retired.

It jumps through the hoops of declaring an "output_converter" function to write
the date-time format in the same fashion as the Perl script did.

The handling of dates to match the old Perl format was made possible by the
documentation at

https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function

"""

import argparse
import configparser
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import os.path
import struct

from pssh.clients import SSHClient
import pyodbc
import afl.odbc_connections


@dataclass
class CompanySpec:
    dbname: str
    date_from: str
    mda_columns: list[str] | None


@dataclass
class ReportSpec:
    table_name: str
    suffix: str
    has_mda_columns: bool


COMPANIES = {
    'aflcio': CompanySpec(
        'AFLCI', '2017-01-01',
        ['Date', 'Check #', 'Commit ID', 'Request ID',
         'LM2 Code', 'LM2 Desc', 'LM2 Amt',
         'Project Code', 'Project Desc', 'Project Amt',
         'Affiliate Code', 'Affiliate Desc', 'Affiliate Amt',
         'State Code', 'State Desc', 'State Amt',
         'Employee Code', 'Employee Desc', 'Employee Amt',
         'Period Code', 'Period Desc', 'Period Amt',
         'Other1 Code', 'Other1 Amt', 'Other1 Desc',
         'Other2 Code', 'Other2 Amt', 'Other2 Desc',
         'DEX_ROW_TS', 'ORPSTDDT', 'ACCT_ROW_ID', 'GL_ROW_ID',
         'Jrn Entry', 'SEQNUMBR', 'ORTRXSRC', 'Generation']),
    'cope': CompanySpec('PAC', '2014-01-01', None),
    'wpr': CompanySpec('WPR', '2014-01-01', None)
}

REPORTS = (
    ReportSpec('Poliops_MDA_View', '', True),
    ReportSpec('S2_GL20_Poliops_exp', '_narrow', False)
)

LOCAL_DIRECTORY = '/home/runner/poliops2'

ROWS_PER_FILE = 49999      # leaving one for the header

# keys for the configuration

LOCAL = 'local'
REMOTE = 'remote'
DIRECTORY = 'directory'
HOST = 'host'
USER = 'user'
KEYNAME = 'keyname'
REMOTE_KEYS = [HOST, USER, KEYNAME]


def copy_fiscal_year_data(config_path: str,
                          debugging: bool):
    """
    config_path: the details on key and directories
    debugging: are we debugging this run?
    """

    def get_configuration(config_path: str) -> tuple[str, dict[str:str]]:
        """ for the local component, we need only the directory,
        for the remote we need also host and key.
        """

        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            local_directory = config.get(LOCAL, DIRECTORY)
            remote_specs = {}
            for key in REMOTE_KEYS:
                remote_specs[key] = config.get(REMOTE, key)

            return local_directory, remote_specs

        except configparser.Error:
            print("""The configuration file provided requires two sections:

            a. local, with the key 'directory'
            b. remote, with the keys 'host', 'user' 'directory', and 'keyname'.

            The file provided is missing one or more of these components.
            """)
            exit()

    def get_connection() -> pyodbc.Connection:
        """ return a connection to the GreatPlains database, being sure to
        set it up to print datetime values in the format we always have,
        e.g Jul  4 1776 09:00AM.
        """

        def datetime_to_string(raw_value: bytes):
            days_since_1900, partial_day = struct.unpack('<2l', raw_value)
            partial_day = round(partial_day / 300.0, 3)
            date_time = datetime(1900, 1, 1) + (
                timedelta(days=days_since_1900)
                + timedelta(seconds=partial_day))
            retval = date_time.strftime('%b %d %Y %I:%M%p')
            if retval[4] == '0':
                retval = retval[0:4] + ' ' + retval[5:]

            return retval

        conn = afl.odbc_connections.connect('gps')
        conn.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP,
                                  datetime_to_string)

        return conn

    def create_reports(local_directory: str,
                       conn: pyodbc.Connection,
                       company_name: str,
                       company_spec: CompanySpec) -> list[str]:

        def make_query_text(company_spec: CompanySpec,
                            report_spec: ReportSpec):
            if (
                    report_spec.has_mda_columns
                    and
                    company_spec.mda_columns is not None
            ):
                columns = ',  \n'.join([f'[{col}]'
                                        for col in company_spec.mda_columns])
            else:
                columns = '*'

            return f"""SELECT {columns}
            FROM {company_spec.dbname}.dbo.{report_spec.table_name}
            WHERE [Date] >= Convert(datetime, ?, 120)
            ORDER BY dex_row_ts DESC"""

        def make_pathname_out(company_name, suffix):

            return os.path.join(local_directory, f'{company_name}{suffix}.tsv')

        def make_writer(header: list[str],
                        local_directory: str,
                        company_name: str,
                        type_suffix: str):
            prefix = os.path.join(local_directory, company_name)

            def _inner(rows: list[list[str]],
                       sequence_suffix: str):
                pathname_out = f'{prefix}{type_suffix}{sequence_suffix}.tsv'
                with open(pathname_out, 'w') as ofh:
                    writer = csv.writer(ofh, delimiter='\t',
                                        lineterminator='\n', escapechar='\\')
                    writer.writerow(header)
                    for row in rows:
                        writer.writerow(row)

                return pathname_out

            return _inner

        retval = []
        for report_spec in REPORTS:
            query_text = make_query_text(company_spec, report_spec)
            pathname_out = make_pathname_out(company_name,
                                             report_spec.suffix)
            cur = conn.cursor()
            cur.execute(query_text, [company_spec.date_from
                                     + ' 00:00:00.000'])
            header = [descr[0] for descr in cur.description]
            write_rows = make_writer(header, local_directory,
                                     company_name, report_spec.suffix)
            sequence_suffix = ''
            rows = cur.fetchmany(ROWS_PER_FILE)
            while len(rows) > 0:
                pathname_out = write_rows(rows, sequence_suffix)
                retval.append(pathname_out)
                rows = cur.fetchmany(ROWS_PER_FILE)
                if len(rows) == 0:
                    logging.debug(f'exhausted on {company_name},'
                                  + f'suffix "{sequence_suffix}"')
                    break
                if sequence_suffix == '':
                    sequence_suffix = '2'
                else:
                    sequence_suffix = str(int(sequence_suffix) + 1)
        return retval

    def copy_files(remote_specs: dict[str:str],
                   pathnames: list[str]):
        """ we don't in this case specify the remote directory, since the
        files go to /home/poliops. """

        client = SSHClient(host=remote_specs[HOST],
                           user=remote_specs[USER],
                           pkey=remote_specs[KEYNAME])
        for name in pathnames:
            filename = os.path.split(name)[1]
            client.copy_file(name, filename)

    if debugging:
        logging.basicConfig(level=logging.DEBUG)
    local_directory, remote_specs = get_configuration(config_path)
    logging.debug(f'local directory is {local_directory}')
    logging.debug('remote specs are ' + remote_specs)
    pathnames_all = []
    conn = get_connection()
    for company, spec in COMPANIES.items():
        pathnames_all += create_reports(local_directory, conn, company, spec)
    copy_files(remote_specs, pathnames_all)


parser = argparse.ArgumentParser(
    "A script to copy Great Plains data to trfr.aflcio.org for retrieval by"
    + " Poliops")
parser.add_argument(
    'config_path',
    help='The pathname of an ini file with the details we need to operate.')
parser.add_argument(
    '--debug',
    help='set the logging level to DEBUG',
    action='store_true')
args = parser.parse_args()
copy_fiscal_year_data(args.config_path, args.debug)

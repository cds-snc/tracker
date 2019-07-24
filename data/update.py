##
# This file must be run as a module in order for it to access
# modules in sibling directories.
#
# Run with:
#   python -m data.update

import subprocess
import typing

# Import all the constants from data/env.py.
from data import env

from data import logger
import os
import csv

LOGGER = logger.get_logger(__name__)


# Orchestrate the overall regular Tracker update process.
#
# Steps:
#
# 1. Kick off domain-scan to scan each domain for each measured thing.
#    - Should drop results into data/output/parents (or a symlink).
#    - If exits with non-0 code, this should exit with non-0 code.
#
# 1a. Subdomains.
#    - Gather latest subdomains from public sources, into one condensed deduped file.
#    - Run pshtt and sslyze on gathered subdomains.
#    - This creates 2 resulting CSVs: pshtt.csv and sslyze.csv
#
# 2. Run processing.py to generate front-end-ready data as data/db.json.
#


# Options:
# scanners
#     list of scanners to use
# domains
#     location of domain list to scan
# output
#     location to store scan output
# options
#     options to pass along to scan and gather operations

def update(scanners: typing.List[str], domains: str, output: str, options):
    scan_command = env.SCAN_COMMAND
    option = ""
    flag = False
    found = False

    file = open(domains, 'r')
    curReader = csv.reader(file, delimiter=',')

    while flag is False:
        option = input("Would you like to skip previously scanned domains? (Y/N)")
        if option.lower() != 'y' and option.lower() != 'n':
            print("Please make a valid selection.")
        else:
            flag = True

    # If user opted NOT to submit previously scanned duplicate domains
    if option.lower() == 'y':

        # If the domainHistory directory has already been created
        if os.path.exists(str(os.path.join(os.getcwd(), 'data/domainHistory'))):
            LOGGER.info("Iterating through domains.csv to find previously scanned domains")
            domainHistory = open(str(os.path.join(os.getcwd(), 'data/domainHistory/domains.csv')), 'r')
            deduped = open(str(os.path.join(os.getcwd(), 'data/domainHistory/dedupedDomains.csv')), 'w+')
            dedupedWriter = csv.writer(deduped)
            histReader = csv.reader(domainHistory, delimiter=',')
            first_row = True

            # Append new domains to the domainHistory and create the intermediary deduped domain list
            with open(str(os.path.join(os.getcwd(), 'data/domainHistory/domains.csv')), 'a') as r:
                histWriter = csv.writer(r)
                for curRow in curReader:
                    for histRow in histReader:
                        if curRow == histRow:
                            found = True
                            break
                    if found is False:
                        histWriter.writerow(curRow)
                        dedupedWriter.writerow(curRow)
                    elif first_row:
                        dedupedWriter.writerow(curRow)
                    else:
                        found = False

            dedupedPath = str(os.path.join(os.getcwd(), 'data/domainHistory/dedupedDomains.csv'))

            LOGGER.info("Scanning new domains.")
            scan_domains(options, scan_command, scanners, dedupedPath, output)
            LOGGER.info("Scan of new domains complete.")

            os.remove(str(os.path.join(os.getcwd(), 'data/domainHistory/dedupedDomains.csv')))

        # If the domainHistory directory has NOT been created, create the directory and load the csv with domain list
        else:
            os.makedirs(str(os.path.join(os.getcwd(), 'data/domainHistory')))
            with open(str(os.path.join(os.getcwd(), 'data/domainHistory/domains.csv')), 'w+') as r:
                histWriter = csv.writer(r)
                for curRow in curReader:
                        histWriter.writerow(curRow)
                        # 1c. Scan domains for all types of things.
            LOGGER.info("Scanning new domains.")
            scan_domains(options, scan_command, scanners, domains, output)
            LOGGER.info("Scan of new domains complete.")

    # If user opted to scan entire domain list
    if option.lower() == 'n':
        # If the domainHistory file exists, update the file
        if os.path.exists(str(os.path.join(os.getcwd(), 'data/domainHistory'))):
            domainHistory = open(str(os.path.join(os.getcwd(), 'data/domainHistory/domains.csv')), 'r')
            histReader = csv.reader(domainHistory, delimiter=',')
            with open(str(os.path.join(os.getcwd(), 'data/domainHistory/domains.csv')), 'a') as r:
                histWriter = csv.writer(r)
                for curRow in curReader:
                    for histRow in histReader:
                        if curRow == histRow:
                            found = True
                            break
                    if found is False:
                        histWriter.writerow(curRow)
                    else:
                        found = False
        # Else create the file and load it with all domains on the domain list
        else:
            os.makedirs(str(os.path.join(os.getcwd(), 'data/domainHistory')))
            with open(str(os.path.join(os.getcwd(), 'data/domainHistory/domains.csv')), 'w+') as r:
                histWriter = csv.writer(r)
                for curRow in curReader:
                    histWriter.writerow(curRow)

        # 1c. Scan domains for all types of things.
        LOGGER.info("Scanning domains.")
        scan_domains(options, scan_command, scanners, domains, output)
        LOGGER.info("Scan of domains complete.")


# Run pshtt on each gathered set of domains.
def scan_domains(
        options: typing.Dict[str, typing.Union[str, bool]],
        command: str,
        scanners: typing.List[str],
        domains: str,
        output: str) -> None:

    full_command = [
        command,
        domains,
        "--scan=%s" % ','.join(scanners),
        "--output=%s" % output,
        # "--debug", # always capture full output
        "--sort",
        "--meta",
    ]

    # Allow some options passed to python -m data.update to go
    # through to domain-scan.
    # Boolean flags.
    for flag in ["cache", "serial", "lambda"]:
        value = options.get(flag)
        if value:
            full_command += ["--%s" % flag]

    # Flags with values.
    for flag in ["lambda-profile"]:
        value = options.get(flag)
        if value:
            full_command += ["--%s=%s" % (flag, str(value))]

    # If Lambda mode is on, use way more workers.
    if options.get("lambda") and (options.get("serial", None) is None):
        full_command += ["--workers=%i" % env.LAMBDA_WORKERS]

    shell_out(full_command)


## Utils function for shelling out.
def shell_out(command, env=None):
    try:
        LOGGER.info("[cmd] %s", str.join(" ", command))
        response = subprocess.check_output(command, shell=False, env=env)
        output = str(response, encoding="UTF-8")
        LOGGER.info(output)
        return output
    except subprocess.CalledProcessError:
        LOGGER.critical("Error running %s.", str(command))
        exit(1)
        return None

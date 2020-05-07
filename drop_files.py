"""
Script used to simulate file drop from source directory to another
Input file used:
    https://drive.google.com/drive/folders/1mDmzNS47-q5ehSJ29kRlWRD-8allSRz_
    split -l 100000 london_crime_by_lsoa.csv
"""
import os
import sys
import shutil
import sched
import logging
from dynaconf import settings

UPSTREAM_DIR = settings.UPSTREAM_DIR
INBOUND_DIR = settings.INBOUND_DIR
TRANSFER_INTERVAL = 2  # seconds

logging.basicConfig(level=logging.DEBUG,
                    datefmt='%T',
                    format='%(asctime)s %(levelname)s %(message)s'
                    )
logger = logging.getLogger(__name__)

s = sched.scheduler()
file_counter = 0


def get_file_list():
    os.chdir(UPSTREAM_DIR)
    file_list = os.listdir('.')
    return file_list


def drop_file(file_name):
    src_file_path = os.path.join(UPSTREAM_DIR, file_name)
    dest_file_path = os.path.join(INBOUND_DIR, file_name)
    global file_counter
    file_counter += 1
    logger.info(f'[{str(file_counter):>3}] copying {src_file_path} to {dest_file_path} started')
    shutil.copy(src_file_path, dest_file_path)
    logger.info(f'[{str(file_counter):>3}] copy {src_file_path} to {dest_file_path} completed')


def main():
    logger.info(f'Started {sys.argv[0]}')
    logger.info(f'Source dir: {UPSTREAM_DIR}')
    logger.info(f'Source dir: {INBOUND_DIR}')
    file_list = get_file_list()
    logger.info(f'Number of files: {len(file_list)}')
    for sequence, file_name in enumerate(file_list):
        s.enter(delay=sequence*TRANSFER_INTERVAL,
                priority=1,
                action=drop_file,
                argument=(file_name,)
                )
    logger.info('Executing events')
    s.run()
    logger.info(f'Completed {sys.argv[0]}')


if __name__ == '__main__':
    main()

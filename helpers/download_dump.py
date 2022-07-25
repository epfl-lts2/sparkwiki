import argparse
from pySmartDL import SmartDL, HashFailedException
from urllib.parse import urljoin
import requests
import os
import hashlib
import logging

baseurl = 'https://dumps.wikimedia.org/{}/{}/'

def parse_info(info):
    if info['status'] != 'done':
        return None
    info_file = list(info['files'].values())
    return info_file[0]

def get_status(lang_code, date_code):
    joblist = ['pagetable', 'pagelinkstable', 'redirecttable', 'categorylinkstable']
    dumpstatus_url = urljoin(baseurl.format(lang_code + 'wiki', date_code), 'dumpstatus.json')
    r = requests.get(dumpstatus_url).json()
    info_files = [parse_info(r['jobs'][p]) for p in joblist]
    return info_files

def check_file(info_file, path):
    # check if file exists and has correct size
    if not os.path.exists(path) or os.path.getsize(path) != info_file['size']:
        return False
    
    # if size match, compute the checksum
    with open(path, 'rb') as file_to_check:
        # read contents of the file
        data = file_to_check.read()    
        # pipe contents of the file through
        sha1_returned = hashlib.sha1(data).hexdigest()
    logging.debug('File {} sha1={} current sha1={}'.format(path, info_file['sha1'], sha1_returned))
    return sha1_returned == info_file['sha1']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--lang', help='language code')
    parser.add_argument('--date', help='date code')
    parser.add_argument('--output-dir', help='output directory')
    args = parser.parse_args()
    info_files = get_status(args.lang, args.date)
    logging.debug('Info found {}'.format(info_files))
  
    for p in info_files:
        filename = os.path.basename(p['url'])
        output_file = os.path.join(args.output_dir, filename)
        if not check_file(p, output_file):
            download_url = urljoin(baseurl.format(args.lang + 'wiki', args.date), filename)
            logging.info('Downloading {}'.format(filename))
            logging.debug('Download url {}'.format(download_url))
            sdl = SmartDL(download_url, output_file, threads=1)
            sdl.add_hash_verification('sha1', p['sha1'])
            try:
                sdl.start()
            except HashFailedException:
                logging.error('File hash verification failed {}', filename)
        else:
            logging.info('Found local {} with matching checksum - skipping download.'.format(filename))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()

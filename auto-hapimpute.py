"""Haplotype Imputer Automatic Submission and Job Tracking

Author: Simon Liu
Date created: April 25, 2021
Date last modified: April 29, 2021
Python Version: 3.8.2

This script allows the user submit all files in a directory to the
Haplotype Imputer at http://hapimpute.opencravat.org/. The script will
also monitor the progress of each job and automatically download the
resulting imputed data file once the job is complete.

This script will open a Chrome window that is automatically controlled,
which can be minimized for the duration of the run.

This script requires that `tqdm` and `selenium` be installed within the
Python environment you are running this script in. The chromedriver
executable is also needed, which can be downloaded from
https://chromedriver.chromium.org/downloads. Please download the
version corresponding to the version of Chrome that is installed on
your system.
"""

import os
import sys
import time
import pickle
import argparse
import requests

from tqdm import tqdm
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


def submit(filepaths, driver):
    """Submits each file as a job onto hapimpute.

    Parameters
    ----------
    filepaths : list
        The list of 23andme files to submit as jobs
    driver : Chrome
        The browser object to use for web operations

    Returns
    -------
    dict
        str:str, mapping job ID to input filename
    """

    jobids = {}

    # submit jobs
    for n, f in enumerate(tqdm(filepaths)):
        if '23andme' not in f: continue
        driver.get('http://hapimpute.opencravat.org/')
        time.sleep(0.25)
        upload = driver.find_element_by_id('inputfile');
        upload.send_keys(f);
        time.sleep(0.25)
        driver.find_element_by_xpath('//input[@value="Submit"]').click()
        time.sleep(0.25)
        id = driver.find_element_by_id('jobid').text
        jobids[id] = f.split('/')[-1]
        time.sleep(0.25)

    return jobids

def check(jobids, dest, driver):
    """Checks job status and downloads imputed output files.

    The function checks in the order in which the jobs were submitted.
    Once it encounters a job that is still processing, the function
    will stop checking and sleep for 5 minutes before starting the
    process again. This is to reduce the amount of requests that is
    sent to the server.

    Parameters
    ----------
    jobids : dict
        The dict of job IDs to input filename
    dest : str
        The output directory path
    driver : Chrome
        The browser object to use for web operations
    """

    complete = []
    error = []

    if not os.path.exists(dest):
        os.mkdir(dest)

    # check each file id
    while len(jobids) > len(complete):
        print('sleeping for 5 minutes')
        for i in tqdm(range(300)): time.sleep(1)
        for id in jobids:
            if id in complete: continue
            driver.get('http://hapimpute.opencravat.org/')
            time.sleep(0.25)
            inputid = driver.find_element_by_id('jobid');
            inputid.send_keys(id)
            time.sleep(0.25)
            driver.find_element_by_xpath('//input[@value="Check"]').click()
            time.sleep(0.25)
            download = driver.find_element_by_id('outlink');
            if download.is_displayed():
                sys.stderr.write('\rDownloaded %d of %d - %s' % (len(complete) + 1, len(jobids), id))
                link = download.get_attribute('href') # get link
                with open(os.path.join(dest, jobids[id]), 'wb') as f:
                    f.write(requests.get(link).content)
                complete.append(id)
            elif driver.find_element_by_id('status').text == 'ERROR':
                complete.append(id)
                error.append(jobsid[id])
                print('ERROR on file %s' % jobids[id])
            else:
                break
            time.sleep(0.25)
    print('Downloaded %d of %d - %s' % (len(complete) + 1, len(jobids), id))
    if error: print('ERRORS in', error)

if __name__ == '__main__':
    # parse command line arguments
    parser = argparse.ArgumentParser(description='Automatic Haplotype Imputer Job Submission and Tracking.')
    parser.add_argument('-i', nargs=1, help='input directory of 23andme files', required=True)
    parser.add_argument('-o', nargs=1, help='output directory for imputed files', required=True)
    parser.add_argument('-c', nargs=1, help='path to chromedriver executable (default is current working directory)')
    parser.add_argument('-f', action='store_true', help='use saved ID file')
    args = vars(parser.parse_args())

    # define required arguments
    input_dir = args['i'][0]
    output_dir = args['o'][0]
    chromedriver_path = args['c'][0] if args['c'] else os.getcwd()

    driver = Chrome(chromedriver_path)
    driver.get('http://hapimpute.opencravat.org/')

    if args['f']:
        fn = os.path.join(os.path.join(input_dir, os.pardir), 'ids.pickle')
        if not os.path.isfile(fn):
            sys.exit('ERROR: file does not exist')
        with open(fn, 'rb') as f:
            ids = pickle.load(f)
    else:
        paths = [os.path.join(input_dir, file) for file in os.listdir(input_dir)]
        ids = submit(paths, driver)

        # save ids in case checking process is disrupted later
        fn = os.path.join(os.path.join(input_dir, os.pardir), 'ids.pickle')
        with open(fn, 'wb') as f:
            pickle.dump(ids, f)

    # check for completed jobs
    check(ids, output_dir, driver)
    driver.close()

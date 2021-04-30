"""OpenSNP Data Scraper by Phenotype

Author: Simon Liu
Date created: March 31, 2021
Date last modified: April 29, 2021
Python Version: 3.8.2

This script allows the user to grab all 23andme genomes for a given
phenotype. It is assumed that the given phenotype is valid. The script
also performs a simple validation check and moves files that fail to
a separate directory. 

This tool saves phenotypes into a folder structure separated by
variant name as shown below:

* (root)
    * [phenotype#]
        * [variant1]
            * file1
            * file2
            * ...
        * [variant2]
            * file3
            * ...
        * ...

This script requires that `bs4` be installed within the Python
environment you are running this script in.
"""

from __future__ import division

import os
import re
import sys
import time
import pickle
import argparse
import requests
import multiprocessing as mp

from bs4 import BeautifulSoup


def save(files, fn):
    """Saves list of file URLs to allow user to resume download.

    Parameters
    ----------
    files : list
        The list of all user 23andme files
    pheno : str
        The phenotype ID of interest
    """

    with open(fn, 'wb') as f:
        pickle.dump(files, f)

def read(fn):
    """Reads list of file URLs saved previously.

    Parameters
    ----------
    pheno : str
        The phenotype ID of interest
    """

    if not os.path.isfile(fn):
        sys.exit('..ERROR: file does not exist')
    with open(fn, 'rb') as f:
        return pickle.load(f)

def get_html(url):
    """Gets and parses HTML information at the given URL.

    If the URL get fails, an error message is displayed and the
    function returns None. This allows the program to continue running
    without being interrupted by an invalid URL.

    Parameters
    ----------
    url : str
        The url at which to grab the html source

    Returns
    -------
    BeautifulSoup
        a BeautifulSoup object of the HTML response from the given URL
    """

    page = requests.get(url)
    if page.status_code != 200:
        print('..ERROR: server returned %d for %s' % (page.status_code, url))
        return None
    return BeautifulSoup(page.text, 'html.parser')

def get_users(pheno):
    """Creates a list of users with listed variants for a given pheno.

    It is assumed that the given pheno is valid. The phenotype number
    can be found by either looking for the corresponding ID in the
    list of phenotypes found at https://opensnp.org/phenotypes or
    isolated from the URL of a specific phenotype page, e.g. 24 for
    astigmatism (https://opensnp.org/phenotypes/24).

    Parameters
    ----------
    pheno : str
        The phenotype ID of interest

    Returns
    -------
    dict
        str:list(str), mapping variants to list of users
    """

    page = get_html('https://opensnp.org/phenotypes/%s' % pheno)
    if not page:
        sys.exit('..ERROR: get phenotype page unsuccessful')

    users = {}
    for u in page.find(id='users').find_all('a', href=re.compile(r'/users/*')):
        ph = u.parent.parent.find_all('td')[1].text
        usr_num = u['href'].split(os.path.sep)[-1]
        if ph in users:
            users[ph].append(usr_num)
        else:
            users[ph] = [usr_num]

    return users

def get_file(user):
    """Gets 23andme file URL for a given user.

    This function specifically looks for 23andme files by checking for
    the '23andme' string in the filename. It can be assumed that a
    similar approach can be used for ancestry or other file formats but
    they have not been tested.

    Parameters
    ----------
    user : (str, str)
        The user ID and variant name

    Returns
    -------
    tuple
        (user ID, variant name, 23andme file URL)
    """

    u, p = user
    page = get_html('https://opensnp.org/users/' + u)
    if not page:
        return (u,p,None)
    files = page.find(id='genotypes').find_all('a')
    for f in files:
        if f['href'].split(os.path.sep)[-1].split('.')[1] == '23andme':
            return (u,p,f['href'])
    return (u,p,None)

def get_files(users, n_processes=mp.cpu_count()):
    """Gets 23andme file URLs for all users grouped by variant.

    This function uses a pool of processes to speed up request
    processing for user 23andme file URLs. By default, the number of
    processes is set at the number of available CPU cores.

    Parameters
    ----------
    users : dict
        The dictionary of lists of users separated by variant
    n_processes : int
        The number of concurrent processes to create (default is
        mp.cpu_count())

    Returns
    -------
    list
        A list of (user ID, variant name, 23andme file URL)
    """

    users_rev = [(u,p) for p in users for u in users[p]]
    with mp.Pool(processes=n_processes) as pool:
        return pool.map(get_file, users_rev)

def scrape(pheno, n_processes=mp.cpu_count()):
    """Saves list of user 23andme file URLs scraped from OpenSNP
    for given pheno.

    This function assumes a valid phenotype ID. The list of failed
    users is also saved but only for debugging purposes. To access that
    file, pass in the file path to the read function.

    Parameters
    ----------
    pheno : str
        The phenotype ID of interest
    """

    print('Scraping phenotype %s' % pheno)
    users = get_users(pheno)
    n = sum([len(users[p]) for p in users])
    print('..Found %d users' % n)

    start = time.time()
    files = get_files(users, n_processes=n_processes)

    tmp = {}
    err = []
    for u,p,f in files:
        if f:
            if p in tmp:
                tmp[p].append(f)
            else:
                tmp[p] = [f]
        else:
            err.append(u)
    save(tmp, 'scrape_%s.pickle' % pheno)
    save(err, 'err_%s.pickle' % pheno)
    print('..Success %d, Failed %d' % (n - len(err), len(err)))

def download_file(file):
    """Downloads file at given URL.

    This function skips the download if a file with the same filename
    is found. To ensure valid file download, delete all existing files.

    Parameters
    ----------
    file : str
        The URL of the file to download
    """

    ext, path = file
    if os.path.isfile(path):
        print('..WARNING: file exists %s' % ext)
        return
    r = requests.get('https://opensnp.org/' + ext, allow_redirects=True)
    open(path, 'wb').write(r.content)

def download(pheno, path=os.getcwd(), n_processes=mp.cpu_count()):
    """Downloads 23andme files of interest.

    This function checks for existing folder structures and creates new
    ones if they aren't found. To ensure valid file downloads, delete
    all existing files and folders before download.

    Parameters
    ----------
    pheno : str
        The phenotype ID of interest
    path : str
        The download root path (default is os.getcwd())
    n_processes : int
        The number of concurrent processes to create (default is
        mp.cpu_count())
    """

    print('Downloading files for phenotype %s' % pheno)
    files = read('scrape_%s.pickle' % pheno)
    root = os.path.join(path, pheno)
    if not os.path.exists(root):
        os.mkdir(root)

    tmp = []
    n = sum([len(files[p]) for p in files])
    for p in files:
        wd = os.path.join(root, p)
        if not os.path.isdir(wd):
            os.mkdir(wd)
        tmp.extend([(f, os.path.join(wd, f.split(os.path.sep)[-1].split('?')[0])) for f in files[p]])

    with mp.Pool(processes=n_processes) as pool:
        for i, _ in enumerate(pool.imap_unordered(download_file, tmp), start=1):
            sys.stderr.write('\r..Downloaded %d of %d' % (i, n))
        print('\r..Downloaded %d of %d' % (i, n))

def move_bad(bad, path, name):
    """Helper function to rename and move bad files.

    Parameters
    ----------
    bad : str
        The root directory to hold the bad files
    path : str
        The original path of the bad file
    name : str
        The name of the bad file
    """

    old = os.path.join(path, name)
    new = os.path.join(bad, os.path.basename(os.path.normpath(path)), 'bad_' + name)
    os.renames(old, new)

def valid_23andme(root, pheno):
    """Simple verification of 23andme files based on OpenCRAVAT's
    23andme converter check.

    This function checks if the string '23andMe' exists in the first
    line of the downloaded file. This mirrors the same check that
    OpenCRAVAT's 23andme converter uses so that you don't have to wait
    until you've uploaded your files to find out that one of them isn't
    valid.

    Parameters
    ----------
    dir : str
        The root directory of the downloaded files
    """

    print('Running simple 23andme validation')

    # path for folder to hold bad downloads
    bad = os.path.join(root, '24-bad')
    downloads = os.path.join(root, pheno)

    err_count = 0
    for path, subdirs, files in os.walk(downloads):
        for name in files:
            if '23andme' in name:
                try:
                    with open(os.path.join(path, name), 'r') as f:
                        first = f.readline()
                        if '23andMe' not in first.split():
                            print('..ERROR: does not contain 23andme in first line,', name)
                            move_bad(bad, path, name)
                            err_count += 1
                except Exception as e:
                    print('..ERROR: an exception occurred,', name, ',', e)
                    move_bad(bad, path, name)
                    err_count += 1
    print('..Moved %d bad files' % err_count)

if __name__ == '__main__':
    # parse command line arguments
    parser = argparse.ArgumentParser(description='OpenSNP Data Scraper by Phenotype.')
    parser.add_argument('-p', nargs=1, help='phenotype of interest', type=int, required=True)
    parser.add_argument('-o', nargs=1, help='download path (default is current working directory)')
    parser.add_argument('-n', nargs=1, help='number of concurrent processes to use (default is total CPU cores)', type=int)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', action='store_true', help='only scrape for 23andme file URLs')
    group.add_argument('-d', action='store_true', help='only download 23andme files from saved file')
    group.add_argument('-c', action='store_true', help='only run validation check')
    args = vars(parser.parse_args())

    # define required arguments
    pheno = str(args['p'][0])
    root = args['o'][0] if args['o'] else os.getcwd()
    n = args['n'][0] if args['n'] else mp.cpu_count()
    both = not args['s'] and not args['d'] and not args['c']

    # run scraper
    if args['s'] or both:
        scrape(pheno, n_processes=n)

    # run downloader
    if args['d'] or both:
        download(pheno, path=root, n_processes=n)

    # run simple file validation
    if args['c']:
        valid_23andme(root, pheno)

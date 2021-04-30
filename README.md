# fcbb-tools

## Contents
This repository contains some of the script I wrote for 580.488 
Foundations of Computational Biology and Bioinformatics to simplify 
data collection and processing. 

### opensnp-scraper.py
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

### auto-hapimpute.py
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

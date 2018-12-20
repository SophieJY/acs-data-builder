#!/bin/bash
python3.7 sequentialBuildDataCSV.py codebook.csv 1317_ACS_BG c46c024961efcefec09d6a114ceb692cd00706e3
python3.7 sequentialBuildDataCSV.py codebook.csv 1317_ACS_TR c46c024961efcefec09d6a114ceb692cd00706e3
python3.7 sequentialBuildDataCSV.py codebook.csv 1317_ACS_CTY c46c024961efcefec09d6a114ceb692cd00706e3

mkdir output

python acs_cleaner.py "./1317_ACS_BG.csv" "./output/acs_bg_13_17" "bg"
python acs_cleaner.py "./1317_ACS_TR.csv" "./output/acs_tr_13_17" "tr"
python acs_cleaner.py "./1317_ACS_CTY.csv" "./output/acs_cty_13_17" "cty"

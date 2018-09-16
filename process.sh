#!/bin/bash
python buildDataCSV.py codebook.csv 1216_ACS_BG c46c024961efcefec09d6a114ceb692cd00706e3
python buildDataCSV.py codebook.csv 1216_ACS_TR c46c024961efcefec09d6a114ceb692cd00706e3
python buildDataCSV.py codebook.csv 1216_ACS_CTY c46c024961efcefec09d6a114ceb692cd00706e3

mkdir output

python acs_cleaner.py "./1216_ACS_BG.csv" "./output/acs_bg_12_16" "bg"
python acs_cleaner.py "./1216_ACS_TR.csv" "./output/acs_tr_12_16" "tr"
python acs_cleaner.py "./1216_ACS_CTY.csv" "./output/acs_cty_12_16" "cty"
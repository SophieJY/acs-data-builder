#-------------------------------------------------------------------------------
# Author:      Shamik Maganlal
# Created:     08/25/2015
#------------------------------------------------------------------------------

import csv
import math
from itertools import chain
import sys
import json


#----------------------------------------------------------------------

# {
#     'name': 'First Level',
#     'data': null,
#     children: [{
#         'name': 'Second Level',
#         'data': {
#             'variable_name': 'VarName1',
#             'feeder': ['B00000000']
#         },
#         children: []
#     }]
# }

NUMBER = 0
NAME = 1
AREA_REPORT = 2
PMT = 3
TRACT = 4
BG = 5
COUNTY = 6
NEW_VAR_NAME = 7
LEVEL_ONE = 8
LEVEL_TWO = 9
LEVEL_THREE = 10
LEVEL_FOUR = 11

def gen_codebook_dict(path="codebook.csv", flatten=False, level=None):
    level = {
        'area': AREA_REPORT,
        'pmt': PMT,
        'tr': TRACT,
        'bg': BG,
        'cty': COUNTY,
    }[level] if level else PMT

    with open(path, 'r') as inputfile:
        reader = csv.reader(inputfile)
        next(reader, None); next(reader, None) # skip two rows of headers

        # build feeder dict
        feeder_dict = {}
        for row in reader:
            number = row[NUMBER].replace("E/M", "").strip()
            new_var_name = row[NEW_VAR_NAME]
            if new_var_name and new_var_name is not '-':
                if new_var_name not in feeder_dict:
                    feeder_dict[new_var_name] = []
                feeder_dict[new_var_name].append(number)

        inputfile.seek(0) # go back to the beginning of the file
        next(reader, None); next(reader, None) # skip two rows of headers

        # build codebook dict
        done = []
        codebook_dict = []
        for row in reader:
            if row[NEW_VAR_NAME] and row[NEW_VAR_NAME] in done:
                continue
            elif row[NEW_VAR_NAME]:
                done.append(row[NEW_VAR_NAME])

            if not row[level]:
                continue

            if row[LEVEL_ONE] and row[LEVEL_ONE] is not '-':
                codebook_dict.append({
                    'name': row[LEVEL_ONE],
                    'data': { 'variable_name': row[NEW_VAR_NAME],
                              'feeder': feeder_dict[row[NEW_VAR_NAME]], } if row[NEW_VAR_NAME] else None,
                    'children': [],
                })
            elif row[LEVEL_TWO] and row[LEVEL_TWO] is not '-':
                (codebook_dict if flatten else codebook_dict[-1]['children']).append({
                    'name': row[LEVEL_TWO],
                    'data': { 'variable_name': row[NEW_VAR_NAME],
                              'feeder': feeder_dict[row[NEW_VAR_NAME]], } if row[NEW_VAR_NAME] else None,
                    'children': [],
                })
            elif row[LEVEL_THREE] and row[LEVEL_THREE] is not '-':
                (codebook_dict if flatten else codebook_dict[-1]['children'][-1]['children']).append({
                    'name': row[LEVEL_THREE],
                    'data': { 'variable_name': row[NEW_VAR_NAME],
                              'feeder': feeder_dict[row[NEW_VAR_NAME]], } if row[NEW_VAR_NAME] else None,
                    'children': [],
                })
            elif row[LEVEL_FOUR] and row[LEVEL_FOUR] is not '-':
                (codebook_dict if flatten else codebook_dict[-1]['children'][-1]['children'][-1]['children']).append({
                    'name': row[LEVEL_FOUR],
                    'data': { 'variable_name': row[NEW_VAR_NAME],
                              'feeder': feeder_dict[row[NEW_VAR_NAME]], } if row[NEW_VAR_NAME] else None,
                    'children': [],
                })

        return codebook_dict


if __name__ == "__main__":
    if len(sys.argv) < 1:
        raise Exception('Too few arguments! Syntax: \n python gen_codebook_dict.py codebookcsv [level]')
    codebook_file = sys.argv[1]
    codebook = gen_codebook_dict(codebook_file, level=sys.argv[2])
    print ("""define(function () {
        //Define the module value by returning a value.
        return""" + json.dumps(codebook, sort_keys=True, indent=4, separators=(',', ': ')) + """
        });""")

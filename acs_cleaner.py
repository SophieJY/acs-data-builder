#-------------------------------------------------------------------------------
# Author:      Shamik Maganlal
# Created:     03/03/2015
#------------------------------------------------------------------------------

import csv
import math
from itertools import chain
import sys
from gen_codebook_dict import gen_codebook_dict

#----------------------------------------------------------------------
accepted_county_prefixes = [
        "05000US06025",
        "05000US06037",
        "05000US06059",
        "05000US06065",
        "05000US06083",
        "05000US06111"
    ]
moe_suffix = "_moe"
metadata_columns = ['COMPONENT','TRACT','GEOID','SUMLEVEL','ST','COUNTY','BLKGRP']

#----------------------------------------------------------------------

def acs_cleaner(codebook, reader_obj, output_path, level=None):
    with open(output_path + ".csv", "w") as out_file, open(output_path + moe_suffix + ".csv", "w") as moe_file:
        missing_columns = set()
        fieldnames = [var['data']['variable_name'] for var in codebook if var['data']] + metadata_columns
        writer = csv.DictWriter(out_file, delimiter=',', fieldnames=fieldnames)
        writer.writeheader()
        moe_writer = csv.DictWriter(moe_file, delimiter=',', fieldnames=fieldnames)
        moe_writer.writeheader()

        for input_line in reader_obj:
            # TODO: test to see if this is in a county we want
            output_row = {}
            moe_row = {}
            for new_var in codebook:
                if new_var['data']:
                    output_row[new_var['data']['variable_name']] = 0
                    moe_row[new_var['data']['variable_name']] = 0
                    squared_moe = 0

                    for input_column in new_var['data']['feeder']:
                        if (input_column + "E") in input_line:
                            try:
                                output_row[new_var['data']['variable_name']] += int(float(input_line[input_column + "E"]))
                                moe = float(input_line[input_column + "M"])
                                squared_moe += moe**2
                            except:
                                missing_columns.add(input_column)
                        else:
                            missing_columns.add(input_column)

                    moe_row[new_var['data']['variable_name']] = math.trunc(math.sqrt(squared_moe)*1e7)/1e7

            # add metadata columns
            for metadata_column in metadata_columns:
                if metadata_column in input_line:
                    output_row[metadata_column] = input_line[metadata_column].replace('15000US', '1500000US') # clean up some oddities in the data
                    moe_row[metadata_column] = input_line[metadata_column].replace('15000US', '1500000US')
                else:
                    missing_columns.add(input_column)

            # add Herfindahl-Hirschman Diversity Index
            if level == "bg" or level == "cty":
                race_variables = ["B03002_003", "B03002_004", "B03002_005", "B03002_006", "B03002_007", "B03002_008", "B03002_009", "B03002_012",]
                race_total = sum([float(input_line[variable+"E"]) for variable in race_variables])
                if race_total != 0:
                    output_row["HHIDI_race"] = sum([(float(input_line[variable+"E"])/race_total)**2 for variable in race_variables])
                    moe_row["HHIDI_race"] = 0

            # add Average Travel Time
            if level == "bg":
                try:
                    output_row["ATT_Total"] = int(float(input_line["B08135_001E"]) / float(input_line["B08303_001E"]))
                    moe_row["ATT_Total"] = float(input_line["B08135_001M"]) / float(input_line["B08303_001M"])
                except:
                    missing_columns.add("B08135_001")

            # add COUNTY
            if level == "cty":
                output_row["COUNTY"] = input_line["GEOID"][-3:]

            writer.writerow(output_row)
            moe_writer.writerow(moe_row)

        return missing_columns

if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise Exception('Too few arguments! Syntax: \n python acs_cleaner.py inputfile outputfile [level]')
    input_file = sys.argv[1] #"0913_ACS_BG.csv"
    output_file = sys.argv[2] #"acs_bg_09_13"
    codebook = gen_codebook_dict("codebook.csv", flatten=True, level = sys.argv[3])

    f_obj = open(input_file)
    reader = csv.DictReader(f_obj, delimiter=',')

    acs_cleaner(codebook, reader, output_file, level=sys.argv[3])

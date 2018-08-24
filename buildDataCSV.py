from census import Census
from us import states
import sys
import csv

#----------------------------------------------------------------------
counties = {'037'}
geoid_prefix = '15000US'

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

geoid_array = []

#----------------------------------------------------------------------

def request_builder(variables):
    c = Census("put your census API key here")
    output_dict = {}
    for county in counties:
        first_time = True
        for variable in variables:
            ret_array = c.acs5.state_county_blockgroup(('NAME', variable), states.CA.fips, county, Census.ALL)
            for ret_element in ret_array:
                geoid = geoid_prefix + ret_element['state'] + ret_element['county'] + ret_element['tract'] + ret_element['block group']
                print(geoid)
                if first_time:
                    output_dict[geoid] = {}
                    geoid_array.append(geoid)
                    output_dict[geoid]['GEOID'] = geoid
                    output_dict[geoid][variable] = ret_element[variable]
                else:
                    output_dict[geoid][variable] = ret_element[variable] 
            first_time = False
    return output_dict

def write_data_to_csv(output_dict, output_path, variables):
    with open(output_path + ".csv", "w") as output_file:
        writer = csv.DictWriter(output_file, delimiter=',', fieldnames=['GEOID'] + variables)
        writer.writeheader()
        for geoid in geoid_array:
            writer.writerow(output_dict[geoid])

def generate_variable_array(path, level):
    level = {
        'AREA': AREA_REPORT,
        'PMT': PMT,
        'TR': TRACT,
        'BG': BG,
        'CTY': COUNTY,
    }[level]
    variables = ['B01001_001E']
    return variables

if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise Exception('Too few arguments! Syntax: \npython ' + sys.argv[0] + ' codebookcsv outputFilename')
    codebook_csv_file = sys.argv[1] #"codebook.csv"
    output_csv_file = sys.argv[2] #"1216_ACS_BG"
    variables = generate_variable_array(codebook_csv_file, output_csv_file[-2:])
    output_dict = request_builder(variables)
    write_data_to_csv(output_dict, output_csv_file, variables)
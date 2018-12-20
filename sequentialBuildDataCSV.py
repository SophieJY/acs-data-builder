from census import Census
from us import states
import sys
import csv

 #----------------------------------------------------------------------
counties = [
    '037', 
    '059', 
    '111', 
    '071', 
    '065', 
    '025'
    ]

bg_geoid_prefix = '15000US'
tr_geoid_prefix = '14000US'
cty_geoid_prefix = '05000US'

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
output_dict = {}

#----------------------------------------------------------------------

def request_builder(variables, API_key, level, year):
    c = Census(API_key, year=2000+int(year))
    for county in counties:
        for variable in variables:
            print()
            request_by_level(county, variable, level, c)

def request_by_level(county, variable, level, c):
    if level == BG:
        ret_array = c.acs5.state_county_blockgroup(('NAME', variable), states.CA.fips, county, Census.ALL)
        for ret_element in ret_array:
            geoid = bg_geoid_prefix + ret_element['state'] + ret_element['county'] + ret_element['tract'] + ret_element['block group']
            if ret_element[variable]:
                if isinstance(ret_element[variable], str):   #sometimes the result can be string-represented int
                    if int(ret_element[variable]) >= 0:
                        output_dict[geoid][variable] = int(ret_element[variable])
                elif ret_element[variable] >= 0:
                    output_dict[geoid][variable] = ret_element[variable]
            else:
                output_dict[geoid][variable] = 0
    elif level == TRACT:
        ret_array = c.acs5.state_county_tract(('NAME', variable), states.CA.fips, county, Census.ALL)
        for ret_element in ret_array:
            geoid = tr_geoid_prefix + ret_element['state'] + ret_element['county'] + ret_element['tract']
            if ret_element[variable]:
                if ret_element[variable] >= 0:
                    output_dict[geoid][variable] = ret_element[variable]
            else:
                output_dict[geoid][variable] = 0
    elif level == COUNTY:
        ret_array = c.acs5.state_county(('NAME', variable), states.CA.fips, county)
        for ret_element in ret_array:
            geoid = cty_geoid_prefix + ret_element['state'] + ret_element['county']
            if ret_element[variable]:
                if isinstance(ret_element[variable], str):   #sometimes the result can be string-represented int
                    if int(ret_element[variable]) >= 0:
                        output_dict[geoid][variable] = int(ret_element[variable])
                elif ret_element[variable] >= 0:
                    output_dict[geoid][variable] = ret_element[variable]
            else:
                output_dict[geoid][variable] = 0

def extractLevel(level):
    level = {
        'AREA': AREA_REPORT,
        'PMT': PMT,
        'TR': TRACT,
        'BG': BG,
        'CTY': COUNTY,
    }[level]
    return level

def write_data_to_csv(output_path, variables):
    with open(output_path + ".csv", "w") as output_file:
        writer = csv.DictWriter(output_file, delimiter=',', fieldnames=['GEOID'] + variables)
        writer.writeheader()
        for geoid in geoid_array:
            writer.writerow(output_dict[geoid])

def generate_variable_array(path, level):
    variables = []
    with open(path, 'r') as inputfile:
        reader = csv.reader(inputfile)
        next(reader, None); next(reader, None) # skip two rows of headers
        
        for row in reader:
            if not row[level]:
                continue

            if "E/M" not in row[NUMBER]:
                continue

            number = row[NUMBER].replace("E/M", "").strip()
            if number[0:6] == "B08135":
                continue

            variables.append(number + "E")
            variables.append(number + "M")

    if level == BG:
        variables.append("B08135_001E")
        variables.append("B08135_001M")
        variables.append("B08303_001E")
        variables.append("B08303_001M")

    return variables

def init_first_column(API_key, level):
    c = Census(API_key)
    for county in counties:
        if level == BG:
            v = 'B01001_001E'
            ret_array = c.acs5.state_county_blockgroup(('NAME', v), states.CA.fips, county, Census.ALL)
            for ret_element in ret_array:
                geoid = bg_geoid_prefix + ret_element['state'] + ret_element['county'] + ret_element['tract'] + ret_element['block group']
                print(geoid)
                output_dict[geoid] = {}
                geoid_array.append(geoid)
                output_dict[geoid]['GEOID'] = geoid
        elif level == TRACT:
            v = 'B07001_001E'
            ret_array = c.acs5.state_county_tract(('NAME', v), states.CA.fips, county, Census.ALL)
            print(ret_array)
            for ret_element in ret_array:
                geoid = tr_geoid_prefix + ret_element['state'] + ret_element['county'] + ret_element['tract']
                print(geoid)
                output_dict[geoid] = {}
                geoid_array.append(geoid)
                output_dict[geoid]['GEOID'] = geoid
        elif level == COUNTY:
            v = 'B01001_001E'
            ret_array = c.acs5.state_county(('NAME', v), states.CA.fips, county)
            for ret_element in ret_array:
                geoid = cty_geoid_prefix + ret_element['state'] + ret_element['county']
                print(geoid)
                output_dict[geoid] = {}
                geoid_array.append(geoid)
                output_dict[geoid]['GEOID'] = geoid

if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise Exception('Too few arguments! Syntax: \npython ' + sys.argv[0] + ' codebookcsv outputFilename API_key')
    codebook_csv_file = sys.argv[1] #"codebook.csv"
    output_csv_file = sys.argv[2] #"1216_ACS_BG"
    API_key = sys.argv[3]
    year = output_csv_file.split('_')[0][2:]
    # print(year)
    level = extractLevel(output_csv_file.split('_')[-1])
    init_first_column(API_key,level)
    variables = generate_variable_array(codebook_csv_file, level)
    request_builder(variables, API_key, level, year)
    write_data_to_csv(output_csv_file, variables)
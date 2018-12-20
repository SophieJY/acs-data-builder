from census import Census
from us import states
import sys
import csv
import queue
from threading import Thread

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
class GeographicArguments:
    def __init__(self, county, variable, API_key, level):
        self.county = county
        self.variable = variable
        self.API_key = API_key
        self.level = level

    def get_county(self):
        return self.county

    def get_variable(self):
        return self.variable

    def get_API_KEY(self):
        return self.API_key

    def get_level(self):
        return self.level

class ThreadPool:
    def __init__(self, worker_num=20):
        self.q = queue.Queue(maxsize=0)
        for i in range(worker_num):
            self.worker = Thread(target=wrapper_target_func, args=(self.q,request_builder))
            self.worker.setDaemon(True)
            self.worker.start()

    def putWorkInQueue(self, geographic_arguments):
        self.q.put_nowait(geographic_arguments)

    def waitForCompletion(self):
        self.q.join()

def wrapper_target_func(q, request_builder):
    while True:
        try:
            ga = q.get()
            request_builder(ga.get_county(), ga.get_variable(), ga.get_API_KEY(), ga.get_level())
            q.task_done()
        except queue.Empty:
            break

def request_builder(county, variable, API_key, level):
    c = Census(API_key)
    request_by_level(county, variable, level, c)

def request_by_level(county, variable, level, c):
    if level == BG:
        ret_array = c.acs5.state_county_blockgroup(('NAME', variable), states.CA.fips, county, Census.ALL)
        for ret_element in ret_array:
            geoid = bg_geoid_prefix + ret_element['state'] + ret_element['county'] + ret_element['tract'] + ret_element['block group']
            output_dict[geoid][variable] = ret_element[variable]
    elif level == TRACT:
        ret_array = c.acs5.state_county_tract(('NAME', variable), states.CA.fips, county, Census.ALL)
        for ret_element in ret_array:
            geoid = tr_geoid_prefix + ret_element['state'] + ret_element['county'] + ret_element['tract']
            output_dict[geoid][variable] = ret_element[variable]
    elif level == COUNTY:
        ret_array = c.acs5.state_county(('NAME', variable), states.CA.fips, county)
        for ret_element in ret_array:
            geoid = cty_geoid_prefix + ret_element['state'] + ret_element['county']
            output_dict[geoid][variable] = ret_element[variable]

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

def process_data_multi_thread(variables, API_key, level):
    thread_pool = ThreadPool()
    for county in counties:
        for variable in variables:
            geographic_arguments = GeographicArguments(county, variable, API_key, level)
            thread_pool.putWorkInQueue(geographic_arguments)
    thread_pool.waitForCompletion()

def write_data_to_csv(output_path, variables):
    with open(output_path + ".csv", "w") as output_file:
        writer = csv.DictWriter(output_file, delimiter=',', fieldnames=['GEOID'] + variables)
        writer.writeheader()
        for geoid in geoid_array:
            writer.writerow(output_dict[geoid])

def extractLevel(level):
    level = {
        'AREA': AREA_REPORT,
        'PMT': PMT,
        'TR': TRACT,
        'BG': BG,
        'CTY': COUNTY,
    }[level]
    return level

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

if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise Exception('Too few arguments! Syntax: \npython ' + sys.argv[0] + ' codebookcsv outputFilename API_key')
    codebook_csv_file = sys.argv[1] #"codebook.csv"
    output_csv_file = sys.argv[2] #"1216_ACS_BG"
    API_key = sys.argv[3]
    level = extractLevel(output_csv_file.split('_')[-1])
    variables = generate_variable_array(codebook_csv_file, level)
    init_first_column(API_key, level)
    process_data_multi_thread(variables, API_key, level)
    write_data_to_csv(output_csv_file, variables)
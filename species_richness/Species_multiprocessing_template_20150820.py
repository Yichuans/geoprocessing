# run richness using the multiprocessing template

import multiprocessing
import time
import os, sys

# the number of cores used - the following ensures there is one core remaining for other tasks
WORKER = multiprocessing.cpu_count() - 2

# specify num of workers
# WORKER = 4
import arcpy
from Yichuan10 import GetUniqueValuesFromFeatureLayer_mk2


# CONSTANT
speciesLyr="Species_Lyr" # The species layer
hexagonLyr="Hexagons_Lyr" # The hexagons layer
overLapOption = 'INTERSECT'


# input
## speciesData = sys.argv[1]
## speciesID = sys.argv[2] # unique identifier, number

## hexagonData = sys.argv[3]
## hexagonID = sys.argv[4] # unique identifier, number
## 
## # unfinished id list pickled object
## output_result_path = sys.argv[5]
## unfinished_file_path = sys.argv[6]

speciesData = r"E:\Yichuan\Red_List_data\iucn_rl_2015_2.gdb\iucn_rl_2015_2_pos_dice5000"
speciesID = 'id_no'

hexagonData = r"E:\Yichuan\Comparative_analysis_2015\wh_nomi.shp"
hexagonID = "wdpaid"

output_result_path = r'E:\Yichuan\Comparative_analysis_2015\result.csv'


def get_id():
    idlist = GetUniqueValuesFromFeatureLayer_mk2(speciesData, speciesID)
    # sort list
    idlist.sort()
    return idlist


def species_richness_calculation(id):

    # make species layer
    if type(id) in [str, unicode]:
        exp = '\"' + speciesID + '\" = ' + '\'' + str(id) + '\''
    elif type(id) in [int, float]:
        exp = '\"' + speciesID + '\" = ' + str(id)
    else:
        raise Exception('ID field type error')

    # make layers

    arcpy.MakeFeatureLayer_management(speciesData, speciesLyr, exp)
    arcpy.MakeFeatureLayer_management(hexagonData, hexagonLyr)

    # select by locations
    arcpy.SelectLayerByLocation_management(hexagonLyr, overLapOption, speciesLyr)

    # record it
    hex_ids = GetUniqueValuesFromFeatureLayer_mk2(hexagonLyr, hexagonID)


    result = list()
    #
    for hex_id in hex_ids:
        result.append(str(id) + ',' + str(hex_id) + '\n')

    # get rid of layers
    arcpy.Delete_management(speciesLyr)
    arcpy.Delete_management(hexagonLyr)

    return result

# -------------- NEED MODIFICATION ---------------
def job(job_id):
    # ADD: the main work process, 
    # the result should catch errors
    result = None
    result = species_richness_calculation(job_id)

    return result

def get_queue():
    # create a queue to be populated by a list of ids to process
    q = multiprocessing.Queue()
    
    ids = get_id()

    # ADD: queue logic here
    for i in ids:
        q.put(i)

    return q

def process_result(result):
    if not os.path.exists(output_result_path):
        with open(output_result_path, 'w') as f:
            f.write('ID_NO, WDPAID')
            f.write('\n')
    else:
        # ADD: process result logic here
        with open(output_result_path, 'a') as f:
            for line in result:
                f.write(line)

    pass

# --------------- TEMPLATE -----------------------
def worker_writer(q_out):
    while True:
        # get result from q_out
        result = q_out.get()
        if result == 'STOP':
            break

        process_result(result)


def worker(q, q_out):
    while True:
        # monitoring
        if q.qsize() %100 == 0:
            print('Remaining jobs:', q.qsize())

        # get and ID from job id queue
        job_id = q.get()
        if job_id == 'STOP':
            break

        result = job(job_id)
        q_out.put(result)


def main():
    print('Total number of workers:', WORKER)
    # get queue
    q_out = multiprocessing.Queue()

    # Add queue of a list of ids to process
    q = get_queue()

    # setup and run worker processes
    p_workers = list()
    for i in range(WORKER):
        print('Starting worker process:', i)
        p = multiprocessing.Process(target=worker, args=(q, q_out))
        p_workers.append(p)
        
    # start
    for p in p_workers:
        p.start()

    # add stop flag to the queue
    for p in p_workers:
        q.put('STOP')

    # setup and run writer process
    p_w = multiprocessing.Process(target=worker_writer, args=(q_out,))
    p_w.start()


    # wait for workers to terminate
    for p in p_workers:
        p.join()

    # add stop signal for processing result
    q_out.put('STOP')

    # wait for the writer to finish
    p_w.join()


if __name__ == '__main__':
    main()
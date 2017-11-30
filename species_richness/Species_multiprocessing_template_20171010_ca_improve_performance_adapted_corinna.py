# run richness using the multiprocessing template
import multiprocessing
import time, datetime
import os, sys
import arcpy
import gzip

# the number of cores used - the following ensures there is one core remaining for other tasks
WORKER = multiprocessing.cpu_count() - 2

# specify num of workers
# WORKER = 4

def GetUniqueValuesFromFeatureLayer_mk2(inputFc, inputField):
    """<string>, <string> -> pythonList
    can be both feature class or feature layer"""
    pySet = set()
    with arcpy.da.SearchCursor(inputFc, inputField) as cursor:
        for row in cursor:
            pySet.add(row[0])

    return pySet

# CONSTANT
speciesLyr="Species_Lyr" # The species layer
hexagonLyr="Hexagons_Lyr" # The hexagons layer
overLapOption = 'INTERSECT'

# WH results
speciesData = r"E:\Yichuan\Red_List_data\2017\allspecies_2017_2.gdb\allspecies_2017_2_pos"
speciesID = 'id_no'

hexagonData = r"E:\Yichuan\TEMP\test.gdb\Grid"
hexagonID = "FISH_ID"
# output_result_path = r'E:\Yichuan\test_result2017_reso10.csv'
output_result_path = r'E:\Yichuan\test_result2017_reso10_memo.csv.gz'


def get_id():
    idlist = GetUniqueValuesFromFeatureLayer_mk2(speciesData, speciesID)
    # sort list
    # idlist.sort()
    return idlist

def species_richness_calculation(id, hexagonLyr):

    # make species layer
    if type(id) in [str, unicode]:
        exp = '\"' + speciesID + '\" = ' + '\'' + str(id) + '\''
    elif type(id) in [int, float]:
        exp = '\"' + speciesID + '\" = ' + str(id)
    else:
        raise Exception('ID field type error')

    # make layers
    arcpy.MakeFeatureLayer_management(speciesData, speciesLyr, exp)
    
    # select by locations
    arcpy.SelectLayerByLocation_management(hexagonLyr, overLapOption, speciesLyr)

    # record it
    hex_ids = GetUniqueValuesFromFeatureLayer_mk2(hexagonLyr, hexagonID)


    result = list()
    #
    for hex_id in hex_ids:
        result.append(str(int(id)) + ',' + str(hex_id) + '\n')

    # get rid of layers
    arcpy.Delete_management(speciesLyr)
    
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

def process_result_v2(result):
    if not os.path.exists(output_result_path):
        with gzip.open(output_result_path, 'w') as f:
            f.write('ID_NO, WDPAID')
            f.write('\n')
    else:
        # ADD: process result logic here
        with gzip.open(output_result_path, 'a') as f:
            for line in result:
                f.write(line)

# --------------- TEMPLATE -----------------------
def worker_writer(q_out):
    while True:
        # get result from q_out
        result = q_out.get()
        if result == 'STOP':
            break

        process_result_v2(result)


def worker(q, q_out):
    # make layer here to reduce overhead
    arcpy.MakeFeatureLayer_management(hexagonData, hexagonLyr)

    while True:
        # monitoring
        if q.qsize() %100 == 0:
            print('Time:', datetime.datetime.now().time())
            print('Remaining jobs:', q.qsize())

        # get and ID from job id queue
        job_id = q.get()
        if job_id == 'STOP':
            break

        result = species_richness_calculation(job_id, hexagonLyr)
        q_out.put(result)

    arcpy.Delete_management(hexagonLyr)


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

    # needed if the species data is copied to in_memory
    # arcpy.Delete_management(speciesData)


if __name__ == '__main__':
    main()
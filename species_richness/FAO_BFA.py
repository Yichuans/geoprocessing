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

# Species
speciesData = r"E:\Yichuan\Red_List_data\2017\allspecies_2017_2.gdb\bfa_species"
speciesID = 'row_id'

hexagonData = r"E:\Yichuan\Basedata\hex_grid.gdb\hex_10k"
hexagonID = "hexid"
# output_result_path = r'E:\Yichuan\test_result2017_reso10.csv'
output_result_path = r'E:\Yichuan\Barbara_IUCN\hex_bfa.csv.gz'
log_path = r"E:\Yichuan\Barbara_IUCN\hex_bfa_log.csv"



# Species_failed
speciesData = r"E:\Yichuan\Red_List_data\2017\allspecies_2017_2.gdb\bfa_species_failed"
speciesID = 'row_id'

hexagonData = r"E:\Yichuan\Basedata\hex_grid.gdb\hex_10k"
hexagonID = "hexid"
# output_result_path = r'E:\Yichuan\test_result2017_reso10.csv'
output_result_path = r'E:\Yichuan\Barbara_IUCN\hex_bfa_add.csv.gz'
log_path = r"E:\Yichuan\Barbara_IUCN\hex_bfa_log_add.csv"


# Species_failed dice method
speciesData = r"E:\Yichuan\Red_List_data\2017\allspecies_2017_2.gdb\bfa_species_failed_dice"
speciesID = 'row_id'

hexagonData = r"E:\Yichuan\Basedata\hex_grid.gdb\hex_10k"
hexagonID = "hexid"
# output_result_path = r'E:\Yichuan\test_result2017_reso10.csv'
output_result_path = r'E:\Yichuan\Barbara_IUCN\hex_bfa_add_dice.csv.gz'
log_path = r"E:\Yichuan\Barbara_IUCN\hex_bfa_log_add_dice.csv"


# Species_failed dice method
speciesData = r"E:\Yichuan\Red_List_data\2017\allspecies_2017_2.gdb\bfa_species_failed_dice_still_fail_fix"
speciesID = 'row_id'

hexagonData = r"E:\Yichuan\Basedata\hex_grid.gdb\hex_10k"
hexagonID = "hexid"
# output_result_path = r'E:\Yichuan\test_result2017_reso10.csv'
output_result_path = r'E:\Yichuan\Barbara_IUCN\hex_bfa_add_dice_still_fail.csv.gz'
log_path = r"E:\Yichuan\Barbara_IUCN\hex_bfa_log_add_dice_still_fail.csv"

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

# normal
def process_result(result):
    if not os.path.exists(output_result_path):
        with open(output_result_path, 'w') as f:
            f.write('{},{}\n'.format(speciesID, hexagonID))

    else:
        # ADD: process result logic here
        with open(output_result_path, 'a') as f:
            for line in result:
                f.write(line)

# output to compressed csv
def process_result_v2(result):
    if not os.path.exists(output_result_path):
        with gzip.open(output_result_path, 'w') as f:
            f.write('{},{}\n'.format(speciesID, hexagonID))
    else:
        # ADD: process result logic here
        with gzip.open(output_result_path, 'a') as f:
            for line in result:
                f.write(line)





# --------------- TEMPLATE -----------------------
def worker_logger(q_log):
    if not os.path.exists(log_path):
        with open(log_path, 'w') as f:
            msg = """speciesLyr: {}\n\t
                    hexagonLyr: {}\n\t
                    overLapOption: {}\n\t
                    speciesData: {}\n\t
                    speciesID: {}\n\t
                    hexagonData: {}\n\t
                    hexagonID: {}\n\t
                    output_result_path: {}\n\t
                    log_path: {}\n\t\n\n""".format(speciesLyr,
                    hexagonLyr,
                    overLapOption,
                    speciesData,
                    speciesID,
                    hexagonData,
                    hexagonID,
                    output_result_path,
                    log_path)
            f.write(msg)

    while True:
        result = q_log.get()
        if result == 'STOP':
            break

        with open(log_path, 'a') as f:
            f.write(result)
        print(result)

def worker_writer(q_out):
    while True:
        # get result from q_out
        result = q_out.get()
        if result == 'STOP':
            break

        process_result_v2(result)


def worker(q, q_out, q_log):
    # make layer here to reduce overhead
    arcpy.MakeFeatureLayer_management(hexagonData, hexagonLyr)

    while True:
        # monitoring
        if q.qsize() % 100 == 0:
            msg = '{}; jobs left: {}\n'.format(datetime.datetime.now().strftime('%c'), q.qsize())
            q_log.put(msg)

        # get and ID from job id queue
        job_id = q.get()
        if job_id == 'STOP':
            break

        try:
            result = species_richness_calculation(job_id, hexagonLyr)
            q_out.put(result)
        except Exception as e:
            result = 'job_id: {} failed, {}'.format(job_id, e)
            q_log.put(result)

    arcpy.Delete_management(hexagonLyr)


def main():
    print('Total number of workers:', WORKER)
    # get queue for logging
    q_log = multiprocessing.Queue()

    # get queue for output
    q_out = multiprocessing.Queue()

    # Add queue of a list of ids to process
    q = get_queue()

    # setup and run worker processes
    p_workers = list()
    for i in range(WORKER):
        print('Starting worker process:', i)
        p = multiprocessing.Process(target=worker, args=(q, q_out, q_log))
        p_workers.append(p)
        
    # start
    for p in p_workers:
        p.start()

    # add stop flag to the queue, the number of stop flags equal the number of workers
    for p in p_workers:
        q.put('STOP')

    # setup and run writer process
    p_w = multiprocessing.Process(target=worker_writer, args=(q_out,))
    p_w.start()

    # setup and run logger process
    p_l = multiprocessing.Process(target=worker_logger, args=(q_log,))
    p_l.start()

    # =============================
    # wait for workers to terminate
    for p in p_workers:
        p.join()

    # add stop signal for processing result
    q_out.put('STOP')
    q_log.put('STOP')

    # wait for the writer to finish
    p_w.join()
    p_l.join()

    # needed if the species data is copied to in_memory
    # arcpy.Delete_management(speciesData)


if __name__ == '__main__':
    main()
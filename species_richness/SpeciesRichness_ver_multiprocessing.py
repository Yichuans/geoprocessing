#-------------------------------------------------------------------------------
# Name:        Species richness multiprocessing
# Purpose:     Fixed a number of issues and improved coding style
#
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
#
# Created:     2013/10/18
# Copyright:   (c) Yichuan Shi 2013

#-------------------------------------------------------------------------------
import os, sys, pickle, copy
import Yichuan10
import arcpy
from multiprocessing import Process, Queue

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

speciesData = r"D:\Yichuan\Red_List_data\iucn_rl_species_2014_2.gdb\iucn_rl_species_2014_2_no_sens_filter"
speciesID = 'ID_NO'

hexagonData = r"D:\Yichuan\WHS.gdb\whs_dump_140716"
hexagonID = "wdpaid"

output_result_path = r'D:\Yichuan\Scripts\scripts\multi\allspp.csv'
unfinished_file_path = r'D:\Yichuan\Scripts\scripts\multi\allsppunfinish'

def get_id():
    idlist = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(speciesData, speciesID)
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
    hex_ids = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(hexagonLyr, hexagonID)


    result = list()
    #
    for hex_id in hex_ids:
        result.append(str(id) + ',' + str(hex_id) + '\n')

    # get rid of layers
    arcpy.Delete_management(speciesLyr)
    arcpy.Delete_management(hexagonLyr)

    return result

## def species_richness_calculation(id):
## 
##     result = list()
##     #
## 
##     result.append(str(id) + ',' + 'str(hex_id) '+ '\n')
##     # print result
##     return result

def worker(q_input, q_output, q_log):
    # multiprocessing worker
    while True:
        # wait until get an input
        id = q_input.get()

        # monitor
        print("Processing species ID: " + str(id))

        if q_input.qsize() %100 == 0:
            print '----------------------'
            print q_input.qsize()
            print 'remaining'

        # end signal 'STOP', if so break loop
        if id == 'STOP':
            break

        # run species richness
        try:
            result = species_richness_calculation(id)
           # print result

        except Exception as e:
            print("Failed running analysis for ID = %s"%(id,))
            q_log.put(id)

        # put only non empty result
        if result:
            q_output.put(result)


def write_to_file(q_output):
    while True:
        result = q_output.get()

        # print result
        if result == 'STOP':
            break

        # write to file
        f = open(output_result_path, 'a')
        for each in result:
            # print each
            f.write(each)

        f.close()



def main():
    # check if it is a continuation of failed attempt
    if os.path.exists(unfinished_file_path):
        unfinished_file = open(unfinished_file_path, 'r')
        idlist = pickle.load(unfinished_file)
        unfinished_file.close()
        print("restore species id successful")


    else:
        idlist = get_id()
        print("fresh species id list created")


    unfinished_idlist = copy.deepcopy(idlist)
    print("number of species: " + str(len(idlist)))


    # debug
    # print(idlist)
    q_input = Queue()
    q_output = Queue()
    q_log = Queue()

    # populate ids
    for id in idlist:
        q_input.put(id)

    # write header
    f = open(output_result_path, 'w')
    f.write('species_id,base_id\n')
    f.close()

    # worker processes start
    WORKER = 6
    p_workers = list()
    for i in range(WORKER):
        p = Process(target=worker, args=(q_input, q_output, q_log))
        p_workers.append(p)
        p.start()


    # writer process start
    p_w = Process(target=write_to_file, args=(q_output,))
    p_w.start()



    # terminate worker processes
    for p in p_workers:
        q_input.put('STOP')
        p.join()

    # terminate writer - signal
    q_output.put('STOP')

    # wait until it finishes
    p_w.join()

    # pickle unfinished and dump
    q_log.put('STOP')
    unfinished_idlist = list()
    for id in iter(q_log, 'STOP'):
        unfinished_idlist.append(id)

    pickle.dump(unfinished_idlist, unfinished_file)


##
if __name__ == '__main__':
    main()


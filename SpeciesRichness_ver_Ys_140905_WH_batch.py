#-------------------------------------------------------------------------------
# Name:        Species richness generic -
#           1. it now supports feature layers and selections
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

# CONSTANT

speciesLyr="Species_Lyr" # The species layer
hexagonLyr="Hexagons_Lyr" # The hexagons layer
overLapOption = 'INTERSECT'


# input
##speciesData = sys.argv[1]
##speciesID = sys.argv[2] # unique identifier, number
##
##hexagonData = sys.argv[3]
##hexagonID = sys.argv[4] # unique identifier, number
##
### unfinished id list pickled object
##output_result_path = sys.argv[5]
##unfinished_file_path = sys.argv[6]


def get_id(speciesData, speciesID):
    idlist = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(speciesData, speciesID)
    # sort list
    idlist.sort()
    return idlist

def species_richness_calculation(id, speciesData, speciesID, hexagonData, hexagonID, output_result_path):
    # id is the species ID
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

    f = open(output_result_path, 'a')
    for hex_id in hex_ids:
        f.write(str(id) + ',' + str(hex_id) + '\n')

    # get rid of layers
    arcpy.Delete_management(speciesLyr)
    arcpy.Delete_management(hexagonLyr)

    f.close()


def run(speciesData,speciesID,hexagonData,hexagonID, output_result_path, unfinished_file_path):

    Yichuan10.Printboth(sys.argv)

    # check if it is a continuation of failed attempt
    if os.path.exists(unfinished_file_path):
        unfinished_file = open(unfinished_file_path, 'r')

        idlist = pickle.load(unfinished_file)
        unfinished_file.close()

        Yichuan10.Printboth("restore species id successful")
        Yichuan10.Printboth(idlist)

    else:
        # initial

        idlist = get_id(speciesData, speciesID)

        Yichuan10.Printboth("fresh species id list created")

        f = open(output_result_path, 'w')
        f.write('species_id,base_id\n')
        f.close()


    unfinished_idlist = copy.deepcopy(idlist)
    Yichuan10.Printboth("number of species: " + str(len(idlist)))


    # debug
    Yichuan10.Printboth(idlist)

    # for each species (denoted by ID)
    try:
        for id in idlist:
            # do something here
            Yichuan10.Printboth("Processing species ID: " + str(id))
            species_richness_calculation(id, speciesData, speciesID, hexagonData, hexagonID, output_result_path)

            unfinished_idlist.remove(id)

    except Exception as e:
        Yichuan10.Printboth("Failed running analysis for ID = %s"%(id,))
        Yichuan10.Printboth("Remaining IDs are here: " + unfinished_file_path)

        # dump unfinished IDs
        unfinished_file = open(unfinished_file_path, 'w')
        pickle.dump(unfinished_idlist, unfinished_file)
        unfinished_file.close()

        Yichuan10.Printboth('Error: ' + str(e))

##        # debug
##        raise Exception("Failed")


def main():
    rl_species = ['rl_species', 'ID_NO', r"D:\Yichuan\Red_List_data\iucn_rl_species_2014_2.gdb\iucn_rl_species_2014_2_no_sens_filter"]

    nomi = ['nomi', 'WDPAID', r"D:\Yichuan\sites2014\merge_site_2014.shp"]
    whs = ['whs', 'wdpaid', r"D:\Yichuan\WHS.gdb\whs_dump_140716"]

    os.chdir(r"D:\Yichuan\Comparative_analysis_2014\species")

##    run(speciesData,speciesID,hexagonData,hexagonID, output_result_path, unfinished_file_path):
    try:
        run(rl_species[2], rl_species[1], nomi[2], nomi[1], rl_species[0]+'_'+nomi[0]+'.csv', rl_species[0]+'_'+nomi[0]+'_log.txt')
    except Exception as e:
        print 'STEP FAILED:', str(e)
##
    try:
        run(rl_species[2], rl_species[1], whs[2], whs[1], rl_species[0]+'_'+whs[0]+'.csv', rl_species[0]+'_'+whs[0]+'_log.txt')
        
    except Exception as e:
        print 'STEP FAILED:', str(e)
##    try:
##        run(non_bird[2], non_bird[1], nomi[2], nomi[1], non_bird[0]+'_'+nomi[0]+'.csv', non_bird[0]+'_'+nomi[0]+'_log.txt')
##    except Exception as e:
##        print 'STEP FAILED:', str(e)
##
##    try:
##        run(bird[2], bird[1], whs[2], whs[1], bird[0]+'_'+whs[0]+'.csv', bird[0]+'_'+whs[0]+'_log.txt')
##    except Exception as e:
##        print 'STEP FAILED:', str(e)
##
##    try:
##        run(non_bird[2], non_bird[1], whs[2], whs[1], non_bird[0]+'_'+whs[0]+'.csv', non_bird[0]+'_'+whs[0]+'_log.txt')
##    except Exception as e:
##        print 'STEP FAILED:', str(e)


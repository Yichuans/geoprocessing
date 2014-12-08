#-------------------------------------------------------------------------------
# Name:        WDPA inside out
# Purpose:     To understand the WDPA better
#
# Author:      Yichuans
#
# Created:     24/07/2014
# Copyright:   (c) Yichuans 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import Yichuan10, itertools
from Yichuan10 import time_tracker, simple_time_tracker

# initial
sr_mwd = Yichuan10.createSpatialRefBySRID101(54009)

# debug
inputlayer = r"D:\Yichuan\WDPA\WDPA_May2014_Proteus\WDPA_May2014.gdb\WDPA_poly_May2014"

### debug
##id_pool = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(inputlayer, 'wdpaid')
##id_iterable = itertools.combinations(id_pool,2)

# decorator class to track how much time a function takes to finish
# multiple calls of the same function

# debug
@time_tracker
def _test_draw_speed(num, a_iterable):
    a = list()
    while num:
        try:
            value = a_iterable.next()
            a.append(value)
        except StopIteration:
            break
        num -= 1

    return a

# @simple_time_tracker
def _dummy_function(iterations):
    iter = range(iterations)
    for pair in itertools.combinations(iter, 2):
        a = pair[0] * pair[1]

    return a

def _test_tracker(times, load=100):
    while times:
        _dummy_function(load)
        times -= 1

    pass

@time_tracker
def _test_tracker_mk2(times, load=100):
    while times:
        _dummy_function(load)
        times -=1

def wdpa_geom_identical_check(inputlayer):
    """main function to investigation identical geom"""
    # need to have index first
    id_pool = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(inputlayer, 'wdpaid')

    # debug
    id_pool = id_pool[:1000]

    # get combinations of twos
    id_iterable = itertools.combinations(id_pool, 2)

    # get chunks
    chunk_size = 1000000

    while True:
        id_pool_chunk = get_id_chunk(id_iterable, chunk_size)
        if len(id_pool_chunk) > 0:
            # do something, debug
            result = process_a_chunk(id_pool_chunk)
            print result

        else:
            break


@simple_time_tracker
def process_a_chunk(id_pool_chunk):
    chunk_result = list()
    counter = 0
    for pair in id_pool_chunk:
        counter += 1
        if counter % 100 == 0:
            print 'Processed:', counter

        try:
            result = compare_geom_pair(pair[0], pair[1])
            if result is None:
                # no intersect
                pass
            else:
                chunk_result.append(result)
        except Exception as e:
            print 'failed:', pair
            print e.message

    return chunk_result



def get_id_chunk(id_iterable, chunk_size):
    a = list()
    num = chunk_size
    while num:
        try:
            value = id_iterable.next()
            a.append(value)
        except StopIteration:
            break

        num -= 1

    return a

@time_tracker
def compare_geom_pair(wdpaid1, wdpaid2):

    geom1 = Yichuan10.GetFieldValueByID_mk2(inputlayer, wdpaid1)
    geom2 = Yichuan10.GetFieldValueByID_mk2(inputlayer, wdpaid2)

    if geom1.overlaps(geom2):

        geom1 = geom1.projectAs(sr_mwd)
        geom2 = geom2.projectAs(sr_mwd)

        # area
        area_geom1 = geom1.getArea('PLANAR')/1000000
        area_geom2 = geom2.getArea('PLANAR')/1000000

        area_intersect = geom1.intersect(geom2).getArea('PLANAR')/1000000

        return ((wdpaid1, area_geom1), (wdpaid2, area_geom2),('-'.join([wdpaigi1, wdpaid2]), area_intersect))

    else:
        pass

    return None

#---------------- above deals with geom comparison -------------------
# This section examines the differences in performance (time) of employing
# single-part vs multi-part, vertex limits and perhaps multiprocessing 
# where appropriate, using randam samples

def select_samples():
    pass

def multipart_to_single_part():
    pass


def dice_vertices():
    pass


def process_without_optimisation():
    pass


def process_with_optimisation():
    pass

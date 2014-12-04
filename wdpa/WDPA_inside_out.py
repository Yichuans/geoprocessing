#-------------------------------------------------------------------------------
# Name:        WDPA inside out
# Purpose:
#
# Author:      Yichuans
#
# Created:     24/07/2014
# Copyright:   (c) Yichuans 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import Yichuan10, itertools

# initial
sr_mwd = Yichuan10.createSpatialRefBySRID101(54009)

# debug
inputlayer = r"D:\Yichuan\WDPA\WDPA_May2014_Proteus\WDPA_May2014.gdb\WDPA_poly_May2014"

### debug
##id_pool = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(inputlayer, 'wdpaid')
##id_iterable = itertools.combinations(id_pool,2)

# decorator class to track how much time a function takes to finish
# multiple calls of the same function
class time_tracker(object):
    def __init__(self, f):
        """function pass to the constructor"""
        self.f = f
        self.counter = 0
        self.total_time = 0
        self.__name__ = f.__name__
    def __call__(self, *args):
        from time import time
        start_time = time()

        # call function
        result = self.f(*args)

        end_time = time()
        # in ms
        spent_time = (end_time - start_time) * 1000
        self.counter += 1
        self.total_time += spent_time

        # time tracker output
        if self.counter % 50 == 0:
            print 'Function', '\''+ self.__name__ + '\'', 'called', self.counter, 'times'
            print 'Total time spent:', '{:.2f}'.format(self.total_time), 'ms'

        return result

# simple function running time cost
class simple_time_tracker(object):
    def __init__(self, f):
        """function pass to the constructor"""
        self.f = f

    def __call__(self, *args):
        from time import time
        start_time = time()

        # call function
        result = self.f(*args)

        end_time = time()
        # in ms
        spent_time = (end_time - start_time) * 1000

        print 'Total time spent:', '{:.2f}'.format(spent_time), 'ms'

        return result

# debug
@time_tracker
def test_draw_speed(num, a_iterable):
    a = list()
    while num:
        try:
            value = a_iterable.next()
            a.append(value)
        except StopIteration:
            break
        num -= 1

    return a

@simple_time_tracker
def dummy_function(iterations):
    iter = range(iterations)
    for pair in itertools.combinations(iter, 2):
        a = pair[0] * pair[1]

    return a

def test_tracker(times, load=100):
    while times:
        dummy_function(load)
        times -= 1

    pass



def main(inputlayer):
    # sr for area


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

        return ((wdpaid1, area_geom1), (wdpaid2, area_geom2),('-'.join([wdpaid1, wdpaid2]), area_intersect))

    else:
        pass

    return None


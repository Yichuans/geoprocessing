# -*- coding: latin-1 -*-

# this library contains all commonly used geoprocessing functions

import os, sys, arcpy, time, shutil, string, psutil
import datetime as dtm
from datetime import datetime

dt = datetime.now()
CurrentTime = str(dt.hour) + ':' + str(dt.minute) + ':' +str(dt.second) + ' ' + \
              str(dt.year)+'.'+str(dt.month)+'.' + str(dt.day)
ModifiedDate = 'Ver: 8 Nov 2013'


MUTLIPOINT_TEMPLATE = r'E:\Yichuan\MyGDB.gdb\multipoint_template'

# srid, reference from ESRI projected_coordinate_system
sr_dict = {'wgs': 4326,
           'plate_carree': 54001,
           'robinson': 54030,
           'molleweide_ea': 54009,
           'cylindrical_ea': 54034,
           'behrmann_ea': 54017,
           'azimuth_ed': 54032,
           'winkel_2': 54019,
           'mercator': 3395} # ea: equal area, ed: equal distance


def Printboth(msg):
    """<string> -> print to both command line and arcmap geoprocessing window"""
    print(msg)
    arcpy.AddMessage(msg)

def util_printlist_duplicate(flist):
    for each in flist:
        length = len(flist) - int(flist.index(each)) - 1
        while length>0:
            print(each == flist[length])
            length -= 1

class memory_tracker(object):
    """track memory usage in current process"""
    def __init__(self, f):
        self.f = f
        self.__name__ = f.__name__

    def __call__(self, *args):
        # get current process
        p = psutil.Process(os.getpid())
        print(time.ctime(), "[MEM]Current process memory (MB):", p.memory_info()[0]/1024/1024.0)
        print(time.ctime(), "[MEM]Current function:", self.__name__)
        result = self.f(*args)
        print(time.ctime(), "[MEM]Current process memory (MB):", p.memory_info()[0]/1024/1024.0)
        return result


class time_tracker(object):
    """track number of function calls, cost of time, and total time"""
    def __init__(self, f):
        """function pass to the constructor"""
        self.f = f
        self.counter = 0
        self.total_time = 0
        self.__name__ = f.__name__
    def __call__(self, *args):
        from time import time, ctime
        start_time = time()

        # call function
        result = self.f(*args)

        end_time = time()
        # in ms
        spent_time = (end_time - start_time) * 1000
        self.counter += 1
        self.total_time += spent_time

        # time tracker output
        # if self.counter % 50 == 0:
        print ctime(),'[TIME]Function', '\''+ self.__name__ + '\'', 'called', self.counter, 'times'
        print ctime(),'[TIME]Time spent:,', '{:.2f}'.format(spent_time), 'ms'
        print ctime(),'[TIME]Total time spent:', '{:.2f}'.format(self.total_time), 'ms'

        return result

# simple function running time cost
class simple_time_tracker(object):
    def __init__(self, f):
        """function pass to the constructor"""
        self.f = f

    def __call__(self, *args):
        from time import time, ctime
        start_time = time()

        # call function
        result = self.f(*args)

        end_time = time()
        # in ms
        spent_time = (end_time - start_time) * 1000

        print ctime(),'[TIME]Time spent:', '{:.2f}'.format(spent_time), 'ms'

        return result


class Timer():
    # this class provides very simple time tracking functions
    def __init__(self):
        self._init_time = self.current_time()
        self._init_clock = time.clock()
        self._timer_clock = time.clock()

    def start_time(self):
        return self._init_time

    def current_time(self):
        # return current time
        dt = dtm.datetime.now()
        current_time = str(dt.year)+'.'+str(dt.month)+'.' + str(dt.day) + ' ' +\
                       str(dt.hour) + ':' + str(dt.minute) + ':' +str(dt.second)
        return current_time

    def time_passed(self):
        # as a timer, return time passed since initiation
        self._timer_clock = time.clock()
        time_passed = self._timer_clock - self._init_clock
        return str(dtm.timedelta(seconds = time_passed))

def GetFieldValueByID(featurelayer, input_id, value_field="SHAPE@", id_field="wdpaid"):
    # this function return the value of the fieldname by giving id
    # id must be unique
    # print (DATA_IDFIELD, value_field)
    with arcpy.da.SearchCursor(featurelayer, (id_field, value_field)) as cursor:
        for row in cursor:
    #        print row
            if row[0] == input_id:
                return row[1]

        print "no value found"
        return None


def GetFieldValueByID_mk2(featurelayer, input_id, value_field="SHAPE@", id_field="wdpaid"):
    # more efficient - not having to loop through the whole dataset..., input_id
    # this function return the value of the fieldname by giving id
    # id must be unique
    # print (DATA_IDFIELD, value_field)

    if type(input_id) is int:
        whereclause = '\"' + id_field + '\" = ' + str(input_id)
    elif type(input_id) is str:
        whereclause = '\"' + id_field + '\" = ' + '\'' + str(input_id) + '\''
    else:
        print "Invalid ID type"
        return None

    with arcpy.da.SearchCursor(featurelayer, (id_field, value_field), whereclause) as cursor:
        for row in cursor:
    #        print row
            if row[0] == input_id:
                return row[1]

        print "no value found"
        return None

def GetFieldValueByID_ogr(featurelayer, input_id, value_field="SHAPE@", id_field="wdpaid"):
    # waiting to be done
    # print (DATA_IDFIELD, value_field)

    if type(input_id) is int:
        whereclause = '\"' + id_field + '\" = ' + str(input_id)
    elif type(input_id) is str:
        whereclause = '\"' + id_field + '\" = ' + '\'' + str(input_id) + '\''
    else:
        print "Invalid ID type"
        return None

    # with arcpy.da.SearchCursor(featurelayer, (id_field, value_field), whereclause) as cursor:
    #     for row in cursor:
    # #        print row
    #         if row[0] == input_id:
    #             return row[1]

    #     print "no value found"
    #     return None


def createSpatialRefBySRID(srid):
    """<srid> --> SR object"""
    sr = arcpy.SpatialReference()
    sr.factoryCode = srid
    sr.create()
    return sr

def createSpatialRefBySRID101(srid):
    """<srid> --> SR object"""
    sr = arcpy.SpatialReference(srid)
    return sr

def GetUniqueValuesFromShapefile(inputShp, inputField):
    """<string>, <string> -> pythonList
    can be both feature class or feature layer"""
    if arcpy.Exists('featurelayer'):
        arcpy.Delete_management('featurelayer')
    layer = "featurelayer"
    arcpy.MakeFeatureLayer_management(inputShp, layer)

    rows = arcpy.SearchCursor(layer)
    row = rows.next()

    pyList = []
    while row:
        if row.getValue(inputField) not in pyList:
            pyList.append(row.getValue(inputField))
        row = rows.next()
    del row, rows
    arcpy.Delete_management(layer)
    return pyList

def GetUniqueValuesFromFeatureLayer_mk2(inputFc, inputField):
    """<string>, <string> -> pythonList
    can be both feature class or feature layer"""
    pySet = set()
    with arcpy.da.SearchCursor(inputFc, inputField) as cursor:
        for row in cursor:
            pySet.add(row[0])

    return list(pySet)

def GetUniqueValuesFromFeatureLayer_ogr(inputFc, inputField):
    """<string>, <string> -> pythonList
    can be both feature class or feature layer"""
    from osgeo import ogr
    ds = ogr.Open(inputFc, 0)
    dl = ds.GetLayer()

    field_value_list = [feature.GetField(inputField) for feature in dl]

    return list(set(field_value_list))


def GetUniqueLookupFromFeatureLayer_mk2(inputFc, inputField_key, inputField_value):
    """<string>, <string>, <string> -> pythondict dict[key] = set()
    can be both feature class or feature layer"""
    result = dict()
    with arcpy.da.SearchCursor(inputFc, [inputField_key, inputField_value]) as cursor:
        for row in cursor:
            if result.has_key(row[0]):
                result[row[0]].add(row[1])
            else:
                result[row[0]] = set([row[1]])

    return result


def GetLookupValuePairsFromTable(inputTab, inputField, LookupValueField):
    """<string>, <string>, <string> -> pythonDict
    can be both feature class or feature layer"""
    layer = "featurelayer"
    arcpy.MakeTableView_management(inputTab, layer)

    rows = arcpy.SearchCursor(layer)
    row = rows.next()

    pyDict = dict()
    while row:
        if row.getValue(inputField) not in pyDict.keys():
            pyDict[row.getValue(inputField)] = row.getValue(LookupValueField)
        row = rows.next()
    del row, rows
    arcpy.Delete_management(layer)
    return pyDict

def GetLookupValuePairsFromTable_StringConcat(inputTab, inputField, *LookupValueField):
    """<string>, <string>, <string> -> pythonDict
    can be both feature class or feature layer"""
    layer = "featurelayer"
    arcpy.MakeTableView_management(inputTab, layer)

    rows = arcpy.SearchCursor(layer)
    row = rows.next()

    pyDict = dict()
    while row:
        if row.getValue(inputField) not in pyDict.keys():
            value = ''
            for each in LookupValueField:
                if row.getValue(each) != None:
                    value += row.getValue(each).encode('utf-8').strip() + ';'
            pyDict[row.getValue(inputField)] = value
        row = rows.next()
    del row, rows
    arcpy.Delete_management(layer)
    return pyDict

def GetLookupValuePairsFromShapefile(inputShp, inputField, LookupValueField):
    """<string>, <string>, <string> -> pythonDict
    can be both feature class or feature layer"""
    layer = "featurelayer"
    arcpy.MakeFeatureLayer_management(inputShp, layer)

    rows = arcpy.SearchCursor(layer)
    row = rows.next()

    pyDict = dict()
    while row:
        if row.getValue(inputField) not in pyDict.keys():
            pyDict[row.getValue(inputField)] = row.getValue(LookupValueField)
        row = rows.next()
    del row, rows
    arcpy.Delete_management(layer)
    return pyDict

def CalcuateTotalArea(inputshp, field = '#', spatialref='#'):
    """<shp/lyr>, {summarize field}, {spatialref}--> python dictionary
    Input shapefile MUST have coordinate systems!!
    this function is used to calcuate total areas of the input shapefile or layer
    , optionally in a user defined spatial reference system (projection on the fly)
    and/or aggregated by a field specified by user
    function added 101124"""
    #optional spatial reference
    dict = {}
    if spatialref == '#':
        spatialreference = ''
    else:
        spatialreference = spatialref

    #optional field clause, if field is not specified
    if field == '#' or field == '':
        rows = arcpy.SearchCursor(inputshp, '', spatialreference)
        row = rows.next()
        # reset area value
        area = 0
        while row:
            area += row.shape.area
            row = rows.next()
        dict['totalarea'] = area
        return dict

    # if field is specified
    else:
        uniquevalues = GetUniqueValuesFromShapefile(inputshp, field)

    # if given field has unique values
    if uniquevalues:
        for eachvalue in uniquevalues:
            # need to separate string with numbers, also uniquecode
            if type(eachvalue) == type('a') or type(eachvalue) == type(u'a'):
                whereclause = "\"" + field + "\" = '" + eachvalue + "'"
            else:
                whereclause = "\"" + field + "\" = " + str(eachvalue)

            rows = arcpy.SearchCursor(inputshp, whereclause, spatialreference)
            row = rows.next()
            # reset area value
            area = 0
            while row:
                area += row.shape.area
                row = rows.next()

            dict[eachvalue] = area
            del row, rows
    return dict

def SingeQuoteToDoubleSQL(strName):
    """<string> -> <string>
    converts to a valid name that is stripped and change ' to '' for SQL statement"""
    if '\'' in strName:
        validstrName = strName.replace('\'', '\'\'')
    else:
        validstrName = strName

    return validstrName

def Log_output(msg, textfile = 'result.txt'):
    """<string>, <string>"""
    """ <string>, {text file name}, will be saved under arcpy.env.workplace\ default: result.txt}
    This function is called to record the current status, error message, time etc"""
    fi = open(arcpy.env.workspace + os.sep + textfile, 'a')
    fi.write(msg)
    fi.close()

def ExportDictionaryToTxt(inputDict, outputFile):
    """<python dict> --> <string> text file
    this function exports a python dictionary to a local text file in the
    format of key (sorted) - value pairs"""
    fo = open(outputFile, 'w')

    keys = inputDict.keys()
    keys.sort()
    for eachkey in keys:
        fo.write(str(eachkey) + ',' + str(inputDict[eachkey]) + '\n')

    fo.close()

def ExportDictionaryToTxt_split(inputDict, outputFile):
    """<python dict> --> <string> text file
    this function exports a python dictionary to a local text file in the
    format of key (sorted) - value pairs"""
    fo = open(outputFile, 'w')
    fo.write('dkey,dvalue\n')

    for eachkey in inputDict.keys():
        for eachvalue in inputDict[eachkey]:
            fo.write(str(eachkey) + ',' + str(eachvalue) + '\n')

    fo.close()

def ExportListToTxt(inputList, outputFile):
    fo = open(outputFile, 'w')

    for each in inputList:
        fo.write(str(each) + '\n')

    fo.close()


def CopyFilesByList(inputList, src, dst):
    """<python list>, <string path>, <string path>
    make a copy of the file from the src to dst using the list as filter"""
    srcfiles = os.listdir(src)

    for eachfile in srcfiles:
        if eachfile.split('.')[0] in inputList:
            inputsrc = src + os.sep + eachfile
            outputdst = dst + os.sep + eachfile
            print inputsrc, outputdst
            shutil.copy(inputsrc, outputdst)

def GetListFilesFromFolder(src, ext = ''):
    """<path>, <file extension>
    only files will be counted, extension default is for all file types
    specify 'jpg' if only interested in jpgs"""
    items = os.listdir(src)
    result = list()
    for item in items:
        if os.path.isfile(src+os.sep+item):
            if ext == '':
                result.append(item)
            else:
                if item[-3:] == ext:
                    result.append(item)
    return result

def DifListSet(list1, list2):
    """<list>, <list>
    return difference between these two list"""
    set1 = set(list1)
    set2 = set(list2)
    result = {}
    result['1-2'] = list(set1.difference(set2))
    result['2-1'] = list(set2.difference(set1))
    return result

def StripFileExtension(filename):
    """strip extension"""
    items = filename.split('.')
    if len(items) > 1:
        return '.'.join(items[:-1])
    else:
        #if no extension found
        return filename

def CreateDictionaryFromTxt(txtfile):
    """<string> -> dictionary
    separated by comma, two columns"""
    fo = open(txtfile, 'r')
    dictionary = {}
    line = fo.readline()
    while line and line!='\n':
        # the first element would be the key(the value to be updated in the shapefile)
        # the second would be the updating value

        key = line.split(',')[0].strip()
        value = line.split(',')[1].strip()
        if key not in dictionary.keys():
            dictionary[key] = value
        else:
            raise MyError("Error: Multiple keys found in the table, exiting...")

        line= fo.readline()

    return dictionary

def CreateDictionaryFromTxtMK2(txtfile):
    """<string> -> dictionary
    separated by comma, two columns"""
    fo = open(txtfile, 'r')
    dictionary = {}
    line = fo.readline()
    while line and line!='\n':
        # the first element would be the key(the value to be updated in the shapefile)
        # the second would be the updating value

        key = line.split(',')[0].strip()
        value = line.split(',')[1].strip()
        if key not in dictionary.keys():
            dictionary[key] = [value]
        else:
            dictionary[key].append(value)

        line= fo.readline()

    return dictionary

def CreateListFromTxtTable(txtfile):
    """<string> -> python list
    convert a txt file to a python list"""
    f = open(txtfile, 'r')
    list = []
    line = f.readline()
    while line:
        list.append(line.strip())
        line = f.readline()
    f.close()
    return list

def CreateCSVFileFromList(pylist, txtfile):
    f = open(txtfile, 'w')
    for each in pylist:
        f.write(str(each))
        f.write('\n')

    f.close()

def assignClassAttr(instanceofClass, vardict):
    """ built for setting attributes in instances
    instanceofClass usually 'self'
    """
    for key, value in vardict.iteritems():
        if key != 'self':
            setattr(instanceofClass, key, value)

def FieldAliasNameChange(field_object, newaliasname):
    """This function is used to give a new alias name
    for the given field"""
    field_object.aliasName = newaliasname

def GuessedNames(ori_name):
    """<string> -> pylist of strign names
    This function returns a list of 'guessed' names which are similar
    e.g. Banc d'Arguin -> Banc d"""
    return_list = []
    return_list.append(ori_name)

    # situation '\'' chr(39) versus '’' chr(146)
    if chr(39) in ori_name:
        new_name = ori_name.replace(chr(39), chr(146))
        return_list.append(new_name)
    if chr(146) in ori_name:
        new_name = ori_name.replace(chr(146), chr(39))
        return_list.append(new_name)


    return return_list

def CreateMultiPointFromXYdecimal(lonlatlist):
    """lonlatlist in a form of [[px,py],[px', py']...]
    returns a multipoint geometry, need insert cursor to make it a feature class"""
    point = arcpy.Point()
    array = arcpy.Array()
    for eachpoint in lonlatlist:
        point.X = float(eachpoint[0])
        point.Y = float(eachpoint[1])
        array.add(point)
    multipoint = arcpy.Multipoint(array)
    return multipoint

def CreatePolygonFromXYdecimal(lonlatlist):
    # need to have spatial ref, otherwise default tolerance for 'unknown' will set to 0.001 unknown unit
    sref = arcpy.SpatialReference(arcpy.GetInstallInfo()["InstallDir"] + os.sep + 'Coordinate Systems\\Geographic Coordinate Systems\\World\\WGS 1984.prj')
    point = arcpy.Point()
    array = arcpy.Array()
    for eachpoint in lonlatlist:
        point.X = float(eachpoint[0])
        point.Y = float(eachpoint[1])
        array.add(point)
    array.add(array.getObject(0))
    polygon = arcpy.Polygon(array, sref)

    #debug
    print array.count
    for each in array:
        print each

    for part in polygon:
        for point in part:
            print point.X,point.Y

    return polygon

def CreatePointFSFromMultiPointGeom(multipoint, outpath, fcname):
    fc = arcpy.CreateFeatureclass_management(outpath, fcname, 'MULTIPOINT', MUTLIPOINT_TEMPLATE, '', '', 'Coordinate Systems\\Geographic Coordinate Systems\\World\\WGS 1984.prj')
    rows = arcpy.InsertCursor(fc)
    row = rows.newRow()
    row.shape = multipoint
    rows.insertRow(row)
    del row, rows

def CreatePolygonFSFromPolygonGeom(polygon, outpath, fcname):
##    fc = arcpy.CreateFeatureclass_management(outpath, fcname, 'POLYGON', r'D:\Yichuan\MyGDB.gdb\polygon_template_2', '', '', 'Coordinate Systems\\Geographic Coordinate Systems\\World\\WGS 1984.prj')
##    rows = arcpy.InsertCursor(fc)
##    row = rows.newRow()
##    row.shape = polygon
##    rows.insertRow(row)
##    del row, rows
    fc = arcpy.CopyFeatures_management([polygon], outpath+os.sep+fcname)

def FindFileByExtension(path, ext, call_func):
    """This function finds appropriate file by extension and run call_func on
    each file"""
    counter = 0
    for path, dir, files in os.walk(path):
        for file in files:
            if os.path.splitext(file)[1] == ext:
                call_func(os.path.join(path, file))
                counter += 1

def ExportMXDtoMap(mxdpath, outpath, pagewidth, pageheight, resolution, format):
    # get mapdocument object
    mxd = arcpy.mapping.MapDocument(mxdpath)

    # export by format
    if string.upper(format) == 'PNG':
        arcpy.mapping.ExportToPNG(mxd, outpath, "PAGE_LAYOUT",
                                  pagewidth, pageheight, resolution)
    elif string.upper(format) == 'JPG':
        arcpy.mapping.ExportToJPEG(mxd, outpath, "PAGE_LAYOUT",
                                  pagewidth, pageheight, resolution)
    elif string.upper(format) == 'PDF':
        arcpy.mapping.ExportToPDF(mxd, outpath, "PAGE_LAYOUT",
                                  pagewidth, pageheight, resolution)
    elif string.upper(format) == 'EPS':
        arcpy.mapping.ExportToEPS(mxd, outpath, "PAGE_LAYOUT",
                                  pagewidth, pageheight, resolution,
                                  colorspace = 'CMYK', convert_markers = True)
    elif string.upper(format) == 'AI':
        arcpy.mapping.ExportToAI(mxd, outpath, 'PAGE_LAYOUT',
                                 pagewidth, pageheight, resolution, image_quality = 'BEST',
                                 colorspace = 'CMYK', picture_symbol = 'RASTERIZE_BITMAP',
                                 convert_markers = True)


class DummyClass(object):
    """test class"""
    def __init__(self, a=0, b=0, c=0):
        self.a = a
        self.b = b
        self.c = c
    def addall(self):
        return self.a + self.b + self.c

class IUCNstring(object):
    """IUCN string class to be used in the ESRI geoprocessing environment"""
    def __init__(self, inputstring):
        """input string to initialise a IUCNstring instance"""
        self.origin_string = inputstring

    def replace_single_quote_sql(self):
        """this function is used to replace single quote with two
        double single quote in SQL queries in the ESRI environment"""
        if '\'' in self.origin_string:
            validstrName = self.origin_string.replace('\'', '\'\'')
        else:
            validstrName = self.origin_string

        return validstrName

    def valid_output_file_name(self):
        """<String> -> string
         convert all illegal character (/ \ : * ? " ' < > | )"""
        newname = self.origin_string
        newname = newname.strip()

        invalid_chr_list = ('\\', '/', ':', '*', '?', '\"', '<', '>', '|', '\'', '.', ' ')

        for letter in newname:
            if letter in invalid_chr_list:
                newname = newname.replace(letter, '_')

        # remove multiple '_'s
        enum = enumerate(newname)

        # letter is an tuple, (index, value)
        letter = enum.next()
        newname2 = letter[1]
        while letter:
            _1stletter = letter[1]
            try:
                letter = enum.next()
                _2ndletter = letter[1]

                if not (_1stletter == '_' and _2ndletter =='_'):
                    newname2 += _2ndletter
            except StopIteration:
                break

        return newname2

##def convertListToSQL(pylist):
##    string = '('
##    for each in pylist:
##        string + = each +

class IUCNresult(object):
    """IUCN result holder"""
    def __init__(self, title = 'Default title', firstrow =''):
        self.title = title
        self.firstrow = firstrow
        self.body = []
    def clear(self):
        """clear all body content"""
        self.body = []
    def log(self, line):
        """<string>,
        record a line in the body"""
        self.body.append(line)

class IUCNresult_exporter(object):
    """"""
    def __init__(self, init_result = None):
        """<IUCNresult object>"""
        self.resultList = []
        if init_result:
            self.resultList.append(init_result)

    def __len__(self):
        return len(self.resultList)

    def __str__(self):
        outstr = 'result objects:\n%s\n'%len(self.resultList)
        for each in self.resultList:
            outstr += each.title + '\n'
        return outstr

    def add_result_object(self, result):
        self.resultList.append(result)

    def add_result_object_list(self, resultList):
        self.resultList.extend(resultList)

    def clear(self):
        self.resultList = []

    def write(self, exportFile):
        fo = open(exportFile, 'a')
        fo.write('----*** IUCN GEOPROCESSING RESULTS AT ' + CurrentTime + ' ***----\n\n')
        for result in self.resultList:
            if result:
                fo.write(result.title + '\n' + result.firstrow + '\n')
                for eachline in result.body:
                    fo.write(eachline + '\n')
                fo.write('\n')

        fo.close()

    def printtostring(self):
        for result in self.resultList:
            if result:
                print result.title + '\n' + result.firstrow + '\n'
                for eachline in result.body:
                    print eachline + '\n'
        print '\n'

class IUCNcheck(object):
    """IUCN check class to be used to do all pre-uploading check"""
    def __init__(self, inputFL, outputLoc, fields, FlagSR = False, FlagRF = False, FlagG = False, FlagFS= False):
        """<inputFL>, <outLoc>, [fieldflags], [field list]
        set class variables"""
        assignClassAttr(self, locals())
        self.resultList = []

        # get a list of field names from inputFL
        self._inputFL_field_list = set()
        for eachfield in arcpy.ListFields(self.inputFL):
            self._inputFL_field_list.add(eachfield.name.upper())

        # allow overwrite
        arcpy.env.overwriteOutput = True
##        Printboth(fields)
        result = IUCNresult(inputFL)
        self.resultList.append(result)

    def run(self):
        """The congregate method to check all flags and run all methods if required"""
        if self.FlagSR:
            self.check_spatial_reference()

        if self.FlagRF:
            self.check_required_fields()

        if self.FlagG:
            self.check_geometry()

        if self.FlagFS:
            self.calculate_field_stats()

##        self.export_to_csv()

        Printboth('Checks completed! Please see output for detail')



    def check_spatial_reference(self):
        # wkid 4326, default wgs84 geographic
        SRSTRING = u"GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1111948742.96404;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision"

        Printboth('Checking spatial reference...')
        result = IUCNresult('-----CHECK SPATIAL REFERENCE-----')
        desc = arcpy.Describe(self.inputFL)
        if desc.spatialreference.exporttostring() != SRSTRING:
            result.log("\tWarning! Spatial reference is NOT WGS1984\n\tYour SR is: \n\t\t%s"%desc.spatialreference.exporttostring())
        else:
            result.log("\tChecked! Spatial reference is WGS1984")

        self.resultList.append(result)

    def check_required_fields(self, fwflag = False):
        # check names only
        fieldlist = ('BINOMIAL','PRESENCE', 'ORIGIN', 'SEASONAL', 'COMPILER', 'YEAR', \
                    'CITATION', 'SOURCE', 'DIST_COMM', 'ISLAND', 'SUBSPECIES', 'SUBPOP'\
                    , 'TAX_COMMEN', 'DATA_SENS', 'SENS_COMM', 'LEGEND')
        fwfieldlist = ('HSHEDID',) + fieldlist

        if not fwflag:
            checklist = set(fieldlist)
        else:
            checklist = set(fwfieldlist)

        Printboth('Checking required field...')
        result = IUCNresult('-----CHECK REQUIRED FIELDS-----')

        # get a field set from input
        # see private function
##        fcnamelist = set()
##        for eachfield in arcpy.ListFields(self.inputFL):
##            fcnamelist.add(eachfield.name.upper())

        if len(checklist.difference(self._inputFL_field_list)) > 0:
            for each in checklist.difference(self._inputFL_field_list):
                result.log('\tField: '+ each + ', not found in data')
        else:
            result.log('\tField check completed and no field is missing')

        self.resultList.append(result)

    def check_geometry(self):
        Printboth('Checking geometry...')
        result = IUCNresult('-----CHECK GEOMETRY-----')

        try:
            arcpy.CheckGeometry_management(self.inputFL, self.outputLoc + os.sep + 'geometry_checks.dbf')
        except:
            Printboth(arcpy.GetMessage(2))
            result.log('\t'+arcpy.GetMessage(2))
        else:
            result.log('\tPlease check \'geometry_checks.dbf\' in the same folder for detailed information')
        finally:
            self.resultList.append(result)

    def calculate_field_stats(self):
        Printboth('Calculating field stats...')
        result = IUCNresult('-----CALCULATE FIELD STATS-----')
        # get row iterator
        layer = "featurelayer"
        arcpy.MakeFeatureLayer_management(self.inputFL, layer)

        try:
            rows = arcpy.SearchCursor(layer)
            row = rows.next()

            #pydict to store dictionaries for each field (a dictionray)
            pyDict = {}
            for eachfield in self.fields:
                pyDict[eachfield] = dict()

            while row:
                for eachfield in self.fields:
                    # need to test to see if the selected field is in the table
                    if eachfield in self._inputFL_field_list:
                        if row.getValue(eachfield) not in pyDict[eachfield].keys():
                            pyDict[eachfield][row.getValue(eachfield)] = 1
                        else:
                            pyDict[eachfield][row.getValue(eachfield)] += 1
                    else:
                        pass

                row = rows.next()

        except:
            result.log('\t'+ arcpy.GetMessage(2))

        else:
            # summary stats for all
            result.log('\t[SUMMARY]\n\tFieldname, Count of unique values')
            for key in pyDict.keys():
                try:
                    result.log('\t' + str(key) + ',' + str(len(pyDict[key])))
                except:
                    pass
            result.log('\n')

            # key for each field
            for key in pyDict.keys():
                result.log('\t[FIELD]: '+ str(key))
                result.log('\tvalue, count')
                # key for each distinct value in field
                for subkey in pyDict[key].keys():
                    try:
                        result.log('\t' + str(subkey) + ',' + str(pyDict[key][subkey]))
                    except:
                        pass
                result.log('\n')

        finally:
            if row:
                del row
            if rows:
                del rows

        del layer
        self.resultList.append(result)

    def export_to_csv(self, filename = 'result.csv'):
        exporter = IUCNresult_exporter()
        exporter.add_result_object_list(self.resultList)
        exporter.write(self.outputLoc + os.sep + filename)
        Printboth('Finished writing to output!')


# if __name__ != "__main__":
#     print "Yichuan10 module imported at %s, %s"%(CurrentTime,ModifiedDate)
# else:
#     pass

#-----------------------------------------
# Yichuan SHI
# Date: 7 November 2013
# Module on useful functions for mapping
#
# ----------------------------------------

import os, sys, arcpy, string, YichuanSR



from datetime import datetime

dt = datetime.now()
CurrentTime = str(dt.hour) + ':' + str(dt.minute) + ':' +str(dt.second) + ' ' + \
              str(dt.year)+'.'+str(dt.month)+'.' + str(dt.day)
ModifiedDate = 'ver: 7 November 2013'


# constant of the dimension of the picture
PIC_PARAM = {'elementPositionX':1,
'elementPositionY': 10,
'elementWidth': 15,
'elementHeight': 10}

# constant for data field id
DATA_IDFIELD = "wdpaid"

# default mxd
# MXD =


def insert_picture(pictureElement, source, param):
    # this function finds the source picture and puts it in the pictureElement
    original_name = pictureElement.name

    pictureElement.elementPositionY = param['elementPositionY']
    pictureElement.elementPositionX = param['elementPositionX']
    pictureElement.elementWidth = param['elementWidth']
    pictureElement.elementHeight = param['elementHeight']
    pictureElement.sourceImage = source

    # for some reason arcgis changes its default name
    pictureElement.name = original_name

    arcpy.RefreshActiveView()

def get_field_value(layername, id, value_field, id_field = DATA_IDFIELD):
    # this function return the value of the fieldname by giving id
    # id must be unique
    # print (DATA_IDFIELD, value_field)
    with arcpy.da.SearchCursor(layername, (id_field, value_field)) as cursor:
        for row in cursor:
    #        print row
            if row[0] == id:
                return row[1]

        return None

def set_txt_element_value(txtElement, value):
    # set txtElement value
    txtElement.text = value

def find_layer_by_name(layername, mxd):
    # finds the layer by its name
    layer_list = arcpy.mapping.ListLayers(mxd)
    for layer in layer_list:
        if layer.name == layername:
            return layer

    print "Warning: cannot find the specified layer by name - return None"
    return None


def find_element_by_name(element_name, mxd):
    # finds the element by its name
    element_list = arcpy.mapping.ListLayoutElements(mxd)
    for element in element_list:
        if element.name == element_name:
            return element

    print "Warning: cannot find the specified element by name - return None"
    return None


def export_map(outpath, outformat, mxd, reso = 300):
    """<outputfilepath>, <png, jpg, ...>, <mxdpath>, <resolution>"""

    if string.upper(outformat) == 'PNG':
        arcpy.mapping.ExportToPNG(mxd, outpath, "PAGE_LAYOUT", resolution = reso)
    elif string.upper(outformat) == 'JPG' or string.upper(outformat) == 'JPEG':
        arcpy.mapping.ExportToJPEG(mxd, outpath, "PAGE_LAYOUT", resolution = reso)
    elif string.upper(outformat) == 'PDF':
        arcpy.mapping.ExportToPDF(mxd, outpath, "PAGE_LAYOUT", resolution = reso, layers_attributes = "NONE")

    # the following methods should be used for printing only
    elif string.upper(outformat) == 'EPS':
        arcpy.mapping.ExportToEPS(mxd, outpath, "PAGE_LAYOUT", resolution = reso, colorspace = 'CMYK', convert_markers = True)
    elif string.upper(outformat) == 'AI':
        arcpy.mapping.ExportToAI(mxd, outpath, 'PAGE_LAYOUT',
                                 resolution = reso, image_quality = 'BEST',
                                 colorspace = 'CMYK', picture_symbol = 'RASTERIZE_BITMAP',
                                 convert_markers = True)


def get_current_mxd():
    return arcpy.mapping.MapDocument("CURRENT")

def set_df_to_geom_by_wdpaid(layername, wdpaid, mxd='CURRENT', dfname='*'):
    # get geom
    geom = get_field_value(layername, wdpaid, 'SHAPE@')

    # get sr
    sr = YichuanSR.get_desirable_sr(geom)

    # set df sr
    YichuanSR.set_sr_to_df(sr, mxd, dfname)


def export_mxd_folder_map(folder, outformat='PNG'):
    for path, dir, files in os.walk(folder):
        for file in files:
            # get filename and fileextension
            base = os.path.basename(file)
            filename, fileext = os.path.splitext(base)

            if fileext == '.mxd':
                mxdfolder = os.path.join(folder, 'mxd_export')
                # make export folder if non exist
                if not os.path.exists(mxdfolder):
                    os.mkdir(mxdfolder)

                mxdpath = os.path.join(folder, file)
                mxd = arcpy.mapping.MapDocument(mxdpath)

                # format
                outpath = os.path.join(mxdfolder, filename + '.' + outformat)
                export_map(outpath, outformat, mxd, reso = 300)

if __name__ != "__main__":
    print "Mapping module imported %s, %s"%(CurrentTime,ModifiedDate)
else:
    pass


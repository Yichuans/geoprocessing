
import os, sys
##sys.path.append("D:\Yichuan\Scripts\scripts")

# import myheader
import arcpy

# import Yichuan libraries
import YichuanM
import YichuanSR
import Yichuan10

# constant for data field id
DATA_IDFIELD = "wdpaid"
wh_layer = "whs_dump_160129"

mapdocument = r"E:\Yichuan\WHS_map_batcher\Mapbatcher_WHS_160617.mxd"
basefolder = r"E:\Yichuan\WHS_map_batcher\export"


def main(reso = 300):
    exportfolder = basefolder + os.sep + str(reso)
    if not os.path.exists(exportfolder):
        os.mkdir(exportfolder)

    # Initialising
    mxd = arcpy.mapping.MapDocument(mapdocument)
    layer_index = YichuanM.find_layer_by_name(wh_layer, mxd)

    # get a list of all wdpa ids
    wdpalist = Yichuan10.GetUniqueValuesFromFeatureLayer_mk2(layer_index, DATA_IDFIELD)

    wdpalist.sort()

    # dataframe extent
    df = arcpy.mapping.ListDataFrames(mxd)[0]

    # Go loop!
    Yichuan10.Printboth('Start batching...')
    for each in wdpalist:
        try:
            # export files
            exportpng = exportfolder + os.sep + str(each) + '.jpg'

            query = '\"wdpaid\" = ' + str(each)

            # tried page definition in the arcmap application
            layer_index.definitionQuery = query

            # set dataframe coordinate system
            sr =arcpy.SpatialReference()

            sr_string = Yichuan10.GetFieldValueByID_mk2(layer_index, each, value_field='utm')

            sr.loadFromString(sr_string)

            # load from its attribute
            df.spatialReference = sr

            df.extent = layer_index.getExtent()
            df.scale = df.scale * 1.1

            # YichuanM.export_map(exportpng, 'png', mxd, reso=50)
            # need to specify a low quality
            arcpy.mapping.ExportToJPEG(mxd, exportpng, "PAGE_LAYOUT", resolution = reso, jpeg_quality=60)

        except Exception as e:
            Yichuan10.Printboth('Error occurred')
            Yichuan10.Printboth(str(e))
            Yichuan10.Printboth(sys.exc_info()[0])
            pass

        else:
            Yichuan10.Printboth('Export successful\n')

        finally:
            layer_index.definitionQuery = ""

    del layer_index
    del mxd
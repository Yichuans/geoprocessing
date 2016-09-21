#-----------------------------------------
# Yichuan SHI
# Date: 27 July 2011
# rev Date: 15 Aug 2011
# rev Date: 31 Jan 2013
# rev Date: 8 Sep 2016
# WHS mapbatcher datadriven
#     Create maps in batches
# ----------------------------------------

import os, sys

import arcpy

RESOLUTION = 300
EXPORT_FOLDER = os.getcwd() + os.sep + "output"

# mxd map template
MXD = arcpy.mapping.MapDocument(r"template.mxd")

# layer name
COUNTRY = "Country boundary"
COUNTRY_MASK = "EEZ"

def main(iso_text, reso = 300, mxd = MXD, country = COUNTRY, country_mask = COUNTRY_MASK, exportfolder = EXPORT_FOLDER):
    # get dataframe
    df = arcpy.mapping.ListDataFrames(mxd)[0]

    # make output if does not exist
    if not os.path.exists(exportfolder):
        os.mkdir(exportfolder)

    # get iso3 list
    iso3s = get_list_from_txt_table(iso_text)
    print('Total iterations: ' + str(len(iso3s)))
    print(iso3s)

    # get layers
    lyr_country = find_layer_by_name(country, mxd)
    lyr_country_mask = find_layer_by_name(country_mask, mxd)

    # Go loop!
    print('Start batching...')
    for iso3 in iso3s:
    # try:
        # apply def query
        print('Exporting: '+iso3)
        lyr_country.definitionQuery = "\"ISO3\" = '" + iso3 + "'"
        lyr_country_mask.definitionQuery = "\"ISO3\" <> '" + iso3 + "'"

        output_img = exportfolder + os.sep + str(iso3) + '.png'

        # adjust extent
        df.extent = lyr_country.getExtent()
        df.scale = df.scale * 1.1

        arcpy.mapping.ExportToPNG(mxd, output_img, "PAGE_LAYOUT", resolution = reso)

        # except Exception as e:
        #     print(str(e))
        #     print(sys.exc_info()[0])
        #     pass

        # clear definition query
        for lyr in [lyr_country, lyr_country_mask]:
            clear_def_query(lyr)

    del mxd

def find_layer_by_name(layername, mxd):
    # finds the layer by its name
    layer_list = arcpy.mapping.ListLayers(mxd)
    for layer in layer_list:
        if layer.name == layername:
            return layer

    print("Warning: cannot find the specified layer by name - return None")
    return None


def get_list_from_txt_table(textfile):
    # ***very important to call .strip() otherwise \n will disrupt the export function and cause it to fail ****
    list_from_text = [line.strip() for line in open(textfile, 'r')]
    list_from_text.sort()
    return list_from_text


def clear_def_query(layer):
    layer.definitionQuery = ""


def _add_def_query(layer, field, value):
    if type(value) == str:
        query = '\"{0}\" = \''.format(field) + value + '\''
    elif type(value) == int:
        query = '\"{0}\" = '.format(field) + str(value)
    else:
        raise Exception('Error: input definition query field must be either str or int')

    layer.definitionQuery = query

if __name__ == "__main__":
    iso_text = "iso3.txt"
    main(iso_text)

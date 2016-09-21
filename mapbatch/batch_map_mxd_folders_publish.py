# this script is used to find all given file extentions
import os
import Yichuan10
##from Yichuan10 import ExportMXDtoMap as call_func

def export_for_publish_all_folders(path, ext, foldername, ExportMXDtoMap):
    counter = 0
    for path, dir, files in os.walk(path):
        for file in files:
            # get filename and fileextension
            base = os.path.basename(file)
            filename, fileext = os.path.splitext(base)

            if fileext == ext:
                mxdfolder = os.path.join(path, foldername)
                # make export folder if non exist
                if not os.path.exists(mxdfolder):
                    os.mkdir(mxdfolder)

                # thumb
                outpath = os.path.join(mxdfolder, filename + '.JPG')
                ExportMXDtoMap(os.path.join(path, file),outpath, 3509, 2481, 100, 'JPG')

                # png
                outpath = os.path.join(mxdfolder, filename + '.PNG')
                ExportMXDtoMap(os.path.join(path, file),outpath, 3509, 2481, 300, 'PNG')

##                # eps
##                outpath = os.path.join(mxdfolder, filename + '.EPS')
##                ExportMXDtoMap(os.path.join(path, file),outpath, 3509, 2481, 300, 'EPS')

                # ai
                outpath = os.path.join(mxdfolder, filename + '.AI')
                ExportMXDtoMap(os.path.join(path, file),outpath, 3509, 2481, 300, 'AI')
####
                counter += 1

def export_for_publish(path, ext, foldername, ExportMXDtoMap, publish=True):
    counter = 0
    for file in os.listdir(path):
        # get filename and fileextension
        base = os.path.basename(file)
        filename, fileext = os.path.splitext(base)

        if fileext == ext:
            mxdfolder = os.path.join(path, foldername)
            # make export folder if non exist
            if not os.path.exists(mxdfolder):
                os.mkdir(mxdfolder)

            if publish:
                # thumb
                outpath = os.path.join(mxdfolder, filename + '.JPG')
                ExportMXDtoMap(os.path.join(path, file),outpath, 3509, 2481, 100, 'JPG')

                # png
                outpath = os.path.join(mxdfolder, filename + '.PNG')
                ExportMXDtoMap(os.path.join(path, file),outpath, 3509, 2481, 300, 'PNG')

                # ai
                outpath = os.path.join(mxdfolder, filename + '.AI')
                ExportMXDtoMap(os.path.join(path, file),outpath, 3509, 2481, 300, 'AI')
    ####
                counter += 1
            else:

                outpath = os.path.join(mxdfolder, filename + '.PNG')
                ExportMXDtoMap(os.path.join(path, file),outpath, 3509, 2481, 300, 'PNG')

def export_for_publish_one_mxd(filepath, foldername, ExportMXDtoMap):
    base = os.path.basename(filepath)
    path = os.path.dirname(filepath)
    filename, fileext = os.path.splitext(base)

    if fileext == '.mxd':
        print "export mxd"
        mxdfolder = os.path.join(path, foldername)
        if not os.path.exists(mxdfolder):
            os.mkdir(mxdfolder)

        outpath = os.path.join(mxdfolder, filename + '.JPG')
        ExportMXDtoMap(filepath, outpath, 3509, 2481, 100, 'JPG')

        # png
        outpath = os.path.join(mxdfolder, filename + '.PNG')
        ExportMXDtoMap(filepath, outpath, 3509, 2481, 300, 'PNG')

        # ai
        outpath = os.path.join(mxdfolder, filename + '.AI')
        ExportMXDtoMap(filepath, outpath, 3509, 2481, 300, 'AI')
##
##        # eps
##        outpath = os.path.join(mxdfolder, filename + '.EPS')
##        ExportMXDtoMap(filepath, outpath, 3509, 2481, 300, 'EPS')

    else:
        print 'file extension:', fileext, 'is not mxd'
        print 'export fail'

def set_relative_path():
    Workspace = r"D:\Yichuan\Bastian\MarineGap\maps130412"
    arcpy.env.workspace = Workspace

    #list map documents in folder
    mxdList = arcpy.ListFiles("*.mxd")

    #set relative path setting for each MXD in list.
    for file in mxdList:
        #set map document to change
        filePath = os.path.join(Workspace, file)
        mxd = arcpy.mapping.MapDocument(filePath)
        #set relative paths property
        mxd.relativePaths = False
        #save map doucment change
        mxd.save()


##mxd_folder = r"D:\Yichuan\Bastian\GGA\final_figures\review_figures"
##mxd_folder = r"D:\Yichuan\Bastian\GGA\final_figures\fr_vectors"
##
##mxd_folder = r"D:\Yichuan\Jon_hutton\Korea_DMZ\maps"
####
##export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap)

##mxd_folder = r"D:\Yichuan\Bastian\GGA\final_figures\vector_figures"
##export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap)

##mxd_folder = r"D:\Yichuan\Bastian\GGA\final_figures\fr_vectors"
##export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap)


##mxd_folder = r"D:\Yichuan\Bastian\MarineGap\Final_graphics\map_mxd"
##export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap)

##filepath = r"D:\Yichuan\Bastian\MarineGap\Final_graphics\map_mxd\Fig1.1 current_mwh_distribution.mxd"
##export_for_publish_one_mxd(filepath, "mxd_export", Yichuan10.ExportMXDtoMap)


##mxd_folder = r"D:\Yichuan\Bastian\MarineGap\Final_graphics\map_mxd_fr"
##export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap)

##mxd_folder = r"D:\Yichuan\Comparative_analysis_2013\maps\mxd"
##export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap, False)


# mxd_folder = r"D:\Yichuan\Bastian\Wilderness_mapping"
# export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap, True)


# mxd_folder = r"E:\Yichuan\Diana\multiple_designations"
# export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap, True)


mxd_folder = r"E:\Yichuan\Wilderness_guidance\maps"
export_for_publish(mxd_folder, '.mxd', 'mxd_export', Yichuan10.ExportMXDtoMap, True)


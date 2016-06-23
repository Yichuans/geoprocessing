import sys, os, arcpy, time


def clip_raster(featurelayer, in_raster, out_raster, no_data=0):
    """wrapper around arcpy.clip_management, note no_data may scale up pixel depth
    thus requiring more computation time and space; e.g clipping 30m over
    GBR takes a long time, consider to change this to 254, thus fitting int8 """
    starttime = time.time()
    # print 'Clipping ', in_raster, 'by', featurelayer, ', output:', out_raster, ', nodata:', no_data
    try:
        arcpy.Clip_management(in_raster, "#", out_raster, featurelayer, no_data, "ClippingGeometry")
        print 'Clip successful'
    except Exception as e:
        print "Clip failed:"
        print arcpy.GetMessages(), str(e), sys.exc_info()[0]
    finally:
        stoptime = time.time()
        print 'Time taken:', stoptime - starttime, 's'


def convert_overlap_to_csv(fc, fc_id, in_raster, out_csv):

	f = open(out_csv, 'a')

	# heading
	f.write(fc_id + ',' + 'ras_val\n')

	# counter
	num_done = 0
	with arcpy.da.SearchCursor(fc, [fc_id, 'shape@']) as cur:
		for row in cur:
			# for each row geom row[-1], clip and convert to numpy

			# temp raster
			ras_temp = r'in_memory\temp_ras'

			clip_raster(row[-1], in_raster, ras_temp, 0)

			# numpy 
			ras_numpy = arcpy.RasterToNumPyArray(ras_temp)

			# flatten to 1D, get rid of unwanted values
			ras_numpy = ras_numpy.flatten()[ras_numpy.flatten()!=0]

			# write
			for each in ras_numpy:
				f.write(str(row[0]) + ',' + str(each) + '\n')

			# clean up
			arcpy.Delete_management(ras_temp)

			num_done += 1
			if num_done%100 ==0:
				print num_done, ' clip done'

	f.close()


# test
def meow200_run():
	fc = r"E:\Yichuan\MyGDB.gdb\meow_200m_depth"
	fc_id = "ECO_CODE"
	in_raster = r"E:\Yichuan\Wilderness_guidance\marine\global_cumul_impact_2013_all_layers.tif"
	out_csv = 'test.csv'

	convert_overlap_to_csv(fc, fc_id, in_raster, out_csv)


def wh47_run():
	fc = r"E:\Yichuan\Fanny\marine_nwhs_160429.shp"
	fc_id = 'wdpaid'
	in_raster = r"E:\Yichuan\Wilderness_guidance\marine\global_cumul_impact_2013_all_layers.tif"
	out_csv = 'wh47.csv'

	convert_overlap_to_csv(fc, fc_id, in_raster, out_csv)


def meow_pelagic_run():
	fc = r"E:\Yichuan\Wilderness_guidance\marine\marine_data.gdb\meow_meowv_pelagic"
	fc_id = "OBJECTID"
	in_raster = r"E:\Yichuan\Wilderness_guidance\marine\global_cumul_impact_2013_all_layers.tif"
	out_csv = r'E:\Yichuan\Wilderness_guidance\marine\result.csv'

	convert_overlap_to_csv(fc, fc_id, in_raster, out_csv)


def meow_pelagic_run_no_antarctica():
	fc = r"E:\Yichuan\Wilderness_guidance\marine\marine_data.gdb\meow_meowv_pelagic_no_antarctica"
	fc_id = "OBJECTID"
	in_raster = r"E:\Yichuan\Wilderness_guidance\marine\global_cumul_impact_2013_all_layers.tif"
	out_csv = r'E:\Yichuan\Wilderness_guidance\marine\result.csv'

	convert_overlap_to_csv(fc, fc_id, in_raster, out_csv)


def run_wh_base_intersect():
	fc = r"E:\Yichuan\Wilderness_guidance\marine\marine_data.gdb\meow_meowv_pelagic_no_antarctica_wh_intersect"
	fc_id = "OBJECTID_12"
	in_raster = r"E:\Yichuan\Wilderness_guidance\marine\global_cumul_impact_2013_all_layers.tif"
	out_csv = r'E:\Yichuan\Wilderness_guidance\marine\wh_base_intersect.csv'

	convert_overlap_to_csv(fc, fc_id, in_raster, out_csv)

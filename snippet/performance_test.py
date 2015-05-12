from Yichuan10 import simple_time_tracker, GetUniqueValuesFromFeatureLayer_ogr, GetUniqueValuesFromFeatureLayer_mk2
import os

@simple_time_tracker
def test_ogr(path, field):
	return GetUniqueValuesFromFeatureLayer_ogr(path, field)

@simple_time_tracker
def test_arcpy(path, field):
	return GetUniqueValuesFromFeatureLayer_mk2(path, field)

def test_get_id_list(path= r"D:\Yichuan\WDPA\WDPA_Apr2015_Public\WDPA_Apr2015_Public.gdb", field='wdpaid'):
	a = test_ogr(path, field)
	b = test_arcpy(path + os.sep + 'WDPA_poly_Apr2015', field)
	return a, b

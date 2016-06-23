import arcpy

OUTCSV = 'test.csv'

# last one has to be shape
FIELD_NAMES = ['OID@', 'id_no', 'shape@']

def check_dataset(datapath, outputcsv=OUTCSV, field_names = FIELD_NAMES):

	# open file and write header
	f = open(outputcsv, 'w')
	f.write('oid, id, partcount, pointcount\n')

	# write output
	with arcpy.da.SearchCursor(datapath, field_names) as cur:
		for row in cur:
			f.write(','.join([str(row[0]), str(row[1]), str(row[2].partCount), str(row[2].pointCount)]))
			f.write('\n')

	f.close()







def to_mm_yy(julian_days):
	"""
	Takes a julian day: 1 ~ 365 and converts to mmyy (0101 ~ 1231)
	"""
	jd = julian_days
	if jd<0 or jd>365:
		print 'julian days can be less than 0 or greater than 365!'
		return None
	elif jd<= 31:
		mm = '01'
		dd = str(jd)
	elif jd > 31 and jd <=59:
		mm = '02'
		dd = str(jd - 31)
	elif jd > 59 and jd <=90:
		mm = '03'
		dd = str(jd - 59)

	elif jd > 90 and jd <=120:
		mm = '04'
		dd = str(jd - 90)
	elif jd > 120 and jd <=151:
		mm = '05'
		dd = str(jd - 120)
	elif jd > 151 and jd <=181:
		mm = '06'
		dd = str(jd - 151)
	elif jd > 181 and jd <=212:
		mm = '07'
		dd = str(jd - 181)
	elif jd > 212 and jd <=243:
		mm = '08'
		dd = str(jd - 212)
	elif jd > 243 and jd <=273:
		mm = '09'
		dd = str(jd - 243)
	elif jd > 273 and jd <=304:
		mm = '10'
		dd = str(jd - 273)
	elif jd > 304 and jd <=334:
		mm = '11'
		dd = str(jd - 304)
	else:
		mm = '12'
		dd = str(jd - 334)

	if len(dd) == 1:
		dd = '0' + dd

	return mm + dd


def land_sat_to_ymd(landsat_string):
	yyyy = landsat_string[9:13]
	julian_days = landsat_string[13:16]
	return yyyy+to_mm_yy(int(julian_days))
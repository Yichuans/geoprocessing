import traceback

class myexception(Exception):
	print('myexception')


try:
	if True:
		raise myexception
	1/0

except myexception as t:
	print(type(t))

except Exception as e:
	print(e)
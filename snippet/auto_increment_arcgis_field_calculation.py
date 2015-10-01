binolist = {}
rec = 0

def add_num(bino):
	global binolist
	global rec

	if bino not in binolist.keys():
		binolist[bino] = rec
		id_no = rec
		rec+=1
    
    else:
    	id_no = binolist[bino]

    return id_no
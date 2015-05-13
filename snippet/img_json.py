import json, random
import os
from Yichuan10 import CreateListFromTxtTable

# get wh data
csv_file = r"D:\Yichuan\WHS_dump_ATTR\wh2014.csv"
wh = CreateListFromTxtTable(csv_file)[1:] # get rid of header
# get wh image enumerator, from a random selection

# random seed
random.seed(123)

# get random pictures for the WH
folder = r"D:\web2py_2014\applications\wh_app\views\experiment\unesco_pic_resize"
rand_sample_img = random.sample(os.listdir(folder), len(wh))
enum_imgs = enumerate(rand_sample_img)

# create list of dicts to be convert to json
wh_list = list()
for each in wh:
	wdpaid = each.split(',')[0]
	wh_name = ','.join(each.split(',')[1:])
	wh_list.append({'wdpaid': wdpaid, 'wh_name':wh_name, 'back': "unesco_pic_resize/" + enum_imgs.next()[1]})

# dump lines to the file
lines = json.dumps(wh_list)
with open(r"D:\web2py_2014\applications\wh_app\views\experiment\wh.json", 'w') as f:
	f.write(lines)

# debug
print lines
	
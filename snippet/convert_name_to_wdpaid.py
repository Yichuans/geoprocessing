# to convert name to WDPAID
import os
from Yichuan10 import time_tracker
from YichuanDB import getCurrentWH, getDictFromDBTable

# require fuzz from fuzzywuzzy library
from fuzzywuzzy import fuzz

HIHG_TRES = 80 

conn = getCurrentWH()
wh_dict = getDictFromDBTable('arcgis', 'v_wh_non_spatial', 'wdpaid', 'en_name', conn)

# reverse key and values
wh_dict_rv = dict((v,k) for k,v in wh_dict.iteritems())

# print(wh_dict)

# @time_tracker
def find_most_similar(input_str, ref_dict = wh_dict_rv):
    guess_wdpaid = None
    guess_name = None
    highest_ratio = 0
    
    # 
    for wh_name in ref_dict.keys():
        ratio1 = fuzz.ratio(input_str, wh_name)
        ratio2 = fuzz.token_set_ratio(input_str, wh_name)

        ratio = max(ratio1, ratio2)

        if ratio > highest_ratio:
            highest_ratio = ratio
            guess_name = wh_name
            guess_wdpaid = ref_dict[wh_name]
        else:
            pass

        # if a good treshold reached, stop looking for more
        if ratio > HIHG_TRES:
            break

    return (guess_wdpaid, guess_name, ratio)


def get_clean_wh_name(path):
    """return the clean name 
    aaa - bbb .docx.md -> bbb

    """
    tmp = path.split('.')[0]

    tmp = '-'.join(tmp.split('-')[1:])

    return tmp


def main(folder = os.getcwd()):
    # change to folder
    os.chdir(folder)

    # convert docx to md file
    # os.system("""for %x in (*.docx) do pandoc -s "%x" -t markdown -o "%x".md""")

    # find the proximate name
    paths = os.listdir(folder)

    for path in paths:
        # ensure correct file extension
        if path.endswith('.md'):
            clean_file_name = get_clean_wh_name(path)
            guess_wdpaid, guess_name, ratio = find_most_similar(clean_file_name)
            if ratio > HIHG_TRES:
                os.rename(path, str(guess_wdpaid) + '.md')





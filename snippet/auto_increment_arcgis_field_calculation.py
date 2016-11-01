# update cursor
def auto_increment(fc, sort_field, update_field, ascend=True, isnum=True):
    """
    <fc/fl>, <input field on which the result is based>, <target field to save auto-increment value>, <sort order of the input field>, <type of the target field: int/chr>
    """

    # find char based on ascii num
    def find_chr_mk2(rec):
        # starting from 'a'
        if rec<97:
            raise Exception('value can\'t be less than 97 for character')
            
        # still within a-z
        elif rec<=122:
            return chr(rec)

        else:
            # work out how many 26s?
            rest = rec - 97
            rec_1 = (rest / 26) # 1->
            rec_2 = (rest % 26) + 1 # 0-25 -> 1-26, i.e. a-z

            if rec_1 > 26:
                raise Exception('maximum value:zz reached')

            return chr(rec_1 + 96) + chr(rec_2 + 96)

    ## cannot use arcpy.update sort fields: does not support text field sort

    ## use native python to sort
    keys = [row[0].upper() for row in arcpy.da.SearchCursor(fc, field_names=sort_field)]

    # default ascending
    if ascend:
        keys.sort()
    else:
        keys.sort(reverse=True)

    # update value in number: such as 9
    if isnum:
        update_values = {key: i + 1 for i, key in enumerate(keys)}

    # update value in chr: such as 'a'
    else:
        update_values = {key: find_chr_mk2(i + 97) for i, key in enumerate(keys)}

    # return update_values

    # actual updates
    with arcpy.da.UpdateCursor(fc, field_names=[sort_field, update_field]) as cur:
        for row in cur:
            key = row[0].upper()

            if not key in update_values:
                raise Exception('key not in updates dict')

            row[1] = update_values[key]

            cur.updateRow(row)



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


# on OID, field calculator
rec=0 
def autoIncrement(): 
    global rec 
    pStart = 1  
    pInterval = 1 
    if (rec == 0):  
      rec = pStart  
    else:  
      rec += pInterval  
    return rec


# ===================================
def find_chr_mk2(rec):
    # starting from 'a'
    if rec<97:
        raise Exception('value can\'t be less than 97 for character')
        
    # still within a-z
    elif rec<=122:
        return chr(rec)

    else:
        # work out how many 26s?
        rest = rec - 97
        rec_1 = (rest / 26)
        rec_2 = (rest % 26) + 1

        return chr(rec_1 + 96) + chr(rec_2 + 96)

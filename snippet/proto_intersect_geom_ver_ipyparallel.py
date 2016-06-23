# A test case to solve the perenniel issue of intersection

import os, sys, time
import arcpy

from ipyparallel import Client
import multiprocessing


# the output template with fields: OIDFC1, OIDFC2, shape@
TEMPLATE = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\template.shp"

# TEST CASE
# INPUTFC = r"E:\Yichuan\WHS.gdb\whs_dump_160129"
# INPUTFC2 =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_p11_dice5000"

# RUN ALL - failed
# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_dice5000"
# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_dice5000_combined"
# INPUTFC2 = r"E:\Yichuan\WHS.gdb\whs_dump_160129_dice1000"

# OUTPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\all_4srun"
# OUTLOG = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\log7.txt"

INPUTFC2 = r"E:\Yichuan\WHS.gdb\whs_dump_160129"

OUTLOG = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\new_log.txt"
q_log = multiprocessing.Queue()

# # RUN P11
# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_p11_dice5000"
# INPUTFC2 = r"E:\Yichuan\WHS.gdb\whs_dump_160129_dice1000"

# OUTPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\p11_dice5000"
# OUTLOG = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\p11.txt"

# # RUN P12
# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_p12_dice5000"
# INPUTFC2 = r"E:\Yichuan\WHS.gdb\whs_dump_160129_dice1000"

# OUTPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\p12_dice5000"
# OUTLOG = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\p12.txt"

# # RUN P21
# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_p21_dice5000"
# INPUTFC2 = r"E:\Yichuan\WHS.gdb\whs_dump_160129_dice1000"

# OUTPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\p21_dice5000"
# OUTLOG = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\p21.txt"

# # RUN P22
# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_p22_dice5000"
# INPUTFC2 = r"E:\Yichuan\WHS.gdb\whs_dump_160129_dice1000"

# OUTPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\p22_dice5000"
# OUTLOG = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\p22.txt"


# a row and input Feature layer return an output row
def proto_intersect_mk2(in_row, q_log=q_log, inputFC = INPUTFC2):
    import arcpy
    # create layer
    inputFL = 'temp_layer'
    arcpy.MakeFeatureLayer_management(inputFC, inputFL)
    out_rows = []

    # select only those intersects. Last item geometry
    arcpy.SelectLayerByLocation_management(inputFL, "INTERSECT", in_row[-1])
    with arcpy.da.SearchCursor(inputFL, ['oid@','shape@']) as cursor:
        for row in cursor:
            try:
                clippedFeature = row[-1].intersect(in_row[-1], 4)

                # construct an object of the result
                out_row = (in_row[0], row[0], clippedFeature)
                out_rows.append(out_row)
            except Exception as e:
                msg = '[ERROR];Failed to intersect for OIDFC1, OIDFC2: {0}, {1}. '.format(in_row[0], row[0]) 
                q_log.put(msg)

    # delete layer
    arcpy.Delete_management(inputFL)

    return out_rows

def proto_write_mk2(out_rows, outputFC, q_log=q_log):
    import arcpy
    template = TEMPLATE

    if not arcpy.Exists(outputFC):
        arcpy.CreateFeatureclass_management(os.path.dirname(outputFC), 
            os.path.basename(outputFC),
            'POLYGON',
            template = template,
            spatial_reference=template)

    fields = ['oidfc1', 'oidfc2', 'shape@']
    try:
        insert_cur = arcpy.da.InsertCursor(outputFC, fields)
    except Exception as e:
        msg = '[ERROR];failed to create insertcursor'
        q_log.put(msg)

    # finer control
    for out_row in out_rows:
        try:
            insert_cur.insertRow(out_row)
        except Exception as e:
            msg = '[ERROR];Failed to write result for OIDFC1, OIDFC2: {0}, {1}. '.format(out_row[0], out_row[1]) 
            q_log.put(msg)

    del insert_cur


def worker_logger(q_log, outLog=OUTLOG):
    while q_log.qsize()>0:
        msg = q_log.get()

        print(time.strftime("%c") + ';' + msg)
        f = open(outLog, 'a')
        f.write(time.strftime("%c") + ';' + msg + '\n')
        f.close()


def main(inputFC, outputFC):
    # connect to pool of workers \ipcluster start -n 4\
    client = Client()
    dview = client[:]

    print('Total number of workers: {0}'.format(','.join(map(str, client.ids))))

    # log for errors

    # counter
    counter = 0

    # empty list to host row objects
    task_list = list()

    # solve memory issue...
    with arcpy.da.SearchCursor(inputFC, ['oid@', 'shape@']) as cur:
        while True:

            try:
                row = cur.next()
                task_list.append(row)

                # check size of list
                if len(task_list)%100 == 0:

                    # distribute across workers
                    ar = dview.map(proto_intersect_mk2, task_list)

                    # wait for this batch to finish
                    for rows in ar.get():
                        proto_write_mk2(rows, outputFC, q_log)

                    # reset
                    task_list = list()
                    print('reset list')
                    worker_logger(q_log)
                else:
                    pass



            except StopIteration:
                # flush the rest 
                ar = dview.map(proto_intersect_mk2, task_list)

                # wait for this batch to finish
                for rows in ar.get():
                    proto_write_mk2(rows, outputFC, q_log)

                worker_logger(q_log)

                break

            except Exception as e:
                msg = '[ERROR];failed to put input into queue. ' + str(e)
                q_log.put(msg)

                worker_logger(q_log)

    # # full read version
    # with arcpy.da.SearchCursor(INPUTFC, ['oid@', 'shape@']) as cur:
    #     for row in cur:
    #         q_in.put(row)


    # add stop signals to the queue
    for p in p_workers:
        q_in.put('STOP')

    # wait for workers to terminate
    for p in p_workers:
        p.join()

    # add stop signal for processing result
    q_out.put('STOP')
    q_log.put('STOP')

    # wait for the writer to finish
    p_w.join()
    p_log.join()



# ============= TEST ===============
def test():
    inputFC = r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_dice5000"
    outputFC = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\new_run"

    main(inputFC, outputFC)


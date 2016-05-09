# A test case to solve the perenniel issue of intersection

import os, sys, time
import arcpy
import multiprocessing

# import logging

# mpl = multiprocessing.log_to_stderr()
# mpl.setLevel(logging.INFO)

# the number of cores used - the following ensures there is one core remaining for other tasks

# WORKER = multiprocessing.cpu_count() - 2
WORKER = 4


# the output template with fields: OIDFC1, OIDFC2, shape@
TEMPLATE = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\template.shp"

# TEST CASE
# INPUTFC = r"E:\Yichuan\WHS.gdb\whs_dump_160129"
# INPUTFC2 =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_p11_dice5000"

# RUN ALL - failed
# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_dice5000"
INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_dice5000_single"
# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_dice5000_combined"
INPUTFC2 = r"E:\Yichuan\WHS.gdb\whs_dump_160129_dice1000_single"

OUTPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\test_run2"
OUTLOG = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\test_run2.txt"


# INPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4"
# INPUTFC2 = r"E:\Yichuan\WHS.gdb\whs_dump_160129"

# OUTPUTFC =  r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\all_5srun"
# OUTLOG = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\all_5s.txt"

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


# don't use this function. Creating a layer everytime a function is run is hugely expensive. instead nest it inside each worker process
def proto_intersect_mk2(in_row, q_log, inputFL):
    # create layer
    out_rows = []

    # select only those intersects. Last item geometry
    arcpy.SelectLayerByLocation_management(inputFL, "INTERSECT", in_row[-1])
    with arcpy.da.SearchCursor(inputFL, ['oid@','shape@']) as cursor:
        for row in cursor:
            try:
                clippedFeature = row[-1].intersect(in_row[-1], 4)

                # this is important - parallel seems to crash on writing null geometry
                # the exception is somehow not caught!
                if clippedFeature.area == 0:
                    msg = '[INFO];intersection resulting in 0 area, OIDFC1, OIDFC2: {0}, {1}.'.format(in_row[0], row[0])
                    q_log.put(msg)
                    continue

                # construct an object of the result
                out_row = (in_row[0], row[0], clippedFeature)
                out_rows.append(out_row)
            except Exception as e:
                msg = '[ERROR];Failed to intersect for OIDFC1, OIDFC2: {0}, {1}; {2} '.format(in_row[0], row[0], e) 
                q_log.put(msg)

    # delete layer
    arcpy.Delete_management(inputFL)

    return out_rows

def worker(q_input, q_output, q_log):
    inputFL = 'temp_layer'
    arcpy.MakeFeatureLayer_management(INPUTFC2, inputFL)

    # multiprocessing worker
    while True:
        # wait until get an input
        in_row = q_input.get()

        if in_row == 'STOP':
            break

        # run species richness
        try:
            # out_rows = proto_intersect(in_row)
            # out_rows = proto_intersect_mk2(in_row, q_log, inputFL)
           # print result
            out_rows = []

            # select only those intersects. Last item geometry
            arcpy.SelectLayerByLocation_management(inputFL, "INTERSECT", in_row[-1])
            with arcpy.da.SearchCursor(inputFL, ['oid@','shape@']) as cursor:
                for row in cursor:
                    try:
                        clippedFeature = row[-1].intersect(in_row[-1], 4)

                        # this is important - parallel seems to crash on writing null geometry
                        # the exception is somehow not caught!
                        if clippedFeature.area == 0:
                            msg = '[INFO];intersection resulting in 0 area, OIDFC1, OIDFC2: {0}, {1}.'.format(in_row[0], row[0])
                            q_log.put(msg)
                            continue

                        # construct an object of the result
                        out_row = (in_row[0], row[0], clippedFeature)
                        out_rows.append(out_row)
                    except Exception as e:
                        msg = '[ERROR];Failed to intersect for OIDFC1, OIDFC2: {0}, {1}; {2} '.format(in_row[0], row[0], e) 
                        q_log.put(msg)

        except Exception as e:
            msg = "[ERROR];Failed running analysis for OIDFC1: {0}; {1}".format(in_row[0], e)
            q_log.put(msg)

        # put only non empty result
        if out_rows:
            q_output.put(out_rows)

    # delete layer
    arcpy.Delete_management(inputFL)

def proto_write_mk2(out_rows, outputFC, q_log):
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


def worker_writer_mk2(q_output, q_log, outputFC=OUTPUTFC):
    total_done = 0
    while True:
        try:
            out_rows = q_output.get()

            # if len(out_rows) ==1 and out_rows == 'STOP':
            #     break

            # eval directly triggers an exception
            if out_rows == 'STOP':
                break

            # write to target
            proto_write_mk2(out_rows, outputFC, q_log)
            
        except Exception as e:
            msg = '[ERROR];failed to get rows from queue: {0}'.format(out_rows)
            q_log.put(msg)

            for oid1, oid2 in [(out_row[0], out_row[1]) for out_row in out_rows]:
                msg = '[ROWID];row OIDFC1, OIDFC2; {0}, {1}'.format(oid1, oid2)
                q_log.put(msg)

            pass
        
        total_done +=1
        # log
        if total_done%100 == 0:
            msg = '[INFO];Total groups of intersected features written: {0}'.format(total_done)
            q_log.put(msg)


def worker_logger(q_log, outLog=OUTLOG):
    while True:
        msg = q_log.get()

        # print result
        if msg == 'STOP':
            break

        print(time.strftime("%c") + ';' + msg)
        f = open(outLog, 'a')
        f.write(time.strftime("%c") + ';' + msg + '\n')
        f.close()


def main():
    print('Total number of workers: {0}'.format(WORKER))

    # pipeline for input
    q_in = multiprocessing.Queue()

    # pipeline for output
    q_out = multiprocessing.Queue()

    # log for errors
    q_log = multiprocessing.Queue()

    # setup and run worker processes
    p_workers = list()
    for i in range(WORKER):
        print('Starting worker process: {0}'.format(i))
        p = multiprocessing.Process(target=worker, args=(q_in, q_out, q_log))
        p_workers.append(p)
        
    # start worker process
    for p in p_workers:
        p.start()

    # run writer and logger processes
    p_w = multiprocessing.Process(target=worker_writer_mk2, args=(q_out, q_log))
    p_w.start()

    p_log = multiprocessing.Process(target=worker_logger, args=(q_log,))
    p_log.start()

    # debug
    counter = 0
    # solve memory issue...
    with arcpy.da.SearchCursor(INPUTFC, ['oid@', 'shape@']) as cur:
        while True:
            try:
                row = cur.next()

                # if counter < 37000:
                #     if counter%10000 == 0:
                #         print('skipped {}'.format(counter))
                #     continue

                if counter%1000 == 0:
                    q_log.put('[INFO];Current features processed {}'.format(counter))

                # debug
                counter += 1

                while row:

                    if q_in.qsize()<400:
                        q_in.put(row)
                        break
                    else:
                        pass

            except StopIteration:
                break

            # except Exception as e:
            #     msg = '[ERROR];failed to put input into queue. ' 
            #     q_log.put(msg)

            #     msg = '[ROWID];{0}'.format(row[0])
            #     q_log.put(msg)

            except RuntimeError as e:
                msg = '[ERROR];failed to put input into queue. ' 
                q_log.put(msg)

                msg = '[ROWID];{0}'.format(row[0])
                q_log.put(msg)


            except:
                msg = "Unexpected error:", sys.exc_info()[0]
                print(msg)
                raise


    # # full read version
    # with arcpy.da.SearchCursor(INPUTFC, ['oid@', 'shape@']) as cur:
    #     for row in cur:
    #         q_in.put(row)


    # add stop signals to the queue: poison pill
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



if __name__ == '__main__':
    main()


# ============= TEST ===============
def test():
    FC1 = r"E:\Yichuan\WHS.gdb\whs_dump_160129_2"
    FC2 = r"E:\Yichuan\MyGDB.gdb\wwf_terr_ecos"
    cur = arcpy.da.SearchCursor(FC1, ['oid@', 'shape@'])

    result = []
    for each in cur:
        out_rows = proto_intersect(each, FC2)   
        result.append(out_rows)

    return FC1, FC2, result

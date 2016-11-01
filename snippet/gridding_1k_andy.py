# changed to no actual intersection

import os, sys, time
import arcpy
import multiprocessing

WORKER = multiprocessing.cpu_count() - 2
# WORKER = 8



# run 1 ===============
INPUTFC = r"E:\Yichuan\Andy\wdpa_desig_inputs.gdb\merge_admin_fcs_unique"
ID = "countSm"

INPUTFC2 = r"E:\Yichuan\Andy\wdpa_desig_inputs.gdb\Fishnet_1km_clip_to_Land_Moll"
ID2 = "oid@"

OUTPUT_RESULT =  r"E:\Yichuan\Andy\result2.csv"
OUTLOG = r"E:\Yichuan\Andy\log2.log"


# run 2 ==============
INPUTFC = r"E:\Yichuan\Andy\wdpa_desig_inputs.gdb\GBR_preciseAdminIntUnionUniqFC"
ID = "countSm"

INPUTFC2 = r"E:\Yichuan\Andy\wdpa_desig_inputs.gdb\Fishnet_1km_clip_to_Land_Moll"
ID2 = "oid@"

OUTPUT_RESULT =  r"E:\Yichuan\Andy\result_gbr.csv"
OUTLOG = r"E:\Yichuan\Andy\log2.log"

# run 3 ============
INPUTFC = r"E:\Yichuan\Andy\wdpa_desig_inputs.gdb\USA_preciseAdminIntUnionUniqFC"
ID = "countSm"

INPUTFC2 = r"E:\Yichuan\Andy\wdpa_desig_inputs.gdb\Fishnet_1km_clip_to_Land_Moll"
ID2 = "oid@"

OUTPUT_RESULT =  r"E:\Yichuan\Andy\result_usa.csv"
OUTLOG = r"E:\Yichuan\Andy\log2.log"

# run 4 ============
INPUTFC = r"E:\Yichuan\Andy\used_this_instead.gdb\merge_uniq_join_0dp"
ID = "oid@"

INPUTFC2 = r"E:\Yichuan\Andy\grids.gdb\grid50km_intersecting_wdpa"
ID2 = "oid@"

OUTPUT_RESULT =  r"E:\Yichuan\Andy\result.csv"
OUTLOG = r"E:\Yichuan\Andy\log3.log"

# run 5 ============
INPUTFC = r"E:\Yichuan\Andy\used_this_instead.gdb\merge_uniq_join_0dp"
ID = "oid@"

INPUTFC2 = r"E:\Yichuan\Andy\wdpa_desig_inputs.gdb\Fishnet_1km_clip_to_Land_Moll"
ID2 = "oid@"

OUTPUT_RESULT =  r"E:\Yichuan\Andy\result_1k.csv"
OUTLOG = r"E:\Yichuan\Andy\log4.log"


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

            with arcpy.da.SearchCursor(inputFL, [ID2]) as cursor:
                for row in cursor:
                    out_row = (in_row[0], row[0])
                    out_rows.append(out_row)

            # put only non empty result
            if out_rows:
                q_output.put(out_rows)

        except Exception as e:
            msg = "[ERROR];Failed running analysis for IDS: {0}; {1}".format(in_row[0], e)
            q_log.put(msg)

    # delete layer once all done
    arcpy.Delete_management(inputFL)

def worker_writer_mk2(q_output, q_log):
    total_done = 0
    f = open(OUTPUT_RESULT, 'w')
    f.write('{0}, {1}\n'.format(ID, ID2))
    f.close()

    while True:
        try:
            out_rows = q_output.get()

            # if len(out_rows) ==1 and out_rows == 'STOP':
            #     break

            # eval directly triggers an exception
            if out_rows == 'STOP':
                break

            # write to target
            f = open(OUTPUT_RESULT, 'a')
            for out_row in out_rows:
                f.write('{0}, {1}\n'.format(out_row[0], out_row[1]))
            f.close()
            
        except Exception as e:
            msg = '[ERROR];failed to get rows from queue: {0}'.format(out_rows)
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
    with arcpy.da.SearchCursor(INPUTFC, [ID, 'shape@']) as cur:
        while True:
            try:
                row = cur.next()

                # if counter < 37000:
                #     if counter%10000 == 0:
                #         print('skipped {}'.format(counter))
                #     continue

                if counter%100 == 0:
                    q_log.put('[INFO];Current features processed {}'.format(counter))

                # debug
                counter += 1

                while row:

                    if q_in.qsize()<40:
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



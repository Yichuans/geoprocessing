# A test case to solve the perenniel issue of intersection

import os, sys, time
import arcpy
import multiprocessing
import logging

# logging.basicConfig(filename='result.log', filemode='w')

# number of workers
WORKERS = 8

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
def proto_intersect_mk2(in_row, inputFC = INPUTFC2):
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

                # this is important - parallel seems to crash on writing null geometry
                # the exception is not caught!
                if clippedFeature.area == 0:
                    msg = '[INFO]; intersection zero 0 skipped'
                    print(msg)
                    continue

                # construct an object of the result
                out_row = (in_row[0], row[0], clippedFeature)
                out_rows.append(out_row)

            except RuntimeError as e:
                msg ='[ERROR];Failed to intersect for OIDFC1, OIDFC2: {0}, {1}. '.format(in_row[0], row[0]) 
                print(msg)

            except:
                msg ='unspecificed error' 
                print(msg)

    # delete layer
    arcpy.Delete_management(inputFL)

    return out_rows

def proto_write_mk2(out_rows, outputFC):
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
        print(msg)

    # finer control
    for out_row in out_rows:
        try:
            insert_cur.insertRow(out_row)

        except Exception as e:
            msg = '[ERROR];Failed to write result for OIDFC1, OIDFC2: {0}, {1}. '.format(out_row[0], out_row[1]) 
            print(msg)

    del insert_cur



def main(inputFC, outputFC):
    multiprocessing.log_to_stderr()
    logger = multiprocessing.get_logger()
    # logger.setLevel(logging.INFO)
    logger.setLevel(multiprocessing.SUBDEBUG)
    pool = multiprocessing.Pool(WORKERS)


    print('Total number of workers: {0}'.format(WORKERS))

    # log for errors

    # empty list to host row objects
    task_list = list()
    counter = 0

    # # solve memory issue...
    # with arcpy.da.SearchCursor(inputFC, ['oid@', 'shape@']) as cur:
    #     while True:
    #         try:
    #             row = cur.next()

    #             counter += 1


    #             # if there is a row add to 
    #             task_list.append(row)



    #             # check size of list
    #             if len(task_list)%100 == 0:

    #                 print('start pool')

    #                 # distribute across workers

    #                 result = pool.map(proto_intersect_mk2, task_list)

    #                 print('intersection complete {}'.format(counter))

    #                 # wait for this batch to finish
    #                 for rows in result:
    #                     proto_write_mk2(rows, outputFC)

    #                 print('writing complete {}'.format(counter))

    #                 # reset
    #                 task_list = list()
    #                 # print('reset list {}'.format(counter))

    #             else:
    #                 pass

    #         except StopIteration:

    #             result = pool.map(proto_intersect_mk2, task_list)

    #             # wait for this batch to finish
    #             for rows in result:
    #                 proto_write_mk2(rows, outputFC)

    #             break

    #         except RuntimeError as e:
    #             msg = '[ERROR];failed to put row {} input into queue. '.format(row[0])
    #             print(msg)


    #         except:
    #             msg = "Unexpected error:", sys.exc_info()[0]
    #             print(msg)
    #             raise

#================TEST==============DEBUG
    # singleton
    # with arcpy.da.SearchCursor(inputFC, ['oid@', 'shape@']) as cur:
    #     while True:
    #         try:
    #             row = cur.next()

    #             counter += 1

    #             if counter < 38200:
    #                 continue


    #             # if there is a row add to 
    #             task_list.append(row)
    #             rows = proto_intersect_mk2(row)
    #             print('intersection complete {}'.format(counter))

    #             proto_write_mk2(rows, outputFC)
    #             print('writing complete {}'.format(counter))
    #         except Exception as e:
    #             print(e)

# ===============TEST=================DEBUG MULTI
    with arcpy.da.SearchCursor(inputFC, ['oid@', 'shape@']) as cur:
        while True:
            try:
                row = cur.next()

                counter += 1

                # skipping to where things had gone wrong
                if counter < 38200:
                    continue

                # if there is a row add to 
                task_list.append(row)



                # check size of list
                if len(task_list)%100 == 0:

                    print('start pool')

                    # distribute across workers

                    result = pool.map(proto_intersect_mk2, task_list)

                    print('intersection complete {}'.format(counter))

                    # wait for this batch to finish
                    for rows in result:
                        proto_write_mk2(rows, outputFC)

                    print('writing complete {}'.format(counter))

                    # reset
                    task_list = list()
                    # print('reset list {}'.format(counter))

                else:
                    pass

            except StopIteration:

                result = pool.map(proto_intersect_mk2, task_list)

                # wait for this batch to finish
                for rows in result:
                    proto_write_mk2(rows, outputFC)

                break

            except RuntimeError as e:
                msg = '[ERROR];failed to put row {} input into queue. '.format(row[0])
                print(msg)


            except:
                msg = "Unexpected error:", sys.exc_info()[0]
                print(msg)
                raise
# ============= TEST ===============
def test():
    inputFC = r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_dice5000"
    outputFC = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\new_run"

    main(inputFC, outputFC)


if __name__ == '__main__':
    inputFC = r"E:\Yichuan\Climate_vulnerability_wh\iucn_rl_2015_4.gdb\iucn_rl_2015_4_dice5000"
    outputFC = r"E:\Yichuan\Climate_vulnerability_wh\multiprocessing_workspace\intersect.gdb\new_run"

    main(inputFC, outputFC)
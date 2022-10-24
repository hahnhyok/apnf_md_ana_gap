# -*- coding: utf-8 -*-

import csv
import numpy as np
import pandas as pd


def dg_ins_odlist(conn_dg, dbc_dg, ins_dt, tbnm_odlist):
    rval = 0
    # upcols = ["route_id", "seq", "obid", "dbid", "obtype", "dbtype", "gthdt", "qty", "o_tsec", "o_week", "weektp", "ox", "oy", "ox_5178", "oy_5178", "dx",
    #            "dy", "dx_5178", "dy_5178"]
    # upcols = ["obid", "dbid", "o_tsec", "weektp", "ox_5178", "oy_5178", "dx_5178", "dy_5178"]
    # query_insert = "insert into %s (%s) values " % (tbnm_odlist, ",".join(x for x in upcols))
    query_insert = "upsert into %s values " % tbnm_odlist

    nopart, line, inslist = 1000, 0, []
    for odkey in ins_dt:
        tmpwstr = "(\'%s\',\'%s\',%d,%d,%s)" % (odkey[2], odkey[3], odkey[0], odkey[1], ",".join(str(x) for x in ins_dt[odkey]))
        inslist.append(tmpwstr)
        line += 1
        if line % nopart == 0:
            query_insert_do = "%s%s;" % (query_insert, ",".join(x for x in inslist))
            try:
                dbc_dg.execute(query_insert_do)
                conn_dg.commit()
            except Exception as e:
                rval = 1
                print(str(e))
            inslist.clear()
    if len(inslist) > 0:
        query_insert_do = "%s%s;" % (query_insert, ",".join(x for x in inslist))
        try:
            dbc_dg.execute(query_insert_do)
            conn_dg.commit()
        except Exception as e:
            rval = 1
            print(str(e))
        inslist.clear()

    return rval


def db_ins_binfo(conn_dg, dbc_dg, folderp, fnm_binfo, tbnm_binfo):
    rval = 0

    print("insert build info with nearnids...")
    query_insert = "upsert into %s values " % (tbnm_binfo)

    fp = "%s\\%s" % (folderp, fnm_binfo)
    fr = open(fp, "r")
    csvreader = csv.reader(fr, skipinitialspace=True, doublequote=False)
    ins_dt = []
    line, nopart = 0, 1000
    for tmpcl in csvreader:
        # if line == 1:
        #     line += 1
        #     continue
        # tmpwlist = tmpcl[0:4] + ["-".join(str(x) for x in tmpcl[4:])]
        tmpwstr = "(\'%s\',%s,\'%s\')" % (tmpcl[0], ",".join(x for x in tmpcl[1:4]), "-".join(str(x) for x in tmpcl[4:]))
        ins_dt.append(tmpwstr)
        line += 1
        if line % nopart == 0:
            print(line)
            if len(ins_dt) >= nopart:
                query_insert_do = "%s%s;" % (query_insert, ",".join(x for x in ins_dt))
                try:
                    dbc_dg.execute(query_insert_do)
                    conn_dg.commit()
                except Exception as e:
                    rval = 1
                    print(str(e))
                ins_dt.clear()
    fr.close()
    print(line)
    if len(ins_dt) > 0:
        query_insert_do = "%s%s;" % (query_insert, ",".join(x for x in ins_dt))
        try:
            dbc_dg.execute(query_insert_do)
            conn_dg.commit()
        except Exception as e:
            rval = 1
            print(str(e))
        ins_dt.clear()

    return rval


def db_ins_brxyinfo(conn_dg, dbc_dg, folderp, fnm_brinfo, tbnm_brinfo):
    rval = 0

    print("insert brand xy infos with csv...")
    query_insert = "upsert into %s values " % (tbnm_brinfo)

    fp = "%s\\%s" % (folderp, fnm_brinfo)
    fr = open(fp, "r")
    csvreader = csv.reader(fr, skipinitialspace=True, doublequote=False)
    ins_dt = []
    line, nopart = 0, 1000
    next(csvreader)     # skip title
    for tmpcl in csvreader:
        # if line == 1:
        #     line += 1
        #     continue
        # tmpwlist = tmpcl[0:4] + ["-".join(str(x) for x in tmpcl[4:])]
        tmpwstr = "(\'%s\',\'%s\',\'%s\',%s)" % (tmpcl[0], tmpcl[1], tmpcl[2], ",".join(str(x) for x in tmpcl[3:]))
        ins_dt.append(tmpwstr)
        line += 1
        if line % nopart == 0:
            print(line)
            if len(ins_dt) >= nopart:
                query_insert_do = "%s%s;" % (query_insert, ",".join(x for x in ins_dt))
                try:
                    dbc_dg.execute(query_insert_do)
                    conn_dg.commit()
                except Exception as e:
                    rval = 1
                    print(str(e))
                ins_dt.clear()
    fr.close()
    print(line)
    if len(ins_dt) > 0:
        query_insert_do = "%s%s;" % (query_insert, ",".join(x for x in ins_dt))
        try:
            dbc_dg.execute(query_insert_do)
            conn_dg.commit()
        except Exception as e:
            rval = 1
            print(str(e))
        ins_dt.clear()

    return rval


def db_ins_res_gap(conn_dg, dbc_dg, tbnm_res, dt_res):
    rval = 0
    print("insert result of searched routes...")
    query_insert = "upsert into %s values " % tbnm_res
    dt_np = np.array(dt_res)
    nopart, line, inslist = 1000, 0, []
    # "route_id", "tot_qty", "tottime", "tot_r_time", "tot_servtime", "tot_pt"
    for tmprow in dt_np:
        gthdt, empnum, gid = tmprow[0].split("_")
        tmpwstr = "(\'%s\',\'%s\',%s)" % (gthdt, empnum, ",".join(str(x) for x in list(tmprow[1:])))
        inslist.append(tmpwstr)
        line += 1
        if line % nopart == 0:
            print(line)
            query_insert_do = "%s%s;" % (query_insert, ",".join(x for x in inslist))
            try:
                dbc_dg.execute(query_insert_do)
                conn_dg.commit()
            except Exception as e:
                rval = 1
                print(str(e))
            inslist.clear()
    print(line)
    if len(inslist) > 0:
        query_insert_do = "%s%s;" % (query_insert, ",".join(x for x in inslist))
        try:
            dbc_dg.execute(query_insert_do)
            conn_dg.commit()
        except Exception as e:
            rval = 1
            print(str(e))
        inslist.clear()

    return rval

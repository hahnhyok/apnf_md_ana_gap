### 업데이트 시점: 당일 오후 2시?
### 데이터 가져오는 기준: 집화일자(gthdt) = 오늘 날짜 - 2일
### 착지 집계 -> OD 리스트 추출 -> 이동시간 추출(c++, 1to1) -> 기사별 예측시간 계산 -> 테이블 insert

# -*- coding: utf-8 -*-

import os
import schedule
import time
import datetime
from dateutil.relativedelta import relativedelta
import sys
import subprocess
import shutil
import funcs_com as fc
import funcs_insert as fi
import funcs_makebin as fm
from pyproj import Transformer
import pandas as pd


ifcasetest = True   # 속도 향상 테스트
tbnm_odlist_ct = "a1_apolo_nf.mdout_apolo_nf_odlist_btob_u10"
tbnm_res_ct = "a1_apolo_nf.mdout_apolo_nf_ana_gap_u10"


def job(mdir):
    maxlp = 3
    sleepsec = float(10)
    # seldates = ["20220923"]  # ["20220919", "20220920", "20220921", "20220922", "20220923"]
    fw_log = open("log_all.txt", "w")
    # sdatestr = "20220901"
    # edatestr = "20221010"

    # sdate = datetime.datetime.strptime("20221016", "%Y%m%d")
    curyear = datetime.datetime.now().year
    curmon = datetime.datetime.now().month  # - relativedelta(months=3)   # - datetime.timedelta(days=90)
    mgap = 3    # 3개월 전부터
    curmon_str = str(curmon - mgap)
    if curmon < 10:
        curmon_str = "0%s" % curmon_str
    sdate = datetime.datetime.strptime("%s%s01" % (str(curyear), curmon_str), "%Y%m%d")
    edate = datetime.datetime.now() #  - datetime.timedelta(days=1)
    if ifcasetest:
        print("Case Test version start !")
        sdate = datetime.datetime.strptime("20221114", "%Y%m%d")
        edate = datetime.datetime.strptime("20221128", "%Y%m%d")
    edatestr = edate.strftime("%Y%m%d")
    print("job start... %s-%s" % (sdate.strftime("%Y%m%d"), edatestr))

    curlp = 0
    print("get exist odlists...")
    tbnm_odlist = "a1_apolo_nf.mdout_apolo_nf_odlist_btob"
    if ifcasetest:
        tbnm_odlist = tbnm_odlist_ct
    dt_existod = {}
    try:
        dt_existod = get_odlist_res(tbnm_odlist)
    except Exception as e:
        print("get exist odlists failed...\n(Errmsg: %s)" % str(e))
        sys.exit()
    while True:
        if sdate >= edate:
            break
        tmpstr = sdate.strftime("%Y%m%d")
        weekval = sdate.weekday()
        print("Generate gap data with gthdt=%s" % tmpstr)
        try:
            chkerr, ifsearched = process(mdir, tmpstr, fw_log, dt_existod)
            if chkerr == -1 and weekval < 5:
                curlp += 1
                print("update failed...(date: %s)\nretry after %f seconds..." % (tmpstr, sleepsec))
                time.sleep(sleepsec)
            else:
                sdate += datetime.timedelta(days=1)
            if ifsearched:
                try:
                    print("get exist odlists(in loop)...")
                    dt_existod.clear()
                    dt_existod = get_odlist_res(tbnm_odlist)
                except Exception as e:
                    print("get exist odlists failed(in loop)...\n(Errmsg: %s)" % str(e))
                    sys.exit()
        except Exception as e:
            curlp += 1
            print("update failed...(date: %s)\n(Errmsg: %s)" % (tmpstr, str(e)))
        if curlp >= maxlp:
            sdate += datetime.timedelta(days=1)
        fw_log.flush()
    fw_log.close()
    print("job finished...")
    # process("20220922")


def process(mdir, seldt, fw_log, dt_existod):
    tf1 = Transformer.from_crs("epsg:4326", "epsg:5178")
    # ntm = datetime.datetime.now()
    # basedt = (ntm - datetime.timedelta(days=8)).strftime("%Y%m%d")  ## 실제 운영 시 days=1로 설정
    basedt = seldt
    # weektp = datetime.datetime.strptime(basedt, "%Y%m%d").weekday()
    # baseweektp = 0
    # if weektp > 4:
    #     baseweektp = 1

    conn_dg, dbc_dg = fc.getImpalaConn('Prod')

    tbnm_base = "a1_apolo_nf.mrt_apolo_nf_base"
    tbnm_odlist = "a1_apolo_nf.mdout_apolo_nf_odlist_btob"
    tbnm_binfo = "a1_apolo_nf.mdout_apolo_nf_buildinfo"
    tbnm_res = "a1_apolo_nf.mdout_apolo_nf_ana_gap"
    if ifcasetest:
        tbnm_odlist = tbnm_odlist_ct
        tbnm_res = tbnm_res_ct
    tbnm_brinfo = "a1_apolo_nf.brinfo_apolo_nf"
    tbnm_s_bid = "a2_tes.mrt_gis_g_new_lmd_total_time_result"   # "nwdt.est_b_servtime"
    tbnm_s_btp = "a2_tes.mrt_gis_g_new_lmd_total_time_result_other" # "nwdt.est_btp_servtime"
    folder_exe = mdir   # os.getcwd()
    folder_bin = "%s\\bin" % folder_exe
    folder_binfo = "%s\\out" % folder_exe
    folder_res = "%s\\res" % folder_exe
    folder_reskeep = "%s\\res_keep" % folder_exe
    folder_log = "%s\\log" % folder_exe

    fnm_binfo = "zone_binfo_mul_add.csv"
    fnm_res = "res_od_attri_all.csv"
    fnm_log = "log.txt"
    ## 거점 좌표 입력
    # folder_brinfo = "E:\\00_과제_\\APOLO"
    # fnm_brinfo = "sub_좌표정보.csv"
    # fi.db_ins_brxyinfo(conn_dg, dbc_dg, folder_brinfo, fnm_brinfo, tbnm_brinfo)
    # sys.exit()

    maxqty_serv = 20
    ifsearched = False

    msgstr = "Initializing..."
    print(msgstr)
    fw_log.write("%s\n" % msgstr)

    ## 서브 좌표 정보 가져오기
    selcols = ["opruprbrancd", "x", "y"]
    query_sel = "select %s from %s" % (",".join(x for x in selcols), tbnm_brinfo)
    dbc_dg.execute(query_sel)
    dt_n = dbc_dg.fetchall()
    subxy = {}
    for tmprow in dt_n:
        subxy[tmprow[0]] = tmprow[1:]

    selcols = ["gthdt", "wrkdt", "wrkhr", "rcvzpid", "bldgtype", "rcvx", "rcvy", "dlvopruprbrancd", "rvptempnum", "trspbillseq"]
    colstr = ",".join(x for x in selcols)
    query_sel = "select %s from %s where gthdt=\'%s\' and skustscd=\'91\' and trspbillseq is not null order by %s, %s;" % (colstr, tbnm_base, basedt, selcols[-2], selcols[-1])
    # query_sel = "select %s,rcvrnm from %s where gthdt=\'%s\' and skustscd=\'91\' and (recheckyn=\'N\' or (recheckyn=\'Y\' and checktype=\'5\')) " \
    #             "order by rvptempnum, wrkdt, wrkhr" % (colstr, tbnm_base, basedt)
    dbc_dg.execute(query_sel)
    dt_base = dbc_dg.fetchall()
    if len(dt_base) == 0:
        msgstr = "Cannot select any data from nf base mart..."
        print(msgstr)
        fw_log.write("%s\n" % msgstr)
        return -1, ifsearched
    defval_info = ["", "", "", -1, 3, -1, -1, -1, -1, -1]  # 건물번호, 건물유형, ROUTE_ID, SEQ, 시간대, 요일, X, Y, x_5178, y_5178
    pre_bldinfo = defval_info
    stime_def = "%s 03:00:00" % basedt
    line, nopart, qty = 0, 4000, 1
    ins_dt, keep_dt, selodkeys, gpid, bidset, btpset, pre_dt = {}, [], set(), {}, set(), set(), []
    keep_dt_test = []
    for tmprow in dt_base:
        opruprcd = tmprow[selcols.index("dlvopruprbrancd")].strip()
        seq = tmprow[selcols.index("trspbillseq")]
        id_bld = tmprow[selcols.index("rcvzpid")].strip()
        tp_bld = tmprow[selcols.index("bldgtype")].strip()
        bidset.add(id_bld)
        btpset.add(tp_bld)
        gthdt = tmprow[selcols.index("gthdt")].strip()
        stime = stime_def
        if tmprow[selcols.index("wrkdt")] is not None and tmprow[selcols.index("wrkhr")] is not None:
            stime = "%s %s" % (tmprow[selcols.index("wrkdt")].strip(), tmprow[selcols.index("wrkhr")].strip())
        stimedt = datetime.datetime.strptime(stime, "%Y%m%d %H%M%S")
        stimeh = stimedt.hour
        stimew = stimedt.weekday()

        if tmprow[selcols.index("rvptempnum")].strip() == '655316':
            kk = 0
        ridkey = "%s_%s" % (gthdt, tmprow[selcols.index("rvptempnum")].strip())
        if ridkey not in gpid:
            gpid[ridkey] = 1
        elif seq == 1 and seq != pre_bldinfo[3]:
            gpid[ridkey] += 1
        rid = "%s_%d" % (ridkey, gpid[ridkey])
        if len(id_bld) == 0 or tmprow[selcols.index("rcvx")] is None or tmprow[selcols.index("rcvy")] is None:
            # cur_bldinfo = defval_info
            # pre_bldinfo = cur_bldinfo
            line += 1
            if line % nopart == 0:
                print(line)
            continue
        x, y = float(tmprow[selcols.index("rcvx")]), float(tmprow[selcols.index("rcvy")])
        # x_5178, y_5178 = transform(p1, p2, x, y)
        y_5178, x_5178 = tf1.transform(y, x)
        cur_bldinfo = [id_bld, tp_bld, rid, seq, stimeh, stimew, x, y, x_5178, y_5178]

        weektp = 0
        if pre_bldinfo[5] > 4:
            weektp = 1
        ostr = opruprcd
        for i in range(0, 25 - len(opruprcd)):
            ostr = "%sz" % ostr
        if seq > 1:
            ostr = pre_bldinfo[0]
        else:
            y_5178, x_5178 = tf1.transform(subxy[opruprcd][1], subxy[opruprcd][0])
            pre_bldinfo[-2:] = x_5178, y_5178
        odkey_l = [pre_bldinfo[4], weektp, ostr, cur_bldinfo[0]]
        odkey_l_str = "_".join(str(x) for x in odkey_l)
        # odkey_k = tuple([rid, tp_bld, cur_bldinfo[0]])  # tuple([rid, tp_bld, seq, cur_bldinfo[0]])
        if rid == pre_bldinfo[2] and id_bld == pre_bldinfo[0]:
            qty += 1
            pre_dt[3:5] = seq, qty
        else: #if rid != pre_bldinfo[2]:
            qty = 1
            if len(pre_bldinfo[0]) > 0:
                keep_dt.append(pre_dt)
            pre_dt = [rid, tp_bld, cur_bldinfo[0], seq, qty, odkey_l_str]
        # keep_dt_test.append([rid, tp_bld, cur_bldinfo[0], seq, qty, odkey_l_str])

        # if odkey_k not in keep_dt:
        #     keep_dt[odkey_k] = [seq, qty, odkey_l_str]
        # else:
        #     keep_dt[odkey_k][0] = seq
        #     keep_dt[odkey_k][1] = qty
        #     # keep_dt[odkey_k][1] += [odkey_l_str]

        # if seq == 1 or (seq > 1 and pre_bldinfo[-1] < 0) or (seq > 1 and pre_bldinfo[0] == cur_bldinfo[0]):
        if (seq > 1 and pre_bldinfo[-1] < 0) or (seq > 1 and pre_bldinfo[0] == cur_bldinfo[0]):
            pre_bldinfo = cur_bldinfo
            line += 1
            if line % nopart == 0:
                print(line)
            continue
        odkey = tuple(odkey_l)
        selodkeys.add(odkey)
        if odkey in dt_existod:
            pre_bldinfo = cur_bldinfo
            line += 1
            if line % nopart == 0:
                print(line)
            continue
        # tmpwlist = [qty, pre_bldinfo[4], pre_bldinfo[5], weektp] + pre_bldinfo[-4:] + cur_bldinfo[-4:]
        # tmpwstr = "(\'%s\',%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%s)" % (rid, seq, pre_bldinfo[0], cur_bldinfo[0], pre_bldinfo[1], cur_bldinfo[1], gthdt, ",".join(str(x) for x in tmpwlist))

        # find_dt.append([pre_bldinfo[4], weektp, pre_bldinfo[0], cur_bldinfo[0]] + pre_bldinfo[-2:] + cur_bldinfo[-2:])
        # tmpwlist = [pre_bldinfo[4], weektp] + pre_bldinfo[-2:] + cur_bldinfo[-2:]
        # tmpwstr = "(\'%s\',\'%s\',%s)" % (pre_bldinfo[0], cur_bldinfo[0], ",".join(str(x) for x in tmpwlist))
        tmpwlist = pre_bldinfo[-2:] + cur_bldinfo[-2:]
        ins_dt[odkey] = tmpwlist
        # tmpwlist = [pre_bldinfo[0], cur_bldinfo[0], pre_bldinfo[4], weektp] + pre_bldinfo[-2:] + cur_bldinfo[-2:]
        # ins_dt.append(tmpwlist)
        # ins_dt.append(tuple(tmpwlist))
        pre_bldinfo = cur_bldinfo
        line += 1
        if line % nopart == 0:
            print(line)
    if len(pre_bldinfo[0]) > 0:
        keep_dt.append(pre_dt)
    print(line)
    msgstr = "Initializing done...line: %d || ins_dt length is %d" % (line, len(ins_dt))
    print(msgstr)
    fw_log.write("%s\n" % msgstr)
    dt_base.clear()
    if len(ins_dt) > 0:
        ifsearched = True
        msgstr = "Searching shortest paths..."
        print(msgstr)
        fw_log.write("%s\n" % msgstr)
        ## bin 파일 생성
        find_dt = [list(x) + ins_dt[x] for x in ins_dt]
        fm.make_input_bin_od(dbc_dg, folder_bin, find_dt, tbnm_binfo)
        conn_dg.close()
        ## 경로 탐색(c++) 호출
        # resval = os.popen("%s\\apolon_spa_od.exe" % folder_exe).read()
        p = subprocess.Popen("%s\\apolon_spa_od_rqdir.exe %s" % (folder_exe, folder_exe), shell=True, stderr=subprocess.PIPE)
        # (output, err) = p.communicate()
        resval = p.wait()

        conn_dg, dbc_dg = fc.getImpalaConn('Prod')
        # resval = 0
        if resval == 0:
            msgstr = "Saving results..."
            print(msgstr)
            fw_log.write("%s\n" % msgstr)
            ## 결과 파일 복사 및 이동
            try:
                if not os.path.exists(folder_reskeep):
                    os.makedirs(folder_reskeep)
                fnm_part = "%s_%s_" % (fnm_binfo[0:-4], basedt)
                newfnm = get_newfnm(folder_reskeep, fnm_part, "csv")
                shutil.copy("%s\\%s" % (folder_binfo, fnm_binfo), "%s\\%s" % (folder_reskeep, newfnm))
                fnm_part_res = "%s_%s_" % (fnm_res[0:-4], basedt)
                newfnm = get_newfnm(folder_reskeep, fnm_part_res, "csv")
                shutil.copy("%s\\%s" % (folder_res, fnm_res), "%s\\%s" % (folder_reskeep, newfnm))
                fnm_part_log = "%s_%s_" % (fnm_log[0:-4], basedt)
                newfnm = get_newfnm(folder_reskeep, fnm_part_log, "txt")
                shutil.copy("%s\\%s" % (folder_log, fnm_log), "%s\\%s" % (folder_reskeep, newfnm))
            except Exception as e:
                msgstr = "res files copy error !\n%s" % str(e)
                print(msgstr)
                fw_log.write("%s\n" % msgstr)

            ## 경로 탐색(c++) 결과 저장
            fp = "%s\\%s" % (folder_res, fnm_res)
            fr = open(fp, "r")
            while True:
                tmpcl = fr.readline()
                if not tmpcl:
                    break
                tmpline = tmpcl.strip().split(",")
                tsec = int(tmpline[0])
                weektp = 0
                if tsec > 23:
                    tsec -= 24
                    weektp = 1
                odkey = tuple([tsec, weektp] + tmpline[1:3])
                ins_dt[odkey] += [float(x) for x in tmpline[-2:]]
            fr.close()
            chkrval = fi.dg_ins_odlist(conn_dg, dbc_dg, ins_dt, tbnm_odlist)
            if chkrval > 0:
                msgstr = "Failed to insert od attributes..."
                print(msgstr)
                fw_log.write("%s\n" % msgstr)
            chkrval = fi.db_ins_binfo(conn_dg, dbc_dg, folder_binfo, fnm_binfo, tbnm_binfo)
            if chkrval > 0:
                msgstr = "Failed to insert buildinfos..."
                print(msgstr)
                fw_log.write("%s\n" % msgstr)
        else:
            msgstr = "search min path error with ErrCode %d" % resval
            print(msgstr)
            fw_log.write("%s\n" % msgstr)

    msgstr = "Select and calculate gaps..."
    print(msgstr)
    fw_log.write("%s\n" % msgstr)
    strlist = []
    for odkey in selodkeys:
        strlist.append("\'%s\'" % "_".join(s for s in [str(x) for x in odkey]))
    dt_n.clear()
    maxsize, nostr, sindex, flag = min(9000, len(strlist)), len(strlist), 0, True
    eindex = maxsize
    while sindex < nostr and eindex <= nostr:
        dt_n.append(",".join(x for x in strlist[sindex:eindex]))
        sindex = eindex
        eindex += maxsize
        if eindex > nostr:
            eindex = nostr
    wherestr = "a.odkey in (%s)" % dt_n[0]
    for i in range(1, len(dt_n)):
        wherestr = "%s or a.odkey in (%s)" % (wherestr, dt_n[i])
    query_sel = "select a.odkey, a.r_time from (select concat(cast(o_tsec as string),\'_\',cast(weektp as string),\'_\',obid,\'_\',dbid) odkey, r_time from %s) a " \
                "where %s" % (tbnm_odlist, wherestr)
    dbc_dg.execute(query_sel)
    pd_selod = pd.DataFrame(dbc_dg.fetchall(), columns=["odkey", "r_time"])
    # keep_dt_list = [list(x) + keep_dt[x] for x in keep_dt]
    pd_all = pd.DataFrame(keep_dt, columns=["route_id", "bldgtype", "rcvzpid", "trspbillseq", "qty", "odkey"])
    dt_s_bid, dt_s_btp = fc.get_servtime(conn_dg, dbc_dg, bidset, btpset, tbnm_s_bid, tbnm_s_btp)
    s_time_bid, s_time_btp = {}, {}
    for tmprow in dt_s_bid:
        s_time_bid[tmprow[0]] = tmprow[1:]
    for tmprow in dt_s_btp:
        s_time_btp[tmprow[0]] = tmprow[1:]
    # pd_all = pd_all.join(pd_selod, on="odkey", validate="m:1").reset_index()
    pd_all = pd.merge(pd_all, pd_selod, how="left", on='odkey')
    # pd_all = pd.concat([pd_all, pd_selod], keys=['odkey'], axis=1).reset_index()
    pd_all.fillna(0, inplace=True)
    pd_all["servtime"] = float(0)
    pd_all["tottime"] = float(0)
    # for index, row in pd_all.iterrows():
    for row in pd_all.itertuples():
        index, zpid, btp, rtime = getattr(row, "Index"), getattr(row, "rcvzpid"), getattr(row, "bldgtype"), getattr(row, "r_time")
        qtyindex = min(getattr(row, "qty") - 1, maxqty_serv - 1)
        servt = float(0)
        if zpid in s_time_bid:
            servt = s_time_bid[zpid][qtyindex]
            if servt is None:
                servt2 = s_time_btp[btp][qtyindex]
                if servt2 is None:
                    servt = s_time_btp["전체평균"][qtyindex]
                else:
                    servt = servt2
        elif btp in s_time_btp:
            servt = s_time_btp[btp][qtyindex]
            if servt is None:
                servt = s_time_btp["전체평균"][qtyindex]
        else:
            servt = s_time_btp["전체평균"][qtyindex]
        servt_min = servt / float(60)
        pd_all.loc[index, "servtime"] = servt_min
        pd_all.loc[index, "tottime"] = servt_min + rtime
    pd_res = pd_all.groupby("route_id").agg({"qty": "sum", "tottime": "sum", "r_time": "sum", "servtime": "sum", "rcvzpid": "size"}).reset_index()
    # pd_res.columns = ["route_id", "tot_qty", "tottime", "tot_r_time", "tot_servtime", "tot_pt"]
    chkrval = fi.db_ins_res_gap(conn_dg, dbc_dg, tbnm_res, pd_res)
    if chkrval > 0:
        msgstr = "Failed to insert results..."
        print(msgstr)
        fw_log.write("%s\n" % msgstr)
    conn_dg.close()

    msgstr = "All Done...%s" % basedt
    print(msgstr)
    fw_log.write("%s\n" % msgstr)
    return 0, ifsearched


def get_newfnm(folder, fnm, ext):
    i = 0
    newfnm = ""
    while True:
        newfnm = "%s%d.%s" % (fnm, i, ext)
        fp = "%s\\%s" % (folder, newfnm)
        if not os.path.exists(fp):
            break
        i += 1
    return newfnm


def get_odlist_res(tbnm_odlist):
    conn_dg, dbc_dg = fc.getImpalaConn('Prod')
    ## 속성이 존재하는 OD 가져오기
    selcols = ["o_tsec", "weektp", "obid", "dbid", "r_time"]
    query_sel = "select %s from %s where r_time is not null" % (",".join(x for x in selcols), tbnm_odlist)
    dbc_dg.execute(query_sel)
    dt_n = dbc_dg.fetchall()
    dt_existod = {}
    for tmprow in dt_n:
        odkey = tuple(tmprow[0:4])
        dt_existod[odkey] = tmprow[-1]
    conn_dg.close()
    return dt_existod


if __name__ == "__main__":
    # maindir = sys.argv[1]
    # print(maindir)
    maindir = os.getcwd()
    job(maindir)
    sys.exit()
    # schedule.every(3).seconds.do(job) # 3초마다 job 실행
    # schedule.every(3).minutes.do(job) # 3분마다 job 실행
    # schedule.every(3).hours.do(job) # 3시간마다 job 실행
    # schedule.every(3).days.do(job)  # 3일마다 job 실행
    # schedule.every(3).weeks.do(job) # 3주마다 job 실행
    # schedule.every().minute.at(":23").do(job) # 매분 23초에 job 실행
    # schedule.every().hour.at(":42").do(job) # 매시간 42분에 작업 실행
    # 5시간 20분 30초마다 작업 실행
    # schedule.every(5).hours.at("20:30").do(job)
    # 매일 특정 HH:MM 및 다음 HH:MM:SS에 작업 실행
    schedule.every().day.at("09:30").do(job, maindir)
    schedule.every().day.at("16:30").do(job, maindir)
    # schedule.every().day.at("10:30:42").do(job)
    # 주중 특정일에 작업 실행
    # schedule.every().monday.do(job)
    # schedule.every().wednesday.at("13:15").do(job)

    print("start schedule...")
    while True:
        schedule.run_pending()
        time.sleep(1)

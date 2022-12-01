# -*- coding: utf-8 -*-


from impala.dbapi import connect
import sys


maxiter = 100   # 반복 접속수


def getImpalaConn(DBENV):
    try:
        if DBENV == 'Prod':
            CONNECT_HOST = "proxy.digitfarm.net"
        else:
            CONNECT_HOST = "ddgfpd1.digitfarm.net"

        CONNECT_PORT = 21050
        CONNECT_AUTHMECH = "LDAP"
        CONNECT_USER = "apolo_nf"
        CONNECT_PASSWD = "cj1234~!"
        conn = connect(host=CONNECT_HOST, port=CONNECT_PORT, auth_mechanism=CONNECT_AUTHMECH,
                       user=CONNECT_USER, password=CONNECT_PASSWD)
        # conn = connect(host=CONNECT_HOST, port=CONNECT_PORT,
        #                user=CONNECT_USER, password=CONNECT_PASSWD, kerberos_service_name='impala', auth_mechanism = 'GSSAPI')
        dbc = conn.cursor()
        query = "SET MEM_LIMIT=1g;"
        dbc.execute(query)
        # conn.close()
        # sys.exit()
        return conn, dbc

    except Exception as e:
        print(e)


def get_servtime(conn, dbc, bidlist_s, tbnm_bid, tbnm_btp):
    # conn = get_conn_postgresql()
    # dbc = conn.cursor()

    bidlist = list(bidlist_s)
    maxsize, nostr, sindex, flag, dt_n = min(9000, len(bidlist)), len(bidlist), 0, True, []
    eindex = maxsize
    while sindex < nostr and eindex <= nostr:
        dt_n.append("\',\'".join(x for x in bidlist[sindex:eindex]))
        sindex = eindex
        eindex += maxsize
        if eindex > nostr:
            eindex = nostr
    wherestr = "rcvzpid_c in (\'%s\')" % dt_n[0]
    for i in range(1, len(dt_n)):
        wherestr = "%s or rcvzpid_c in (\'%s\')" % (wherestr, dt_n[i])
    # wherestr = "(\'%s\')" % "\',\'".join(x for x in bidlist)
    selcols = ["rcvzpid_c"]
    for i in range(1, 21):
        selcols.append("q%d" % i)
    query_sel = "select %s from %s where %s;" % (",".join(x for x in selcols), tbnm_bid, wherestr)
    # dbc.execute(query_sel)
    # dt_s_bid = dbc.fetchall()
    dt_s_bid, msg = get_dt_digifarm_by_iter(dbc, query_sel, maxiter)
    if len(dt_s_bid) == 0:
        return [], []

    # wherestr = "(\'%s\')" % "\',\'".join(x for x in btplist)
    selcols[0] = "bldg_type"
    # query_sel = "select %s from %s where bldg_type in %s;" % (",".join(x for x in selcols), tbnm_btp, wherestr)
    query_sel = "select %s from %s" % (",".join(x for x in selcols), tbnm_btp)
    # dbc.execute(query_sel)
    # dt_s_btp = dbc.fetchall()
    dt_s_btp, msg = get_dt_digifarm_by_iter(dbc, query_sel, maxiter)
    if len(dt_s_btp) == 0:
        return [], []
    sum_serv = [float(0) for x in range(0, len(dt_s_btp[0]) - 1)]
    cnt_serv = [float(0) for x in range(0, len(dt_s_btp[0]) - 1)]
    for tmprow in dt_s_btp:
        j = -1
        for i in range(1, len(tmprow)):
            j += 1
            if tmprow[i] is not None:
                sum_serv[j] += tmprow[i]
                cnt_serv[j] += float(1)
    tmplist = ["전체평균"] + [sum_serv[x] / cnt_serv[x] for x in range(0, len(dt_s_btp[0]) - 1)]
    dt_s_btp.append(tuple(tmplist))
    # conn.close()
    return dt_s_bid, dt_s_btp


def get_servtime_all(conn, dbc, tbnm_bid, tbnm_btp):
    # conn = get_conn_postgresql()
    # dbc = conn.cursor()
    selcols = ["rcvzpid_c", "q1"]
    query_sel = "select %s from %s;" % (",".join(x for x in selcols), tbnm_bid)
    # dbc.execute(query_sel)
    # dt_s_bid = dbc.fetchall()
    dt_s_bid, msg = get_dt_digifarm_by_iter(dbc, query_sel, maxiter)
    if len(dt_s_bid) == 0:
        return [], []

    # wherestr = "(\'%s\')" % "\',\'".join(x for x in btplist)
    selcols[0] = "bldg_type"
    # query_sel = "select %s from %s where bldg_type in %s;" % (",".join(x for x in selcols), tbnm_btp, wherestr)
    query_sel = "select %s from %s" % (",".join(x for x in selcols), tbnm_btp)
    # dbc.execute(query_sel)
    # dt_s_btp = dbc.fetchall()
    dt_s_btp, msg = get_dt_digifarm_by_iter(dbc, query_sel, maxiter)
    if len(dt_s_btp) == 0:
        return [], []
    sum_serv = [float(0) for x in range(0, len(dt_s_btp[0]) - 1)]
    cnt_serv = [float(0) for x in range(0, len(dt_s_btp[0]) - 1)]
    for tmprow in dt_s_btp:
        j = -1
        for i in range(1, len(tmprow)):
            j += 1
            if tmprow[i] is not None:
                sum_serv[j] += tmprow[i]
                cnt_serv[j] += float(1)
    tmplist = ["전체평균"] + [sum_serv[x] / cnt_serv[x] for x in range(0, len(dt_s_btp[0]) - 1)]
    dt_s_btp.append(tuple(tmplist))
    # conn.close()
    return dt_s_bid, dt_s_btp


def get_dt_digifarm_by_iter(dbc, query, iter):
    for i in range(0, iter):
        try:
            dbc.execute(query)
            dt = dbc.fetchall()
            return dt, "success"
        except Exception as e:
            return [], "failed(%s)" % str(e)
    return [], "failed(iter)"


if __name__ == "__main__":
    fr = open("out_all_q1_급지.csv", "r")
    sumdt = float(0)
    cnt = 0
    while True:
        tmpcl = fr.readline()
        if not tmpcl:
            break
        tmpline = tmpcl.strip().split(",")
        sumdt += float(tmpline[1])
        cnt += 1
    print(sumdt / float(cnt))
    sys.exit()

    tbnm_s_bid = "a2_tes.mrt_gis_g_new_lmd_total_time_result"  # 건물 번호 기준 서비스타임 테이블(단위: 초)
    tbnm_s_btp = "a2_tes.mrt_gis_g_new_lmd_total_time_result_other"  # 건물 타입 기준 서비스타임 테이블(단위: 초)
    conn_dg, dbc_dg = getImpalaConn('Prod')  # DigitFarm 연결정보 구성
    dt_s_bid, dt_s_btp = get_servtime_all(conn_dg, dbc_dg, tbnm_s_bid, tbnm_s_btp)
    fw = open("out_all_q1_급지.txt", "w")
    for tmprow in dt_s_bid:
        fw.write("%s\n" % ",".join(str(x) for x in tmprow))
    fw.flush()
    fw.close()
    sys.exit()

    ## 건물번호 기준 데이터 구성
    dt_build = {}
    fr = open("샘플_건물기준운송장건수.txt", "r")
    # title skip
    tmpcl = fr.readline()
    while True:
        tmpcl = fr.readline()
        if not tmpcl:
            break
        tmpline = tmpcl.strip().split(chr(9))
        dt_build[tmpline[0]] = [int(tmpline[1])]
    fr.close()
    ## DigitFarm에서 건물별 서비스타임(단위: 초) 정보 불러오기
    bidset = set(dt_build.keys())

    conn_dg, dbc_dg = getImpalaConn('Prod')     # DigitFarm 연결정보 구성
    # (list)dt_s_bid: 건물 번호 기준 서비스타임 테이블
    # (list)dt_s_btp: 건물 타입 기준 서비스타임 테이블(착지건수별 평균 서비스타임 추가)
    dt_s_bid, dt_s_btp = get_servtime(conn_dg, dbc_dg, bidset, tbnm_s_bid, tbnm_s_btp)
    if len(dt_s_bid) == 0:
        print("서비스타임 가져오기 실패...")
        sys.exit()
    s_time_bid, s_time_btp = {}, {}     # dict로 재구성
    for tmprow in dt_s_bid:
        s_time_bid[tmprow[0]] = tmprow[1:]
    for tmprow in dt_s_btp:
        s_time_btp[tmprow[0]] = tmprow[1:]

    maxqty_serv = 20    # 급지 모형 최대 착지
    for zpid in dt_build:
        qty = dt_build[zpid][0]     # 건물별 착지건수
        qtyindex = min(qty - 1, maxqty_serv - 1)    # 서비스타임 테이블에서 가져오기 위한 index 생성: 20건 이상은 20건 기준으로
        if zpid in s_time_bid:
            servt = s_time_bid[zpid][qtyindex]
        #     if servt is None:
        #         servt2 = s_time_btp[btp][qtyindex]
        #         if servt2 is None:
        #             servt = s_time_btp["전체평균"][qtyindex]
        #         else:
        #             servt = servt2
        # elif btp in s_time_btp:
        #     servt = s_time_btp[btp][qtyindex]
            if servt is None:
                servt = s_time_btp["전체평균"][qtyindex]
        else:
            servt = s_time_btp["전체평균"][qtyindex]
        dt_build[zpid].append(servt)
    # 샘플 출력
    fw = open("out_test_급지.txt", "w")
    for zpid in dt_build:
        tmpwlist = [zpid] + dt_build[zpid]
        fw.write("%s\n" % ",".join(str(x) for x in tmpwlist))
    fw.flush()
    fw.close()
    conn_dg.close()

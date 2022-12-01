# -*- coding: utf-8 -*-

import sys
import psycopg2
import cx_Oracle as cxo
import ibm_db, ibm_db_dbi
from impala.dbapi import connect


def get_conn_oracle():
    try:
        host = "10.68.1.86"
        port = "1533"
        dbnm = "APOLONDV"
        user = "routgo_app"
        pwd = "routgo1!"
        conn = cxo.connect(user, pwd, "%s:%s/%s" % (host, port, dbnm))
        return conn
    except Exception as e:
        print("Connect Error!!")


def get_conn_postgresql():
    try:
        host = "10.212.80.94"
        # dbnm = "spd_tmap"
        dbnm = "db_nw"
        user = "tes_nw1"
        pwd = "1234"
        conn = psycopg2.connect(host=host, dbname=dbnm, user=user, password=pwd)
        return conn
    except Exception as e:
        print("Connect Error!!")


def get_conn_iiasd():
    try:
        databasenm = 'BLUDB'
        hostaddr = '128.10.1.175'
        port = '50000'
        uid = 'hahnhyok'  # 본인 ID
        pwd = '!KX@hah#2022'  # 본인 PW
        con = ibm_db.connect(
            'DATABASE=' + databasenm + '; HOSTNAME=' + hostaddr + '; PORT=' + port + '; UID=' + uid + '; PWD=' + pwd + '',
            '', '')
        conn = ibm_db_dbi.Connection(con)
        # conn = getImpalaConn('Prod')
        return conn
    except Exception as e:
        print("Connect Error!!")


def getImpalaConn(DBENV):
    try:
        if DBENV == 'Prod':
            CONNECT_HOST = "proxy.digitfarm.net"
        else:
            CONNECT_HOST = "ddgfpd1.digitfarm.net"

        CONNECT_PORT = 21050
        CONNECT_AUTHMECH = "LDAP"
        CONNECT_USER = "hahnhyok"
        CONNECT_PASSWD = "tesnetwork1!"
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


def get_servtime(conn, dbc, bidlist_s, btplist_s, tbnm_bid, tbnm_btp):
    # conn = get_conn_postgresql()
    # dbc = conn.cursor()

    bidlist, btplist = list(bidlist_s), list(btplist_s)
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
    dbc.execute(query_sel)
    dt_s_bid = dbc.fetchall()

    # wherestr = "(\'%s\')" % "\',\'".join(x for x in btplist)
    selcols[0] = "bldg_type"
    # query_sel = "select %s from %s where bldg_type in %s;" % (",".join(x for x in selcols), tbnm_btp, wherestr)
    query_sel = "select %s from %s" % (",".join(x for x in selcols), tbnm_btp)
    dbc.execute(query_sel)
    dt_s_btp = dbc.fetchall()
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

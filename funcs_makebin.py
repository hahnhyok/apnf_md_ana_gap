# -*- coding: utf-8 -*-

import struct


def make_input_bin_od(dbc_dg, folder_bin, dt_od, tbnm_binfo):
    fw_od = open("%s\\odlist.bin" % folder_bin, "wb")
    fw_zones = open("%s\\zone_build.bin" % folder_bin, "wb")
    print("reading od list and writing...")

    zonelist = {}
    line = 0
    for tmprow in dt_od:
        zonelist[str(tmprow[2])] = [float(tmprow[4]), float(tmprow[5]), 0]
        zonelist[str(tmprow[3])] = [float(tmprow[6]), float(tmprow[7]), 0]
        tmpwlist = [int(tmprow[0]), int(tmprow[1]), tmprow[2].encode("utf-8"), tmprow[3].encode("utf-8")] + list(tmprow[4:])
        fw_od.write(struct.pack('=ii25s25sdddd', *tuple(tmpwlist)))
        line += 1
        if line % 20000 == 0:
            print(line)
    print(line)

    selcols = ["bid", "x_5178", "y_5178", "noofnds", "nearnid"]
    zidlist = list(zonelist.keys())
    maxsize, nozid, sindex, flag, dt_n = min(9000, len(zidlist)), len(zidlist), 0, True, []
    eindex = maxsize
    while sindex < nozid and eindex <= nozid:
        print(len(zidlist[sindex:eindex]))
        str1 = "(\'%s\')" % "\',\'".join(x for x in [zid for zid in zidlist[sindex:eindex]])
        query = "select %s from %s where nearnid is not null and bid in %s" % (",".join(x for x in selcols), tbnm_binfo, str1)
        dbc_dg.execute(query)
        dt_n += dbc_dg.fetchall()
        sindex = eindex
        eindex += maxsize
        if eindex > nozid:
            eindex = nozid
    for tmprow in dt_n:
        bid = str(tmprow[0])
        if bid not in zonelist:
            continue
        zonelist[bid][-1] = int(tmprow[3])
        zonelist[bid] += [int(x) for x in tmprow[4].strip().split("-")]
    fmt_init = '=25sddi'
    for bid in zonelist:
        if len(bid) != 25:
            print("length error??")
        tmpnonds = zonelist[bid][2]
        tmpfmt = fmt_init
        for i in range(0, tmpnonds):
            tmpfmt = "%si" % tmpfmt
        tmpwlist = [bid.encode("utf-8")] + zonelist[bid]
        fw_zones.write(struct.pack(tmpfmt, *tuple(tmpwlist)))
    fw_zones.close()
    fw_od.close()


if __name__ == "__main__":
    print('aaa')

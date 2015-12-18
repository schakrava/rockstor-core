#!/usr/bin/env python

"""This tool collects key performance indicators to help identify bottlenecks,
study how the workload is taxing all the systems in the process.

Inputs: pool name, network interface.

For each current value/datapoint, we'll also display min,average and max since
the start of the run.

We are interested in performance of these systems
1. Disk IO: (Aggregated by Pool, input to the tool) (-dp)
* tps/iops: Transactions per second or IOPs (higher the better)
* r_MB/s, w_MB/s: read and write throughput (higher the better)
* avg_req_size: Average size of requests in KB
* avg_q_len: Average queue length of the requests (lower the better?)
* avg_wait: Average wait time(ms) for IO requests issued to the Pool to be served. (time in queue + servicing time)
* bw_util: Bandwidth utilization % for the Pool. Closer to 100% means device saturation.

2. CPU: (All CPUs combined) (-uq)
* %iowait: % of time CPU is idle while waiting for outstanding IO (lower the better)
* %idle: % of time CPU is idle (without any outstanding IO) (lower the better)
* %user, %nice, %system: % of time CPU is busy doing stuff.
* run_q_len: number of tasks waiting for CPU (higher doesn't mean bad, but could be a clue for a bottleneck)
* load5: load average for last 5 minutes. (same note as above)
* blocked: number of tasks currently blocked waiting for IO. (lower the better)

3. Memory and Swap: (-rS)
* %memused: % of memory in use. Close to 100% is normal in Linux because of cache/buffers.
* %memcached: % of memory used for cache. Higher, closer to %memuse is normal.
* %swapused: % of swap used. (lower the better, preferrably 0)

4. Network: For a given interface(input to the tool) (-n DEV)
* rx_MB/s, tx_MB/s: receive and transmit throughput.

5. NFS Server: (-n NFSD)
* c_rpc/s: number of RPC calls per second.
* r_rpc/s, w_rpc/s: number of read and write RPC calls per second.

"""

import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
import sys
from datetime import datetime
import re
from django.conf import settings
from system.osi import run_command
from system.pkg_mgmt import install_pkg
from storageadmin.models import (Pool, NetworkInterface)


SAR = '/usr/bin/sar'
INTERVAL = 10 #seconds
fstr = '{:<16} {:>18} {:>18} {:>18} {:>18} {:>18} {:>18}'


def check_deps():
    if (not os.path.isfile(SAR)):
        print ('%s not found. Installing it...' % SAR)
        install_pkg('sysstat')
        print ('Done.')

class KPIAggregator(object):

    min_sentinel = 100000
    max_sentinel = 0.0
    # Pool IO
    min_iops = min_tp = min_req_size = min_q_len = min_wait = min_bw_util = min_sentinel
    max_iops = max_tp = max_req_size = max_q_len = max_wait = max_bw_util = max_sentinel

    # CPU
    min_iowait = min_blocked = min_idle = min_system = min_user = min_run_q_len = min_load5 = 100000
    max_iowait = max_blocked = max_idle = max_system = max_user = max_run_q_len = max_load5 = 0.0

    # Mem and Swap
    kbtotalmem = None
    min_memused = min_memcached = min_swapused = 100000
    max_memused = max_memcached = max_swapused = 0.0

    # NFSD
    min_tcalls = min_rcalls = min_wcalls = 100000
    max_tcalls = max_rcalls = max_wcalls = 0.0

    # Network
    min_rx = min_tx = 100000
    max_rx = max_tx = 0.0

    def __init__(self, pool, dnames, no):
        KPIAggregator.pool = pool
        KPIAggregator.dnames = dnames
        KPIAggregator.no = no

    @classmethod
    def disk_io(cls, v, o):
        iops = tp = req_size = q_len = wait = bw_util = 0.0
        for i in range(*v):
            fields = o[i].split()
            if (len(fields) != 10):
                continue #last item will be empty
            if (fields[1] in cls.dnames):
                iops += float(fields[2])
                #Add read and write sectors/sec * 512 (sector size) to convert to bytes/sec
                tp += ((float(fields[3]) + float(fields[4])) * 512)
                #request size is also in 512 Byte sectors
                req_size += (float(fields[5]) * 512)
                q_len += float(fields[6])
                #wait is in ms.
                wait += float(fields[7])
                #bandwidth utilization is %
                bw_util += float(fields[8])

        iops = int(round(iops))
        tp = int(round(tp / (1024 * 1024))) # convert throughput to MB
        req_size = int(round(req_size / 1024))
        q_len = int(round(q_len))
        # Average the wait by disks in the pool.
        wait = int(round((wait / len(cls.dnames))))
        # Average the % by disks in the pool.
        bw_util = int(round(bw_util / len(cls.dnames)))
        cls.set_min_max({'iops': iops, 'tp': tp, 'req_size': req_size,
                         'q_len': q_len, 'wait': wait, 'bw_util': bw_util,})

        print(fstr.format('Disk IO', 'IOPS', 'Throughput', 'Wait time(ms)',
                          '% BW Utilization', 'Request size(KB)', 'Queue size',))
        print(fstr.format('', iops, tp, req_size, q_len, wait, bw_util))
        print(fstr.format('', '(%d - %d)' % (cls.min_iops, cls.max_iops),
                          '(%d - %d)' % (cls.min_tp, cls.max_tp),
                          '(%d - %d)' % (cls.min_wait, cls.max_wait),
                          '(%d - %d)' % (cls.min_bw_util, cls.max_bw_util),
                          '(%d - %d)' % (cls.min_req_size, cls.max_req_size),
                          '(%d - %d)' % (cls.min_q_len, cls.max_q_len)) + '\n')
        return iops, tp, req_size, q_len, wait, bw_util

    @classmethod
    def set_min_max(cls, attr_map):
        for k,v in attr_map.items():
            min_attr = 'min_%s' % k
            max_attr = 'max_%s' % k
            cur_min = getattr(cls, min_attr)
            cur_max = getattr(cls, max_attr)
            if (v < cur_min):
                setattr(cls, min_attr, v)
            if (v > cur_max):
                setattr(cls, max_attr, v)

    @classmethod
    def cpu(cls, v1, v2, o):
        iowait = idle = user = system = run_q_len = load5 = blocked = 0.0
        for i in range(*v1):
            if (re.search('all', o[i]) is not None):
                fields = o[i].split()
                user = int(round(float(fields[2])))
                system = int(round(float(fields[4])))
                iowait = int(round(float(fields[5])))
                idle = int(round(float(fields[7])))
                break
        for i in range(*v2):
            if (len(o[i].strip()) > 0):
                fields = o[i].split()
                run_q_len = int(fields[1])
                load5 = float(fields[4])
                blocked = int(fields[6])
                break
        cls.set_min_max({'iowait': iowait, 'idle': idle, 'user': user,
                         'system': system, 'run_q_len': run_q_len,
                         'load5': load5, 'blocked': blocked,})
        cpu_fstr = '%s {:>14}' % fstr
        print(cpu_fstr.format('CPU', '% IO Wait', 'Blocked tasks', '% Idle', '% System',
                              '% User', 'Run-q size', 'Load(5 min)'))
        print(cpu_fstr.format('', iowait, blocked, idle, system, user, run_q_len, load5))
        print(cpu_fstr.format('', '(%d - %d)' % (cls.min_iowait, cls.max_iowait),
                              '(%d - %d)' % (cls.min_blocked, cls.max_blocked),
                              '(%d - %d)' % (cls.min_idle, cls.max_idle),
                              '(%d - %d)' % (cls.min_system, cls.max_system),
                              '(%d - %d)' % (cls.min_user, cls.max_user),
                              '(%d - %d)' % (cls.min_run_q_len, cls.max_run_q_len),
                              '(%d - %d)' % (cls.min_load5, cls.max_load5)) + '\n')
        return user, system, iowait, idle, run_q_len, load5, blocked

    @classmethod
    def mem_swap(cls, v1, v2, o):
        memused = memcached = swapused = 0.0
        for i in range(*v1):
            if (re.match('Average:', o[i]) is not None):
                fields = o[i].split()
                memused = int(round(float(fields[3])))
                if (cls.kbtotalmem is None):
                    kbmemused = float(fields[2])
                    cls.kbtotalmem = (kbmemused * 100) / memused
                memcached = int((float(fields[5]) * 100) / cls.kbtotalmem)
                break
        for i in range(*v2):
            if (re.match('Average:', o[i]) is not None):
                fields = o[i].split()
                swapused = int(round(float(fields[3])))
                break
        cls.set_min_max({'memused': memused, 'memcached': memcached,
                         'swapused': swapused,})
        print(fstr.format('Mem/Swap', '% Mem used', '% Mem cached', '% Swap used', '', '', '', ''))
        print(fstr.format('', memused, memcached, swapused, '', '', '', ''))
        print(fstr.format('', '(%d - %d)' % (cls.min_memused, cls.max_memused),
                          '(%d - %d)' % (cls.min_memcached, cls.max_memcached),
                          '(%d - %d)' % (cls.min_swapused, cls.max_swapused),
                          '', '', '', '') + '\n')
        return memused, memcached, swapused

    @classmethod
    def network(cls, v, o):
        rx = tx = 0.0
        for i in range(*v):
            if (re.search(cls.no.name, o[i]) is not None):
                fields = o[i].split()
                rx = int(round(float(fields[4]) / 1024)) #convert to MB
                tx = int(round(float(fields[5]) / 1024))
                break
        cls.set_min_max({'rx': rx, 'tx': tx})
        print(fstr.format('Network(%s)' % cls.no.name, 'rx_MB/sec', 'tx_MB/sec',
                          '', '', '', '', ''))
        print(fstr.format('', rx, tx, '', '', '', '', ''))
        print(fstr.format('', '(%d - %d)' % (cls.min_rx, cls.max_rx),
                          '(%d - %d)' % (cls.min_tx, cls.max_tx),
                          '', '', '', '', '') + '\n')
        return rx, tx

    @classmethod
    def nfsd(cls, v, o):
        tcalls = rcalls = wcalls = 0.0
        for i in range(*v):
            if (re.search('Average:', o[i]) is not None):
                fields = o[i].split()
                tcalls = int(round(float(fields[1])))
                rcalls = int(round(float(fields[8])))
                wcalls = int(round(float(fields[9])))
                break
        cls.set_min_max({'tcalls': tcalls, 'rcalls': rcalls, 'wcalls': wcalls,})
        print(fstr.format('NFS Server', 'Calls/sec', 'Reads/sec', 'Writes/sec',
                          '', '', '', ''))
        print(fstr.format('', tcalls, rcalls, wcalls, '', '', '', ''))
        print(fstr.format('', '(%d - %d)' % (cls.min_tcalls, cls.max_tcalls),
                          '(%d - %d)' % (cls.min_rcalls, cls.max_rcalls),
                          '(%d - %d)' % (cls.min_wcalls, cls.max_wcalls),
                          '', '', '', '') + '\n')
        return tcalls, rcalls, wcalls

def main():
    if (len(sys.argv) < 3):
        print ('Usage: %s <pool_name> <network_interface> [ report_interval ]' % sys.argv[0])
        print ('default report_interval is 5 seconds.')
        sys.exit(0)

    pname = sys.argv[1]
    try:
        pool = Pool.objects.get(name=pname)
    except Pool.DoesNotExist:
        sys.exit('Pool(%s) does not exist.' % pname)

    ni = sys.argv[2]
    try:
        no = NetworkInterface.objects.get(name=ni)
    except Pool.DoesNotExist:
        sys.exit('Network Interface(%s) does not exist.' % ni)

    report_interval = INTERVAL
    if (len(sys.argv) > 3):
        try:
            report_interval = int(sys.argv[3])
            if (report_interval < 1):
                sys.exit('report_interval must be a positive integer')
        except:
            sys.exit('report_interval must be a positive integer')

    dnames = [d.name for d in pool.disk_set.all()]
    check_deps()
    ag = KPIAggregator(pool, dnames, no)

    cmd = [SAR, '-dpuqrS', '-n', 'DEV,NFSD', str(report_interval), '1',]
    failed_attempts = 0
    count = 0
    section_ranges = {}
    while (True):
        o, e, rc = run_command(cmd)
        t = datetime.now()
        cur_section = prev_section = None
        for i in range(len(o)):
            fields = o[i].strip().split()
            if (len(fields) > 0 and fields[0] == 'Average:'):
                if (fields[1] in ('CPU', 'IFACE', 'scall/s', 'kbmemfree', 'kbswpfree', 'runq-sz', 'DEV',)):
                    prev_section = cur_section
                    if (prev_section is not None):
                        section_ranges[prev_section].append(i)
                    cur_section = fields[1]
                    section_ranges[cur_section] = [i+1,]
        section_ranges[cur_section].append(len(o))
        tstr = t.strftime('%m-%d-%Y %H:%M:%S')
        fsuf = t.strftime('%m-%d-%Y')
        of = '%svar/log/perf.%s.txt' % (settings.ROOT_DIR, t.strftime('%m-%d-%Y'))
        mode = 'w'
        if (os.path.exists(of)):
            mode = 'a'
        with open(of, mode) as sfo:
            if (mode == 'w'):
                sfo.write('Time,%user,%system,%iowait,%idle,run_q_lenth,load5,'
                          'blocked,iops,throughput,request_size,queue_length,'
                          'iowait,bandwidth_util,%memused,%memcached,%swapused,'
                          'rpc calls/sec, rpc reads/sec, rcp writes/sec,'
                          'rx_MB/sec,tx_MB/sec\n')
            print(t.strftime('%m-%d-%Y %H:%M:%S'))
            cpu = ag.cpu(section_ranges['CPU'], section_ranges['runq-sz'], o)
            iostats = ag.disk_io(section_ranges['DEV'], o)
            mem = ag.mem_swap(section_ranges['kbmemfree'], section_ranges['kbswpfree'], o)
            nfsd = ag.nfsd(section_ranges['scall/s'], o)
            net = ag.network(section_ranges['IFACE'], o)
            sfo.write('%s' % tstr)
            for s in cpu + iostats + mem + nfsd + net:
                sfo.write(',%s' % s)
            sfo.write('\n')

if __name__ == '__main__':
    main()

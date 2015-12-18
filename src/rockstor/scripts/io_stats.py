#!/usr/bin/env python

import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
import sys
from datetime import datetime
import re
from system.osi import run_command
from system.pkg_mgmt import install_pkg
from storageadmin.models import Pool


IOSTAT = '/usr/bin/iostat'
INTERVAL = 5 #seconds
HEADER = 'Time\tIOPS\tMin-IOPS\tMax-IOPS\tThroughput\tMin-Throughput\tMax-Throughput\tLatency\tUtilization'
"""
iostat notes.
-m to collect stats in MB
-x for extended metrics
-y to ignore t0 report since boot
-t timestamp for each report
-T display only global stats
-g {d1,d2,,,} : disks of the given pool
"""


def check_deps():
    if (not os.path.isfile('/usr/bin/iostat')):
        print ('%s not found. Installing it...' % IOSTAT)
        install_pkg('sysstat')
        print ('Done.')

def main():
    if (len(sys.argv) == 1):
        print ('Usage: %s <pool_name> [ report_interval ]' % sys.argv[0])
        print ('default report_interval is 5 seconds.')
        sys.exit(0)

    pname = sys.argv[1]
    try:
        pool = Pool.objects.get(name=pname)
    except Pool.DoesNotExist:
        sys.exit('Pool(%s) does not exist.' % pname)

    report_interval = INTERVAL
    if (len(sys.argv) > 2):
        try:
            report_interval = int(sys.argv[2])
            if (report_interval < 1):
                sys.exit('report_interval must be a positive integer')
        except:
            sys.exit('report_interval must be a positive integer')

    dnames = [d.name for d in pool.disk_set.all()]
    check_deps()

    min_iops = min_tp = None
    max_iops = max_tp = 0.0
    min_tp = max_tp = 0.0
    cmd = [IOSTAT, '-d', '-m', '-x', '-y', '-T', '-g', pname]
    cmd.extend(dnames)
    cmd.extend([str(report_interval), '1'])
    #print ("Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util")
    failed_attempts = 0
    count = 0
    print (HEADER)
    while (True):
        o, e, rc = run_command(cmd)
        t = datetime.now()
        if (len(o) < 4 or re.match(pname, o[3].strip()) is None):
            print('Cannot parse output: %s' % o)
            failed_attempts += 1
            if (failed_attempts > 50):
                sys.exit('Too many failed attempts, Aborting.')
            continue
        failed_attempts = 0
        fields = o[3].strip().split()
        iops = reduce(lambda x, y: float(x) + float(y), fields[1:5])
        if (min_iops is None or iops < min_iops):
            min_iops = iops
        if (iops > max_iops):
            max_iops = iops
        throughput = float(fields[5]) + float(fields[6])
        if (min_tp is None or throughput < min_tp):
            min_tp = throughput
        if (throughput > max_tp):
            max_tp = throughput
        latency = float(fields[9])
        pct_util = float(fields[13])
        if (count == 15):
            print (HEADER)
            count = 0
        count += 1
        print ('%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f' %
               (t.strftime('%m-%d-%Y %H:%M:%S'), iops, min_iops, max_iops, throughput, min_tp, max_tp, latency, pct_util))

if __name__ == '__main__':
    main()

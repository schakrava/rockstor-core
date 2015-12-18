

import time
from system.osi import run_command

def read_test():
    for i in range(10):
        o, e, rc = run_command(['hdparm', '-Tt', '/dev/sda'])
        print o

def main():
    #with write-cache on
    run_command(['hdparm', '-W1', '/dev/sda'])
    distribution = {}
    for i in range(10):
        o, e, rc = run_command(['dd', 'if=/dev/zero', 'of=/root/testfile', 'bs=1G', 'count=1', 'oflag=direct'])
        time.sleep(5)
        for l in o:
            if (re.match('1073741824 bytes') is not None):
                pass


#http://benjamin-schweizer.de/measuring-disk-io-performance.html
#

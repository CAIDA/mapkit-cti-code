import csv
#import matplotlib.pylab as plt
#import pylab as pl
import bz2
import operator
import sys
import os
import re
import radix
import pickle
import math
import getopt
import gzip
from collections import defaultdict
#from netaddr import *

def main():
    dd_fn = None
    bgp_pfx_fn = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdf:b:", ["help", "debug", "rir=", "bgp="])
    except getopt.GetoptError as err:
        #print (str(err))
        sys.exit(1)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d","--debug"):
            DEBUG=1
        elif o in ("-f","--rir"):
            dd_fn = a
        elif o in ("-b","--bgp"):
            bgp_pfx_fn = a
        else:
            assert False, "unhandled option"

    rtree = radix.Radix()

    try: 
        #sys.stderr.write('reading RIR DD file %s\n' % dd_fn)
        RIR = open(dd_fn,'r')
    except OSError as o:
        #sys.stderr.write('pfx2AS file error: %s\n' % o)
        exit(1)
    except IOError as i:
        #sys.stderr.write('File open failed: %s\n' % i)
        exit(1)
    else:    
        for line in RIR:
            if re.match(r'#', line): continue
            fields = line.strip().split("|")
            if len(fields) != 6 or fields[0] != "ipv4" : continue
            for p in fields[4].split(","):
                rnode = rtree.add(p)
                rnode.data["origin"] = fields[5]
                rnode.data["RIR"] = (fields[1]+"|"+fields[2])
        RIR.close()
        
    ipv4Dict = defaultdict(list)

    try: 
        #sys.stderr.write('reading BGP prefix file %s\n' % bgp_pfx_fn)
        #BGP = bz2.BZ2File(bgp_pfx_fn, 'r') #AGG replaces this line to read routevies pfx2as instead of AS-rank's for consistency
        BGP = gzip.open(bgp_pfx_fn, 'r')
    except OSError as o:
        #sys.stderr.write('pfx2AS file error: %s\n' % o)
        exit(1)
    except IOError as i:
        #sys.stderr.write('File open failed: %s\n' % i)
        exit(1)
    else:
        for line in BGP:
            if line.startswith("#"): continue
            fields = line.strip().split()
            bgp_as = fields[2]
            nwk = fields[0] + "/" + fields[1]
            #print(fields[0])
            rnode = rtree.search_worst(fields[0])
            if rnode is None:
                #sys.stderr.write('not found in allocation file\n')
                continue
            #print "rnode", str(rnode), rnode.data["origin"], rnode.data["RIR"]
            ipv4Dict[rnode.data["RIR"]].append((nwk,bgp_as))
        BGP.close()
        
    for k, v in ipv4Dict.items():
        #print k 
        b_str = ",".join(map(lambda x: x[0]+":"+x[1], ipv4Dict[k]))
        sys.stdout.write('%s|%s\n' % (k,b_str))
        
if __name__=="__main__":
    main()

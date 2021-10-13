import sys
import getopt
import os
import netaddr
import math
import re
#from sets import Set

def get_pfxlen(cnt):
    for pl in range(0,33):
        if pfxlen2cnt[pl] <= cnt:
            return pl

def get_prefixes(start_ip,count):
    prefixes = set()	# A set is an unordered collection with no duplicate elements.
    st_ip_dec = int(netaddr.IPAddress(start_ip))
    while count > 0:
        pl = get_pfxlen(count)
        st_ip_str = str(netaddr.IPAddress(st_ip_dec))
        #print(st_ip_dec,st_ip_str,start_ip)
        end_ip_str = str(netaddr.IPAddress(st_ip_dec + pfxlen2cnt[pl] -1))
        #print(end_ip_str)
        prefixes.add((st_ip_str,pl,end_ip_str))
        st_ip_dec = st_ip_dec + pfxlen2cnt[pl]
        #print(st_ip_dec)
        count = count - pfxlen2cnt[pl]
    return prefixes

pfxlen2cnt = {}

def main():
    deleg_fn = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdf:", ["help", "debug", "delegfile="])
    except getopt.GetoptError as err:
        print (str(err))
        sys.exit(1)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d","--debug"):
            DEBUG=1
        elif o in ("-f","--delegfile"):
            deleg_fn = a
        else:
            assert False, "unhandled option"

    for pl in range(0,33):
        #print pl, int(math.pow(2,32-pl))
        pfxlen2cnt[pl] = int(math.pow(2,32-pl))

    DELEG = open(deleg_fn,'r')    
    for line in DELEG:
        if re.match("^#",line): continue
        fields = line.strip().split("|")
        if fields[5] == "summary": continue
        if fields[2] != "ipv4" and fields[2] != "asn": continue
        
        if fields[6]=="assigned" or fields[6]=="allocated":
            org = ""
            try:
                if fields[7] is not None:
                    org = fields[7]
            except IndexError:
                org = ""
            if fields[2] == "ipv4":
                #get CIDR prefixes for the address block represented by start_addr + count_addr
                pfxs = get_prefixes(fields[3],int(fields[4]))
                sys.stdout.write(fields[2]+"|"+fields[3]+"|"+fields[4]+"|"+fields[1]+"|"+",".join(x[0]+"/"+str(x[1]) for x in sorted(pfxs,key=lambda x: x[1])) + "|" + org + "\n" )	    	
            if fields[2] == "asn":
                sys.stdout.write(fields[2]+"|"+fields[3]+"|"+fields[1]+"|"+org + "\n")
    DELEG.close()

    exit(0)    

if __name__=="__main__":
    main()

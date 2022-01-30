#!/usr/bin/env python
#Developer: Alistair King (CAIDA/UCSD, USA)
#reads prefixes from file
#Geolocates each IP address in each prefix to a country
#output described in MapKit Wiki https://github.com/CAIDA/mapkit/wiki/Datasets
#SIGCOMM output in /project/mapkit/agamerog/country_asn_analysis/geolocation_bgp/pfxgeo

import argparse
import json
import logging
import pyipmeta
import re
import wandio
import numpy as np 
import socket, struct

def ip2long(ip):
        """
        Convert an IP string to long
        """
        packedIP = socket.inet_aton(ip)
        return struct.unpack("!L", packedIP)[0]

def long2ip(ip):
    return socket.inet_ntoa(struct.pack('!L', ip))

def systemCall(parameter):
    os.system(parameter)

def SplitINTOSlash24(prefix):
    network,netmask=prefix.split('/')
    numberOFslash24=divmod(np.power(2,32-int(netmask)),256)
    output_v=[]
    if numberOFslash24[1]==0:
        r=0
        while r<numberOFslash24[0]:
            Network_long=ip2long(network)
            Network_long+=(r*256)
            Network_output=long2ip(Network_long)
            output_v.append(Network_output)
            r=r+1
    return output_v

def main():
    parser = argparse.ArgumentParser(description="""
    Perform AS and prefix geolocation based on a pfx2as DB
    """)
    parser.add_argument('-p', '--provider',
                        nargs='?', required=True,
                        help="Metadata provider to use ('netacq-edge' or 'maxmind')",
                        choices=["maxmind", "netacq-edge"])
    parser.add_argument('-m', '--pfx2as',
                        nargs='?', required=True,
                        help="Prefix2AS DB to use")
    parser.add_argument('-o', '--outdir',
                        nargs='?', required=True,
                        help="Directory to write output to")

    opts = vars(parser.parse_args())

    # extract the date from the pfx2as filename
    match = re.match(r".+routeviews-rv2-(\d\d\d\d)(\d\d)(\d\d)-\d+.pfx2as.gz", opts["pfx2as"])
    date = "%s-%s-%s" % (match.group(1), match.group(2), match.group(3))

    # spin up an IpMeta instance we can use to query about prefixes
    ipm = pyipmeta.IpMeta(provider=opts["provider"], time=date)

    asinfo = {}  # asinfo[asn][pfx_cnt|ip_cnt]
    asgeo = {}  # asgeo[asn][country][pfx_cnt|ip_cnt]

    asgeo_fh = wandio.open("%s/asn-to-country.%s.gz" % (opts["outdir"], date), "w")
    asgeo_fh.write("#asn|country-code|pfx-cnt|pfx-pct|ip-cnt|ip-pct\n")

    pfxgeo_fh = wandio.open("%s/pfx-to-country.%s.gz" % (opts["outdir"], date), "w")
    pfxgeo_fh.write("#pfx|country-code|ip-cnt|asn\n")

    with wandio.open(opts["pfx2as"]) as fh:
        for line in fh:
            if '#' in line:
                continue
            pfix_list = []
            line = line.strip()
            (ip, length, asn) = line.split()
            lenght = int(length)
            pfx = "%s/%s" % (ip, length)
            ip_cnt = 2**(32-int(length))

            # skip MOAS or AS SETs
            if "_" in asn or "," in asn:
                logging.warn("Skipping MOAS/AS Set: '%s'" % asn)
                continue

            # fix stupid dotted 32bit ASN
            if "." in asn:
                (hi, lo) = asn.split(".")
                asn = (int(hi) << 16) | int(lo)

            length = int(length)
            pfx = "%s/%s" % (ip, length)
            
            if length > 24:
                logging.warn("Skipping illegal prefix Set: '%s'" % pfx)
                continue 

            elif length > 7:
                ip_cnt = 2**(32-int(length))
                pfix_list.append(pfx)
                #break prefix into /24s (unless its > /24, in which case leave as is)
            else:
                logging.warn("Skipping illegal prefix length: '%s'" % pfx)
                continue
                #pfix_list = SplitINTOSlash24(pfx)
                #ip_cnt = 256 #hardcode the IP count to /24

            for i in range(len(pfix_list)):
                #if "/24" not in pfix_list[i]:
                #    pfx =  "%s/%s" % (pfix_list[i], "24")
                #else:
                pfx = pfix_list[i]
                # geolocate this prefix
                geos = ipm.lookup(pfx)

                if asn in asinfo:
                    asinfo[asn]["pfx_cnt"] += 1
                    asinfo[asn]["ip_cnt"] += ip_cnt
                else:
                    asinfo[asn] = {
                        "pfx_cnt": 1,
                        "ip_cnt": ip_cnt,
                    }

                if asn not in asgeo:
                    asgeo[asn] = {}
                ag = asgeo[asn]

                pg = {}  # pg[cc] = geo_ip_cnt
                ccs = set()

                for geo in geos:
                    cc = geo["country_code"]
                    geo_ip_cnt = geo["matched_ip_count"]

                    if cc not in ag:
                        ag[cc] = {
                            "pfx_cnt": 0,
                            "ip_cnt": 0,
                        }
                    agc = ag[cc]

                    if cc not in ccs:
                        # only count the prefix once, not once per geo
                        agc["pfx_cnt"] += 1
                        ccs.add(cc)

                    agc["ip_cnt"] += geo_ip_cnt

                    if cc in pg:
                        pg[cc] += geo_ip_cnt
                    else:
                        pg[cc] = geo_ip_cnt

                # dump out the pg info
                for cc in pg:
                    pfxgeo_fh.write("%s|%s|%d|%s\n" % (pfx, cc, pg[cc],str(asn)))

    # dump out the ASN info
    for asn in asinfo:
        ai = asinfo[asn]
        ag = asgeo[asn]
        for cc in ag:
            agc = ag[cc]
            pfx_pct = agc["pfx_cnt"] * 100.0 / ai["pfx_cnt"]
            ip_pct = agc["ip_cnt"] * 100.0 / ai["ip_cnt"]
            asgeo_fh.write("%s|%s|%s|%s|%s|%s\n" % (
                asn, cc,
                agc["pfx_cnt"], pfx_pct,
                agc["ip_cnt"], ip_pct
            ))

    asgeo_fh.close()
    pfxgeo_fh.close()


if __name__ == "__main__":
    main()

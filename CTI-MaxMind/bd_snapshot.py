from __future__ import with_statement
#usage python run_ddc_queries.py >> supplemental_data/multiprocessing_ddc_log.txt 2>&1
#gets list of ipmap files to process from hardcoded list below
#list created by running:
#ls rtt_and_loss_data/rtt/book_keeping/*/*/*ipmap* > supplemental_data/list_of_ipmaps.txt
import subprocess
from multiprocessing import Pool
import os
import signal
import time
import csv
import sys
import numpy as np
import socket, struct #Para las funciones de transformacion
#--------------------
# Functions to generate /24

current_snapshot = sys.argv[1]

geo_filename = "/project/mapkit/agamerog/country_asn_analysis/geolocation_bgp/pfxgeo/asn-to-country.2018-03-01"
del_filename = "/project/mapkit/agamerog/country_asn_analysis/dd_and_bd/dd.txt"
cc_filename = "/project/mapkit/agamerog/country_asn_analysis/cc_to_name.csv"
bd_filename = "/project/mapkit/agamerog/country_asn_analysis/dd_and_bd/" + current_snapshot + "_bd.txt"
dd_filename = "/project/mapkit/agamerog/country_asn_analysis/dd_and_bd/" + current_snapshot + "_dd.txt"

bd_output_filename = "/project/mapkit/agamerog/country_asn_analysis/bd/" + current_snapshot + ".txt"
output_filename = "/project/mapkit/agamerog/country_asn_analysis/country_dd_bg_bd.csv"
sys.stderr.write("writing " + output_filename + "\n")
def read_cc(filename):
    cc_dict = {}
    with open (filename,'rb') as f:
        rows = f.readlines()
        for i in range(len(rows)):
            cc = rows[i].split(',')[1]
            name = rows[i].split(',')[0]
            cc_dict[cc] = name
        return cc_dict

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

def read_delegation_file(filename):
    #read ipmap lines into list
    country_dict = {}

    with open(filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            cc = row.split('|')[2]
            asn = row.split('|')[1]
            if "_" in asn:
                continue #skipping MOAS
            if cc not in country_dict:
                country_dict[cc] = set()
            country_dict[cc].add(asn)
        return country_dict
 
def read_bd_file(dd_filename, bd_filename):
    #read ipmap lines into list
    country_dict = {}
    dd_blocks_to_cc = {}
    bd_blocks_to_asn = {}
    pfix_dict = {}
    with open(dd_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            block = row.split('|')[1]
            size = row.split('|')[2]
            block_size = block + '.' + size
            country = row.split('|')[3]
            dd_blocks_to_cc[block_size] = country

    with open(bd_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        for i in range(len(ipmap_list)):
            asn_set = set()
            row = ipmap_list[i].strip('\n')
            block = row.split('|')[0]
            size = row.split('|')[1]
            block_size = block + '.' + size
            prefix_list = row.split('|')[2].split(',')
            current_prefixes = []
            for j in range(len(prefix_list)):
                if ':' not in prefix_list[j]: #skipping odd cases with no prefix
                    continue
                current_asn = prefix_list[j].split(':')[1]
                current_prefixes.append(prefix_list[j])
                # skip MOAS or AS SETs
                if "_" in current_asn or "," in current_asn:
                    sys.stderr.write("Skipping MOAS/AS Set: " + str(current_asn) + '\n')
                    continue

                # fix stupid dotted 32bit ASN
                if "." in current_asn:
                    (hi, lo) = current_asn.split(".")
                    current_asn = (int(hi) << 16) | int(lo)
                #print prefix_list[j]
                asn_set.add(current_asn)
            
                bd_blocks_to_asn[block_size] = [asn_set, current_prefixes]

    for block in bd_blocks_to_asn:
        try:
            cc = dd_blocks_to_cc[block]
            asns = bd_blocks_to_asn[block][0]
            #grab prefixes, cc, asn
            for i in range(len(bd_blocks_to_asn[block][1])):
                pfix_dict[ (bd_blocks_to_asn[block][1][i]) ] = cc

            if cc not in country_dict:
                country_dict[cc] = set()
            #grab asn, cc
            for asn in asns:  
                country_dict[cc].add(asn)
             
        except KeyError:
            sys.stderr.write(str(block) + ' block not found in DD\n')

    return country_dict, pfix_dict
            
def read_geolocation_file(filename):
    #read ipmap lines into list
    country_dict = {}
    with open(filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        for i in range(len(ipmap_list)):
            if i == 0:
                continue #skip header
            row = ipmap_list[i].strip('\n')
            cc = row.split('|')[1]
            asn = row.split('|')[0]
            if cc not in country_dict:
                country_dict[cc] = set()
            country_dict[cc].add(asn)

        return country_dict

def write_output_table(geolocation, delegation, cc_dict, bd):
    
    already_analyzed = set() #set to determine if some ccs are only in delegation file (unlikely)
    output = []
    output.append("Country Code, Country Name, BG, DD, BD, BG & DD & BD, BG - (DD U BD), DD - (BG U BD), BD - (BG U DD), BG U DD U BD")
    for cc in geolocation:
        already_analyzed.add(cc)        
        BG = str(len(geolocation[cc])) #grab length of set
        try:
            name = cc_dict[cc]
        except KeyError:
            name = "Unknown"
        if cc not in delegation:
            line = cc + ',' + name + ',' + BG + ',-,-,-,-,-,-,' + BG
            output.append(line)
        elif cc not in bd:
            DD = str(len(delegation[cc]))
            BG_intersect_DD = str(len(delegation[cc].intersection(geolocation[cc])))
            BG_minus_DD = str(len(geolocation[cc].difference(delegation[cc])))
            DD_minus_BG = str(len(delegation[cc].difference(geolocation[cc])))
            BG_union_DD = str(len(delegation[cc].union(geolocation[cc])))
            line = cc + ',' + name + ',' + BG + ',' + DD + ',-,' + BG_intersect_DD \
                + ',' + BG_minus_DD + ',' + DD_minus_BG + ',' + BG_union_DD
            output.append(line)
        else:
            DD = str(len(delegation[cc]))
            BD = str(len(bd[cc]))
            #awfully hard to read set math below...
            BG_intersect_DD_BD = str(len((delegation[cc].intersection(geolocation[cc])).intersection(bd[cc])))
            BG_minus_DD_BD = str(len(geolocation[cc].difference((delegation[cc].union(bd[cc])))))
            DD_minus_BG_BD = str(len(delegation[cc].difference((geolocation[cc].union(bd[cc])))))        
            BD_minus_BG_DD = str(len(bd[cc].difference((delegation[cc].union(geolocation[cc])))))
            BG_union_DD_BD = str(len((delegation[cc].union(geolocation[cc])).union(bd[cc])))
            line = cc + ',' + name + ',' + BG + ',' + DD + ','+ BD +',' + BG_intersect_DD_BD \
                + ',' + BG_minus_DD_BD + ',' + DD_minus_BG_BD + ',' + BD_minus_BG_DD + ',' + BG_union_DD_BD
            output.append(line)
 
    with open (output_filename, 'w+') as g:
        for i in range(len(output)):
            g.write(output[i] + '\n')

def countries_missing(geolocation, delegation):
    cset = set()
    for country in geolocation:
        cset.add(country)
        if country not in delegation:
            try:
                print ("geo country not in delegation   " + cc_dict[country])
            except KeyError:
                print ("geo country not in delegation   " + country)
    for country in delegation:
        if country in cset:
            continue
        elif country not in geolocation:
            try:
                print ("geo country not in delegation   " + cc_dict[country])
            except KeyError:
                print ("geo country not in delegation   " + country)

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

def write_bd_output(pfix_dict, bd_output_filename):
    output = []
    output.append("pfx|country-code|asn\n")
    for pfix in pfix_dict:
        pfix_list = []
        asn = pfix.split(':')[1]
        cc = pfix_dict[pfix]

        prefix = pfix.split(':')[0]
        length = int(prefix.split('/')[1])
#        print prefix   
#        if length >= 24:
        pfix_list.append(prefix)
            #break prefix into /24s (unless its > /24, in which case leave as is)
#        else:
#            pfix_list = SplitINTOSlash24(prefix)

        for i in range(len(pfix_list)):
 #           if "/24" not in pfix_list[i]:
 #               line = "%s/%s" % (pfix_list[i], "24") + '|' + cc + '|' + asn + '\n'
#                output.append(line)
#            else:
            line = pfix_list[i] + '|' + cc + '|' + asn + '\n'
            output.append(line)

    with open (bd_output_filename, 'w+') as g:
        for i in range(len(output)):
            g.write(output[i])

def main():
    
    #cc_dict = read_cc(cc_filename)
    
    #geolocation = read_geolocation_file(geo_filename)

    #delegation = read_delegation_file(del_filename)

    bd, pfix_dict = read_bd_file(dd_filename, bd_filename)
    
    write_bd_output(pfix_dict, bd_output_filename)

    #write_output_table(geolocation, delegation, cc_dict, bd)

main()

sys.stderr.write("writing " + output_filename + "\n")
sys.stderr.write("writing " + bd_output_filename + "\n")

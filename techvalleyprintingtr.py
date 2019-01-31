from __future__ import with_statement
#usage python overtime_transit_path_metric.py country False False 20180301 > logs/country.txt 2>&1
import subprocess
from multiprocessing import Pool
import os
import bz2
import signal
import time
import csv
import sys
import numpy as np
import socket, struct #Para las funciones de transformacion
import radix
import json_lines
import json
# Create a new tree
rtree = radix.Radix()
stub_input = str(sys.argv[3])
if stub_input == 'True': #use stub input file
    testing_string = '.stub'
else:
    testing_string = ''
#testing_string = '' 
#testing_file = False
#testing_file = True
#current_country = "UY"
current_country = str(sys.argv[1])
testing_input = str(sys.argv[2])
if testing_input == 'True': #print paths from specific ASes
    testing_file = True
else:
    testing_file = False

#parsing the required AS-path file to read 
current_snapshot = sys.argv[4]

peer_address_geo_file = "20180301.geo_line.tr.jsonl"

heatmap_filename = "/project/mapkit/agamerog/country_asn_analysis/heatmap/" + current_country + ".csv"
cc_filename = "/project/mapkit/agamerog/country_asn_analysis/cc_to_name.csv"
#cc_filename = "/project/mapkit/agamerog/country_asn_analysis/20180301.asns.jsonl"
bd_filename = "/project/mapkit/agamerog/country_asn_analysis/bd/" + current_snapshot + ".txt" + testing_string
#BG Files need to be ungzipped
bg_filename = "/project/mapkit/agamerog/country_asn_analysis/geolocation_bgp/pfxgeo/pfx-to-country." + current_snapshot[:4] + "-" + current_snapshot[4:6]+ "-" + current_snapshot[6:] + testing_string

ratio_file = "/project/mapkit/agamerog/country_asn_analysis/logs/ratio/" + current_country + ".txt"
global_summary_file = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/global_summary.txt"

first_as = sys.argv[5]
second_as = sys.argv[6]

aspath_filename = "/project/mapkit/agamerog/country_asn_analysis/as-rank-ribs/" + current_snapshot + ".all-paths.bz2" + testing_string #+ '.stub' #AGG2 remove

#if current_snapshot == '20180301':
#    aspath_filename = current_snapshot + ".all-paths.bz2" + testing_string

if stub_input == 'True':
    aspath_filename = "/project/mapkit/agamerog/country_asn_analysis/20180301.all-paths.v4only.txt.stub"

directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/" + current_snapshot + "/"

os.system(directory_creation_string)
directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/origin/" + current_snapshot + "/"

os.system(directory_creation_string)

directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/sorted_summaries/" + current_snapshot

os.system(directory_creation_string)

directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/sorted_summaries/" + current_snapshot + "/"
os.system(directory_creation_string)
country_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/" + current_snapshot + "/" + current_country + ".nodiscount.csv"
filter_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/" + current_snapshot + "/" + "filter." + current_country + ".csv" 

#asrel_filename = '/project/mapkit/agamerog/country_asn_analysis/20180301.as-rel.txt'
#asrel_filename = '/data/external/as-rank-ribs/' + current_snapshot + '/' + current_snapshot + '.as-rel.txt.bz2'
asrel_filename = '/project/mapkit/agamerog/country_asn_analysis/as-rank-ribs/' + current_snapshot + '.as-rel.txt.bz2'
if current_snapshot == '20180301':
    asrel_filename = current_snapshot + '.as-rel.txt.bz2'
if testing_file:
    country_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/" + current_country + ".test.csv"
origin_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/origin/"+ current_snapshot + '/' + current_country + ".csv"

transit_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/top." + current_country + ".nodiscount.csv"

nationality_filename = "/project/comcast-ping/stabledist/mapkit/code/ATIstatistics/DomesticInternational/" + current_country + ".csv"

extended_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/" + current_snapshot + "/" + "ext." + current_country + ".nodiscount.csv"

os.system('ulimit -d 20000000; ulimit -m 20000000; ulimit -v 20000000')

#sys.stderr.write("writing " + output_filename + "\n")

ignore_multi_hop_collectors = set(["routeviews2","routeviews3","routeviews4","nmax","rrc18","sg"])

def read_origin_file():
    origin_dict = dict()
    with open(origin_filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + origin_filename + "\n")
            sys.exit()

        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers
            origin_perc = float(row.split(',')[2])
            origin_num = 256 * int(float(row.split(',')[1])) #number of addresses
                #and percentage of country
            asn = row.split(',')[0].split('-')[0]
            origin_dict[asn] = [origin_num, origin_perc]

    return origin_dict

#global_origin_as_dict = read_origin_file()

def read_transit_file():
    transit_dict = dict()
    with open(transit_filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + transit_filename + "\n")
            sys.exit()

        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers
            transit_rank = int(row.split(',')[2])
            #if transit_rank > 10: #look at top 10 of Transit ASes ONLY
            #    break
            country_ases = int(row.split(',')[14])
            asn = row.split(',')[3].split('-')[0]
            transit_dict[asn] = [transit_rank, country_ases]

    return transit_dict

#global_top_transit_as_dict = read_transit_file()

def read_nationality_file():
    nation_dict = dict()
    with open(nationality_filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + nationality_filename + "\n")
            return nation_dict

        for i in range(len(ipmap_list)):
            if i == 0:
                continue
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers
            cc_nationality = row.split(',')[2]
            if cc_nationality == "XX":
                as_nationality = "Global"
            if cc_nationality == current_country:
                as_nationality = "Domestic"
            else:
                as_nationality = "International"
            #as_nationality = row.split(',')[2]
            #if transit_rank > 10: #look at top 10 of Transit ASes ONLY
            #    break
            asn = row.split(',')[1].split('-')[0]

            nation_dict[asn] = as_nationality

    return nation_dict

global_nationality_as = read_nationality_file()

def init_dig_deeper_file():
    with open (extended_filename, 'w+') as f:
        f.write('#Transit AS ATI rank, Nr. addresses in country, transit ASN-ASName, origin ASN-ASName, TI, Origin AS Nr. Addresses, Origin AS Perc. Country Addresses, Transit AS Nr. Addresses originated, Transit AS Perc. Country Addresses, Transit AS Nationality, Origin AS Nationality \n')
        f.close()

def read_geo_peers():
    peer_ip_countries = dict()
    try:
        with json_lines.open(peer_address_geo_file) as f:
            #thing = json.load(f)
            for item in f:
                #print(item["collector"])
                if item["collector"] in ignore_multi_hop_collectors:
                    continue #ignore multi-hop collectors for now
                col = item["collector"]
                #print (col)
                for monitor in item["peers"]:
                    if col != "caida":
                        ip_address = str(monitor["peer_address"])
                        if ip_address in peer_ip_countries:
                            continue #ignore peers already read from a different collector
                        #checksum to determine if monitor is both full feed and confidence == 1
                        try:
                            ffeed = int(monitor["full_feed"])
                        except ValueError:
                            #print ("monitorFails full-feed " + str(monitor["peer_address"]))
                            continue #no full feed value, ignore

                        try:
                            conf = int(monitor["confidence"])
                        except ValueError:
                            #print ("monitorFails confidence " + str(monitor["peer_address"]))
                            continue #no confidence value, ignore

                        csum = conf + ffeed
                        if csum < 2:
                            #print ("monitorFails confidence or full feed " + str(monitor["peer_address"]))
                            continue #ignore monitors that aren't both full feed and confidence 1
                    else:
                        ip_address = str(monitor["dns_name"])

                    try:
                        country = str(monitor["final_country"])
                    except KeyError or ValueError:
                        #print ("monitorFails final country " + str(monitor["peer_address"]))
                        continue #no final country value, ignore
                    #print ("monitorPass " + str(monitor["peer_address"]))
                    peer_ip_countries[ip_address] = country
    except:
        sys.stderr.write("\n something went wrong opening " + peer_address_geo_file + "\n")
        sys.exit()
    return peer_ip_countries

def read_astypes():
    AT = open('/project/mapkit/agamerog/country_asn_analysis/20180301.as2types.txt', 'rb')
    ATdict = {}
    for lines in AT:
        if len(lines) > 0 and lines[0] != '#':
            ASnumstr = int(lines.split('|')[0]) #grab ASN
            AStype = str(lines.split('|')[2])
            if 'Transit' in AStype:
                ATdict[ASnumstr] = 0
            elif 'Content' in AStype:
                ATdict[ASnumstr] = 1
            else:
                ATdict[ASnumstr] = 3
    AT.close()
    return ATdict

def read_asrel_file():
    asrel_set = set()
    
    with bz2.BZ2File(asrel_filename, "r") as f:
    #with open (asrel_filename,'rb') as f:
        try:
            rows = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + asrel_filename + "\n")
            sys.exit()

        for i in range(len(rows)):
            if '#' in rows[i]:
                continue
            row = rows[i].strip('\n')
            row_rel = row.split('|')[2]
            if int(row_rel) != -1:
                continue
            else:
                provider = row.split('|')[0]
                customer = row.split('|')[1]
                p2c = provider + ':' + customer
                asrel_set.add(p2c)
        return asrel_set

def read_cc(filename):
    cc_dict = {}
    with open (filename,'rb') as f:
        rows = f.readlines()
        for i in range(len(rows)):
            cc = rows[i].split(',')[1]
            name = rows[i].split(',')[0]
            cc_dict[cc] = name
        return cc_dict

def read_filter():
    cc_filter = set()
    with open (filter_filename,'rb') as f:
        rows = f.readlines()
        for i in range(len(rows)):
            row = rows[i].strip('\n')
            cc_filter.add(row)
    return cc_filter



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

def read_geolocation_file(filename):
    country_set = set()
    prefix_origin = dict()
    with open(filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + filename + "\n")
            sys.exit()
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping comments
            prefix_length = int(row.split('|')[0].split('/')[1])
            if prefix_length > 24:
                continue #ignore anything larger than a /24
            if current_country not in row:
                continue #read only prefixes in this country
            #print row
            country_set.add(row.split('|')[0])
            #asn = row.split('|')[3]
            #ip_count = row.split('|')[2] #AGG keeping asn and ip_count for FUTURE
            asn = row.split('|')[3]
            tmp = 32.0 - np.log2(int(row.split('|')[2]))
            prefix_length = 2**(24 - int(tmp))
            if int(prefix_length) < 1:
                prefix_length = 1 #If less than 256 address still bill one slash 24 for consistency with transit
            key = pad_prefix(row.split('|')[0]) + ':' + asn
            if key in prefix_origin:
                prefix_origin[key] = prefix_origin[key] + prefix_length
            else:
                prefix_origin[key] = prefix_length
        return country_set, prefix_origin

def read_delegation_file(filename, country_set):
    #with open(filename, 'rb') as f: #import file
    #    try:
    #        ipmap_list = f.readlines()
    #    except:
    #        sys.stderr.write("\n something went wrong opening " + filename + "\n")
    #        sys.exit()
    #    for i in range(len(ipmap_list)):
    #        row = ipmap_list[i]
    #        if '#' in row:
    #            continue #skip header #AGG need to automate skipping headers
    #        if current_country not in row:
    #            continue #read only prefixes in this country
    #        prefix_length = int(row.split('|')[0].split('/')[1])
    #        if prefix_length > 24:
    #            continue
    #        country_set.add(row.split('|')[0])
            #asn = row.split('|')[2]
            #ip_count = row.split('|')[XX] #AGG keeping asn and ip_count for FUTURE
            #first we need vishwesh to quantify the overlap

    return country_set

def read_asname():

    #AS = open('/home/agamerog/plots/ddc/AS-table.txt', 'rU')
    #ASdict = {}
    #for lines in AS:
    #    if len(lines) > 0 and lines[0] == 'A':

    #        ASnumstr = lines.split()[0][2:] #throw away the AS
    #        AStextlist = lines.split()[1:10]
    asrank_file = '20180301.asns.jsonl'
    ASdict = dict()
    with open (asrank_file,'rb') as f:
        for data in json_lines.reader(f):
            ASnumstr = str(data["asn"])
            if ASnumstr in ASdict:
                continue
            try:
                AStextlist = list(data["org_name"])
                #print AStextlist
                
            except KeyError:
                AStextlist = 'None'
            AStextlist = "".join(AStextlist).encode('utf-8').replace(',','')
            AStextlist = AStextlist[:36]
            #ASdict[ASnumstr] = "".join(AStextlist).replace(',','')
            ASdict[ASnumstr] = AStextlist
        #AS.close()
    return ASdict

def sort_by_length(subtree):
    for i in range(len(subtree)):
        length = int(subtree[i].split('/')[1])
        if length >= 10:
            continue
        if length == 9:
            subtree[i] = subtree[i].replace('/9','/09')
        elif length == 8:
            subtree[i] = subtree[i].replace('/8','/08')
    return sorted(subtree, key=lambda x: int(x[-2:]), reverse=True)

def get_prefixes(subtree):
    list_pfx = []
    for i in range(len(subtree)):
        list_pfx.append(subtree[i].prefix)
    return list_pfx

def get_tree_keys(sorted_prefixes):
    #returns a list of strings corresponding to monitors
    all_monitors = set()
    for i in range(len(sorted_prefixes)):
        rnode = rtree.search_exact(sorted_prefixes[i])
        current_keys = rnode.data.keys() #key is monitor
        for j in range(len(current_keys)):
            all_monitors.add(current_keys[j])
            
    return all_monitors
    
def pad_prefix(pfx):
        length = int(pfx.split('/')[1])
        if length >= 10:
            return pfx
        if length == 9:
            return pfx.replace('/9','/09')
        elif length == 8:
            return pfx.replace('/8','/08')

def print_specific_ASes(raw_path, row, print_dict):
    path = raw_path.split('|')
    looking_set = set(['27766', '27746', '28089', '61471', '27952', '17147', '11562', '27862','22975', '28089', '27957', '11694', '14795','27952','28102'])
    #looking_set = set(['37468','37271','6453','27064','701','513'])
    for i in range(len(path)):
        if path[i] in looking_set:
            if i == 0:
                string = ' hostas'
            elif i < (len(path)-1):
                string = ' transit'
            else:
                string = ' origin'
            appending = row + string
            if path[i] in print_dict:
                print_dict[path[i]].append(appending)
            else:
                print_dict[path[i]] = [appending]
    return print_dict

def print_test(print_dict):

    outfile = '/project/mapkit/agamerog/country_asn_analysis/testing/<asn>.'  + current_country + '.txt'
    for asn in print_dict:
        filename = outfile.replace('<asn>',asn)
        with open (filename, 'w+') as f:
            sys.stderr.write('saving ' + filename + '\n')
            #print print_dict[asn]
            for i in range(len(print_dict[asn])):
                f.write(print_dict[asn][i])
                f.write('\n')

def path_cut_peak(path, asrel_set):
    split_path = path.split('|')
    ignore_path = True
    excluders = set
    for i in range( (len(split_path) - 1 ) ):
        test_provider = split_path[i]
        test_customer = split_path[i+1]
        possible_p2c = test_provider + ':' + test_customer
        if possible_p2c in asrel_set:
            ignore_path = False
            break
        #remove AS to the left below
        if i == 0:
            continue #make sure we keep host AS (used for collector)
        removing_AS = split_path[i] + '|'
        path = path.replace(removing_AS,'')

    if ignore_path:
        return ''
    else:
        return path

def read_as_path_file(file_list, country_set, asrel_set, mon_dict):
    #print filename
    #print country_set
    #print asrel_set
    #print mon_dict
    print_dict = dict()
    as_dict = dict()
    pfix_dict = dict()
    single_path_enforce = set()
    transit_ases = set()
    origin_ases = set()
    prefix_collector = dict()
    already_parsed_mon_prefixes = set()
    #build tree of prefixes and paths
    path_provider = 0
    path_count = 0
    dup_pfx_monitors = 0
    count_first = 0
    count_second = 0
    count_both = 0
    count_any = 0
    origins = set()
    for j in range(len(file_list)):

        with bz2.BZ2File(file_list[j], 'r') as f:
            try:
                ipmap_list = f.readlines()
            except:
                sys.stderr.write("\n something went wrong opening " + file_list[j] + "\n")
                continue

            for i in range(len(ipmap_list)):
                bool_first = False
                bool_second = False

                row = ipmap_list[i].strip('\n')
                if '#' in row:
                    continue
                aspath_prefix = row.split(' ')[2]
                if aspath_prefix not in country_set:
                    continue #read only prefixes in this country

                path = row.split(' ')[1]
                path_components = path.split('|')
                if '23456' in path_components:
                    continue
                    #skip paths with reserved AS

                new_path = path_cut_peak(path, asrel_set)
                if len(new_path) == 0:
                    path_count = path_count + 1
                    continue #path does not have a p2c relationship
                else:
                    path_count = path_count + 1
                    path_provider = path_provider + 1
                    path = new_path
                #print path
                mon = row.split(' ')[4]
                #print ("monitor " + mon)
                if mon not in mon_dict:
                    #sys.stderr.write('\nMonitor not in geo-dictionary ' + mon + '\n')
                    #print (path)
                    continue #monitor with unknown location

                mon_country = mon_dict[mon]
                if mon_country == current_country:
                    #sys.stderr.write('\nMonitor in target country ' + current_country + ' ' + mon + '\n')
                    continue

                mon_prefix = mon + ':' + aspath_prefix
                if mon_prefix in already_parsed_mon_prefixes:
                    dup_pfx_monitors = dup_pfx_monitors + 1
                    continue
                else:
                    already_parsed_mon_prefixes.add(mon_prefix)
                #collector = row.split(' ')[0]
                if '|' in path:
                    collector = path.split('|')[0]
                    #collector = tmp
                else:
                    collector = path
                new_path_components = path.split('|')
                for j in range(len(new_path_components)):
                    if j == (len(new_path_components) - 1):
                        continue #ignore origin
                    if new_path_components[j] == second_as:
                        count_second = count_second + 1
                        bool_second = True
                    if new_path_components[j] == first_as:
                        count_first = count_first + 1
                        bool_first = True
                if bool_second == True and bool_first == True:
                    count_both = count_both + 1
                count_any = count_any + 1
                origins.add(path.split('|')[-1])

    print(first_as + " = " + str(count_first))
    print(second_as + " = " + str(count_second))
    print("both = " + str(count_both))
    print("number of influenced origins = " + str(len(origins)))
    print(count_any)
    #print("origins influenced = " + str(sorted(list(origins))))
        #print collector

    return as_dict, pfix_dict, origin_ases, transit_ases

def update_matrices(monitor_node, current_weight, used_length, prefix_length, origin_ases, transit_ases, as_dict, pfix_dict, prefix_collector, tree_prefix, parsed_prefix_collectors):
            #count each prefix-path combination once only
#    print prefix_length
#    print used_length
#    print current_weight
#
    
    #print 'prefix_length inside update_matrices = ' + str(prefix_length)
    #print 'used_length inside update_matrices = ' + str(used_length)
    if (used_length >= prefix_length):
        print 'prefix_length inside update_matrices = ' + str(prefix_length)
        print 'used_length inside update_matrices = ' + str(used_length)
        print 'current_weight inside update_matrices = ' + str(current_weight)
        
    prefix_length = float(prefix_length - used_length) #/ float(current_weight) #move weight to later stage
    #print used_length
#    print prefix_length
    #parsed_prefix_collectors = set()
    for i in range(len(monitor_node)):
        #print 'node = ' + str(monitor_node[i])
        path = monitor_node[i][0]
        collector = monitor_node[i][1]
        #print collector
        key = tree_prefix + ':' + collector
        try:
            weight = prefix_collector[key]
        except KeyError:
            weight = 1
            sys.stderr.write('\n Weight key not found. Assigning 1. ' + key)
        this_prefix_length = float(prefix_length) / float(weight)
#        print 'prefix_length after weighting = ' + str(prefix_length)
        #print path
        #print prefix_length
        if '|' in path:
            origin = path.split('|')[-1]
            origin_ases.add(origin)
            components = path.split('|')
            transit_list = []#AGG DEBUG double check that this is working correctly
        else:
            origin = path
            origin_ases.add(origin)
            components = [origin]
            transit_list = []

        for i in range(len(components)):
            if i == 0:
                continue #ignore host AS
            position = len(components) - i
            #transit list is a list of tuples with (transit_ASN, position_along_path_from_origin)
            #this list does not include the host AS
            tup = [components[i], position]
            transit_list.append(tup)

        #compute dictionaries in 2.a and 2.b
        for i in range(len(transit_list)):
            key = collector + ':' + origin + ':' + transit_list[i][0]
            transit_ases.add(transit_list[i][0])
            pos_for_count = transit_list[i][1] 
            if transit_list[i][1] > 1:
                #transit_weight = float(1)/float(transit_list[i][1]-1) # THIS IS WHERE THE WEIGHT FOR POSITION FILTER IS DONEa
                transit_weight = float(1)
            else:
                transit_weight = float(1)
            #print "transit " + str(transit_list[i][0])
            #print "weight " + str(transit_weight)
            if key in pfix_dict: 
                #pfix_dict is a 3-tuple (prefix_length_sum(div by transit weight), position_sum, #paths)

                pfix_dict[key][0] = float(pfix_dict[key][0]) + float(this_prefix_length)*transit_weight
                if transit_list[i][1] != 1:
                    pfix_dict[key][1].append(transit_list[i][1])
                    pfix_dict[key][2] = pfix_dict[key][2] + 1
                    try:
                        pfix_dict[key][3][pos_for_count] = pfix_dict[key][3][pos_for_count] + 1
                    except KeyError:
                        pfix_dict[key][3][pos_for_count] = 1
            else:
                if transit_list[i][1] != 1:
                    pfix_dict[key] = [this_prefix_length*transit_weight, [transit_list[i][1]], 1, dict()]

                    pfix_dict[key][3][pos_for_count] = 1
                else:
                    pfix_dict[key] = [this_prefix_length*transit_weight, [0], 0, dict()]
 
            if origin == transit_list[i][0]: #update denominator (once per prefix-collector)
                key = collector + ':' + origin
                if key in as_dict:
                    as_dict[key] = as_dict[key] + this_prefix_length
                else:
                    as_dict[key] = this_prefix_length
    return as_dict, pfix_dict, origin_ases, transit_ases, parsed_prefix_collectors, weight

def sort_filter_hegemony(heg_key, heg_value, discard_keys, discard_matrix):
    bottom = len(heg_value) // 10 #discard bottom 10% of values
    top = len(heg_value) - len(heg_value) // 10 #discard top 10 % of values
    #print "*****"
    #print heg_value
    #print bottom 
    #print top
    #print "*****"
    
    if top == len(heg_value):

        #print "HERE"
        return discard_keys, discard_matrix #too few values, discard none
    sorted_values = sorted(heg_value, key=lambda x: x[1])
    for i in range(len(sorted_values)):
        if i < bottom or i >= top:
            origin = heg_key.split(':')[0]
            transit = heg_key.split(':')[1]
            if origin == transit:
                continue #no need to discount from denominator yet (that is done separately)
            key = sorted_values[i][0] + ':' + heg_key
            discard_keys.add(key)
            print "hegemony discarding " + str(key) 
            #print "discard value = " + str(sorted_values[i][2])
            if heg_key in discard_matrix:

                discard_matrix[heg_key] = discard_matrix[heg_key] + sorted_values[i][2]
            else:
                discard_matrix[heg_key] = sorted_values[i][2]

    return discard_keys, discard_matrix
            

def write_country_output(as_dict, pfix_dict, origin_ases, \
        transit_ases, country_filename, asname_dict, global_origin_as_dict):

    matrix_dict = dict() #dictionary for saving as-path influence matrix
    denom_dict = dict() #dictionary for saving the denominator per asn
    #denom_dict = as_dict

    heg_dict = dict() #dictionary for implementing hegemony filtering

    already_parsed = set()
    transits_parsed = set()
    printing = False
    discard_keys = set()
    discard_matrix = dict()
    for pfix in pfix_dict:
        col = str(pfix.split(':')[0])
        #print col
        origin = str(pfix.split(':')[1])
        transit = str(pfix.split(':')[2])
        key = origin + ':' + transit    
        #print key       
        tmp_num = pfix_dict[pfix][0]
        tmp_key = col + ':' + origin
        tmp_denom = as_dict[tmp_key]
        #heg_dict is a 4-tuple with (collector, num_prefix_length, denom_prefix_length, hegemony_metric)
        tmp_heg = float(tmp_num) / float(tmp_denom)
        tmp_tup = [col, tmp_heg, tmp_denom]
        #print 'key = ' + str(key)
        #print 'tup = ' + str(tmp_tup)
        if key in heg_dict:
            
            heg_dict[key].append(tmp_tup)
        else:
            heg_dict[key] = [tmp_tup]
    #print heg_dict
    for heg in heg_dict:
        discard_keys, discard_matrix = sort_filter_hegemony(heg, heg_dict[heg], discard_keys, discard_matrix)    
    #print discard_matrix
    overall_positions = dict()
    #compute raw count
    for pfix in pfix_dict:
        if pfix in discard_keys:
            continue
        #hacky code to count distribution values
        for pos_it in pfix_dict[pfix][3]:
            try:
                overall_positions[pos_it] = overall_positions[pos_it] + pfix_dict[pfix][3][pos_it]
            except KeyError:
                overall_positions[pos_it] = pfix_dict[pfix][3][pos_it]
                
        col = str(pfix.split(':')[0])
        origin = str(pfix.split(':')[1])
        transit = str(pfix.split(':')[2])
        key = origin + ':' + transit
        if origin != transit and origin == '27952':
            print ("debugging increasing num by " + str(pfix_dict[pfix][0]) + " for prefix " + str(pfix) + " collector " \
                + str(col))
        if key in matrix_dict and origin != transit:
            matrix_dict[key][0] = matrix_dict[key][0] + pfix_dict[pfix][0]
            matrix_dict[key][2] = matrix_dict[key][2] + pfix_dict[pfix][2]
            if matrix_dict[key][1] is not None:
                if pfix_dict[pfix][1] is not None:
                    matrix_dict[key][1] = matrix_dict[key][1] + pfix_dict[pfix][1]
                else:
                    matrix_dict[key][1] = []
            else:
                matrix_dict[key][1] = []
#            transits_parsed.add(transit_key)
        elif key not in matrix_dict and origin != transit:
            if pfix_dict[pfix][1] is not None:
                matrix_dict[key] = [pfix_dict[pfix][0], pfix_dict[pfix][1], pfix_dict[pfix][2]]
            else:
                matrix_dict[key] = [pfix_dict[pfix][0], [], pfix_dict[pfix][2]]
        #try:
        #    print key
        #    print matrix_dict[key][1]
        #    print "******"
        #except KeyError:
        #    nothing = 1
        if origin == transit:
            key = col + ':' + origin
            if origin == '27952':
                print ("debugging increasing denom by " + str(as_dict[key]) + " for prefix " + str(pfix) + " collector "\
                    + str(col) )
            if origin in denom_dict:
                denom_dict[origin] = denom_dict[origin] + as_dict[key]
            else:
                denom_dict[origin] = as_dict[key]

    #cc_filter = read_filter()

    with open (country_filename,'w+') as f:
        #g = open(heatmap_filename, 'w+')
        ext_file = open(extended_filename, 'a+')
        
        global_summary = open(global_summary_file, 'a')
        
        #g.write('transit,origin,cell\n')
        transits = list(transit_ases)
        origins = list(origin_ases)
        line = 'ASN-ASNAME,'
        for i in range(len(origins)):
            this_asname = fetch_asname(origins[i], asname_dict)
            line = line + this_asname + ','
        line = line + 'pos_num,pos_denom,row_sum,#overzero,nonzero_average,pos_min;pos_25th;pos_median;pos_mean;pos_75th;pos_max'
        f.write(line + '\n') # write header (origins)
        
        #now for the real matrix...
        for i in range(len(transits)):
            this_asname = fetch_asname(transits[i], asname_dict)
            line = this_asname + ','
            row_sum = 0
            row_count = 0
            pos_num = []
            pos_denom = 0
            pos_average = '0;0.0;0;0.0;0.0;0'
            for j in range(len(origins)):
                key = origins[j] + ':' + transits[i]
                try:
                    num = matrix_dict[key][0]
                except KeyError:
                    print "key error origin-transit = " + str(key)
                    num = 0
                try:
                    denom = denom_dict[origins[j]]
                except KeyError:
                    print "key error origin = " + str(origins[j]) 
                    denom = 0
                    #sys.stderr.write('could not find origin key ' + origins[j] + '\n')
                try:
                    discount = discard_matrix[key]
                except KeyError:
                    print "no hegemony found for " + key + " assigning zero"
                    discount = 0
                if num == 0 or denom == 0:
                    cell = '0.0'
                #elif origins[j] == transits[i]:
                #    cell = '1.0'
                    #account for special case which my data structure can't handle (and is trivial)
                else:
                    cell = (float(num) / float(denom-discount))
                    if origins[j] == "27952" and (transits[i] == 'second' or transits[i] == '7004'):
                        print ("debugging " + str(origins[j]) + " " + str(transits[i]) + " " + str(num) + \
                                " " + str(denom) + " " + str(discount))
                    if float(cell) > 1.0:
                        print ' ' + current_country + ' ' + str(cell) +' ' + str(origins[j]) + ' ' + str(transits[i])
                    if origins[j] == transits[i]:
                        cell = '0.0'
                    else:
                        row_sum = row_sum + cell
                        #if matrix_dict[key][1] is not None:
                        pos_num = pos_num + matrix_dict[key][1] #list with all positions
                        #print pos_num
                        pos_denom = pos_denom + matrix_dict[key][2]

                    if float(cell) > 0.0:
                        row_count = row_count + 1
                        #if transits[i] in cc_filter:
                            #g.write(transits[i] + ',' + origins[j] + ',' + str(cell) + '\n')
                    if float(cell) > 1.0 and origins[j] != transits[i]:
                        print "WHY " + str(cell) +' ' + str(origins[j]) + ' ' + str(transits[i])
                        cell = 1.0
                    
                #cell = str(num) + ' / ' + str(denom-discount) #AGG CHANGE BACK TEST
                line = line + str(cell) + ',' # this is where each cell is written
                if float(cell) > 0.0: #write distribution values to file
                    global_summary_line = str(round(cell,12)) + '\n'
                    global_summary.write(global_summary_line)

                #write extended file for top ASes
                if round(float(cell),2) >= 0.49:
                    #if transits[i] in global_top_transit_as_dict:
                    buffer_ext = ''
                    transit_rank = 'ignore' + ',' + 'ignore'
                    buffer_ext = buffer_ext + transit_rank + ','
                    transit_label = fetch_asname(transits[i], asname_dict)
                    buffer_ext = buffer_ext + transit_label + ','
                    orig_label = fetch_asname(origins[j], asname_dict)
                    buffer_ext = buffer_ext + orig_label + ','
                    buffer_ext = buffer_ext + str(cell) + ','
                    if origins[j] in global_origin_as_dict:
                        buffer_ext = buffer_ext + str(global_origin_as_dict[origins[j]][0]) + ',' + str(global_origin_as_dict[origins[j]][1]) + ','
                    else:
                        buffer_ext = buffer_ext + '0' + ',' + '0.0' + ','
                    if transits[i] in global_origin_as_dict:
                        buffer_ext = buffer_ext + str(global_origin_as_dict[transits[i]][0]) + ',' + str(global_origin_as_dict[transits[i]][1]) + ','
                    else:
                        buffer_ext = buffer_ext + '0' + ',' + '0.0' + ','
                    if transits[i] in global_nationality_as:
                        buffer_ext = buffer_ext + str(global_nationality_as[transits[i]]) + ','
                    else:
                        buffer_ext = buffer_ext + 'unassigned,'
                    if origins[j] in global_nationality_as:
                        buffer_ext = buffer_ext + str(global_nationality_as[origins[j]]) 
                    else:
                        buffer_ext = buffer_ext + 'unassigned'
                    buffer_ext = buffer_ext + '\n'
                    ext_file.write(buffer_ext)
 
            if row_count > 0:
                average = round(float(row_sum)/float(row_count), 3)
                #pos_average = round(float(pos_num)/float(pos_denom), 3)
                if pos_num is not None:
                    mean = str(round(np.mean(pos_num),2))
                    median = str(int(np.median(pos_num)))
                    one_quarter = str(int(np.percentile(pos_num, 25)))
                    three_quarters = str(int(np.percentile(pos_num, 75)))
                    minimum = str(int(np.amin(pos_num)))
                    maximum = str(int(np.amax(pos_num)))
                    pos_average = minimum + ';' + one_quarter + ';' + median + ';' + mean + \
                           ';' + three_quarters + ';' + maximum
                    #pos_average = '0;0.0;0;0.0;0.0;0'
            else:
                average = 0.0
                pos_average = '0;0.0;0;0.0;0.0;0'
            line = line + str(pos_denom) + ',' + str(0) + ',' + str(round(row_sum,3)) + ',' + str(row_count) + ',' + str(average) + ',' + str(pos_average)
            f.write(line + '\n') # write header (origins)

    pos_file = country_filename.replace('.csv','.pos.csv')
    with open (pos_file,'w+') as g:
        for pos_it in overall_positions:
            g.write(str(pos_it) + ',' + str(overall_positions[pos_it]) + '\n')    

def fetch_asname(asn, asname_dict):
    try:
        line = asn + '-' + asname_dict[asn] 
    except KeyError:
        line = asn + '-unknown'
    return line

def sort_country_output():
    command = """awk -F, '{print $(NF-3)","$1","$(NF-4)","$(NF-5)","$(NF-2)","$(NF-1)","$(NF)","NF}' /project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/""" + current_snapshot + """/<CC>.nodiscount.csv | sort -nr > /project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/sorted_summaries/""" + current_snapshot + """/<CC>.nodiscount.csv"""
    run = command.replace('<CC>', current_country)
    try:
        os.system(run)
        printing_string = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/sorted_summaries/" + current_snapshot + "/" + \
                current_country + ".nodiscount.csv"
        sys.stderr.write('\nsaving ' + printing_string + '\n')
    except:
        sys.stderr.write('\n could not run ' + run + '\n')

def gen_origin_file(filename, country_set, asrel_set, asname_dict, prefix_origin):
    print_dict = dict()
    as_dict = dict()
    pfix_dict = dict()
    single_path_enforce = set()
    transit_ases = set()
    origin_ases = set()
    prefix_collector = dict()
    already_parsed_mon_prefixes = set()
    #build tree of prefixes and paths
    path_provider = 0
    path_count = 0
    dup_pfx_monitors = 0
    #print(filename)
    if '.stub' in filename:
        #print ("HERE")
        f = open(filename, 'rb')
    else:
        f = bz2.BZ2File(filename, 'rb')
    #with bz2.BZ2File(filename, 'rb') as f: #import file
    ipmap_list = f.readlines()
    for i in range(len(ipmap_list)):
        row = ipmap_list[i].strip('\n')
        if '#' in row or ':' in row:
            continue
        #try:
        aspath_prefix = row.split(' ')[2]
        #except IndexError:
            #print(row)
            #print("ROW ROW ROW ROW")
        if aspath_prefix not in country_set:
            continue #read only prefixes in this country

        path = row.split(' ')[1]
        origin = path.split('|')[-1]
        if origin == '23456':
            continue
            #skipping reserved AS
        new_path = path_cut_peak(path, asrel_set)
        orig_prefix = origin + ':' + aspath_prefix
        if orig_prefix in already_parsed_mon_prefixes:
            continue
        else:
            already_parsed_mon_prefixes.add(orig_prefix)
        #collector = row.split(' ')[0]
        if '|' in path:
            collector = path.split('|')[0]
            #collector = tmp
        else:
            collector = path
        #print collector

        if testing_file:
            print_dict = print_specific_ASes(path, row, print_dict)
        node = rtree.search_exact(aspath_prefix)
        if str(node) == 'None':
            rnode = rtree.add(aspath_prefix)
            #create new tree node only if search result is empty

        try: #append to tree key if it exists
            rnode.data[origin].append(aspath_prefix)
        except KeyError:
            rnode.data[origin] = [aspath_prefix]

    if testing_file:
        print_test(print_dict)
    discarded_overlap = 0
    country_list = list(country_set)
    traversed = set()
    parsed_mon_prefixes = set()
    parsed_prefix_collectors = set()
    orig_dict = dict() #number of prefixes (prefix coverage)
    orig_counts = dict() #number of unique prefixes (prefix and length)
    countrysum = 0.0
    for i in range(len(country_list)):
        used_lengths = dict()
        pfx = country_list[i]
        checking_prefix = pad_prefix(pfx) #fixing the sorting by length issue
        if checking_prefix in traversed:
            continue

        sub_prefixes = []
        #print pfx
        try:
            #print pfx
            #print rtree
            shortest = rtree.search_worst(pfx).prefix
        except AttributeError:
            #sys.stderr.write('\nprefix not in tree ' + pfx + '\n')
            continue
        try:
            subtree = rtree.search_covered(shortest)
            sub_prefixes = get_prefixes(subtree)
            sorted_prefixes = sort_by_length(sub_prefixes)

        except AttributeError:
            sys.stderr.write('\n could not find prefix in tree: ' + pfx)
            continue
#        print sorted_prefixes
        #add list of prefixes to already traversed
        for j in range(len(sorted_prefixes)):
            traversed.add(sorted_prefixes[j])
        #print "CP1"
        origins = get_tree_keys(sorted_prefixes)
        #print monitors
        for j in range(len(sorted_prefixes)):
            #used_length
            tree_prefix = sorted_prefixes[j]
            tmp = int(tree_prefix.split('/')[1])
            prefix_length = 2**(24 - tmp)
            current_weight = 1
            rnode = rtree.search_exact(tree_prefix)

            #print prefix_length
            for orig in origins:
                orig_prefix = orig + ':' + tree_prefix
                if orig_prefix in parsed_mon_prefixes:
                    continue
                else:
                    parsed_mon_prefixes.add(orig_prefix)
                try:
                    monitor_node = rnode.data[orig]
                    #current_weight = len(monitor_node)
                    current_weight = 1
                except KeyError:
                    #print "key error?"
                    continue
                old_prefix_length = float(prefix_length)
                this_prefix_length = float(prefix_length)
                new_key = pad_prefix(tree_prefix) + ':' + orig

                if new_key in prefix_origin:
                    this_prefix_length = prefix_origin[new_key]
                if orig in used_lengths:
                    #path = monitor_node[i][0]

                    if used_lengths[orig] >= this_prefix_length:
                        discarded_overlap = discarded_overlap + 1
                        sys.stderr.write('\nAlready covered > length. Ignoring prefix and carrying up. Prefix = ' + str(tree_prefix) + ' family = ' + str(sorted_prefixes) + ' origin = ' + orig + '\n')
                        continue
                    else:
                        countrysum = countrysum + this_prefix_length
#                        new_key = tree_prefix + ':' + orig
                        if orig in orig_dict:
                            orig_dict[orig] = orig_dict[orig] + this_prefix_length
                        else:
                            orig_dict[orig] = this_prefix_length

                        used_lengths[orig] = used_lengths[orig] + old_prefix_length

                else:
                    #print 'used_lengths = 0'
                    countrysum = countrysum + this_prefix_length
                    if orig in orig_dict:
                        orig_dict[orig] = orig_dict[orig] + this_prefix_length
                        orig_counts[orig] = orig_counts[orig] + 1
                    else:
                        orig_dict[orig] = this_prefix_length
                        orig_counts[orig] = 1

                    used_lengths[orig] = old_prefix_length
    halfsum = 0.50*countrysum
    number_ases = 0
    with open(origin_filename,'w+') as f:
        #print ("ORIGINKLNSDF;LHASLKDFNAS;LKDHNFW")
        f.write('OriginASN,OriginASName,#Prefixes,%country\n')
        currentsum = 0.0
        sys.stderr.write("\n writing " + origin_filename + "\n")
        i = 0
        half_reached = False
        for key, value in sorted(orig_dict.iteritems(), key=lambda (k,v): (v,k), reverse=True):
            i = i +1
            name = fetch_asname(key, asname_dict)
            perc = round((100.0 * float(value) / float(countrysum)),3)
            counts = orig_counts[key]
            f.write(str(name) + ',' + str(value) + ',' + str(perc) + '\n')
            currentsum = currentsum + value
            if half_reached:
                continue
            if currentsum >= halfsum:
                number_ases = i
                half_reached = True
#    with open(global_filename, 'a+') as g:
#        g.write(current_country + ',' +str(number_ases) + '\n')
    print ("saving " + origin_filename)
#        print '***'

def read_traceroute_file_list():
    #file_list = ['/project/mapkit/agamerog/country_asn_analysis/as-rank-ribs/20180301.all-paths.bz2']
    file_list = []
    with open('traceroutes/flist.txt','rb') as f:
        rows = f.readlines()
        for i in range(len(rows)):
            row = rows[i].strip('\n')
            if 'test' in row:
                continue
            #print row
            file_list.append(row)
    return file_list

def main():
#    print ("here0")
    asrel_set = read_asrel_file()
#    print ("here1")
    init_dig_deeper_file() #initialize file for top 10 Transit ASes
#    print ("here1.5")
    #cc_dict = read_cc(cc_filename)
    
    mon_dict = read_geo_peers() #read country of monitors
#    print ("here2")
    asname_dict = read_asname() #read asnames into memory
#    print ("here3")
    geolocation, prefix_origin = read_geolocation_file(bg_filename)
#    print ("here4")
    #print geolocation
    list_of_files = read_traceroute_file_list()

    delegation = read_delegation_file(bd_filename, geolocation)
#    print ("here5")
    #gen_origin_file(aspath_filename, delegation, asrel_set, asname_dict, prefix_origin)
    
    global_origin_as_dict = read_origin_file()
#    print ("here6")
    as_dict, pfix_dict, origin_ases, transit_ases = \
            read_as_path_file(list_of_files, delegation, asrel_set, mon_dict)
    #print as_dict
    #print pfix_dict
    #print origin_ases
    #print transit_ases
#    print ("here7")
    #write_country_output(as_dict, pfix_dict, origin_ases, \
    #        transit_ases, country_filename, asname_dict, global_origin_as_dict)
#    print ("here8")
    #sort_country_output()
#    print ("here9")
main()

sys.stderr.write("\n writing " + country_filename + "\n")
sys.stderr.write("\n writing " + extended_filename + "\n")

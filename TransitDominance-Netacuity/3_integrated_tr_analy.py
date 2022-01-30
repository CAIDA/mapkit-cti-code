from __future__ import with_statement
#usage python overtime_transit_path_metric.py country False False snapshot > logs/country.snapshot.txt 2>&1
#usage python overtime_transit_path_metric.py US False False 20180301 > logs/transit/US.20180301.txt 2>&1
#Reads BG and BD files with prefix-to-country mappings
#Reads origin file with percentage of country's addresses originated by each AS
#Reads AS-path file with prefixes, paths, monitors and host ASes
#Produces ATI matrix and list, and heavy reliance dependencies

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
#import newFinalTargetCountriesVEandBO
import newFinalTargetCountries
#from ripe.atlas.sagan import Result
#from ripe.atlas.sagan.traceroute import TracerouteResult, Hop, Packet

# Create a new radix tree to store network prefix information
rtree = radix.Radix()

#If this input is true, the input files are changed to include .stub at the end
#this is useful to test input/output with small prefix-to-country
#mappings and verify that everything is working correctly
input_filename = str(sys.argv[1])
bg_filename = "/project/mapkit/agamerog/country_asn_analysis/geolocation_bgp/pfxgeo/pfx-to-country.2020-04-01"

bd_filename = "/project/mapkit/agamerog/country_asn_analysis/bd/20200401.txt"
#Input file with AS-relationships (to read provider-customer pairs; and peer pairs)
asrel_filename = '/data/external/as-rank-ribs/20200401/20200401.as-rel.txt.bz2'
#targetDict = newFinalTargetCountriesVEandBO.targetCountries
targetDict = newFinalTargetCountries.targetCountries
#Input file with FAO metric per AS. Used to compute FASR outputs
#origin_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/"+ current_snapshot + '/' + current_country + ".csv"

#Input file with the inferred "nationality" of each AS
#nationality_filename = "/project/comcast-ping/stabledist/mapkit/code/ATIstatistics/DomesticInternational/" + current_country + ".csv"

os.system('ulimit -d 4000000; ulimit -m 4000000; ulimit -v 4000000')

#collectors with multi-hop forwarding, ignored
ignore_multi_hop_collectors = set(["routeviews2","routeviews3","routeviews4","nmax","rrc18","sg"])

def read_origin_file():
    #Read file with number of addresses, and the percentage of the country
	#they represent, for each AS with addresses in this country
	#Returns a dictionary where the key is the AS Number and the value
	#is a list with two variables (#addresses and %country)
    origin_dict = dict()
    with open(origin_filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + origin_filename + "\n")
            sys.exit()
        country_total = 0 #count for the entire country (for the '99999' all ASN origin)
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header 
            origin_perc = float(row.split(',')[2])
            origin_num = 256 * int(float(row.split(',')[1])) #number of addresses
                #and percentage of country
            asn = row.split(',')[0].split('-')[0]
            origin_dict[asn] = [origin_num, origin_perc]
            country_total = country_total + origin_num
            if current_country == 'US' and i >= 1000:
                break
        origin_dict['99999'] = [country_total, 100.0] #the country's addresses (100% of them)

    return origin_dict


def init_dig_deeper_file():
	#Open heavy reliance output file and write its header
    with open (extended_filename, 'w+') as f:
        f.write('#Transit AS ATI rank, Nr. addresses in country, transit ASN-ASName, origin ASN-ASName, TI, Origin AS Nr. Addresses, Origin AS Perc. Country Addresses, Transit AS Nr. Addresses originated, Transit AS Perc. Country Addresses, Transit AS Nationality, Origin AS Nationality \n')
        f.close()

def read_geo_peers():
	#Read BGP monitor inferred country. Ignore those where the geolocation is low confidence
	#Returns a dictionary with the key being the monitor's and the value being the 
	#Final assigned country

    peer_ip_countries = dict()
    try:
        with json_lines.open(peer_address_geo_file) as f:
            #thing = json.load(f)
            for item in f:
                if item["collector"] in ignore_multi_hop_collectors:
                    continue #ignore multi-hop collectors 

                for monitor in item["peers"]:
                    ip_address = str(monitor["peer_address"])
                    if ip_address in peer_ip_countries:
                        continue #ignore peers already read from a different collector
                    #checksum to determine if monitor is both full feed and confidence == 1
                    try:
                        ffeed = int(monitor["full_feed"])
                    except ValueError:
                        continue #no full feed value, ignore

                    try:
                        conf = int(monitor["confidence"])
                    except ValueError:
                        continue #no confidence value, ignore
                    
                    csum = conf + ffeed
                    if csum < 2:
                        continue #ignore monitors that aren't both full feed and confidence 1

                    try:
                        country = str(monitor["final_country"])
                    except KeyError or ValueError:
                        continue #no final country value, ignore
                    peer_ip_countries[ip_address] = country
    except:
        sys.stderr.write("\n something went wrong opening " + peer_address_geo_file + "\n")
        sys.exit()


    return peer_ip_countries

def read_asrel_file():
    #Create a set of AS-AS strings
    #Separated by a ':' where one is an inferred transit provider of the other
    #'provider:customer' where provider and customer are both ASNumbers
    transit_set = set()
    peer_set = set()

    with bz2.BZ2File(asrel_filename, "r") as f:
        try:
            rows = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + asrel_filename + "\n")
            sys.exit()

        for i in range(len(rows)):
            if '#' in rows[i]: #skip header
                continue
            row = rows[i].strip('\n')
            row_rel = row.split('|')[2]
            if int(row_rel) == -1:
                provider = row.split('|')[0]
                customer = row.split('|')[1]
                p2cc = provider + ':' + customer #also need to add the ASes themselves for indirect transit
                transit_set.add(p2cc)
            elif int(row_rel) == 0:
                peerone = row.split('|')[0]
                peertwo = row.split('|')[1]
                p2p = peerone + ':' + peertwo
                peer_set.add(p2p)
                p2pp = peertwo + ':' + peerone
                peer_set.add(p2pp)
            #make sure every transit provider of anyone is a transit provider of the country
    return transit_set, peer_set

def systemCall(parameter): #Simple function to run a system call
    os.system(parameter)

def read_delegation_file(filename, parsed_prefixes):
    #Read prefixes geolocated to each country from delegation files
    #Skip prefixes with length 25 or more, as we are measuring in /24 blocks
    with open(filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + filename + "\n")
            sys.exit()
        for i in range(len(ipmap_list)):
            row = ipmap_list[i]
            if '#' in row or 'asn' in row:
                continue #skip header #AGG need to automate skipping headers
            if '_' in row:
                continue #skip MOAS
            prefix_length = int(row.split('|')[0].split('/')[1])
            if prefix_length > 24 or prefix_length < 8:
                continue

            aspath_prefix = row.split('|')[0]
            if aspath_prefix in parsed_prefixes:
                continue #skip prefix that is already in BG

            node = rtree.search_exact(aspath_prefix)
            
            asn = row.split('|')[2]

            cc_pfx = row.split('|')[1]

            if str(node) == 'None':
                node = rtree.add(aspath_prefix)
                #create new tree branch node only if search result is empty

            try: #append to tree key if it exists
                node.data[cc_pfx] =  asn #save path and BGP host AS ("collector")
            except KeyError:
                node.data[cc_pfx] = asn #create list with a sinble path and collector
            #return country_set

def read_asname():
	#Parse organization names from AS rank (and ensure proper encoding)
	#Return dictionary where key is AS Number and value is AS Name
    asrank_file = '../20180301.asns.jsonl'
    ASdict = dict()
    with open (asrank_file,'rb') as f:
        for data in json_lines.reader(f):
            ASnumstr = str(data["asn"])
            if ASnumstr in ASdict:
                continue
            try:
                AStextlist = list(data["org_name"])
            except KeyError:
                AStextlist = 'None'
            AStextlist = "".join(AStextlist).encode('utf-8').replace(',','')
            AStextlist = AStextlist[:36]
            ASdict[ASnumstr] = AStextlist
    #ASdict['99999'] = current_country
    return ASdict

def sort_by_length(subtree):
	#Sort prefixes from most specific to least specific
	#First pad the prefixes of /8 and /9, then sort using lambda calculus
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
	#return prefixes in the subtree as a list 
    list_pfx = []
    for i in range(len(subtree)):
        list_pfx.append(subtree[i].prefix)
    return list_pfx

def get_tree_keys(sorted_prefixes):
    #returns a list of strings corresponding to countries
    all_monitors = set()
    asn = 0
    found_as = False
    for i in range(len(sorted_prefixes)):
        rnode = rtree.search_exact(sorted_prefixes[i])
        current_keys = rnode.data.keys() #key is monitor
        for j in range(len(current_keys)):
            all_monitors.add(current_keys[j])
            if not found_as:
                asn = rnode.data[current_keys[j]]
                found_as = True
            
    return all_monitors, asn
    
def pad_prefix(pfx):
	#Simple function that pads prefixes so sorting on last two characters of a string
	#Works correctly
        length = int(pfx.split('/')[1])
        if length >= 10:
            return pfx
        if length == 9:
            return pfx.replace('/9','/09')
        elif length == 8:
            return pfx.replace('/8','/08')

def print_specific_ASes(raw_path, row, print_dict):
    #Print paths containing specific ASes and which role they have
    #(host, transit, origin)
    #This function is only called if the appropriate argument is True
    #(False by default, so function is unused)
    #TODO (before releasing to the public): remove testing functions
    path = raw_path.split('|')
    looking_set = set(['99999','25543','37282','328316','37662'])
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
    #Save paths seen for specific ASes
    #This function is only called if the appropriate argument is True
    #(False by default, so function is unused)
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
    #determine if there is a p2c relationship along path
    #if so return path up to highest upstream provider of origin
    #otherwise return empty path
    split_path = path.split('|')
    ignore_path = True
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

def read_approved_targets():
    outset = set()
    f = open('/project/mapkit/agamerog/country_asn_analysis/peeringdb/final_feb_13_target_country_as_ip.csv' + testing_string, "r")
    try:
        ipmap_list = f.readlines()
    except:
        sys.stderr.write("\n something went wrong opening " + filename + "\n")
        sys.exit()

        #Iterate through lines of AS-paths file
    for i in range(len(ipmap_list)):
        row = ipmap_list[i].strip('\n')
        if '#' in row:
            continue #skip header
        rowCountry = row.split(',')[0]
        if rowCountry != current_country:
            continue
        rowAs = row.split(',')[1]
        outset.add(rowAs)
    return outset

def read_pfx_to_as(filename):

    parsed_prefixes = set()
    f = open(filename, "r")
    try:
        ipmap_list = f.readlines()
    except:
        sys.stderr.write("\n something went wrong opening " + filename + "\n")
        sys.exit()

	#Iterate through lines of AS-paths file
    for i in range(len(ipmap_list)):
        row = ipmap_list[i].strip('\n')
        if '#' in row:
            #print ('SKIPPING')
            #print(row)
            continue #skip header
		#Read prefix and path from line
        aspath_prefix = row.split('|')[0]
        pfx_length = int(aspath_prefix.split('/')[1])

        if pfx_length > 24 or pfx_length < 8:
            continue

        asn = row.split('|')[3]
        if '_' in asn:
            continue
        #AGG recent changes May 5
        bg_assigned = int(row.split('|')[2])

                    #calculate number of /24 blocks in this prefix, based on
                    #the number of addresses assigned to this country
                    #in this prefix
        tmp = 2**(32 - pfx_length)
        if tmp != bg_assigned: #pick only targets where whole of prefix is in country
            continue
        parsed_prefixes.add(aspath_prefix)

        cc_pfx = row.split('|')[1]
        node = rtree.search_exact(aspath_prefix)
        #ipcnt = int(row.split('|')[2])
        if str(node) == 'None':
            node = rtree.add(aspath_prefix)
            #create new tree branch node only if search result is empty

        try: #append to tree key if it exists
            node.data[cc_pfx] =  asn #save path and BGP host AS ("collector")
        except KeyError:
            node.data[cc_pfx] = asn #create list with a sinble path and collector
    return parsed_prefixes

def return_countries_covered_in_prefixes(ip_add):
    #for a given IP, return all the countries represented in the prefixes that cover it
    countries = set()
    asn = 0
    try:
        shortest = rtree.search_worst(ip_add).prefix
        #print("shortest")
        #print(shortest)
    except AttributeError:
        return countries, asn #no prefix covers this IP

    try:
        subtree = rtree.search_covered(shortest)
        sub_prefixes = get_prefixes(subtree)

    except AttributeError:
        return countries, asn #no countries
    #print(sub_prefixes)
    countries_tree, asn = get_tree_keys(sub_prefixes)
    
    for country in countries_tree: #add all countries seen in the tree to the output set of countries
        countries.add(country)
    return countries, asn


 

def fetch_asname(asn, asname_dict):
	#Simple function to return formatted ASN-ASName from dictionary
    try:
        line = asn + '-' + asname_dict[asn] 
    except KeyError:
        line = asn + '-unknown'
    return line


def read_target_ases_countries():
    ases = set() #set of ASes to study ['as1','as2',...]
    country_ases = set() #set of country:ases to study ['CC1:as1', ... 'CC2:as1', ... ]
    countries = set()
 #   filename = '/project/mapkit/agamerog/country_asn_analysis/peeringdb/targetsprobed/target_ips_BDandBG_countries_20200503.csv'

#    f = open(filename, "r")
#    try:
#        ipmap_list = f.readlines()
#    except:
#        sys.stderr.write("\n something went wrong opening " + filename + "\n")
#        sys.exit()

	#Iterate through lines of AS-paths file
    for cc in targetDict:
        curr_ases = targetDict[cc]
        for i in range(len(curr_ases)):

        #row = ipmap_list[i].strip('\n')
        #if '#' in row:
            #print ('SKIPPING')
            #print(row)
        #    continue #skip header
		#Read prefix and path from line
            asn = curr_ases[i]

        #cc = row.split(',')[0]
            cc_asn = asn + ':' + cc
            ases.add(asn)
            country_ases.add(cc_asn)
            countries.add(cc)

    return country_ases, ases, countries

def read_bdrmapit_out(ases):
    country_links_file = '/project/mapkit/agamerog/country_asn_analysis/pipeline/output/bdrmapit_output.csv'
    interface_set = set() #save individual interfaces in set
    interface_to_iconnect = dict() #dict with 'IP':set(['AS1':'AS2','AS1:AS3',...])
    interface_to_as = dict() #dictionary with IP to AS (just the owner)
    #the first AS in the pair is the target AS, and the second AS is
    #the neighbor
    with open(country_links_file,'rb') as link_file:
        unknown_nat = set()
        reader = csv.reader(link_file, delimiter=',')
        bdrmapit_providers = 0
        for row in reader:
            if ':' in row[0] or ':' in row[1]:
                continue #ignore IPv6
            first_as = row[2]
            second_as = row[4]
            interface = row[0]
            #only parse links where this AS is present
            #if first_as not in ases and second_as not in ases:
            #    continue
            if first_as == second_as:
                continue #internal link
            if first_as == '0':
                continue
            if second_as == '0':
                continue
            if first_as == '23456' or second_as == '23456':
                continue #reserved ASes
            
            interface_set.add(interface)
            interface_to_as[interface] = first_as #this interface belongs to this AS

            if '-' in second_as:
                continue #this is an IXP AS

            #save interface IP to AS-pair interconnection, if 
            #AS is in set (they could both be in the set, 
            #which explains the two conditions)
            
            #if first_as in ases:
            interconnect = first_as + ':' + second_as #this is the pair of ASes connected here
            if interface in interface_to_iconnect:
                interface_to_iconnect[interface].add(interconnect)
            else:
                interface_to_iconnect[interface] = set([interconnect])
            
            #if second_as in ases:
            interconnect = second_as + ':' + first_as #this is the pair of ASes connected here
            if interface in interface_to_iconnect:
                interface_to_iconnect[interface].add(interconnect)
            else:
                interface_to_iconnect[interface] = set([interconnect])
        return interface_set, interface_to_iconnect, interface_to_as

def read_ripe_file():
    ripe_file = '/project/mapkit/agamerog/country_asn_analysis/ripeprobes/20200322.json'
    probe_id_to_cc = dict()
    probe_id_to_asn = dict()
    with open(ripe_file) as json_file:
        data = json.load(json_file)
        #json_formatted_str = json.dumps(data, indent=2)
        for probe in data['objects']:
            #probe_id = u' '.join(probe['id']).encode('utf-8').strip()

            probe_id = str(probe['id']) #first fetch CC, then ASN
            try:
                #probe_cc = u' '.join(probe["country_code"]).encode('utf-8').strip()
                probe_country = str(probe["country_code"])
                if probe_country == 'None' or probe_country == '':
                    probe_cc = "XX"
                else:
                    probe_cc = probe_country
                
            except KeyError:
                probe_cc = "XX" #country not known
            probe_id_to_cc[probe_id] = probe_cc

            try:
                #probe_cc = u' '.join(probe["country_code"]).encode('utf-8').strip()
                probe_asnumber = str(probe["asn_v4"])
            except KeyError:
                probe_asnumber = 'None'
            probe_id_to_asn[probe_id] = probe_asnumber

        
    #print(print_set)
    return probe_id_to_cc, probe_id_to_asn

def read_traceroute_file(transit_set, peer_set, asname_dict, \
        probe_to_country, probe_to_asn, \
        target_country_ases, target_ases, bdrmapit_interfaces, \
        interface_to_iconnect, countries, interface_to_as, country_nations):

    #ip_add = '1.0.160.0'
    #country_set, asn = return_countries_covered_in_prefixes(ip_add)
    #print(country_set)
    #print(asn)
    #sys.exit()
    #Iterate through traces and keep those that we care about (inbound towards target AS/country)
    rawtrace = 'output/' + input_filename.split('/')[-1] + '.filtered.trace.txt'
       
    rawfile = open(rawtrace, 'w+')
    iplevel_dict = dict()
    aslevel_dict = dict()

    with bz2.BZ2File(input_filename, "r") as f:
        #data = json.load(f)
        for probe in json_lines.reader(f):
        #json_formatted_str = json.dumps(data, indent=2)a
            file_writer = (str(probe))
            probe_id_n = str(probe['prb_id']) #first fetch CC, then ASN
            #Iterate through traces and keep those that we care about (inbound towards target AS/country)
            goodrow = False
            #print(probe_id_n)
            current_interfaces = set() #set of interfaces that we care about that appear on this traceroute
            current_aspath = [] #the AS-level path inferred by bdrmapit
            if int(probe['af']) != 4:
                continue #read only IPv4

            if probe_id_n in probe_to_country:
                probe_country = probe_to_country[probe_id_n]
            else:
                continue #we don't know where the probe is located
            if probe_country == 'XX':
                continue #country unknown
            try:
                destination = str(probe['dst_addr'])
            except KeyError:
                #print('NO DST')
                #print(probe)
                continue #no destination address
            try:
                hops = probe['result']
            except KeyError:
                #No result
                #print("NO RESULT")
                #print(probe)
                continue
            

            latest_parsed_AS = ''
            for hop in probe['result']:
                try:
                    ips = hop['result']
                except KeyError:
                    #print("NO IP")
                    #print(probe)
                    continue
                for ip in hop['result']:
                    try:
                        ipadd = ip['from']
                    except KeyError:
                        continue #not an IP interface, some other type of result
                    if '*' in ipadd:
                        continue #this is a *, not an IP
                    elif ipadd in bdrmapit_interfaces:
                        current_interfaces.add(ipadd)
                        as_from_ip = interface_to_as[ipadd]
                        if as_from_ip != latest_parsed_AS:
                            current_aspath.append(as_from_ip) #we only want to add each AS once
                            latest_parsed_AS = as_from_ip
                    
            
            if len(current_interfaces) == 0 or len(current_aspath) == 0:
                continue #none of these IPs in the traceroute are of interest
            
            country_set, asn = return_countries_covered_in_prefixes(destination)
            for country in country_set:
                if country == probe_country:
                   continue #this is not an inbound trace
                if country not in countries: #we don't care about this country
                    continue
                asn_from_ip = current_aspath[-1]
                country_asn_str = asn_from_ip + ':' + country
                if country_asn_str not in target_country_ases:

                    continue #this AS in this country we don't care about
                nation_dict = country_nations[country]
               #OK, we care about this trace, see if the interface is for this target AS
                #print(current_aspath)
                for i in reversed(range(len(current_aspath))): #start by destination (or last responsive AS)
                    if i == 0:
                        break #we did not find the international border, skip
                                #or this trace only had one AS in it
                    first_as = current_aspath[i]
                    second_as = current_aspath[(i-1)]
                    if second_as not in nation_dict:
                        break #no nationality info for this AS, no way to check
                    elif nation_dict[second_as] == 'foreign':
                        goodrow = True
                        askey = country + ':' + first_as + ':' + second_as + ':' + asn_from_ip
                        if askey in aslevel_dict: #count relative frequency of crossing

                            aslevel_dict[askey] = aslevel_dict[askey] + 1
                        else:
                            aslevel_dict[askey] = 1
                        break
                
            if goodrow == True:
                rawfile.write(file_writer + '\n')
            #except:
                #sys.stderr.write('some issue reading this row')
                #continue #some issue reading some part of the traceroute
    #try:
    #    os.system('rm ' + rawtrace + '.gz')
    #except:
    #    sys.stderr.write('no bzip2 file to remove ' + rawtrace + '\n')
    #try
    #os.system('cd output; gzip ' + rawtrace.replace('output/',''))
    #except:
    #    sys.stderr.write('could not gzip file ' + rawtrace + '\n')
    return aslevel_dict, iplevel_dict
    
def write_ip_as_files(aslevel_dict, iplevel_dict, asname_dict, transit_set, peer_set):
    resultaslevel = './output/' + input_filename.split('/')[-1] + '.aslevel.csv'
    try:
        rawout = './output/' + input_filename.split('/')[-1] + '.filtered.trace.txt'
        os.system('bzip2 -f ' + rawout)
    except:
        sys.stderr.write('could not bzip2 file ' + rawout + '\n')
    aslevelfile = open(resultaslevel, 'w+')

    aslevelfile.write('#CC,target_ASN-ASName,border_crossing_foreign_asn-asname,Relationship,Number_Traces,border_crossing_domestic_asn-asname\n')

    for aslevel in aslevel_dict:
        target = aslevel.split(':')[3]
        
        foreign_as = aslevel.split(':')[2]
        domestic_as = aslevel.split(':')[1]
        country = aslevel.split(':')[0]
        relationship = determine_relationship(domestic_as, foreign_as, transit_set, peer_set)
        #relationship = determine_relationship(target, neighbor, transit_set, peer_set)
        count = str(aslevel_dict[aslevel])

        if target in asname_dict:
            target_str = target + '-' + asname_dict[target]
        else:
            target_str = target + '-Unknown'

        if foreign_as in asname_dict:
            foreign_as_str = foreign_as + '-' + asname_dict[foreign_as]
        else:
            foreign_as_str = foreign_as + '-Unknown'

        if domestic_as in asname_dict:
            domestic_as_str = domestic_as + '-' + asname_dict[domestic_as]
        else:
            domestic_as_str = domestic_as + '-Unknown'

        aslevelfile.write(country + ',' + target_str  + ',' + foreign_as_str + ',' + relationship + ',' + count + ',' + domestic_as_str + '\n')
    aslevelfile.close()
    try:
        os.system('bzip2 -f ' + resultaslevel)
    except:
        sys.stderr.write('could not bzip2 file ' + resultaslevel + '\n')

    
def determine_relationship(target, neighbor, transit_set, peer_set):
    
    isprovider = neighbor + ':' + target
    if isprovider in transit_set:
        return 'provider'
    
    iscustomer = target + ':' + neighbor
    if iscustomer in transit_set:
        return 'customer'

    ispeer = target + ':' + neighbor
    if ispeer in peer_set:
        return 'peer'

    return 'none'

def read_nat_row(nat_row, current_country): #for reading the multi-country lines

    nat_frac = float(nat_row.split(',')[2])
    #determine if origin AS is domestic or foreign
    if nat_frac >= 0.666666666666666666:
        return 'domestic'
    else:
        return 'foreign'

def read_nationality_file(current_country):
    current_snapshot = '20200301'
    as_nationalities = dict()
    #nationality_filename = '/home/agamerog/country/estebannewnat/20181201.csv'
    nat_string = current_snapshot[:4] + '_' + current_snapshot[4:6] + '_01'
    nationality_filename = '/home/esteban/mapkit/data/processed/nationality-orig-customers/' + nat_string + '.csv' #replace this crap AGG
    with open (nationality_filename,'rb') as f:
        rows = f.readlines()
        parsed_ases = set() #create set of parsed ASes to know which ones are foreign
        for i in range(len(rows)):
            if 'ASN' in rows[i]:
                continue

            row = rows[i].strip('\n')
            asn = str(row.split(',')[0])
            country = str(row.split(',')[1])
            if country != current_country:
                parsed_ases.add(asn)
                continue #read only rows pertinent to this country

            as_nationalities[asn] = read_nat_row(row, current_country)

    for asn in parsed_ases:
        if asn not in as_nationalities:
            as_nationalities[asn] = 'foreign' #as was seen for at least one other country
        else:
            continue #AS already has nationality assigned, nothing to do

    return as_nationalities

def read_nations(countries):
    country_nations = dict()
    for cc in countries:
        current_nation = read_nationality_file(cc)
        country_nations[cc] = current_nation
    return country_nations

def main():
    #Get the prefix2AS and country from BG
    #Save to global networking tree
    parsed_prefixes = read_pfx_to_as(bg_filename)
    read_delegation_file(bd_filename, parsed_prefixes) #Expand country to AS assignments using BD

    transit_set, peer_set = read_asrel_file() #read p2c AS-relationships
    #transit set is provider:customer
    #peer set is peerone:peertwo

    asname_dict = read_asname() #Read AS Org Names from AS-Rank into a dictionary

    probe_to_country, probe_to_asn = read_ripe_file()
    target_country_ases, target_ases, countries = read_target_ases_countries() #read set of countries and ASes there that were targeted with the traceroute campaign
    bdrmapit_interfaces, interface_to_iconnect, interface_to_as = \
            read_bdrmapit_out(target_ases)
    
    country_nations = read_nations(countries) #nationality dict
    
    aslevel_dict, iplevel_dict = \
            read_traceroute_file(transit_set, peer_set, asname_dict, \
            probe_to_country, probe_to_asn, \
            target_country_ases, target_ases, bdrmapit_interfaces, \
            interface_to_iconnect, countries, interface_to_as, country_nations)

    write_ip_as_files(aslevel_dict, iplevel_dict, asname_dict, transit_set, peer_set)

main()

#sys.stderr.write("\n writing " + country_filename + "\n")

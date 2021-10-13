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

# Create a new radix tree to store network prefix information
rtree = radix.Radix()

dependencies = {'IO':'GB','WF':'FR','BL':'FR','BM':'GB','HK':'CN','CC':'AU','BQ':'NL','HM':'AU','JE':'GB','FK':'GB','YT':'FR','FO':'DK','PR':'US','TW':'CN','NC':'FR','NF':'AU','RE':'FR','PF':'FR','TK':'NZ','TF':'FR','PN':'GB','TC':'GB','PM':'FR','CK':'NZ','GU':'US','GS':'GB','EH':'MA','VG':'GB','AI':'GB','VI':'US','GG':'GB','GF':'FR','AS':'US','CX':'AU','IM':'GB','AW':'NL','AX':'FI','GP':'FR','GL':'DK','CW':'NL','GI':'GB','MF':'FR','SX':'NL','MO':'CN','BV':'NO','NU':'NZ','UM':'US','SJ':'NO','SH':'GB','MQ':'FR','MP':'US','MS':'GB','KY':'GB'}

#If this input is true, the input files are changed to include .stub at the end
#this is useful to test input/output with small prefix-to-country
#mappings and verify that everything is working correctly
stub_input = str(sys.argv[3])
if stub_input == 'True': #use stub input file
    testing_string = '.stub'
else:
    testing_string = ''

#This is the ISO 2-letter country code
current_country = str(sys.argv[1])

#If this input is True, the code prints out information for specific ASes
#using function print_test(print_dict)
#The dictionary with ASes to print can be defined at the beginning of
#read_as_path_file (empty by default)
testing_input = str(sys.argv[2])
if testing_input == 'True': #print paths from specific ASes
    testing_file = True
else:
    testing_file = False
if testing_file:
    country_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_country + ".test.csv"

#Snapshot to analyse, in YYYYMMDD format
current_snapshot = sys.argv[4]

#File with location of BGP monitors, as well as location confidence level 
peer_address_geo_file = "20180301.geo_line.jsonl"

#Output file with top ASes that we used for a previous visualization of the matrix.
#Currently file is produced but unused
heatmap_filename = "/project/mapkit/agamerog/country_asn_analysis/heatmap/" + current_country + ".csv"

#CSV file with two and three letter ISO codes as well as full country names

#location of the prefix-to-country and AS-to-country mapping from delegation files
bd_filename = "/project/mapkit/agamerog/country_asn_analysis/bd/" + current_snapshot + ".txt" + testing_string

#location of the prefix-to-country and AS-to-country mapping from Netacuity or MaxMind
bg_filename = "/project/mapkit/agamerog/country_asn_analysis/geolocation_bgp/pfxgeo/pfx-to-country." + current_snapshot[:4] + "-" + current_snapshot[4:6]+ "-" + current_snapshot[6:] + testing_string

#Output file with country-level metrics of #paths discarded by the various filters
ratio_file = "/project/mapkit/agamerog/country_asn_analysis/logs/ratio/" + current_country + ".txt"

#Input file with prefixes and paths seen on BGP
#aspath_filename = "/data/external/as-rank-ribs/" + current_snapshot + "/" + current_snapshot + ".all-paths.bz2" + testing_string
#Use short input file if input is true
aspath_filename = "/project/mapkit/agamerog/country_asn_analysis/singlecountryfiles/" + current_country + "." + current_snapshot + ".txt.bz2" #AGG SINGLE COUNTRY TODO MAY NEED TO CHANGE BACK

if stub_input == 'True':
    aspath_filename = "/project/mapkit/agamerog/country_asn_analysis/20180301.all-paths.v4only.txt.stub"

#Create output directories if they don't already exist
directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_snapshot + "/"
os.system(directory_creation_string)
directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/" + current_snapshot + "/"
os.system(directory_creation_string)
directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/country_aspath/sorted_summaries/" + current_snapshot
os.system(directory_creation_string)
directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/country_aspath/sorted_summaries/" + current_snapshot + "/"
os.system(directory_creation_string)

#Output file with country-level TI matrix. One value for each pair of origin and transit AS. 
#File can be large for some countries, up to ~20 GB (gets compressed by a subsequent script)
country_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_snapshot + "/" + current_country + ".cti.csv"

#Input file with AS-relationships (to read provider-customer pairs)
asrel_filename = '/data/external/as-rank-ribs/' + current_snapshot + "/" + current_snapshot + '.as-rel.txt.bz2'

#Input file with FAO metric per AS. Used to compute FASR outputs
origin_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/"+ current_snapshot + '/' + current_country + ".csv"


#Input file with the inferred "nationality" of each AS
nationality_filename = "/project/comcast-ping/stabledist/mapkit/code/ATIstatistics/DomesticInternational/" + current_country + ".csv"

#Output file with heavy-reliance transit relationships, metrics on both the 
#origin and transit ASes involved

#Set limits on memory to 4 GB (can be changed to any arbitrary number)
#limit is skipped for the US due to significantly higher computational reqs
#if current_country != "US":
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

def read_nationality_file():
	#Read nationality assignment file, return dictionary
	#Where key is ASN and value is nationality

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
            asn = row.split(',')[1].split('-')[0]

            nation_dict[asn] = as_nationality

    return nation_dict


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
    asrel_set = set()
    
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
            if int(row_rel) != -1:
                continue
            else:
                provider = row.split('|')[0]
                customer = row.split('|')[1]
                p2c = provider + ':' + '99999'
                #country_rel = provider + ':99999'
                #asrel_set.add(country_rel)
                asrel_set.add(p2c)
                p2cc = provider + ':' + customer #also need to add the ASes themselves for indirect transit
                asrel_set.add(p2cc)
            #make sure every transit provider of anyone is a transit provider of the country
        return asrel_set

def systemCall(parameter): #Simple function to run a system call
    os.system(parameter)

def read_geolocation_file(filename):
    #Read prefixes geolocated to each country, and the number of
    #addresses in that prefix that were specifically assigned to the country
    #Skip prefixes with length 25 or more, as we are measuring in /24 blocks
    #Read this file once for each country, ignore rows that don't have the country ISO2-code
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
            checking_country = row.split('|')[1]
            if checking_country in dependencies: #this is one of the special territories that we're merging into their UN Member associated
                current_country_checking = dependencies[checking_country] #this is the country you depend on
                if current_country_checking != current_country:
                    continue #it's a dependency but not a dependency of this country
                #else here is implicit: if it is a dependency of this country, merge into the prefixes of this country
                else:
                    sys.stderr.write("merging " + checking_country + " into " + current_country + "\n")
            elif current_country not in row:
                continue #read only prefixes in this country
            #print row
            country_set.add(row.split('|')[0])
            asn = row.split('|')[3]
            #asn = '99999'
            tmp = 32.0 - np.log2(int(row.split('|')[2]))
            prefix_length = 2**(24 - int(tmp))
            if int(prefix_length) < 1:
                prefix_length = 1 #If less than 256 address still bill one slash 24 for consistency with transit
            key = pad_prefix(row.split('|')[0]) + ':' + asn
            country_key = pad_prefix(row.split('|')[0]) + ':99999'
            if key in prefix_origin:
                prefix_origin[key] = prefix_origin[key] + prefix_length
            else:
                prefix_origin[key] = prefix_length

        return country_set, prefix_origin

def read_delegation_file(filename, country_set):
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
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers
            if '_' in row:
                continue #skip MOAS
            checking_country = row.split('|')[1]
            if checking_country in dependencies: #this is one of the special territories that we're merging into their UN Member associated
                current_country_checking = dependencies[checking_country] #this is the country you depend on
                if current_country_checking != current_country:
                    continue #it's a dependency but not a dependency of this country
                else:
                    sys.stderr.write("merging " + checking_country + " into " + current_country + "\n")
                #else here is implicit: if it is a dependency of this country, merge into the prefixes of this country
            elif current_country not in row:
                continue #read only prefixes in this country

            prefix_length = int(row.split('|')[0].split('/')[1])
            if prefix_length > 24:
                continue
            country_set.add(row.split('|')[0])

        return country_set

def read_asname():
	#Parse organization names from AS rank (and ensure proper encoding)
	#Return dictionary where key is AS Number and value is AS Name
    asrank_file = '20180301.asns.jsonl'
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
    ASdict['99999'] = current_country
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
    #returns a list of strings corresponding to monitors
    all_monitors = set()
    for i in range(len(sorted_prefixes)):
        rnode = rtree.search_exact(sorted_prefixes[i])
        current_keys = rnode.data.keys() #key is monitor
        for j in range(len(current_keys)):
            all_monitors.add(current_keys[j])
            
    return all_monitors
    
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
#def path_cut_peak(path, asrel_set):
    #determine if there is a p2c relationship along path
    #if so return path up to highest upstream provider of origin
    #otherwise return empty path
#    split_path = path.split('|')
#    ignore_path = True
#    already_parsed_ases = set()
#    for i in range( (len(split_path) - 1 ) ):
#        test_provider = split_path[i]
#        test_customer = split_path[i+1]
        
#        if test_provider in already_parsed_ases:
#            if test_customer == test_provider:
#                removing_AS = test_provider + '|'
#                path = path.replace(removing_AS,'')
#                sys.stderr.write('\n path prepending ' + str(split_path))

#                continue #path prepending
#            else:
#                ignore_path = True
                #sys.stderr.write('\n loop ' + str(split_path))
                #sys.stderr.write('\n already_parsed_ases ' + str(already_parsed_ases))
                #sys.stderr.write('\n test_provider ' + str(test_provider))
                #sys.stderr.write('\n test_customer ' + str(test_customer))
#                break #loop
#        else:
#            already_parsed_ases.add(test_provider)
#        possible_p2c = test_provider + ':' + test_customer
#        if possible_p2c in asrel_set:
#            ignore_path = False
#            break
        #remove AS to the left below
#        if i == 0:
#            continue #make sure we keep host AS (used for collector)
#        removing_AS = test_provider + '|'
#        path = path.replace(removing_AS,'')
        
        #check for loops (and path prepending) in rest of path
    #print('\n' + path)
#    if not ignore_path:
        
#        new_split_path = path.split('|')
#        for i in range( (len(new_split_path) - 1 ) ):
#            if i == 0:
#                continue #still need to ignore collectore
#            test_provider = new_split_path[i]
#            test_customer = new_split_path[i+1]
            #print(already_parsed_ases)
#            if test_customer in already_parsed_ases:
                #print('ENTERING')
                #print(new_split_path)
#                if test_customer == test_provider: #path prepending
#                    removing_AS = test_provider + '|'
#                    path = path.replace(removing_AS,'')
                    #sys.stderr.write('\n path prepending ' + str(new_split_path))
#                else: #loop
#                    ignore_path = True
                    #sys.stderr.write('\n second loop ' + str(new_split_path))
                    #sys.stderr.write('\n PATH WITHOUT PREPENDING ' + str(path))
                    #sys.stderr.write('\n already_parsed_ases ' + str(already_parsed_ases))
                    #sys.stderr.write('\n test_provider ' + str(test_provider))
                    #sys.stderr.write('\n test_customer ' + str(test_customer))
#                    break
#            else:
#                already_parsed_ases.add(test_customer)


#    if ignore_path:
#        return ''
#    else:
#        return path

def read_approved_targets():
    outset = set()
    f = open('/project/mapkit/agamerog/country_asn_analysis/integratedan/rellevel.summary.202003.csv' + testing_string, "r")
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

def read_good_paths():
    paths = set()
    path_filename = "/data/external/as-rank-ribs/" + current_snapshot + "/" + current_snapshot + ".paths.bz2"
    with bz2.BZ2File(path_filename, "r") as f:
        try:
            rows = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + asrel_filename + "\n")
            sys.exit()

        for i in range(len(rows)):
            row = rows[i].strip('\n')
            if '#' in row: #skip header
                continue
            path = row
            paths.add(path) # save acceptable paths to set
    return paths

def read_as_path_file(filename, country_set, asrel_set, mon_dict, global_origin_as_dict, prefix_origin):
    #Read AS-paths file and compute share of country addresses originated by an AS,
    #sort decreasingly and save to file
    #Sample line of AS-paths file
    #ripe/rrc00|5 4777|2497|3257|8452 156.208.128.0/18 i 202.12.28.1
    #Where 8452 is the origin AS, 3257 is a potential direct provider
	# (depending on inferred AS relationship), 2497 is a potential
	#indirect provider, 202.12.28.1 is the monitor IP
	#and 156.208.128.0/18 is the prefix
    #Other fields are not relevant for this analysis
    #if current_country == 'BF':
    #    bfPaths = open('bfpaths.20200101.txt','w+')
    print(filename)
    #global_approved_asns = read_approved_targets()
    print_dict = dict()
    as_dict = dict()
    pfix_dict = dict()
    single_path_enforce = set()
    transit_ases = set()
    origin_ases = set()
    origin_ases.add('99999')
    prefix_collector = dict()
    already_parsed_mon_prefixes = set()
    #build tree of prefixes and paths
    path_provider = 0
    path_count = 0
    dup_pfx_monitors = 0
    good_paths = read_good_paths()

    if current_country == 'BF' and current_snapshot == '20200101':
        f = open('bfpaths.20200101.txt','rb')
    elif '.stub' in filename:
        f = open(filename, "r")
    else:
        f = bz2.BZ2File(filename, "r")      
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
        aspath_prefix = row.split(' ')[2]
        if aspath_prefix not in country_set:
            continue #read only prefixes in this country
        path = row.split(' ')[1]

        if path not in good_paths:
            sys.stderr.write('Discarding path not in good paths ' + path)

            continue #this path is discarded based on AS-Rank criteria
        #sys.stderr.write('NOT discarding path in good paths ' + path + '\n')

        path_components = path.split('|')
        checking_origin_asn = path_components[-1]
        #if checking_origin_asn not in global_approved_asns:
        #    continue #this is not a non-peering AS in this country
        path = ''
        
        origin_as = '99999'
        #CTI hacky thing to see if I can get the US to finish
        #if current_country == 'US' and origin_as not in global_origin_as_dict:
        #    continue
        #CTI I Probably need to add the origin AS for allASN to everywhere below
        #TODO add all reserved ASes to check in path (in case file includes something other than 23456)
        #TODO add check for reserved prefixes
        #Input file is checked by AS-rank, but this reserved AS still appeared

        

        for count in range(len(path_components)):
            if count == (len(path_components) - 1):
                path = path + '99999'
            else:
                path = path + path_components[count] + '|'
            #print(country_path)
        #private_as = False
        #for count in range(len(path_components)):
        #    #https://www.iana.org/assignments/as-numbers/as-numbers.xhtml
        #    check_as = path_components[count]
        #    try:
        #        check_as_int = int(check_as)
        #    except:
        #        private_as = True
        #        break
        #    if check_as_int >= 64496 and check_as_int <= 65535:
        #        private_as = True
        #    if check_as_int >= 65536 and check_as_int <= 131071:
        #        private_as = True
        #    if check_as_int >= 141626 and check_as_int <= 196607:
        #        private_as = True
        #    if check_as_int >= 213404 and check_as_int <= 262143:
        #        private_as = True
        #    if check_as_int >= 272797 and check_as_int <= 327679:
        #        private_as = True
        #    if check_as_int >= 329728 and check_as_int <= 393215:
        #        private_as = True
        #    if check_as_int >= 399261 and check_as_int <= 4294967295:
        #        private_as = True
        #    if check_as_int == 23456:
        #        private_as = True

        #if private_as: #this path is toast because of a private AS or a reserved AS or an unallocated AS
        #    continue
            #skip paths with reserved AS

        #Cut path at highest upstream p2c from origin
        #if no p2c along path skip
        new_path = path_cut_peak(path, asrel_set)
        #NEED TO CREATE A COPY OF THE PATH CTI
        
		
        if len(new_path) == 0:
            path_count = path_count + 1
            continue #path does not have a p2c relationship
        else:
            path_count = path_count + 1 #save some path metrics for country-level ratio file
            path_provider = path_provider + 1
            path = new_path
            
        mon = row.split(' ')[4]
        if mon not in mon_dict:
            #Skip Monitors that are not in geo-dictionary (only use monitors from outside
			#The country for transit determination), or those with unknown location
            continue 

        mon_country = mon_dict[mon]
        if mon_country == current_country: #inbound transit filter
            sys.stderr.write('\nMonitor in target country ' + current_country + ' ' + mon + '\n')
            continue

        mon_prefix = mon + ':' + aspath_prefix
        if mon_prefix in already_parsed_mon_prefixes: #read each prefix seen by each monitor IP just once
            dup_pfx_monitors = dup_pfx_monitors + 1
            continue
        else:
            already_parsed_mon_prefixes.add(mon_prefix)

		#Grab collector, or BGP host AS
        if '|' in path:
            collector = path.split('|')[0]
        else:
            collector = path
        #if current_country == 'BF':
            #bfPaths.write(row + '\n')
        key = aspath_prefix + ':' + collector 

        if key in prefix_collector: #count how many prefix-collector pairs are parsed
            prefix_collector[key] = prefix_collector[key] + 1
        else:
            prefix_collector[key] = 1

        if testing_file: #produce output for specific ASes
            print_dict = print_specific_ASes(path, row, print_dict)

        node = rtree.search_exact(aspath_prefix)
        
        if str(node) == 'None':
            node = rtree.add(aspath_prefix)
            #create new tree branch node only if search result is empty

        try: #append to tree key if it exists
            node.data[mon].append([path, collector, checking_origin_asn]) #save path and BGP host AS ("collector")
        except KeyError:
            node.data[mon] = [[path, collector, checking_origin_asn]] #create list with a sinble path and collector
        #DEBUG
            #AGG does this even work?
    if testing_file:
        print_test(print_dict)
    #TODO print out the elapsed time per country (separate between parsing - up to here - and processing)
    #TODO save into a pickle file 
    discarded_overlap = 0
    country_list = list(country_set)
    traversed = set()
    parsed_mon_prefixes = set()
    parsed_prefix_collectors = set()
    #print('country_list')
    #print(country_list)
    for i in range(len(country_list)):
        # For each prefix, check if it is covered by a less specific prefix
        # If so, look for the least specific prefix, and then find
        # any other prefixes also covered by it
        # Then sort prefixes starting by the most specific
        # and count the number of addresses to assign to each origin AS
        # If a less specific prefix (say /15) is covered by
        # more specific prefixes (say two /16) then ignore the /15
        used_lengths = dict()
        pfx = country_list[i]
        #print('\npfx pfx pfx')
        #print(pfx)
        checking_prefix = pad_prefix(pfx) #fixing the sorting by length issue

        if checking_prefix in traversed:
            continue

        sub_prefixes = []
        try:
            shortest = rtree.search_worst(pfx).prefix
        except AttributeError:
            continue
        try:
            subtree = rtree.search_covered(shortest)
            sub_prefixes = get_prefixes(subtree)

            sorted_prefixes = sort_by_length(sub_prefixes)

        except AttributeError:
            continue

        #add list of prefixes to already traversed
        for j in range(len(sorted_prefixes)):
            traversed.add(sorted_prefixes[j])
        
	#extract set of monitors from prefix data structure
	monitors = get_tree_keys(sorted_prefixes)
	#if current_country == 'BF':
        #    sys.stderr.write('\nmonitors ' + str(monitors))
		#Iterate through prefixes in this tree, sorted from most to least specific
        curr_level = int(sorted_prefixes[0].split('/')[1]) #keep track of the prefix "level", e.g., level 24 if it is a /24
        #The only time when we need to check for double counting is at level
        #changes, because each prefix can only exist once (so
        #there can be no overlap at the same level)
        #sys.stderr.write('\nsorted prefixes ' + str(sorted_prefixes))
        #if current_country == 'BF':
        #    sys.stderr.write('\nsorted prefixes ' + str(sorted_prefixes) + '\n')
        for j in range(len(sorted_prefixes)):
            #if current_country == 'BF':
            #    sys.stderr.write('\nprefix ' + str(sorted_prefixes[j]) + '\n')
            tree_prefix = sorted_prefixes[j]
            tmp = int(tree_prefix.split('/')[1])
            this_level = int(sorted_prefixes[j].split('/')[1])

            prefix_length = 2**(24 - tmp)
            current_weight = 1
            rnode = rtree.search_exact(tree_prefix)

            for mon in monitors:
                mon_prefix = mon + ':' + tree_prefix
                if mon_prefix in parsed_mon_prefixes: #parse each prefix from each monitor IP only once
                    continue
                else:
                    parsed_mon_prefixes.add(mon_prefix)
                try:
                    monitor_node = rnode.data[mon] #monitor_node is the path seen by this monitor for that
                    #prefix, and the BGP host AS where that monitor is hosted
                    current_weight = 1
                except KeyError:
                    continue
                #if current_country == 'CL':
                #    sys.stderr.write('monitor_node for mon = ' + str(mon) + '\n')
                #    sys.stderr.write(str(monitor_node) + '\n')
                origbg = (monitor_node[0][2])
                #This line below discounts the observations of multiple monitors in the same BGP host AS
                keybg = pad_prefix(tree_prefix) + ':' + origbg
                
                if keybg in prefix_origin: #if it is in BG, otherwise just use length
                    prefix_length = prefix_origin[keybg]


                if this_level < curr_level:
                    #if we're going from a more specific level to a less
                    #specific level, and we've already been in any
                    #part of this tree for this origin before
                    curr_level = this_level

                    if mon in used_lengths: #see if this monitor has seen a prefix in this tree (for overlap discount)
                        collector = monitor_node[0][1]
                        key = tree_prefix + ':' + collector
                        try:
                            weight = prefix_collector[key]
                        except KeyError:
                            weight = 1
                        #This line below discounts the observations of multiple monitors in the same BGP host AS    
                        this_prefix_length = float(prefix_length) / float(weight)#divide prefix length by monitor weight
                        #for prioritizing AS diversity
                        #This line below is the prefix overlap check
                        #TODO add a note clarifying how the weight and the overlap are checked for coverage
                        if used_lengths[mon] >= this_prefix_length:
                            #sys.stderr.write('\nignoring covered prefix ' + str(sorted_prefixes[j]))
                            discarded_overlap = discarded_overlap + 1 #this is for the file that we save for each country
                            
                            continue
                        else:
                            #update virtual counters of numerator and denominator for each AS transit-origin paier
                            this_used_length = used_lengths[mon]
                        as_dict, pfix_dict, origin_ases, transit_ases, parsed_prefix_collectors, this_weight =  \
                                update_matrices(monitor_node, current_weight, this_used_length,\
                                prefix_length, origin_ases, transit_ases,\
                                as_dict, pfix_dict, prefix_collector, tree_prefix, parsed_prefix_collectors)
                        used_lengths[mon] = used_lengths[mon] + float(prefix_length) / \
                                float(this_weight)

                    else: #this monitor has not seen a prefix in this tree
                        as_dict, pfix_dict, origin_ases, transit_ases, parsed_prefix_collectors, this_weight =  \
                                update_matrices(monitor_node, current_weight, 0, \
                                prefix_length, origin_ases, transit_ases, \
                                as_dict, pfix_dict, prefix_collector, tree_prefix, parsed_prefix_collectors)

                        used_lengths[mon] = float(prefix_length) / float(this_weight)
                    #sys.stderr.write('\nchanged level used lengths = ' + str(used_lengths[mon]))
                else: #we're still at the same "level" - no need to check for complete overlap coverage
                    if mon in used_lengths: #see if this monitor has seen a prefix in this tree (for overlap discount)
                        collector = monitor_node[0][1]
                        key = tree_prefix + ':' + collector
                        try:
                            weight = prefix_collector[key]
                        except KeyError:
                            weight = 1
                        #This line below discounts the observations of multiple monitors in the same BGP host AS    
                        this_prefix_length = float(prefix_length) / float(weight)#divide prefix length by monitor weight
                        #for prioritizing AS diversity
                        #This line below is the prefix overlap check
                        #TODO add a note clarifying how the weight and the overlap are checked for coverage
                            #update virtual counters of numerator and denominator for each AS transit-origin paier
                        this_used_length = 0
                        as_dict, pfix_dict, origin_ases, transit_ases, parsed_prefix_collectors, this_weight =  \
                            update_matrices(monitor_node, current_weight, this_used_length,\
                            prefix_length, origin_ases, transit_ases,\
                            as_dict, pfix_dict, prefix_collector, tree_prefix, parsed_prefix_collectors)
                        used_lengths[mon] = used_lengths[mon] + float(prefix_length) / \
                            float(this_weight)

                    else: #this monitor has not seen a prefix in this tree
                        as_dict, pfix_dict, origin_ases, transit_ases, parsed_prefix_collectors, this_weight =  \
                                update_matrices(monitor_node, current_weight, 0, \
                                prefix_length, origin_ases, transit_ases, \
                                as_dict, pfix_dict, prefix_collector, tree_prefix, parsed_prefix_collectors)

                        used_lengths[mon] = float(prefix_length) / float(this_weight)
                    #sys.stderr.write('\ndid not change level used lengths = ' + str(used_lengths[mon]))
            #sys.stderr.write('\n values of pfix_dict at end of above prefixs run ' + str(pfix_dict))`
    #if current_country == 'CL':
    #    sys.stderr.write('as_dict\n ' + str(as_dict) + '\n\n\n')
    #    sys.stderr.write('pfix_dict\n ' + str(pfix_dict) + '\n\n\n')
    #    sys.stderr.write('origin_ases\n ' + str(origin_ases) + '\n\n\n')
    #    sys.stderr.write('transit_ases\n ' + str(transit_ases) + '\n\n\n')
    #    sys.stderr.write('retree.prefixes\n ' + str(rtree.prefixes()) + '\n\n\n')
    #    sys.stderr.write('parsed_prefix_collectors\n ' + str(parsed_prefix_collectors) + '\n\n\n')
    #    sys.stderr.write('used_lengths\n ' + str(as_dict) + '\n\n\n')
    with open (ratio_file,'w+') as rr: #count country-level metrics and save to file
        rr.write('p2c_discarded,path_count,dup_pfx_mon,uniq_pfx_mon,overlap_discard,mon_pfixes\n')
        rr.write(str(path_provider) + ',' + str(path_count) + ',' + str(dup_pfx_monitors) + ',' + \
                str(len(already_parsed_mon_prefixes)) + ',' + str(discarded_overlap) + ',' + \
                str(len(parsed_mon_prefixes)) + '\n' )
        sys.stderr.write('\n Saving ' + ratio_file + '\n')

    return as_dict, pfix_dict, origin_ases, transit_ases

def update_matrices(monitor_node, current_weight, used_length, prefix_length, origin_ases, transit_ases, as_dict, pfix_dict, prefix_collector, tree_prefix, parsed_prefix_collectors):
    #This function updates virtual counters of numerator and denominator for each origin-transit pairs

            #count each prefix-path combination once only
        
    prefix_length = float(prefix_length - used_length) #/ float(current_weight) #move weight to later stage

    for i in range(len(monitor_node)):
		#Iterate through paths and create lists of origin and transit ASes; 
		#get monitor weight for this prefix

        path = monitor_node[i][0] #path including origin and transit providers
        collector = monitor_node[i][1] #BGP host AS
        key = tree_prefix + ':' + collector
        try:
            weight = prefix_collector[key]
        except KeyError:
            weight = 1
        this_prefix_length = float(prefix_length) / float(weight)
        if '|' in path:
            origin = path.split('|')[-1]
            origin_ases.add(origin)
            components = path.split('|')
            transit_list = []
        else:
            origin = path
            origin_ases.add(origin)
            components = [origin]
            transit_list = []

        #In this loop we iterate through transit ASes in the path
        for i in range(len(components)):
            if i == 0:
                continue #ignore host AS
            position = len(components) - i
            #transit list is a list of tuples with (transit_ASN, position_along_path_from_origin)
            #this list does not include the host AS
            tup = [components[i], position]
            transit_list.append(tup)

        #Update TI numerator and denominator for each pair of origin/transit AS
        for i in range(len(transit_list)):
            key = collector + ':' + origin + ':' + transit_list[i][0]
            transit_ases.add(transit_list[i][0])
            pos_for_count = transit_list[i][1] 
            if transit_list[i][1] > 1:
                transit_weight = float(1)/float(transit_list[i][1]-1) #indirect transit filter
            else:
                transit_weight = float(1)
            if key in pfix_dict: 
                #pfix_dict is a 3-tuple (prefix_length_sum(div by transit weight), position_sum, #paths)

                pfix_dict[key][0] = float(pfix_dict[key][0]) + float(this_prefix_length)*transit_weight
                if transit_list[i][1] != 1: #if the transit AS is not a direct transit provider
                    pfix_dict[key][1].append(transit_list[i][1])
                    pfix_dict[key][2] = pfix_dict[key][2] + 1
                    try:
                        pfix_dict[key][3][pos_for_count] = pfix_dict[key][3][pos_for_count] + 1
                    except KeyError:
                        pfix_dict[key][3][pos_for_count] = 1 #array keeping track of position of transit
            else: #We have not seen this key, initialize
                if transit_list[i][1] != 1:
                    pfix_dict[key] = [this_prefix_length*transit_weight, [transit_list[i][1]], 1, dict()]

                    pfix_dict[key][3][pos_for_count] = 1
                else:
                    pfix_dict[key] = [this_prefix_length*transit_weight, [0], 0, dict()]
 
            if origin == transit_list[i][0]: #update denominator (once per prefix-collector)
                #This is the total address space for this origin seen by this monitor
                #So we can compute the centrality (TI) of each transit AS
                key = collector + ':' + origin
                if key in as_dict:
                    as_dict[key] = as_dict[key] + this_prefix_length
                else:
                    as_dict[key] = this_prefix_length
    return as_dict, pfix_dict, origin_ases, transit_ases, parsed_prefix_collectors, weight

def sort_filter_hegemony(heg_key, heg_value, discard_keys, discard_matrix):
    #implement TI outlier filter
    bottom = len(heg_value) // 10 #discard bottom 10% of the observations (from multiple BGP host ASes)
    top = len(heg_value) - len(heg_value) // 10 #discard top 10 % of values
    kept = len(heg_value)
    discarded = 0
    if top == len(heg_value):
        
        return discard_keys, discard_matrix, kept, discarded #too few values, discard none
    #Sort TI between origin and transit seen by multiple BGP host ASes
    sorted_values = sorted(heg_value, key=lambda x: x[1])
    for i in range(len(sorted_values)):
        #use the index of the iteration to discard top and bottom 10% 

        if i < bottom or i >= top:
            origin = heg_key.split(':')[0]
            transit = heg_key.split(':')[1]
            if origin == transit:
                continue #no need to discount from denominator yet (that is done separately)
            key = sorted_values[i][0] + ':' + heg_key
            discard_keys.add(key)
            if heg_key in discard_matrix:

                discard_matrix[heg_key] = discard_matrix[heg_key] + sorted_values[i][2]
            else:
                discard_matrix[heg_key] = sorted_values[i][2]
            kept = kept - 1
            discarded = discarded + 1
    return discard_keys, discard_matrix, kept, discarded
            

def write_country_output(as_dict, pfix_dict, origin_ases, \
        transit_ases, country_filename, asname_dict, global_origin_as_dict):
    #save files to disk with country-level matrix

    matrix_dict = dict() #dictionary for saving as-path influence matrix
    denom_dict = dict() #dictionary for saving the denominator per asn

    heg_dict = dict() #dictionary for implementing hegemony filtering

    already_parsed = set()
    transits_parsed = set()
    printing = False
    discard_keys = set()
    discard_matrix = dict()
	#Iterate through prefixes to compute final numerator and denominator for each
	#Transit/origin AS pair
    for pfix in pfix_dict:
        col = str(pfix.split(':')[0])
        origin = str(pfix.split(':')[1])
        transit = str(pfix.split(':')[2])
        key = origin + ':' + transit    
        tmp_num = pfix_dict[pfix][0]
        tmp_key = col + ':' + origin
        tmp_denom = as_dict[tmp_key]
        #heg_dict is a 4-tuple with (collector, num_prefix_length, denom_prefix_length, hegemony_metric)
        tmp_heg = float(tmp_num) / float(tmp_denom)
        tmp_tup = [col, tmp_heg, tmp_denom]
        if key in heg_dict:
            
            heg_dict[key].append(tmp_tup)
        else:
            heg_dict[key] = [tmp_tup]

	#Determine which TI observations to discard as per hegemony filter (BGP Host ASes)
    nohegemony = 0
    overall = 0
    with open(ratio_file + '.heg.txt','w+') as z:    
        z.write('country,origin,transit,discarded,kept,share_kept\n')
        for heg in heg_dict:
            discard_keys, discard_matrix, kept, discarded = sort_filter_hegemony(heg, heg_dict[heg], discard_keys, discard_matrix)    
            if '99999:' in heg:
                z.write(current_country + ',' +heg.replace(':',',') + ',' + str(discarded) + ',' + str(kept) + ',' + str(100*kept/(kept+discarded)) + '\n')
                if discarded == 0:
                    nohegemony = nohegemony + 1
                overall = overall + 1
        z.write(current_country + '-summary,' + str(nohegemony) + ',' + str(overall) + ',' + str(100.0*nohegemony/(overall)) + '\n') 
 
    overall_positions = dict()
   #compute final TI values per origin/transit AS pair
    #overall_positions[pos_it] keeps a global country count of the position observed for this transit AS
    #To report on file (separate from TI)
    for pfix in pfix_dict:
        if pfix in discard_keys:
            continue
        for pos_it in pfix_dict[pfix][3]:
            try:
                overall_positions[pos_it] = overall_positions[pos_it] + pfix_dict[pfix][3][pos_it]
            except KeyError:
                overall_positions[pos_it] = pfix_dict[pfix][3][pos_it]
                
        col = str(pfix.split(':')[0])
        origin = str(pfix.split(':')[1])
        transit = str(pfix.split(':')[2])
        key = origin + ':' + transit

		#Debugging statement
        if origin != transit and origin == '27952':
            print ("debugging increasing num by " + str(pfix_dict[pfix][0]) + " for prefix " + str(pfix) + " collector " \
                + str(col))

		#We have seen this transit/origin before, update counters
                #matrix_dict has the final values that will be saved to file
                #Now that hegemony filter has been applied
                #TODO this is where we stopped in session 2
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

		#We have not seen this transit/origin pair before, initialize dictionary
        elif key not in matrix_dict and origin != transit:
            if pfix_dict[pfix][1] is not None:
                matrix_dict[key] = [pfix_dict[pfix][0], pfix_dict[pfix][1], pfix_dict[pfix][2]]
            else:
                matrix_dict[key] = [pfix_dict[pfix][0], [], pfix_dict[pfix][2]]
        if origin == transit:
            key = col + ':' + origin
            if origin == '27952':
                print ("debugging increasing denom by " + str(as_dict[key]) + " for prefix " + str(pfix) + " collector "\
                    + str(col) )
            if origin in denom_dict:
                denom_dict[origin] = denom_dict[origin] + as_dict[key]
            else:
                denom_dict[origin] = as_dict[key]

	#Open output matrix file for writing
	#Iterate through each transit/AS pair, compute their cell value and write to file
	#Also write to file some summary statistics per transit AS, on their TI and position
    with open (country_filename,'w+') as f:
        
        
        #g.write('transit,origin,cell\n')
        transits = list(transit_ases)
        origins = list(origin_ases)
        line = 'Transit (row) / Origin (Column) ASN-ASNAME,'
        for i in range(len(origins)):
            this_asname = fetch_asname(origins[i], asname_dict)
            line = line + this_asname + ','
        line = line + 'pos_num,pos_denom,row_sum,#overzero,nonzero_average,pos_min;pos_25th;pos_median;pos_mean;pos_75th;pos_max,CTI'
        f.write(line + '\n') # write header (origins)
        
        #now for the real matrix...
        for i in range(len(transits)):
            this_asname = fetch_asname(transits[i], asname_dict)
            line = this_asname + ','
            row_sum = 0
            row_count = 0
            pos_num = []
            pos_denom = 0
            country_cell = 0
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
                try:
                    discount = discard_matrix[key]
                except KeyError:
                    print "no hegemony found for " + key + " assigning zero"
                    discount = 0
                if num == 0 or denom == 0:
                    cell = '0.0'
                    if origins[j] == '99999':#this is the country all-ASN origin
                        country_cell = '0'

                else:
                    cell = round((float(num) / float(denom-discount)),5)
                    if origins[j] == '99999':
                        print(country_cell)
                        country_cell = str(int(1000000000.0 * float(cell))) #save CTI to 8 decimal points)
                    #if origins[j] == "27952" and (transits[i] == '3549' or transits[i] == '7004'):
                    #    print ("debugging " + str(origins[j]) + " " + str(transits[i]) + " " + str(num) + \
                     #           " " + str(denom) + " " + str(discount))
                    if float(cell) > 1.0:
                        print ' ' + current_country + ' ' + str(cell) +' ' + str(origins[j]) + ' ' + str(transits[i])
                    if origins[j] == transits[i]:
                        cell = '0.0'
                    else:
                        row_sum = row_sum + cell
                        pos_num = pos_num + matrix_dict[key][1] #list with all positions
                        pos_denom = pos_denom + matrix_dict[key][2]

                    if float(cell) > 0.0:
                        row_count = row_count + 1
                    if float(cell) > 1.0 and origins[j] != transits[i]: #Debugging statement (doesn't happen)
                        print "WHY " + str(cell) +' ' + str(origins[j]) + ' ' + str(transits[i])
                        cell = 1.0
                    
                line = line + str(cell) + ',' # this is where each cell is written
		#Compute statistics of AS position
            if row_count > 0:
                average = round(float(row_sum)/float(row_count), 3)
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
            line = line + str(pos_denom) + ',' + str(0) + ',' + str(round(row_sum,3)) + ',' + str(row_count) + ',' + str(average) + ',' + str(pos_average) + ',' + str(country_cell)
            f.write(line + '\n') # write header (origins)

    pos_file = country_filename.replace('.csv','.pos.csv')
	#Write positions to file
    with open (pos_file,'w+') as g:
        for pos_it in overall_positions:
            g.write(str(pos_it) + ',' + str(overall_positions[pos_it]) + '\n')    

def fetch_asname(asn, asname_dict):
	#Simple function to return formatted ASN-ASName from dictionary
    try:
        line = asn + '-' + asname_dict[asn] 
    except KeyError:
        line = asn + '-unknown'
    return line

def sort_country_output():
    #sort ASes by ATI
    command = """awk -F, '{print $2","$1","$(NF-4)","$(NF-5)","$(NF-2)","$(NF-1)","$(NF)","NF}' /project/mapkit/agamerog/country_asn_analysis/country_aspath/""" + current_snapshot + """/<CC>.csv | sort -nr > /project/mapkit/agamerog/country_asn_analysis/country_aspath/sorted_summaries/""" + current_snapshot + """/<CC>.csv"""
    run = command.replace('<CC>', current_country)
    try:
        os.system(run)
        printing_string = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/sorted_summaries/" + current_snapshot + "/" + \
                current_country + ".csv"
        sys.stderr.write('\nsaving ' + printing_string + '\n')
    except:
        sys.stderr.write('\n could not run ' + run + '\n')

#Read AS nationalities into global dictionary
#global_nationality_as = read_nationality_file()

def main():
    asrel_set = read_asrel_file() #read p2c AS-relationships

    mon_dict = read_geo_peers() #read assigned country of BGP monitors (with high confidence)

    asname_dict = read_asname() #Read AS Org Names from AS-Rank into a dictionary

	#read Netacuity mappings of pfix to country
    geolocation, prefix_origin = read_geolocation_file(bg_filename) 
	
	#read BD (RIR delegation) mappings of pfix to country
    delegation = read_delegation_file(bd_filename, geolocation) 
	#read FAO metric for each AS: perc. addresses originated by any AS 
    #in the country
    
    global_origin_as_dict = read_origin_file() 
    #print(global_origin_as_dict)	
	#Read AS-path file prefixes relevant to this country, the transit and 
	#origin ASes involved, and virtualize intermediate outputs of ATI    
    as_dict, pfix_dict, origin_ases, transit_ases = \
            read_as_path_file(aspath_filename, delegation, asrel_set, mon_dict, global_origin_as_dict, prefix_origin)
    #print(pfix_dict)
    #Compute final values of TI and ATI and save to file
    write_country_output(as_dict, pfix_dict, origin_ases, \
        transit_ases, country_filename, asname_dict, global_origin_as_dict)

	#Sort ASes decreasingly on their CTI and save to country summary file
    #sort_country_output()

main()

sys.stderr.write("\n writing " + country_filename + "\n")

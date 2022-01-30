from __future__ import with_statement
#usage python overtime_origin_metric.py country False False snapshot > logs/origin/country.snapshot.txt 2>&1
#usage python overtime_origin_metric.py US False False 20180301 > logs/origin/US.20180301.txt 2>&1
#Reads BG and BD files with prefix-to-country mappings
#Computes fraction of country's address space originated by each AS, considering
#prefix overlap

import subprocess
from multiprocessing import Pool
import bz2
import os
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

#If this input is true, the input files are changed to include .stub at the end
#this is useful to test input/output with small prefix-to-country
#mappings and verify that everything is working correctly
dependencies = {'IO':'GB','WF':'FR','BL':'FR','BM':'GB','HK':'CN','CC':'AU','BQ':'NL','HM':'AU','JE':'GB','FK':'GB','YT':'FR','FO':'DK','PR':'US','TW':'CN','NC':'FR','NF':'AU','RE':'FR','PF':'FR','TK':'NZ','TF':'FR','PN':'GB','TC':'GB','PM':'FR','CK':'NZ','GU':'US','GS':'GB','EH':'MA','VG':'GB','AI':'GB','VI':'US','GG':'GB','GF':'FR','AS':'US','CX':'AU','IM':'GB','AW':'NL','AX':'FI','GP':'FR','GL':'DK','CW':'NL','GI':'GB','MF':'FR','SX':'NL','MO':'CN','BV':'NO','NU':'NZ','UM':'US','SJ':'NO','SH':'GB','MQ':'FR','MP':'US','MS':'GB','KY':'GB'}
stub_input = str(sys.argv[3])
if stub_input == 'True':
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
if testing_input == 'True':
    testing_file = True
else:
    testing_file = False

#Snapshot to analyse, in YYYYMMDD format
current_snapshot = sys.argv[4]

#Output file with top ASes that we used for a previous visualization of the matrix.
#Currently file is produced but unused
heatmap_filename = "/project/mapkit/agamerog/country_asn_analysis/heatmap/" + current_country + ".csv"

#location of the prefix-to-country and AS-to-country mapping from delegation files
bd_filename = "/project/mapkit/agamerog/country_asn_analysis/bd/" + current_snapshot + ".txt" + testing_string

#Output file of this script with 
#OriginASN,OriginASName,#Prefixes,%country
#Here #prefixes is the number of addresses in /24 blocks
origin_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/"+ current_snapshot + '/' + current_country + ".csv"

#location of the prefix-to-country and AS-to-country mapping from Netacuity
bg_filename = "/project/mapkit/agamerog/country_asn_analysis/geolocation_bgp/maxmind/pfx-to-country." + current_snapshot[:4] + "-" + current_snapshot[4:6]+ "-" + current_snapshot[6:] + testing_string

#Input file with prefixes and paths seen on BGP
aspath_filename = "/data/external/as-rank-ribs/" + current_snapshot + "/" + current_snapshot + ".all-paths.bz2" + testing_string

#Use short input file if input is true
if stub_input == 'True':
    aspath_filename = "/project/mapkit/agamerog/country_asn_analysis/20180301.all-paths.v4only.txt.stub"

#Create output directory if it doesn't already exist
directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/" + current_snapshot + "/"
os.system(directory_creation_string)
directory_creation_string = "mkdir -p /project/mapkit/agamerog/country_asn_analysis/country_aspath/logs/origin/"

os.system(directory_creation_string)

#Input file with inferred AS-relationships
asrel_filename = '/data/external/as-rank-ribs/' + current_snapshot + "/" + current_snapshot + '.as-rel.txt.bz2'

global_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/half.csv"

#Set limits on memory to 4 GB (can be changed to any arbitrary number)
#limit is higher for the US due to significantly higher computational reqs
#if current_country != "US":
#    os.system('ulimit -d 75000000; ulimit -m 75000000; ulimit -v 75000000')
#else:
os.system('ulimit -d 4000000; ulimit -m 4000000; ulimit -v 4000000')

def read_asrel_file():
    #Create a set of AS-AS strings
    #Separated by a ':' where one is an inferred transit provider of the other
    #'provider:customer' where provider and customer are both ASNumbers

    asrel_set = set()
    with bz2.BZ2File(asrel_filename,'rb') as f:
        rows = f.readlines()
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


def systemCall(parameter):
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
                continue #skip header 

            prefix_length = int(row.split('|')[0].split('/')[1])
            if prefix_length > 24:
                continue #ignore anything larger than a /24
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
            country_set.add(row.split('|')[0])
            asn = row.split('|')[3]

            #calculate number of /24 blocks in this prefix, based on
            #the number of addresses assigned to this country
            #in this prefix
            tmp = 32.0 - np.log2(int(row.split('|')[2]))

            prefix_length = 2**(24 - int(tmp))
            if int(prefix_length) < 1:
                prefix_length = 1 #If less than 256 address still bill one slash 24 for consistency with transit

            #Create key using prefix (padded if /8 or /9 to /08 and /09) and asn    
            key = pad_prefix(row.split('|')[0]) + ':' + asn 

            if key in prefix_origin:
                prefix_origin[key] = prefix_origin[key] + prefix_length
                #If we already saw this prefix for this AS,
                #increase the number of addresses with this new row

            else:
                prefix_origin[key] = prefix_length
                #Otherwise initialize prefix:AS
        #Return set of country prefixes and dictionary with key= 
        #prefixes originated by each AS
        #and value = number of addresses (in /24 blocks)
        #sys.stderr.write("BG country set: " + str(country_set) + "\n\n")
        #sys.stderr.write("BG prefix origin: " + str(prefix_origin) + "\n\n")
        return country_set, prefix_origin

def read_delegation_file(filename, country_set):
    #Read prefixes geolocated to each country from delegation files
    #Skip prefixes with length 25 or more, as we are measuring in /24 blocks
    bd_set = set()
    with open(filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + filename + "\n")
            sys.exit()
        for i in range(len(ipmap_list)):
            row = ipmap_list[i]
            if '#' in row:
                continue #skip headera
            if '_' in row:
                continue #skip MOAS
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

            prefix_length = int(row.split('|')[0].split('/')[1])
            if prefix_length > 24 or prefix_length < 8: #illegal length
                continue

            country_set.add(row.split('|')[0])
            bd_set.add(row.split('|')[0])
        #sys.stderr.write("BD prefixes: " + str(bd_set) + "\n\n")
    #Return set of prefixes
    return country_set

def read_asname():
    #Read AS-Names into memory from AS-Rank's input file (Org. Name)
    #Take care of Unicode issues
    #And cut to about 35 characters
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
            AStextlist = u''.join(AStextlist).encode('utf-8').replace(',','')
            AStextlist = AStextlist[:36]
            ASdict[ASnumstr] = AStextlist
    return ASdict

def sort_by_length(subtree):
    #pad prefixes with /8 and /9 so sorting is done appropriately
    #(last two characters)
    #return sorted prefixes starting by most specific
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
    all_monitors = set() #these are ASes
    for i in range(len(sorted_prefixes)):
        rnode = rtree.search_exact(sorted_prefixes[i])
        current_keys = rnode.data.keys() #key is monitor
        for j in range(len(current_keys)):
            all_monitors.add(current_keys[j])
            
    return all_monitors
    
def pad_prefix(pfx):
    #pad prefixes /8 and /9 so sorting can be done based on last two characters
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
    path = raw_path.split('|')
    looking_set = set(['1299'])
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
    excluders = set
    for i in range( (len(split_path) - 1 ) ):
        test_provider = split_path[i]
        test_customer = split_path[i+1]
        possible_p2c = test_provider + ':' + test_customer

        #see if this pair of ASes is a p2c relationship
        if possible_p2c in asrel_set:
            ignore_path = False
            break
        #remove AS to the left below
        if i == 0:
            continue #make sure we keep host AS (used for collector)
        #Remove further upstream ASes after last upstream provider of origin
        removing_AS = split_path[i] + '|'
        path = path.replace(removing_AS,'')
        
    if ignore_path:
        return ''
    else:
        return path

def read_as_path_file(filename, country_set, asrel_set, asname_dict, prefix_origin):
    #Read AS-paths file and compute share of country addresses originated by an AS,
    #sort decreasingly and save to file
    #Sample line of AS-paths file
    #ripe/rrc00|5 4777|2497|3257|8452 156.208.128.0/18 i 202.12.28.1
    #Where 8452 is the origin AS and 156.208.128.0/18 is the prefix
    #Other fields are not relevant for this analysis
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
    log_file_new = 'logs/log_' + current_country + '.txt'
    log_new = open(log_file_new,'w+')

    #read short input file if appropriate based on arguments
    out_file = 'singlecountryfiles/maxmind.' + current_country + '.' + current_snapshot + '.txt'

    with open(out_file, 'w+') as single:
        if current_country == 'BF' and current_snapshot == '20200101':
            f = open('bfpaths.20200101.txt','rb')
        elif '.stub' in filename:
            f = open(filename, 'rb')
        else:
            f = bz2.BZ2File(filename, 'rb')
            #Open full input file (regular case)
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + filename + "\n")
            sys.exit()
        
        #Iterate through lines of AS-paths file
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                sys.stderr.write('skipping row with h :' + row + '\n') 
                continue

            #Attempt to read prefix from line, if line not separated by
            #spaces save line to log
            try:
                aspath_prefix = row.split(' ')[2]
            except IndexError:
                line = row + '\n'
                log_new.write(line)
                sys.stderr.write('skipping improperly formatted row :' + row + '\n')
                continue
            
            if aspath_prefix not in country_set:
                #TODO AGG1 this is effed up; only exact /24 prefixes will pass this test
                continue #read only prefixes in this country
            
            #Read path and origin AS
            path = row.split(' ')[1]
            origin = path.split('|')[-1] 
            if origin == '23456':
                continue 
                #skipping reserved AS

            #THIS FOR Transit Only; Cut path at highest upstream p2c from origin
            #if no p2c along path skip
            #new_path = path_cut_peak(path, asrel_set)

            #Grab collector (BGP host AS)
            single.write(ipmap_list[i])
    bzipping = 'bzip2 -f ' + out_file
    try:
        os.system(bzipping)
    except:
        sys.stderr.write('could not bzip2 file ' + out_file)

def main():

    asrel_set = read_asrel_file() #read AS-relationships of p2c

    asname_dict = read_asname() #read asnames into memory

    #read prefixes assigned to this country and originated by each AS
    geolocation, prefix_origin = read_geolocation_file(bg_filename) 

    #Include additional prefixes assigned to this country based on delegation files
    delegation = read_delegation_file(bd_filename, geolocation)

    #Read AS-paths file and compute share of country addresses originated by an AS,
    #sort decreasingly and save to file
    read_as_path_file(aspath_filename, delegation, asrel_set, asname_dict, prefix_origin)
    
main()


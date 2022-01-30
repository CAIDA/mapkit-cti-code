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
import targetSet
import newFinalTargetCountries
#from ripe.atlas.sagan import Result
#from ripe.atlas.sagan.traceroute import TracerouteResult, Hop, Packet

# Create a new radix tree to store network prefix information

#If this input is true, the input files are changed to include .stub at the end
#this is useful to test input/output with small prefix-to-country
#mappings and verify that everything is working correctly
#Input file with AS-relationships (to read provider-customer pairs; and peer pairs)
asrel_filename = '/data/external/as-rank-ribs/20200301/20200301.as-rel.txt.bz2'
targetDict = newFinalTargetCountries.targetCountries
#country_target_set = targetSet.target_set
#Input file with FAO metric per AS. Used to compute FASR outputs
#origin_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/"+ current_snapshot + '/' + current_country + ".csv"

#Input file with the inferred "nationality" of each AS
#nationality_filename = "/project/comcast-ping/stabledist/mapkit/code/ATIstatistics/DomesticInternational/" + current_country + ".csv"

os.system('ulimit -d 4000000; ulimit -m 4000000; ulimit -v 4000000')

#collectors with multi-hop forwarding, ignored

def read_origin_file(current_country):
    #Read file with number of addresses, and the percentage of the country
	#they represent, for each AS with addresses in this country
	#Returns a dictionary where the key is the AS Number and the value
	#is a list with two variables (#addresses and %country)
    origin_dict = dict()
    origin_filename = '/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/20200301/' + current_country + '.csv'
    if current_country == 'US':
        origin_dict['7922'] = 6.4
        return origin_dict

    with open(origin_filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + origin_filename + "\n")
            sys.exit()
        country_total = 0 #count for the entire country (for the '99999' all ASN origin)
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            headercheck = row.split(',')[0]
            if headercheck == 'OriginASN':
                continue
            origin_perc = float(row.split(',')[2])
            origin_num = 256 * int(float(row.split(',')[1])) #number of addresses
                #and percentage of country
            asn = row.split(',')[0].split('-')[0]
            origin_dict[asn] = origin_perc

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
            if '#' in row:
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
    for cc in targetDict:
        curr_ases = targetDict[cc]
        for i in range(len(curr_ases)):

            asn = curr_ases[i]

            cc_asn = asn + ':' + cc
            ases.add(asn)
            country_ases.add(cc_asn)
            countries.add(cc)

    return country_ases, ases, countries


def read_integrated_out(ati_dict, origin_dict, cone_set, country_target_set):
    country_links_file = '/project/mapkit/agamerog/country_asn_analysis/integratedan/output/combined.aslevel.csv'
    aslevel_dict = dict() #save individual interfaces in set
    reltype_dict = dict() #dict with 'IP':set(['AS1':'AS2','AS1:AS3',...])
    asnames = dict()
    #the first AS in the pair is the target AS, and the second AS is
    #the neighbor
    country_origin_dict = dict() #'cc:origin':dict('total':total,'peer':peer,...)
    country_origin_neighbor_dict = dict()
    with open(country_links_file,'rb') as link_file:
        reader = csv.reader(link_file, delimiter=',')
        #neighset = set()
        for row in reader:

            if '#' in row[0]:
                continue #skip headers
            country = row[0]
            origin_as = row[1].split('-')[0]
            if origin_as not in asnames:
                asnames[origin_as] = row[1]

            neighbor_as = row[2].split('-')[0]
            if len(neighbor_as) == 0: #TODO fix weird bdrmap set thing when Marder clarifies
                continue
            #neighset.add(neighbor_as) 
            if neighbor_as not in asnames: 
                asnames[neighbor_as] = row[2]
            rel = row[3]
            if rel == 'none':
                conecheck = neighbor_as + ':' + origin_as
                if conecheck in cone_set:
                    rel = 'indirect'
            traces = int(row[4])

            country_origin = country + ':' + origin_as
            #relationship level
            if country_origin in country_origin_dict:
                if rel in country_origin_dict[country_origin]:
                    country_origin_dict[country_origin][rel] = country_origin_dict[country_origin][rel] + traces
                else:
                    country_origin_dict[country_origin][rel] = traces
                country_origin_dict[country_origin]['total'] = country_origin_dict[country_origin]['total'] + traces

            else:
                country_origin_dict[country_origin] = dict()
                country_origin_dict[country_origin]['total'] = traces
                country_origin_dict[country_origin][rel] = traces
            
            #AS level
            country_origin_neighbor = country + ':' + origin_as + ':' + neighbor_as + ':' + rel

            if country_origin_neighbor in country_origin_neighbor_dict:
                country_origin_neighbor_dict[country_origin_neighbor] = country_origin_neighbor_dict[country_origin_neighbor] + traces
            else:
                country_origin_neighbor_dict[country_origin_neighbor] = traces
#    print(country_origin_dict)

    #print(neighset)
    for country_origin_neighbor in country_origin_neighbor_dict:
        country_origin = country_origin_neighbor.split(':')[0] + ':' + country_origin_neighbor.split(':')[1]
        denom = country_origin_dict[country_origin]['total']
        frac = float(country_origin_neighbor_dict[country_origin_neighbor]) / float(denom)
        key = country_origin_neighbor.replace(':',',')
        aslevel_dict[key] = str(frac) + ',' + str(denom)

    for country_origin in country_origin_dict:
        p2c_c2p = 0.0
        denom = float(country_origin_dict[country_origin]['total'])
        if 'provider' in country_origin_dict[country_origin]:
            provider = str(float(country_origin_dict[country_origin]['provider']) / denom)
            p2c_c2p = float(country_origin_dict[country_origin]['provider']) 
        else:
            provider = '0.0'
        if 'customer' in country_origin_dict[country_origin]:
            customer = str(float(country_origin_dict[country_origin]['customer']) / denom)
            p2c_c2p = (p2c_c2p + float(country_origin_dict[country_origin]['customer']))
        else:
            customer = '0.0'
        if 'peer' in country_origin_dict[country_origin]:
            peer = str(float(country_origin_dict[country_origin]['peer']) / denom)
        else:
            peer = '0.0'
        if 'none' in country_origin_dict[country_origin]:
            none = str(float(country_origin_dict[country_origin]['none']) / denom)
        else:
            none = '0.0'
        if 'indirect' in country_origin_dict[country_origin]:
            indirect = str(float(country_origin_dict[country_origin]['indirect']) / denom)
        else:
            indirect = '0.0'
        key = country_origin.replace(':',',')
        p2c_c2p_2 = float(p2c_c2p)/denom
        reltype_dict[key] = provider + ',' + customer + ',' + str(p2c_c2p_2) + ',' + peer + ',' + none + ',' + indirect + ',' + str(int(denom))
#return aslevel_dict, reltype_dict
#aslevel_dict[country,origin,neighbor,rel] = str(frac) + ',' + str(denom)
#reltype_dict[key] = provider + ',' + customer + ',' + str(p2c_c2p) + ',' + peer + ',' + none + ',' + str(denom)
#asnames[neighbor_as] = row[2

    with open('aslevel.summary.202003.csv','w+') as f:
        f.write('country,origin,neighbor,rel,frac,origin_total_traces,ati,originated_add\n')
        for aslevel in aslevel_dict:
            #AGG TODO this is an ugly way to filter ATI but works for now
            checkingkey = aslevel.split(',')[0] + ':' +aslevel.split(',')[1] 
            if checkingkey not in country_target_set:
                continue
            atikey = aslevel.split(',')[0] + ':' +aslevel.split(',')[1] + ':' + aslevel.split(',')[2]  
            if atikey in ati_dict:
                atistr = str(ati_dict[atikey])
            else:
                atistr = '0.0'
            cc = aslevel.split(',')[0]
            if cc == 'US':
                originstr = '6.4'
            else:
                originkey = aslevel.split(',')[1]
                #print (cc)
                #print (originkey)
                originstr = str(origin_dict[cc][originkey])
            rel = aslevel.split(',')[3]
            newaslevel = aslevel
            if rel == 'none':
                conecheck = aslevel.split(',')[2] + ':' + aslevel.split(',')[1]
                if conecheck in cone_set:
                    newaslevel = aslevel.replace('none','indirect')
                
            line = newaslevel + ',' + aslevel_dict[aslevel] + ',' + atistr + ',' + originstr + '\n'
            f.write(line)
    weighted_frac_dict = dict()
    with open('rellevel.summary.202003.csv','w+') as g:
        #reltype_dict[country,origin] = provider + ',' + customer + ',' + str(p2c_c2p) + ',' + peer + ',' + none +','+ indirect + ',' + str(denom)
         g.write('country,origin,provider,customer,p2c_or_c2p,peer,none,indirect,total,origin_add\n')
         for rellevel in reltype_dict:
            cc = rellevel.split(',')[0]
           
            #if cc == 'US':
            #    originstr = '6.4'
            #elif cc == 'NA':
            #    originstr = '0.0'
            #else:
            originkey = rellevel.split(',')[1]
            originstr = str(origin_dict[cc][originkey])
            if cc in weighted_frac_dict:
                proplus = float(reltype_dict[rellevel].split(',')[0]) #+ float(reltype_dict[rellevel].split(',')[-2]) #Fraction of traceroutes going through a direct transit provider
                
                weighted_frac_dict[cc][0] = float(originstr) * proplus + (weighted_frac_dict[cc][0])
                weighted_frac_dict[cc][1] = float(originstr) + weighted_frac_dict[cc][1] 
            else:
                proplus = float(reltype_dict[rellevel].split(',')[0]) #+ float(reltype_dict[rellevel].split(',')[-2])
                weighted_frac_dict[cc] = [proplus*float(originstr), float(originstr)]
            line = rellevel + ',' + reltype_dict[rellevel] + ',' + originstr + '\n'
            g.write(line)
    #skip_set = set(['US','NR','MH','CF','ZM','FJ','GW','KM','PE','PS','HN','FM','CD','MN'])
    transitSetPrint = set()
    with open('country.transit.summary.202003.csv','w+') as f:

        f.write('cc,cc_name,transit_w,orig_frac,transit_w_scaled,meet_cti\n')
        for count in weighted_frac_dict:
            #parse dictionaries to obtain country-level figures of 
            #prevalence of transit in observed as-level links
            #and print boolean to sort countries by
            test = 0
            if count == 'US':
                continue
            CountryName = global_countries[count]
            TransitWeightedFraction = str(weighted_frac_dict[count][0] \
                    / weighted_frac_dict[count][1])
            #if float(TransitWeightedFraction) > 0.6:
            test = test + 1
            OriginatedFraction = str(weighted_frac_dict[count][1]/float(100))
            #if float(OriginatedFraction) > 0.6:
            test = test + 1
            TransitWeightedFractionScaled = str((weighted_frac_dict[count][1]/float(100)) * \
                    (weighted_frac_dict[count][0] / weighted_frac_dict[count][1]))
            if float(TransitWeightedFractionScaled) >= 0.476722434:
                test = test + 1
            if test == 3:
                printBool = 'True'
                transitSetPrint.add(count)
            else:
                printBool = 'False'

            f.write(count + ',' + CountryName + ',' + TransitWeightedFraction + ',' + OriginatedFraction + ',' + TransitWeightedFractionScaled + ',' + printBool + '\n')
    print ("TRANSIT COUNTRIES: " + str(len(transitSetPrint)) + " \n\n")
    print (sorted(list(transitSetPrint)))
    return aslevel_dict
   
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

def read_ati(countries):
    #countries = set(['SY'])
    print(countries)
        #the first AS in the pair is the target AS, and the second AS is
        #the neighbor
    country_origin_neighbor_dict = dict()
    origin_dict = dict()
    #AGG TODO this is being skipped for now because I haven't run ATI
    
    #for country in countries:
    '''
        if country == 'NA' or country == 'US':
            continue #stupid missing Namibia
        country_links_file = '/project/mapkit/agamerog/country_asn_analysis/country_aspath/20200301/' + country + '.csv'
        transits = dict()

        with open(country_links_file,'rb') as link_file:
            reader = csv.reader(link_file, delimiter=',')
            #neighset = set()
            header = True
            numtransits = 0
            for row in reader:

                if header:
                    for i in range(len(row)):
                        if i == 0:
                            continue
                        if row[i] == 'pos_num':
                            break
                        numtransits = numtransits + 1
                        transit = row[i].split('-')[0]
                        transits[i] = transit
                    header = False
                    continue
                origin = row[0].split('-')[0]
                j = 1
                while j <= numtransits:
                    ati = row[j]
                    transitkey = transits[j]
                    key = country + ':' + transitkey + ':' + origin #These are misnamed, but ati is correct
                    country_origin_neighbor_dict[key] = ati
                    j = j + 1
    '''
    for country in countries:
        origin_dict[country] = read_origin_file(country)
    #print(origin_dict)
    
    return country_origin_neighbor_dict, origin_dict
    #country_origin_neighbor_dict[country + ':' + transitkey + ':' + origin] = ati
    #origin_dict[country] = 'as':originated(float)

def read_cone_file():
    #Create a set of AS-AS strings
    #Separated by a ':' where one is an inferred transit provider of the other
    #'provider:customer' where provider and customer are both ASNumbers
    cone_set = set()
    cone_file = '/data/external/as-rank-ribs/20200301/20200301.ppdc-ases.txt.bz2'
    with bz2.BZ2File(cone_file, "r") as f:
        reader = csv.reader(f, delimiter=' ')
        #neighset = set()
        for row in reader:

            if '#' in row[0]:
                continue #skip headers
            transit = row[0]
            #print(transit)
            for i in range(len(row)):
                if i == 0:
                    continue #skip transit
                customer = row[i]
                cone_set.add(transit + ':' + customer)
    return cone_set
def read_summary_file():
    country_dict = dict()

    summary_filename = "/home/agamerog/influenceimc18/country_influence_imc18/data/country/country_info_no_note.csv"
    with open(summary_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()

        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if i == 0:
                continue #skip header #AGG need to automate skipping headers
            code = row.split(',')[0]
            addies = row.split(',')[1]
            country_dict[code] = addies
        return country_dict
global_countries = read_summary_file()
def main():

    #transit_set, peer_set = read_asrel_file() #read p2c AS-relationships
    #transit set is provider:customer
    #peer set is peerone:peertwo

    #asname_dict = read_asname() #Read AS Org Names from AS-Rank into a dictionary
    cone_set = read_cone_file()
    #print (cone_set)
    target_country_ases, target_ases, countries = read_target_ases_countries() #read set of countries and ASes there that were targeted with the traceroute campaign
    ati_dict, origin_dict = read_ati(countries)
    
    aslevel_dict = read_integrated_out(ati_dict, origin_dict, cone_set, target_country_ases) #counts, fractions, etc for each as-level relationship and for each relationship type per origin as
    #aslevel_dict[country,origin,neighbor,rel] = str(frac) + ',' + str(denom)

    #aslevel_dict, iplevel_dict = read_traceroute_file(transit_set, peer_set, asname_dict, probe_to_country, probe_to_asn, \
    #        target_country_ases, target_ases, bdrmapit_interfaces, interface_to_iconnect)

    #write_ip_as_files(aslevel_dict, iplevel_dict, asname_dict, transit_set, peer_set)

main()

#sys.stderr.write("\n writing " + country_filename + "\n")

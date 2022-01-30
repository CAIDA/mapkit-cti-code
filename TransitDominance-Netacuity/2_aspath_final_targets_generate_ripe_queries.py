import json
import csv
import sys
import bz2 
import os
import json_lines
from random import randrange
import random
import newFinalTargetCountries
import time
import os.path
#python aspath_final_targets_generate_ripe_queries.py 20200318 > logs/20200318_targets.txt 2>&1
dependencies = {'IO':'GB','WF':'FR','BL':'FR','BM':'GB','HK':'CN','CC':'AU','BQ':'NL','HM':'AU','JE':'GB','FK':'GB','YT':'FR','FO':'DK','PR':'US','TW':'CN','NC':'FR','NF':'AU','RE':'FR','PF':'FR','TK':'NZ','TF':'FR','PN':'GB','TC':'GB','PM':'FR','CK':'NZ','GU':'US','GS':'GB','EH':'MA','VG':'GB','AI':'GB','VI':'US','GG':'GB','GF':'FR','AS':'US','CX':'AU','IM':'GB','AW':'NL','AX':'FI','GP':'FR','GL':'DK','CW':'NL','GI':'GB','MF':'FR','SX':'NL','MO':'CN','BV':'NO','NU':'NZ','UM':'US','SJ':'NO','SH':'GB','MQ':'FR','MP':'US','MS':'GB','KY':'GB'}

csv.field_size_limit(sys.maxsize) #thing so we can read Elverton's huge csv lines

bg_filename = "/project/mapkit/agamerog/country_asn_analysis/geolocation_bgp/pfxgeo/pfx-to-country.2020-03-01"
ripeDay = sys.argv[1] 
bd_filename = "/project/mapkit/agamerog/country_asn_analysis/bd/20200301.txt"


targetDict = newFinalTargetCountries.targetCountries
country_file = 'newFinalAllCountries.sh'

header = """{"definitions":["""
eu_countries = set(['BE','BG','CZ','DK','DE','EE','IE','GR','ES','FR','HR','IT','CY','LV','LT','LU','HU','MT','NL','AT','PL','PT','RO','SI','SK','FI','SE','GB','IS','LI','NO','CH'])

probe_ids = ['27555','51919','30762','33514','50486','12808','31416','10661','1154','33368','16900','15387','21028','6615','6473','22382','6028','23191','6237','6465','6699','20337','16562','33346','25283','22221','12207','33267','32968','6580','33048','6339','19217','14672','51371','3981','50350','20482','6400','6514','18819','6329','23218','13448','18350','6235','55042','6060','6535','6332','6463','6531','30501','6446','6471','6559','6362','6271','25605','20757','11602','18273','13881','6050','31019','10612','6497','6411','10640','25393','6524','35073','16900','10386','6615','21028','17833','35557','21378','32262','17634','30251','10415','6613','22709','6080','34742','6259','29006','6130','6599','6638','6643','18414','6480','6409','33449','22961','32205','19251']

traceWrap = """{"target":"<address>","af":4,"timeout":4000,"description":"Traceroute measurement to <address>","protocol":"ICMP","resolve_on_probe":false,"packets":1,"size":48,"first_hop":1,"max_hops":32,"paris":16,"destination_option_size":0,"hop_by_hop_option_size":0,"dont_fragment":false,"skip_dns_check":false,"type":"traceroute"}"""

footer = """],"probes":[{"value":"<probeSet>","type":"probes","requested":<probeNumber>}],"is_oneoff":true,"bill_to":"agamerog@eng.ucsd.edu"}"""

def read_ripe_file(newripeDay):
    ripe_file = '/project/mapkit/agamerog/country_asn_analysis/ripeprobes/' + newripeDay + '.json'
    probe_id_to_cc = dict()
    probe_id_to_asn = dict()
    probe_id_to_status = dict()
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
            try:
                probe_status = str(probe["status_name"])
            except KeyError:
                probe_status = 'Unknown'
            probe_id_to_status[probe_id] = probe_status
            probe_id_to_asn[probe_id] = probe_asnumber


    #print(print_set)
    return probe_id_to_cc, probe_id_to_asn, probe_id_to_status

def turn_probes_into_str(probes): #turn list of probes into a string separated by commas
    out_str = ''
    counter = 0
    for probe in probes:
        if counter < (len(probes) - 1):

            out_str = out_str + str(probe) + ','
        else:
            out_str = out_str + str(probe)
        counter = counter + 1
    return out_str

def generate_ripe_query_comcast(targets, probes, newripeDay):
    #need to repeat this first 800 targets at a time for countries, then 800 probes at a time for comcast
    #(another function)
    #with 20min wait in between. Then fill up the rest of the time until 12 hours
    #this function needs to be called twice
    countryFiles = set()
    targetList = list(targets)
    probeList = list(probes)
    num_files = len(probes) // 800 
    if len(probes) % 800 > 0:
        num_files = num_files + 1 #there are some targets left for an additional file
    offset_multiplier = 0 #start at targets 0-799
    while offset_multiplier < num_files:
        country_file_targets = 'ripeQueries/' + ripeDay + '_Comcast_' + str(offset_multiplier) + '.sh'
        #slice list 800 probes at a time and save to different files
        underIndex = offset_multiplier * 800
        overIndex = min((offset_multiplier+1) * 800, len(probeList))
        probeListSlice = probeList[underIndex:overIndex]
        probe_str = turn_probes_into_str(probeListSlice) #get the 800 probes in the right str format

        with open(country_file_targets,'w+') as f:
            f.write(header)
            for i in range(len(targetList)):
                if i < (len(targets)-1):
                    line = traceWrap.replace('<address>',targets[i]) + ','
                else:
                    line = traceWrap.replace('<address>',targets[i])
                f.write(line)
            current_footer = footer.replace('<probeSet>',probe_str)
            final_footer = current_footer.replace('<probeNumber>',str(len(probeListSlice)))
            f.write(final_footer)
        
        offset_multiplier = offset_multiplier + 1
        countryFiles.add(country_file_targets.split('/')[1])

    return countryFiles


def generate_ripe_query(targets, probes, newripeDay):
    #need to repeat this first 800 targets at a time for countries, then 800 probes at a time for comcast
    #(another function)
    #with 20min wait in between. Then fill up the rest of the time until 12 hours
    #this function needs to be called twice
    out_files = set()
    targetList = list(targets)
    num_files = len(targets) // 800 
    probe_str = turn_probes_into_str(probes)
    if len(targets) % 800 > 0:
        num_files = num_files + 1 #there are some targets left for an additional file
    offset_multiplier = 0 #start at targets 0-799
    while offset_multiplier < num_files:
        country_file_targets = 'ripeQueries/' + ripeDay + '_' + str(offset_multiplier) + '.sh'
        #slice list 800 IPs at a time and save to different files
        underIndex = offset_multiplier * 800
        overIndex = min((offset_multiplier+1) * 800, len(targetList))

        targetListSlice = targetList[underIndex:overIndex]
        with open(country_file_targets,'w+') as f:
            f.write(header)
            for i in range(len(targetListSlice)):
                if i < (len(targets)-1):
                    line = traceWrap.replace('<address>',targets[i]) + ','
                else:
                    line = traceWrap.replace('<address>',targets[i])
                f.write(line)
            current_footer = footer.replace('<probeSet>',probe_str)
            final_footer = current_footer.replace('<probeNumber>',str(len(probes)))
            f.write(final_footer)
        
        offset_multiplier = offset_multiplier + 1
        out_files.add(country_file_targets.split('/')[1])

    return out_files

def find_probes(probe_to_country, probe_to_asn, probe_to_status):
    probe_countries = set()
    target_ases = set()
    probe_removal = set()
    final_probes = set(probe_ids)

    for targetcountry in targetDict:
        for i in range(len(targetDict[targetcountry])):
            target_ases.add(targetDict[targetcountry][i]) #add all ASes that are ever a target to set

    for i in range(len(probe_ids)):
        current_id = probe_ids[i] #very that the probe's status is active
        try:
            status_check = probe_to_status[current_id]
        except KeyError:
            status_check = 'Unknown'
        if probe_to_status[current_id] != 'Connected':
            probe_removal.add(current_id)
            #print('disconnected probe ' + str(current_id) + ' ' + str(probe_to_country[current_id]))
        else:
            #print('connected probe ' +   str(current_id) + ' ' + str(probe_to_country[current_id]))
            try:
                current_probe_country = probe_to_country[current_id]
                if current_probe_country in targetDict:
                    probe_removal.add(current_id) #probe in one of the target countries, forget about it
            except KeyError:
                probe_removal.add(current_id) # we don't know the country so best to skip
        try:
            current_probe_asn = probe_to_asn[current_id]
            if current_probe_asn in target_ases:
                probe_removal.add(current_id)
        except KeyError:
            probe_removal.add(current_id)
    returning_probes = final_probes.difference(probe_removal)

    while len(returning_probes) < 100:
        new_probe = find_country_probe(returning_probes, probe_to_country, probe_to_asn, probe_to_status, target_ases)
        returning_probes.add(new_probe)

    return returning_probes

def find_country_probe(returning_probes, probe_to_country, probe_to_asn, probe_to_status, target_ases):
    for probe_id in probe_to_country:

        current_id = probe_id #very that the probe's status is active
        if current_id in returning_probes:
            continue #we're already using this probe
        try:
            status_check = probe_to_status[current_id]
        except KeyError:
            continue #no status, does not help
        if probe_to_status[current_id] != 'Connected':
            continue #probe is not connected
            #print('disconnected probe ' + str(current_id) + ' ' + str(probe_to_country[current_id]))
        else:
            #print('connected probe ' +   str(current_id) + ' ' + str(probe_to_country[current_id]))
            current_probe_country = probe_to_country[current_id]
            if current_probe_country in targetDict:
                continue #can't use this probe
           
            try:
                current_probe_asn = probe_to_asn[current_id]
                if current_probe_asn in target_ases:
                    continue #can't use this probe because it's in a target AS

            except KeyError:
                continue #we don't know the AS so we can't use it
            if current_probe_country == 'US' or current_probe_country in eu_countries:
                return current_id #bingo! found a probe in the US or the EU that we can use

def find_comcast_probes(probe_to_country, probe_to_asn, probe_to_status):
    comcast_probes = set()
    asn_already_has_probe = set()
    for probe_id in probe_to_country:

        current_id = probe_id #very that the probe's status is active
        try:
            status_check = probe_to_status[current_id]
        except KeyError:
            continue #no status, does not help
        if probe_to_status[current_id] != 'Connected':
            continue #probe is not connected
            #print('disconnected probe ' + str(current_id) + ' ' + str(probe_to_country[current_id]))
        else:
            #print('connected probe ' +   str(current_id) + ' ' + str(probe_to_country[current_id]))
            current_probe_country = probe_to_country[current_id]
            if current_probe_country == 'US':
                continue #can't use this probe
           
            try:
                current_probe_asn = probe_to_asn[current_id]
                if current_probe_asn in asn_already_has_probe:
                    continue #can't use this probe because it's in a target AS
                else:
                    asn_already_has_probe.add(current_probe_asn)
            except KeyError:
                continue #we don't know the AS so we can't use it
            comcast_probes.add(current_id)
    return comcast_probes

def generate_country_targets(targets_already_hit, newripeDay, prefix_set, country_dict):
    targets = []
    g=open('targetsprobed/aspath/target_ips_BDandBG_countries_aspath_' + ripeDay + '.csv','w+')
    g.write('country,AS,IP\n')
    num_countries = str(len(targetDict))
    countryCount = 1
    parsedAsnCountry = set()
    bdTargets = dict()
    #targetDict['US'] = ['7922'] #this must be done later because otherwise the probe selection for the 
    #other countries would break
    allAses = set()
    countryAsPairs = set()
    
    for country in targetDict:
        curr_country = country
        curr_ases = []
        curr_ases = targetDict[curr_country] #grab the list of ASes for this country from the dictionary
        allAses = allAses.union(set(curr_ases))

        sys.stderr.write('processing ' + curr_country + ' ' + str(countryCount) + '/' + num_countries + '\n') #just to keep
        #track of progress
        
        countryCount = countryCount + 1
        prefix_parsed = set()
        
        for i in range(len(curr_ases)):
            curr_asn = curr_ases[i]
            #foundTarget = False
            countryAsPairs.add(country + ':' + curr_asn)
    #print(countryAsPairs)
    parsed_prefixes = set()
    foundTarget = set()
    with bz2.BZ2File("/data/external/as-rank-ribs/20180301/20180301.all-paths.bz2", 'rb') as f:
        ipmap_list = f.readlines()
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
                sys.stderr.write('skipping improperly formatted row :' + row + '\n')
                continue

            if aspath_prefix not in prefix_set:
                #TODO AGG1 this is effed up; only exact /24 prefixes will pass this test
                continue #read only prefixes in this country        

            path = row.split(' ')[1]
            origin = path.split('|')[-1]
            if origin not in allAses:
                continue #this prefix may be of interest but it's not originated by an AS we care about
            pfx = aspath_prefix
            pfxCountry = country_dict[pfx]
            #countryAsPairs.add(country + ':' + curr_asn)
            key = pfxCountry + ':' + origin
            if key not in countryAsPairs:
                continue #this AS is not in the set of targets for the country of this prefix
            if key not in foundTarget:

                foundTarget.add(pfxCountry + ':' + origin)
            else:
                continue #we already have a target for this guy

            ipAdd = aspath_prefix.split('/')[0]
            outIp = ipAdd.split('.')[0] + '.' + ipAdd.split('.')[1] + '.' + ipAdd.split('.')[2] + '.' + str(randrange(255))
            while outIp in targets_already_hit:
                outIp = ipAdd.split('.')[0] + '.' + ipAdd.split('.')[1] + '.' + ipAdd.split('.')[2] + '.' + str(randrange(255)) #replace last segment of IP until it has not been hit in the past
                    #prefix_full = ipAdd.split('.')[0] + '.' + ipAdd.split('.')[1] #+ '.' + ipAdd.split('.')[2]
            targets.append(outIp)
            g.write(pfxCountry + ',' + origin + ',' + outIp + '\n')
            if aspath_prefix in parsed_prefixes:
                continue
            else:
                parsed_prefixes.add(aspath_prefix)

            if len(foundTarget) == len(countryAsPairs):
                break #we're done!
    g.close()
    return targets

def read_previously_probed_ips():
    exclude_targets = set()
    #concatenate all previously targeted IPs
    try:
        os.system("""cat targetsprobed/*.csv | awk -F, '{print $3}' > targetsprobed/combined.txt""")
    except:
        sys.stderr.write('could not generate concatenated target file \n')
        return exclude_targets
    with open('targetsprobed/combined.txt', 'r') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + 'targetsprobed/combined.txt' + "\n")
            sys.exit()
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            exclude_targets.add(row)
    return exclude_targets

def generate_comcast_targets(targets_already_hit, newripeDay):
    targets = []
    g=open('targetsprobed/target_ips_BG_comcast_' + ripeDay + '.csv','w+')
    g.write('country,AS,IP\n')
    countryCount = 1
    num_countries = '1'
    parsedAsnCountry = set()
    targetDictComcast = dict()
    targetDictComcast['US'] = ['7922']
    for country in targetDictComcast:
        curr_country = country
        curr_ases = []
        curr_ases = targetDictComcast[curr_country]
        sys.stderr.write('processing ' + curr_country + ' ' + str(countryCount) + '/' + num_countries + '\n')
        prefix_parsed = set()

        for i in range(len(curr_ases)):
            curr_asn = curr_ases[i]
            comcastCount = 0
            foundTarget = False

            with open(bg_filename, 'rb') as f: #import file
                try:
                    ipmap_list = f.readlines()
                except:
                    sys.stderr.write("\n something went wrong opening " + bg_filename + "\n")
                    sys.exit()
                for i in range(len(ipmap_list)):
                    row = ipmap_list[i].strip('\n')
                    if '#' in row:
                        continue #skip header
                    prefix_length = int(row.split('|')[0].split('/')[1])
                    if prefix_length > 24:
                        continue #ignore anything larger than a /24
                    if curr_country not in row:
                        continue #read only prefixes in this country
                    asn = row.split('|')[3]
                    bg_assigned = int(row.split('|')[2])
                    if asn != curr_asn:
                        continue

                    #calculate number of /24 blocks in this prefix, based on
                    #the number of addresses assigned to this country
                    #in this prefix
                    tmp = 2**(32 - prefix_length)
                    if tmp != bg_assigned: #pick only targets where whole of prefix is in country
                        continue
                    ipAdd = row.split('/')[0]
                    outIp = ipAdd.split('.')[0] + '.' + ipAdd.split('.')[1] + '.' + ipAdd.split('.')[2] + '.' + str(randrange(255))
                    prefix_full = ipAdd.split('.')[0] + '.' + ipAdd.split('.')[1] #+ '.' + ipAdd.split('.')[2]
                    #US make sure the prefix isn't a repeat
                    #for some reason BG's output isn't unique
                    if prefix_full in prefix_parsed:
                        continue #we already parsed this prefix for the US
                    else:
                        while outIp in targets_already_hit:
                            outIp = ipAdd.split('.')[0] + '.' + ipAdd.split('.')[1] + '.' + ipAdd.split('.')[2] + '.' + str(randrange(255)) #replace last segment of IP until it has not been hit in the past
                        prefix_parsed.add(prefix_full)
                        foundTarget = True
                        targets.append(outIp)
                        g.write(curr_country + ',' + asn + ',' + outIp + '\n')
                        comcastCount = comcastCount + 1
                        if comcastCount >49:
                            break
            if not foundTarget: #this is not gonna happen for Comcast
                g.write(curr_country + ',' + curr_asn + ',None\n')    
    return targets

def run_queries(queryFiles):
    globalCounter = 0
    targetTime = 43200
    ripeQueryStr = '''curl --dump-header - -H "Content-Type: application/json" -H "Accept: application/json" -X POST -d @<country.sh> https://atlas.ripe.net/api/v2/measurements//?key=344e88de-a270-4051-a6c1-48f27239bf75'''
    for query in queryFiles:
        replacementStr = ripeQueryStr.replace('<country.sh>',query)
        #os.system('cd ripeQueries; ' + replacementStr)
        print('cd ripeQueries; ' + replacementStr)
        #time.sleep(1200)
        print('1200')
        globalCounter = globalCounter + 1200
    if globalCounter < targetTime:
        additionalWait = targetTime - globalCounter
        #time.sleep(additionalWait)
        print(additionalWait)
    
def download_and_decompress_ripe_file():
    newripeDay = ripeDay
    command = 'cd ../ripeprobes; wget https://ftp.ripe.net/ripe/atlas/probes/archive/2020/03/<day>.json.bz2; bunzip2 -f <day>.json.bz2'
    run_command = command.replace('<day>',ripeDay)
    problem = True
    try:
        os.system(run_command)
    except:
        sys.stderr.write('could not download ripe file; defaulting to 20200318\n')
        problem = False

    if not problem:
        newripeDay = '20200318'
    fileTest = '../ripeprobes/' + newripeDay + '.json'
    if os.path.isfile(fileTest):
        sys.stderr.write('successfully downloaded RIPE file \n')
    else:
        newripeDay = '20200318'
    return newripeDay

def read_geolocation_file(filename):
    #Read prefixes geolocated to each country, and the number of
    #addresses in that prefix that were specifically assigned to the country
    #Skip prefixes with length 25 or more, as we are measuring in /24 blocks
    #Read this file once for each country, ignore rows that don't have the country ISO2-code
    country_dict = dict()
    prefix_set = set()

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
                if current_country_checking not in targetDict:
                    continue #it's a dependency but not a dependency of this country
                else:
                    sys.stderr.write("merging " + checking_country + " into " + current_country_checking + "\n")
                    checking_country = current_country_checking
                #else here is implicit: if it is a dependency of this country, merge into the prefixes of this country
            elif checking_country not in targetDict:
                continue #read only prefixes in this country
            bg_assigned = int(row.split('|')[2])

                    #calculate number of /24 blocks in this prefix, based on
                    #the number of addresses assigned to this country
                    #in this prefix
            tmp = 2**(32 - prefix_length)
            if tmp != bg_assigned: #pick only targets where whole of prefix is in country
                continue            
            pfx = row.split('|')[0]
            prefix_set.add(pfx)
            country_dict[pfx] = checking_country
                
    return prefix_set, country_dict

def read_delegation_file(filename, prefix_set, country_dict):
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
                if current_country_checking not in targetDict:
                    continue #it's a dependency but not a dependency of this country
                #else here is implicit: if it is a dependency of this country, merge into the prefixes of this country   
                else:
                    sys.stderr.write("merging " + checking_country + " into " + current_country_checking + "\n")
                    checking_country = current_country_checking
            elif checking_country not in targetDict:
                continue #read only prefixes in this country

            prefix_length = int(row.split('|')[0].split('/')[1])
            if prefix_length > 24 or prefix_length < 8: #illegal length
                continue
            pfx = row.split('|')[0]
            if pfx in prefix_set:
                continue
            
            country_dict[pfx] = checking_country
            prefix_set.add(pfx)

        #sys.stderr.write("BD prefixes: " + str(bd_set) + "\n\n")
    #Return set of prefixes
    return prefix_set, country_dict

def main():

    newripeDay = download_and_decompress_ripe_file()

    probe_to_country, probe_to_asn, probe_to_status = read_ripe_file(newripeDay) #read probe info 

    country_probes = find_probes(probe_to_country, probe_to_asn, probe_to_status) #find 100 probes for countries

    comcast_probes = find_comcast_probes(probe_to_country, probe_to_asn, probe_to_status) #find a probe in every available AS for Comcast

    targets_already_hit = read_previously_probed_ips() #make sure each target is only probed once per day

    prefix_set, country_dict = read_geolocation_file(bg_filename)

    #Include additional prefixes assigned to this country based on delegation files
    prefix_set, country_dict = read_delegation_file(bd_filename, prefix_set, country_dict)       
    countryTargets = generate_country_targets(targets_already_hit, newripeDay, prefix_set, country_dict) #generate targets for the countries and ASes using both BG and BD, excluding previously parsed ASes
'''
    comcastTargets = generate_comcast_targets(targets_already_hit, newripeDay) #generate 50 targets for Comcast
    #countryTargets = set()
    sys.stderr.write('probing towards ' + str(len(countryTargets)) + ' country targets \n')
    sys.stderr.write('probing towards ' + str(len(comcastTargets)) + ' comcast targets \n')

    query_files_country = generate_ripe_query(countryTargets, country_probes, newripeDay) #generate and save query files, return as a list to be run
    query_files_comcast = generate_ripe_query_comcast(comcastTargets, comcast_probes, newripeDay) #generate and save Comcast query files, return as a list to be run 
    query_files_union = query_files_country.union(query_files_comcast)
    run_queries(query_files_union)

    run_queries(query_files_union) # this is done twice in a day
'''
main()

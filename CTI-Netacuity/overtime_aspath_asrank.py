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
import radix
import json_lines
import bz2
# Create a new tree
stub_input = str(sys.argv[3])
current_snapshot = str(sys.argv[4])
if stub_input == 'True':
    testing_string = '.stub'
else:
    testing_string = ''
#testing_string = '' 
#testing_file = False
#testing_file = True
#current_country = "UY"
current_country = str(sys.argv[1])
testing_input = str(sys.argv[2])
if testing_input == 'True':
    testing_file = True
else:
    testing_file = False
cc_filename = "/project/mapkit/agamerog/country_asn_analysis/cc_to_name.csv"

ratio_file = "/project/mapkit/agamerog/country_asn_analysis/logs/ratio/" + current_country + ".txt"

aspath_filename = "/data/external/as-rank-ribs/" + current_snapshot + "/" + current_snapshot + ".all-paths.bz2" + testing_string

country_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/sorted_summaries/" + current_snapshot + "/" + current_country + ".csv"

#asrel_filename = '/data/external/as-rank-ribs/' + current_snapshot + '/' + current_snapshot + '.as-rel.txt.bz2'

asrel_filename = '/data/external/as-rank-ribs/' + current_snapshot + "/" + current_snapshot + '.as-rel.txt.bz2'

country_output = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_snapshot + "/top." + current_country + ".csv"

country_matrix = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_snapshot + "/" + current_country + ".csv"

country_origin = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/" + current_snapshot + "/" + current_country + ".csv"

country_cti = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_snapshot + "/" + current_country + ".cti.csv"

country_ext = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_snapshot + "/ext." + current_country + ".csv"

country_top_cti = """/project/mapkit/agamerog/country_asn_analysis/country_aspath/sorted_summaries/""" + current_snapshot + """/top.""" + current_country + """.cti.csv"""

command = "bzip2 -f "
compress_1 = command + country_output
compress_2 = command + country_matrix
compress_3 = command + country_origin
compress_4 = command + country_ext
compress_5 = command + country_cti
compress_6 = command + country_top_cti

log_1 = "/project/mapkit/agamerog/country_asn_analysis/logs/origin/" + current_country + "." + current_snapshot + ".txt"
log_2 = "/project/mapkit/agamerog/country_asn_analysis/logs/transit/" + current_country + "." + current_snapshot + ".txt"
log_3 = "/project/mapkit/agamerog/country_asn_analysis/logs/aspath/" + current_country + "." + current_snapshot + ".txt"

trimming_1 = "tail -n 20 " + log_1 + " > " + log_1 + ".trim ;" + " rm " + log_1
trimming_2 = "tail -n 20 " + log_2 + " > " + log_2 + ".trim ;" + " rm " + log_2
trimming_3 = "tail -n 20 " + log_3 + " > " + log_3 + ".trim ;" + " rm " + log_3

if testing_file:
        country_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_country + ".test.csv"
#os.system('ulimit -d 4000000; ulimit -m 4000000; ulimit -v 4000000')
#    os.system('ulimit -d 25000000; ulimit -m 25000000; ulimit -v 25000000')
#else:
os.system('ulimit -d 4000000; ulimit -m 4000000; ulimit -v 4000000')
#sys.stderr.write("writing " + output_filename + "\n")

def read_asrel_file():
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

def read_cc(filename):
    cc_dict = {}
    with open (filename,'rb') as f:
        try:
            rows = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + filename + "\n")
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

def labeling(pos_avg):
    try:
        pos = float(pos_avg)
        if pos <= 1.50:
            return 'Origin'
        elif pos > 1.50 and pos <= 2.50:
            return 'Direct'
        elif pos > 2.50:
            return 'Indirect'
    except:
        return 'unknown'

def read_summary_file():
    #read ipmap lines into list
    country_set = set()
    cumulative_sum = 0.0
    top_sum = 0.0
    new_cumul_sum = 0.0
    asnumbers = []
    asnames = []
    country_ranks = []
    metrics = []

    with open(country_filename, 'rb') as f: #import file
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + country_filename + "\n")
            sys.exit()
        num_rows_90th = int( 0.10 * (float(len(ipmap_list)-1)) )

        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping comments
            curr_sum = float(row.split(',')[0])
            cumulative_sum = cumulative_sum + curr_sum

        threshold = 0.50 * float(cumulative_sum)
        break_now = False
        top_50 = 0

        for i in range(len(ipmap_list)):
            if not break_now:
                top_50 = top_50 + 1

            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping comments
            curr_sum = float(row.split(',')[0])
 
            new_cumul_sum = new_cumul_sum + curr_sum
            if new_cumul_sum >= threshold:
                break_now = True
                
            top_sum = top_sum + curr_sum
            perc_sum = round(100.0 * float(top_sum) / float(cumulative_sum),1)
            asnumbers.append(row.split(',')[1].split('-')[0])
            asnames.append(row.split(',')[1])
            country_ranks.append(i+1)
            overzero = row.split(',')[4]
            pos_avg = row.split(',')[6]
            
            label = labeling(pos_avg)
            country_ases = int(row.split(',')[-1]) - 7
            perc_over_zero = round( 100.0 * float(overzero) / float(country_ases) , 2)
            row_sum = str(round(float(row.split(',')[0]),1))
            nonzeroavg = row.split(',')[5]
            pos_num = row.split(',')[3]
            pos_denom = row.split(',')[2]
            cti = row.split(',')[-2]
            metric = row_sum + ',' + nonzeroavg + \
                    ',' + str(perc_over_zero) + ',' + \
                    pos_avg + ',' + pos_num + ',' + pos_denom + \
                    ',' + label + ',' + str(country_ases) + ',' + \
                    str(top_sum) + ',' + str(perc_sum) + ',' + str(cti)
            metrics.append(metric)

        #ratio = str(top_50) + ' ;' + str(round(100.0*float(top_50)/float(len(metrics)),2)) + \
        #        '% of total ASes appearing towards country: ' + str(len(metrics)) 
#        ratio = ''a
        ratio = ''
        return asnumbers, asnames, country_ranks, ratio, metrics

def read_asname():

    #AS = open('/home/agamerog/plots/ddc/AS-table.txt', 'rU')
    #ASdict = {}
    #for lines in AS:
    #    if len(lines) > 0 and lines[0] == 'A':

    #        ASnumstr = lines.split()[0][2:] #throw away the AS
    #        AStextlist = lines.split()[1:10]
    asrank_file = '20180301.asns.jsonl'
    ASdict = dict()
    try:
        with open (asrank_file,'rb') as f:
            for data in json_lines.reader(f):
                ASnumstr = str(data["asn"])
                if ASnumstr in ASdict:
                    continue
                try:
                    AStextlist = data["org_name"]
                except KeyError:
                    name = 'None'
                AStextlist = " ".join(AStextlist).replace(',','')
                AStextlist = AStextlist[:36]
                ASdict[ASnumstr] = " ".join(AStextlist).replace(',','')
                ASdict[ASnumstr] = AStextlist
    except:
        sys.stderr.write("\n something went wrong opening " + asrank_file + "\n")
        #AS.close()
    return ASdict
def fetch_asname(asn, asname_dict):
    try:
        line = asn + '-' + asname_dict[asn] 
    except KeyError:
        line = asn + '-unknown'
    return line

def sort_country_output():
    command = """cat /project/mapkit/agamerog/country_asn_analysis/country_aspath/""" + current_snapshot + """/<CC>.cti.csv | grep -v e-0 |awk -F, '{print $2,$0}' | sort -nr | cut -f2- -d' ' | awk -F, '{print $1","$2}' > /project/mapkit/agamerog/country_asn_analysis/country_aspath/sorted_summaries/""" + current_snapshot + """/top.<CC>.cti.csv; cat /project/mapkit/agamerog/country_asn_analysis/country_aspath/""" + current_snapshot + """/<CC>.cti.csv | grep e-0 | awk -F, '{print $2,$0}' | sort -nr | cut -f2- -d' ' | awk -F, '{print $1","$2}'>> /project/mapkit/agamerog/country_asn_analysis/country_aspath/sorted_summaries/""" + current_snapshot + """/top.<CC>.cti.csv"""
    run = command.replace('<CC>', current_country)
    try:
        os.system(run)
    except:
        sys.stderr.write('\n could not run ' + run + '\n')

def read_asrank():
    asrank_file = '20180301.asns.jsonl'
    asn_data = dict()
    try:
        with open (asrank_file,'rb') as f:
            for data in json_lines.reader(f):
                asn = str(data["asn"])
                try:
                    rank = data["rank"]
                except KeyError:
                    rank = 'None'
                try:    
                    cc = data["country"]
                except KeyError:
                    cc = 'None'
                try:
                    name = data["org_name"]
                except KeyError:
                    name = 'None'
                if asn in asn_data:
                    continue
                else:
                    asn_data[asn] = [rank, cc, name]
                    
    except:
        sys.stderr.write("\n something went wrong opening " + asrank_file + "\n")
    return asn_data

def write_asrank_aspath(asnumbers, asnames, country_ranks, ratio, asranks, metrics, cc_dict):
    with open(country_output,'w+') as g:
        g.write('#Country 50th = ' + str(ratio) + '\n')
        g.write('#CC, Country, Country AS-Path Rank,ASN-ASName,Registration Country, Global AS-Rank, AS-Rank minus AS-Path, Row Sum, Nonzero Cell Avg., Perc. ASes in country, pos_min;pos_25th;pos_median;pos_mean;pos_75th;pos_max, Pos_denom, Pos_num, Label, Country #ASes, Cum. Row Sum, Cum. Row Sum as %Total, CTI\n')
        for i in range(len(asnumbers)):
            name = ''
            if asnumbers[i] in asranks:
                rank = asranks[asnumbers[i]][0]
                cc = asranks[asnumbers[i]][1]
                name  = asranks[asnumbers[i]][2]
                if rank != 'None':
                    diff = int(rank) - country_ranks[i]
                else:
                    diff = 'None'
                if cc != 'None':
                    
                    try:
                        cc = cc_dict[cc]
                    except KeyError:
                        if cc == 'EU':
                            cc = 'EU'
                        else:
                            cc = 'CC = ' + cc
                else:
                    cc = 'None'
            else:
                rank = 'None'
                cc = 'None'
                diff = 'None'
            if name != 'None':
                name = name[:22].replace(',','')
                asnumber = asnames[i].split('-')[0]
                asnamenumber = asnumber + '-' + name
            else:
                asnamenumber = asnames[i]
                #if rank == 'unknown':
                #    continue
            try:
                current_country_full = cc_dict[current_country]
            except KeyError:
                if current_country == 'EU':
                    current_country_full = 'EU'
                else:
                    current_country_full = current_country
            try:
                string = current_country + ',' + current_country_full +',' + str(country_ranks[i]) + ',' + str(asnamenumber) + ',' + str(cc) + ',' + str(rank) + ',' + str(diff) + ',' + str(metrics[i]) + '\n'
            except UnicodeEncodeError:
                asnamenumber = asnames[i]
                string = current_country + ',' + current_country_full +',' + str(country_ranks[i]) + ',' + str(asnamenumber) + ',' + str(cc) + ',' + str(rank) + ',' + str(diff) + ',' + str(metrics[i]) + '\n'
            g.write(string)

def main():
    nothing =1
    #asrel_set = read_asrel_file()

    #cc_dict = read_cc(cc_filename) #Read country names
 
    #asname_dict = read_asname() #read asnames into memory
    
    #asranks = read_asrank() #read AS rank info

    #asnumbers, asnames, country_ranks, ratio, metrics = read_summary_file()
    #read ATI output file

    #write_asrank_aspath(asnumbers, asnames, country_ranks, ratio, asranks, metrics, cc_dict)
    #Regenerate output files and compress with properly formatted headers
#sys.stderr.write("\n writing " + country_output + "\n")

#    try:
#        os.system(compress_1)
#    except:
#        sys.stderr.write("\n could not compress " + compress_1 + "\n")
#    try:
#        os.system(compress_2)
#    except:
#        sys.stderr.write("\n could not compress " + compress_2 + "\n")
#    try:
#        os.system(compress_3)
#    except:
#        sys.stderr.write("\n could not compress " + compress_3 + "\n")
#    try:
#        os.system(compress_4)
#    except:
#        sys.stderr.write("\n could not compress " + compress_4 + "\n")
#    try:
#        os.system(compress_5)
#    except:
#        sys.stderr.write("\n could not compress " + compress_5 + "\n")


    try:
        os.system(trimming_1)
    except:
        sys.stderr.write("\n could not trimming " + trimming_1 + "\n")
    try:
        os.system(trimming_2)
    except:
        sys.stderr.write("\n could not trimming " + trimming_2 + "\n")
    try:
        os.system(trimming_3)
    except:
        sys.stderr.write("\n could not trimming " + trimming_3 + "\n")

    sort_country_output()
#    try:
#        os.system(compress_6)
#    except:
#        sys.stderr.write("\n could not compress " + compress_6 + "\n")

main()

    
#print(compress_1)
#print ()
#print(compress_2)
#print ()
#print(compress_3)
#print ()
#print(compress_4)

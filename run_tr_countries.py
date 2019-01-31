#usage python run_tr_countries.py > logs/daily_log_tr.txt 2>&1
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

influx_command = "python tr_transit_path_metric.py country False False snapshot > logs/transit/country.snapshot.tr.txt 2>&1; python tr_aspath_asrank.py country False False snapshot > logs/aspath/country.snapshot.tr.txt 2>&1"
log = " > "
log_post = " 2>&1 "
timeout_seconds = 1800
countries = ['US','BJ', 'ZA', 'RU', 'CN']
print (len(countries))
snapshots_list = ['20180301']
#snapshots_list = ['20181201','20180901','20180601','20170901','20170601','20170301','20161201','20160901','20160601','20160301','20151201','20150901','20150601','20150301','20141201','20140901','20140601','20140301','20131201']#,'20130901','20130601','20130301','20121201','20120901','20120601','20120301','20111201']#,'20110901','20110601','20110301','20101201','20100901','20100601','20100301']

#snapshots_list = ['20130301','20130901','20141201','20151201','20160901','20161201','20170301','20181201']

bg_command = """cd ~/country/geolocation_bgp; ./as-to-geo.py -p netacq-edge -o ./pfxgeo -m http://data.caida.org/datasets/routing/routeviews-prefix2as/year/month/routeviews-rv2-snapshot-1200.pfx2as.gz"""
#Uncomment below when running from scratch
#bd_command = """cd ~/country/; python generate_dd.py -f dd_and_bd/delegated-combined-extended-snapshot | sort -t "|" -k1,1 -k2,2n > dd_and_bd/dd/snapshot.txt
bd_command = """python generate_bd.py -f dd_and_bd/dd/snapshot.txt -b /data/external/as-rank-ribs/snapshot/snapshot.prefix2as.bz2 > /project/mapkit/agamerog/country_asn_analysis/bd/snapshot.txt"""

os.system('ulimit -u 50')
ipmap_file = "/project/mapkit/agamerog/country_asn_analysis/list_of_country_codes.txt"
#one hour

def systemCall(parameter):
    os.system(parameter)

def create_dir(path):
    if not os.path.exists(path):
        command = 'mkdir -p ' + str(path)
        systemCall(command)

def create_commands(input_file):
    commandsList =[]
#Create list of commands to pass to the multiprocessing manager
    #Run loss summary parser on each loss summary file in the input 
#    with open (input_file,'rb') as f:
#        loss_list = f.readlines()
#        for i in range(len(loss_list)):

#            passing_string = ipmap_list[i].strip('\n')
    #command = influx_command.replace('country'
                      
    commandsList.append(command)

    return commandsList	

def read_ipmap(filename):
    #read ipmap lines into list
    ipmap_list = ''
    with open(filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
    return ipmap_list

def run_commands(command_list):

	myPool = Pool(2)
	sys.stderr.write("running following commands" + '\n')
	for i in range(len(command_list)):
		sys.stderr.write('\t' + str(command_list[i])+ '\n')
	myPool.map_async(systemCall, command_list)
	myPool.close()
	myPool.join()

def print_commands(command_list):
    for i in range(len(command_list)):
        sys.stderr.write('\t' + str(command_list[i]) + '\n\n')

def call_create_commands(ipmap_list):
    
    combined_list = []
    temp_array = []
    for j in range(len(snapshots_list)):
        year = snapshots_list[j][:4]
        month = snapshots_list[j][4:6]
        #try:
            #Uncomment this try/except when running from scratch
            #bg_snap = bg_command.replace('snapshot',snapshots_list[j]).replace('year',year).replace('month',month)
            #print(bg_snap)
            #bd_snap = bd_command.replace('snapshot',snapshots_list[j])
            #os.system(bg_snap)
            #os.system(bd_snap)
        #except:
        #    print ("something went wrong on this snapshot for BG U BD " + snapshots_list[j])
        #    continue
        for i in range(len(countries)):
            
            passing_string = countries[i]
            command_string = influx_command.replace('country',passing_string).replace('snapshot',snapshots_list[j])
            combined_list.append(command_string)
    return combined_list

def create_combined_file():
    create_command = 'rm supplemental_data/summaries/combined.csv; cat supplemental_data/summaries/*.csv > supplemental_data/summaries/combined.csv'
    systemCall(create_command)

def init_summary():
    move_summary_command = 'mv country_aspath/global_summary.txt archive'

    try: #ugly. fix later agg . to create summary of global non-zero ATI values
        systemCall(move_summary_command)
    except:
        nothing = 1


def main():
    
    #clear output files
    move_command = 'mv country_aspath/*.csv archive'
#   systemCall(move_command)

    #init_summary()

    command_list = []

    #get list of ipmap files using below list
    ipmap_list = read_ipmap(ipmap_file)

    #create influx queries for each monitor-month tuple
    command_list = call_create_commands(ipmap_list)

    #print commands
    print_commands(command_list)

    #run country commands 2 at a time
    run_commands(command_list)

		
main()

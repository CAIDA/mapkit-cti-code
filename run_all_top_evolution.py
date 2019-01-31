#usage python run_all_countries.py > logs/daily_log.txt 2>&1
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

#influx_command = "python overtime_origin_metric.py country False False snapshot > logs/origin/country.snapshot.txt 2>&1; python overtime_transit_path_metric.py country False False snapshot > logs/transit/country.snapshot.txt 2>&1; python overtime_aspath_asrank.py country False False snapshot > logs/aspath/country.snapshot.txt 2>&1"
influx_command = "python top_by_ati_evolution.py country snapshot"
log = " > "
log_post = " 2>&1 "
timeout_seconds = 1800

#countries = ['AF','AL','DZ','AD','AO','AG','AR','AM','AU','AT','AZ','BS','BH','BD','BB','BY','BE','BZ','BJ','BT','BO','BA','BW','BR','BN','BG','BF','BI','CV','KH','CM','CA','CF','TD','CL','CN','CO','KM','CG','CD','CR','CI','HR','CU','CY','CZ','DK','DJ','DM','DO','EC','EG','SV','GQ','ER','EE','ET','FJ','FI','FR','GA','GM','GE','DE','GH','GR','GD','GT','GN','GW','GY','HT','VA','HN','HU','IS','IN','ID','IR','IQ','IE','IL','IT','JM','JP','JO','KZ','KE','KI','KP','KR','KW','KG','LA','LV','LB','LS','LR','LY','LI','LT','LU','MK','MG','MW','MY','MV','ML','MT','MH','MR','MU','MX','FM','MD','MC','MN','ME','MA','MZ','MM','NA','NR','NP','NL','NZ','NI','NE','NG','NO','OM','PK','PW','PS','PA','PG','PY','PE','PH','PL','PT','QA','RO','RU','RW','KN','LC','VC','WS','SM','ST','SA','SN','RS','SC','SL','SG','SK','SI','SB','SO','ZA','SS','ES','LK','SD','SR','SZ','SE','CH','SY','TJ','TZ','TH','TL','TG','TO','TT','TN','TR','TM','TV','AE','UG','GB','UA','UY','US','UZ','VU','VE','VN','YE','ZM','ZW']
countries = ['ZA']
#countries = ['ZA', 'CN', 'RU', 'CA', 'MX', 'JP','IN','VE','NG','US','LY']
snapshots_list = ['20181201','20180901','20180601', '20180301', '20171201','20170901','20170601','20170301','20161201','20160901','20160601','20160301','20151201','20150901','20150601','20150301','20141201','20140901','20140601','20140301','20131201','20130901','20130601','20130301','20121201','20120901','20120601']#,'20110901','20110601','20110301','20101201','20100901','20100601','20100301']
#snapshots_list = ['20120301']
#snapshots_list = ['20130301','20130901','20141201','20151201','20160901','20161201','20170301','20181201']


os.system('ulimit -u 50')
#ipmap_file = "/project/mapkit/agamerog/country_asn_analysis/list_of_country_codes.txt"
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

	myPool = Pool(6)
	sys.stderr.write("running following commands" + '\n')
	for i in range(len(command_list)):
		sys.stderr.write('\t' + str(command_list[i])+ '\n')
	myPool.map_async(systemCall, command_list)
	myPool.close()
	myPool.join()

def print_commands(command_list):
    for i in range(len(command_list)):
        sys.stderr.write('\t' + str(command_list[i]) + '\n\n')

def call_create_commands():
    
    combined_list = []
    temp_array = []
    for j in range(len(snapshots_list)):
        #year = snapshots_list[j][:4]
        #month = snapshots_list[j][4:6]
        for i in range(len(countries)):
            out_file = '/project/mapkit/agamerog/country_asn_analysis/evolution/' + countries[i] + '.csv'
            with open(out_file, 'w+') as f:
                f.write('#snapshot,ati1,ati2,ati3,ati4,ati5,ati6,ati7,ati8,ati9,ati10\n')
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

    #create influx queries for each monitor-month tuple
    command_list = call_create_commands()

    #print commands
    print_commands(command_list)

    #run country commands 2 at a time
    run_commands(command_list)

		
main()

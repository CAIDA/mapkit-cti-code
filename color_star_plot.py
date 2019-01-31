from __future__ import with_statement
#usage python run_ddc_queries.py >> supplemental_data/multiprocessing_ddc_log.txt 2>&1
#gets list of ipmap files to process from hardcoded list below
#list created by running:
#ls rtt_and_loss_data/rtt/book_keeping/*/*/*ipmap* > supplemental_data/list_of_ipmaps.txt
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.lines import Line2D
#import subprocess
#from multiprocessing import Pool
import os
#import signal
#import time
#import csv
import sys
import numpy as np
#import socket, struct #Para las funciones de transformacion
#import radix
#import math
# Create a new tree
#testing_string = '' 
#testing_file = False
#testing_file = True
#current_country = "UY"
current_country = str(sys.argv[1])
geo_filename = "/project/mapkit/agamerog/country_asn_analysis/geolocation_bgp/pfxgeo/asn-to-country.2018-03-01"
del_filename = "/project/mapkit/agamerog/country_asn_analysis/dd_and_bd/dd.txt"
heatmap_filename = "/project/mapkit/agamerog/country_asn_analysis/heatmap/" + current_country + ".csv"
cc_filename = "/project/mapkit/agamerog/country_asn_analysis/cc_to_name.csv"
dd_filename = "/project/mapkit/agamerog/country_asn_analysis/dd_and_bd/dd_to_bd.txt"
ratio_file = "/project/mapkit/agamerog/country_asn_analysis/logs/ratio/" + current_country + ".txt"

NumberToColor_dict={
'global': 's',
'foreign': 'p',
'domestic': 'o',
'unassigned':'d'
}

bd_output_filename = "/project/mapkit/agamerog/country_asn_analysis/bd_asn_cc_pfix_2018-03-01.txt"
output_filename = "/project/mapkit/agamerog/country_asn_analysis/country_dd_bg_bd.csv"

plot_filename = "/project/mapkit/agamerog/country_asn_analysis/plots/" + current_country + ".pdf"

country_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/" + current_country + ".csv"
origin_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/20180301/" + current_country + ".csv"

transit_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/20180301/top." + current_country + ".csv"

global_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/half.csv"
filter_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/filter." + current_country + ".csv" 

asrel_filename = '/project/mapkit/agamerog/country_asn_analysis/20180301.as-rel.txt'

os.system('ulimit -d 30000000; ulimit -m 30000000; ulimit -v 30000000')

#sys.stderr.write("writing " + output_filename + "\n")

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
    with open (asrel_filename,'rb') as f:
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

def fetch_asname(asn, asname_dict):
    try:
        line = asn + '-' + asname_dict[asn]
    except KeyError:
        line = asn + '-unknown'
    return line

def systemCall(parameter):
    os.system(parameter)

def read_origin_file():
    origin_dict = dict()
    with open(origin_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers
            transit_rank = float(row.split(',')[2])
            asn = row.split(',')[0].split('-')[0]
            origin_dict[asn] = transit_rank

    return origin_dict

def read_transit_file():
    transit_dict = dict()
    top_dict = dict()
    with open(transit_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers

            transit_ati = float(row.split(',')[7])
            num_ases = float(row.split(',')[-3])
            perc_ases = float(row.split(',')[9])
            norm_ati = transit_ati / num_ases
            asn = row.split(',')[3].split('-')[0]
            avg_inf = float(row.split(',')[8])       
            
            transit_dict[asn] = [norm_ati, perc_ases, avg_inf]
            if i <7:
                top_dict[asn] = row.split(',')[3]
    return transit_dict, top_dict

def read_customer_file():
    customer_dict = dict()
    customer_filename = "country_aspath/" + current_country + ".20180301.heavyreliance.csv" 
    maxorigin = 0.0
    with open(customer_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if i == 0:
                continue #skip header #AGG need to automate skipping headers
            transit_asn = str(row.split(',')[0])
            originated = float(row.split(',')[-1])
            nat = str(row.split(',')[2])
            if nat == 'international':
                nat = 'foreign'
            if transit_asn in customer_dict:
                customer_dict[transit_asn][0] = customer_dict[transit_asn][0] + originated
            else:
                customer_dict[transit_asn] = []
                customer_dict[transit_asn].append(originated)
                customer_dict[transit_asn].append(nat)
        for transit in customer_dict:
            if customer_dict[transit][0] > maxorigin:
                maxorigin = customer_dict[transit][0]
                toptransit = transit
            #print customer_dict
            #asn = row.split(',')[0]
            #customer_dict[asn] = transit_rank

    return customer_dict, toptransit



def read_asname():

    AS = open('/home/agamerog/plots/ddc/AS-table.txt', 'rU')
    ASdict = {}
    for lines in AS:
        if len(lines) > 0 and lines[0] == 'A':
            ASnumstr = lines.split()[0][2:] #throw away the AS
            AStextlist = lines.split()[1:10]
            AStextlist = " ".join(AStextlist).replace(',','')
            AStextlist = AStextlist[:12]
            #ASdict[ASnumstr] = " ".join(AStextlist).replace(',','')
            ASdict[ASnumstr] = AStextlist
    AS.close()
    return ASdict

def read_nationality_file(current_country):
    as_nationalities = dict()
    nationality_filename = '/project/comcast-ping/stabledist/mapkit/code/Nationality/20180301.csv'
    with open (nationality_filename,'rb') as f:
        rows = f.readlines()
        for i in range(len(rows)):
            if 'ASN' in rows[i]:
                continue

            row = rows[i].strip('\n')
            asn = str(row.split(',')[0])
            country = str(row.split(',')[1])
            if country == 'XX':
                as_nationalities[asn] = 'global'
                continue
            elif country == current_country:
                as_nationalities[asn] = 'domestic'
                continue
            else:
                as_nationalities[asn] = 'foreign'
                continue
        return as_nationalities

global_nationalities = read_nationality_file(current_country)

def color_picker(number):
    if number <= 1.0:
        return 'k'
    elif number <= 5.0:
        return 'b'
    elif number <= 10.0:
        return 'g'
    elif number <= 50.0:
        return 'orange'
    else:
        return 'r'

def size_picker(number):
    if number <= 0.01:
        return 2
    #elif number <= 0.05:
        #return 22
    elif number <= 0.10:
        return 22
    elif number < 0.50:
        return 200
    else:
        return 500

def plot_origin_transit(origin, transit, customer_cone, asnames, top_dict, toptransit):
    font = {'family' : 'Times New Roman', 'weight' : 'bold', 'size'   : 11}
    matplotlib.rc('font', **font)
    fig = plt.figure(1, figsize=(6, 6))
    colors = ['k','b','y','g','r']
    ax = fig.add_subplot(1, 1, 1)
    plotx = []
    ploty = []
    i = 0
    yorigin = 0.08
    minsize = 1000.0
    already_colored = set()
    bool_label = True
    for orig in transit:
        x = transit[orig][0]
        y = transit[orig][1] #perc ASes
        avginf = transit[orig][2]
        asn = int(orig)
        size = size_picker(avginf)
        try:
            classification = global_nationalities[orig]
        except KeyError:
            #print orig
            classification = 'unassigned'
        try:
            c = color_picker(customer_cone[orig][0])
            #if asn == 6939:
            #    print customer_cone[orig][0]
        except KeyError:
            c = 'k'

        markery = NumberToColor_dict[classification]

        nlabel = classification  #classificationa
        alfa = 0.4
        #if size < 10.0:
        #    alfa = 1.0
        #else:
        #    alfa = 0.5
        #if nlabel in already_colored:
        ax.scatter(y,x,s=size, marker = markery, color=c, alpha = alfa)
        #elif nlabel not in already_colored and size == 50:
            #ax.scatter(y,x,s=size, color=c, marker = markery, alpha = alfa, label = nlabel)
            #already_colored.add(nlabel)
        #else:
            #ax.scatter(y,x,s=size, marker = markery, color=c, alpha = alfa)
        #else:
        #    ax.scatter(y,x,s=size, color=c, alpha = alfa)
        ax.set_ylabel('Normalized ATI', fontsize = 12)
        ax.set_xlabel('Perc. ASes Influenced', fontsize = 12)
        
        if orig in top_dict:
            label = top_dict[orig]
            
            #if orig == '35994':
            horiz = 80
            if current_country == 'RU':
                if bool_label:
                    horiz = 80
                    vert = 30
                    bool_label = False
                else:
                    horiz = -50
                    vert = -30
                    bool_label = True
            else:    
                if bool_label:
                    vert = 30
                    bool_label = False
                else:
                    vert = -30
                    bool_label = True
            if orig == '12389' and current_country == 'RU':
                horiz = horiz + 60
                vert = vert + 20
            if orig == '20485' and current_country == 'RU':
                vert = vert - 30 + 50
                horiz = horiz + 20 + 80
            if orig == '6939' and current_country == 'US':
                horiz = horiz - 60
                vert = 0 + 15
            if orig == '7018' and current_country == 'US':
                horiz = horiz - 60
                vert = 0 + 15
            if orig == '3216' and current_country == 'RU':
                vert = vert + 40
            if orig == '9002' and current_country == 'RU':
                vert = vert + 25 -90
                horiz = horiz - 30
            if orig == '6762' and current_country == 'PE':
                horiz = horiz + 50
            if orig == '27843' and current_country == 'PE':
                horiz = horiz + 40 + 90
            if orig == '3356' and current_country == 'US':
                vert = 30 - 50
            if orig == '174' and current_country == 'US':
                vert = vert - 80
                horiz = horiz - 30
            if orig == '3356' and current_country == 'US':
                horiz = 140
            if orig == '11172' and current_country == 'MX':
                vert = vert - 60
            if orig == '18734' and current_country == 'MX':
                horiz = horiz -60
                vert = vert +50
            if orig == '3549' and current_country == 'PE':
                vert = vert - 20
                horiz = horiz -60
            if orig == '20485' and current_country == 'RU':
                vert = vert - 20
                horiz = horiz +40 +40
            if orig == '12956' and current_country == 'PE':
                vert = vert - 10
                horiz = horiz +40
            if current_country == 'ZA':
                vert = vert - 15
            #elif orig == '16657':
            #    horiz = -20
            #    vert = 100
            #else:
            #    horiz = -50
            #    vert = 20
            plt.annotate(
                label,
                xy=(y, x), xytext=(horiz, vert),
                textcoords='offset points', ha='right', va='bottom',
                bbox=dict(boxstyle='round,pad=0.5', fc='white', alpha=0.5),
                arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'), fontsize=7)

        if orig == toptransit:
            #print (orig)
            horiz = 80
            vert = 30
            if current_country == 'RU':
                labs = '12389-PJSC Rostelecom'
                horiz = -80 + 160 + 50
                vert = vert - 20
            if current_country == 'MX':
                labs = '6939-Hurricane Electric'
                horiz = horiz + 80
                vert = vert - 50
            if current_country == 'PE':
                labs = '12956-Telefonica Internation'
                vert = vert - 30
                horiz = horiz + 40 +30
            #print (orig) 
            plt.annotate(
                labs,
                xy=(y, x), xytext=(horiz, vert),
                textcoords='offset points', ha='right', va='bottom',
                bbox=dict(boxstyle='round,pad=0.5', fc='red', alpha=0.5),
                arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'), fontsize=7)

    ax.set_xlim([-0.1, 100.1])
    ax.set_ylim([0.001, 0.25])
    #ax.autoscale()
    #print (minsize)
    #ax.legend(loc=2)
    art = []
    #lgd = plt.legend(bbox_to_anchor=(0,1.02,1,0.2), loc="lower left",
    #                    mode="expand", borderaxespad=0, ncol=3, fontsize=12)
    #art.append(lgd)
    ax.set_yscale('log')
    plt.hold(True)
    legend_elements = [Line2D([0], [0], marker='s', color='w', label='Global',
                          markerfacecolor='k', markersize=5),
                        Line2D([0], [0], marker='p', color='w', label='Foreign',
                            markerfacecolor='k', markersize=5),
                        Line2D([0], [0], marker='o', color='w', label='Domestic',
                            markerfacecolor='k', markersize=5),
                        Line2D([0], [0], marker='d', color='w', label='Unassigned',
                            markerfacecolor='k', markersize=5),
                        Line2D([0], [0], marker='o', color='w', label='Heavy Rel. <= 1%',
                          markerfacecolor='k', markersize=5),
                        Line2D([0], [0], marker='o', color='w', label='1% < Heavy Rel. <= 5%',
                            markerfacecolor='b', markersize=5),
                        Line2D([0], [0], marker='o', color='w', label='5% < Heavy Rel. <= 10%',
                            markerfacecolor='g', markersize=5),
                        Line2D([0], [0], marker='o', color='w', label='10% < Heavy Rel. < 50%',
                            markerfacecolor='orange', markersize=5),
                        Line2D([0], [0], marker='o', color='w', label='50% < Heavy Rel. <= 100%',
                            markerfacecolor='r', markersize=5),
                        Line2D([0], [0], marker='o', color='w', label='Mean TI <= 0.01',
                          markerfacecolor='k', alpha = 0.7, markersize=1),
                        Line2D([0], [0], marker='o', color='w', label='0.01 < Mean TI <= 0.10',
                            markerfacecolor='k',alpha=0.7, markersize=5),
                        #Line2D([0], [0], marker='o', color='w', label='0.05 < Mean TI <= 0.10',
                         #   markerfacecolor='k', alpha = 0.7, markersize=7),
                        Line2D([0], [0], marker='o', color='w', label='0.10 < Mean TI < 0.50',
                            markerfacecolor='k', alpha = 0.7, markersize=15),
                        Line2D([0], [0], marker='o', color='w', label='    0.50 <= Mean TI <= 1.00',
                            markerfacecolor='k', alpha=0.7, markersize=22)]

    ax.legend(handles=legend_elements, loc=4, fontsize = 9)
    plt.hold(True)
    plt.tight_layout()
    #ax.set_xscale('log')
    fig.savefig(plot_filename, additional_artists=art,
            bbox_inches="tight")


def main():

    asrel_set = read_asrel_file()

    cc_dict = read_cc(cc_filename)
 
    asname_dict = read_asname() #read asnames into memory
    
    origin = read_origin_file()

    transit, top_dict = read_transit_file()

    customer_dict, toptransit = read_customer_file()
    
    plot_origin_transit(origin, transit, customer_dict, asname_dict, top_dict, toptransit)
    #print origin
    #print transit
main()

print ("saving " + plot_filename)

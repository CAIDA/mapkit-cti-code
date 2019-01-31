from __future__ import with_statement
#usage python run_ddc_queries.py >> supplemental_data/multiprocessing_ddc_log.txt 2>&1
#gets list of ipmap files to process from hardcoded list below
#list created by running:
#ls rtt_and_loss_data/rtt/book_keeping/*/*/*ipmap* > supplemental_data/list_of_ipmaps.txt
import matplotlib.pyplot as plt
import matplotlib
import scipy.stats
import os
import sys
import numpy as np
import bz2
snapshot = str(sys.argv[2])
cc_filename = "/project/mapkit/agamerog/country_asn_analysis/cc_to_name_eng.csv"


plot_filename = "/project/mapkit/agamerog/country_asn_analysis/plots/gdp_scatter.pdf"

asrel_filename = '/project/mapkit/agamerog/country_asn_analysis/20180301.as-rel.txt'

os.system('ulimit -d 30000000; ulimit -m 30000000; ulimit -v 30000000')


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

def read_nationality_file(input_country, input_snapshot):
    as_nationalities = dict()
    nationality_filename = '/project/comcast-ping/stabledist/mapkit/code/Nationality/' + input_snapshot + '.csv'
    with open (nationality_filename,'rb') as f:
        rows = f.readlines()
        for i in range(len(rows)):
            if 'ASN' in rows[i]:
                continue

            row = rows[i].strip('\n')
            asn = int(row.split(',')[0])
            country = str(row.split(',')[1])
            if country == 'XX':
                as_nationalities[asn] = 'global'
                continue
            elif country == input_country:
                as_nationalities[asn] = 'domestic'
                continue
            else:
                as_nationalities[asn] = 'international'
                continue
        return as_nationalities

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

def read_origin_file(country):
    origin_dict = dict()
    addy_dict = dict()
    origin_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/origin/" + snapshot + "/" + country + ".csv.bz2"
    with bz2.BZ2File(origin_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers
            transit_rank = float(row.split(',')[2])
            asn = row.split(',')[0].split('-')[0]
            addresses = int(float(256) * float(row.split(',')[1]))
            origin_dict[asn] = transit_rank #this one has percentage of origin addresses
            #print str(country) + "\t" + str(transit_rank)
            addy_dict[asn] = addresses

    return origin_dict, addy_dict

def read_top_file(country):
    origin_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/" + snapshot + "/top." + country + ".csv.bz2"
    with bz2.BZ2File(origin_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers
            number_ases = int(row.split(',')[14])
            break
        return number_ases

def read_summary_file():
    country_dict = dict()
    
    summary_filename = "/home/agamerog/influenceimc18/country_influence_imc18/data/country/country_info_no_note.csv"
    with open(summary_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if i == 0:
                continue #skip header #AGG need to automate skipping headers
            if '#' in row:
                continue
            code = row.split(',')[0]
            addies = int(row.split(',')[3])
            country_dict[code] = addies
        return country_dict

def read_gdp_file():
    country_dict = dict()
    
    summary_filename = "WITS-Country.csv"
    with open(summary_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        print "IM HERE"
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if i == 0:
                continue #skip header #AGG need to automate skipping headers
            #print "IM HERE"
            code = row.split(',')[1]
            gdp = float(row.split(',')[2])/float(1000)
            country_dict[code] = gdp
        #print country_dict
        return country_dict



def read_transit_file(country, nationalities, input_snapshot, origin_dict):
    transit_dict = dict()
    customer_addy = dict()
    dominated_set = set()
    parsed_origins = set()
    dom_sum = 0.0
    int_sum = 0.0
    glo_sum = 0.0
    transit_sum = 0.0 #addresses originated by the transit provider as long as they have not been counted as origin
    origin_sum = 0.0
    transit_filename = "/project/mapkit/agamerog/country_asn_analysis/nobd_country_aspath/" + snapshot + "/ext." + country + ".csv.bz2"

    with bz2.BZ2File(transit_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if '#' in row:
                continue #skip header #AGG need to automate skipping headers
            transit_influence = round(float(row.split(',')[4]),2)
            if transit_influence < 0.5:
                continue #ignore non-heavy-reliant
            
            transit_rank = float(row.split(',')[7])
            asn_origin = row.split(',')[3].split('-')[0]
            asn_transit = int(row.split(',')[2].split('-')[0])

            dominated_set.add(asn_origin)
            if asn_origin in origin_dict:
                if asn_origin not in customer_addy:
                    customer_addy[asn_origin] = origin_dict[asn_origin] #save number of addresses originated 
                if asn_origin not in parsed_origins:
                    origin_sum = origin_sum + origin_dict[asn_origin]

                    parsed_origins.add(asn_origin)
                    if asn_transit in nationalities:
                        if nationalities[asn_transit] == 'domestic':
                            dom_sum = dom_sum + origin_dict[asn_origin]
                        elif nationalities[asn_transit] == 'international':
                            int_sum = int_sum + origin_dict[asn_origin]
                        else:
                            glo_sum = glo_sum + origin_dict[asn_origin]

            asn_transit = row.split(',')[2].split('-')[0]
            #dom_ases = len(dominated_set)

    return  dom_sum, int_sum, glo_sum, origin_sum

def read_customer_file():
    customer_dict = dict()
    customer_filename = "/project/comcast-ping/stabledist/mapkit/code/AnnouncedPrefixesMatrix/LPM2/AlexSummaryINVERSO/TOPk100ASes_rowsum_" + current_country + ".csv" 
    with open(customer_filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
        
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if 'T' in row:
                continue #skip header #AGG need to automate skipping headers
            transit_rank = float(row.split(',')[2])
            asn = row.split(',')[0]
            customer_dict[asn] = transit_rank

    return customer_dict



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

def plot_origin_transit(country_numbers, country_dict, country_ases, cc_dict, gdp_dict):
    output_file = "/project/mapkit/agamerog/country_asn_analysis/all_countries_ases_addresses_dominated.csv"
    font = {'family' : 'Times New Roman', 'weight' : 'bold', 'size'   : 13}
    matplotlib.rc('font', **font)
    fig = plt.figure(1, figsize=(16,16))
    colors = ['k','b','y','g','r']
    ax = fig.add_subplot(1, 1, 1)
    plotx = []
    ploty = []
    i = 0
    yorigin = 0.08
    already_colored = set()
    low_ases = [] 
    low_addies = []
    mid_ases = [] 
    mid_addies = []
    hi_ases = [] 
    hi_addies = []
    legend = True
    xx = []
    y1 = []
    y2 = []
    #with open (output_file,'w+') as f:
        #f.write('cc,perc_ases,perc_origin_addresses_customers,perc_origin_addreses_incl_originated_by_transit\n')
    for country in country_numbers:

        dom_ases = country_numbers[country][0]
        total_ases = country_ases[country]
        try:
            z = gdp_dict[country]
        except KeyError:
            print ("no GDP data for " + str(cc_dict[country]))
            z = 1000
            continue
        x = float(100) * float(dom_ases) / float(total_ases)
        #y = country_numbers[country][1]

        y = country_numbers[country][1]
        #f.write(country + ',' + str(x) + ',' + str(y) + ',' + str(z) + '\n')
        
        ax.set_ylabel('Heavily Reliant ASes and their Originated Add., \nPerc. of Country (log scale)', fontsize = 32)
        #ax.set_xlabel('GDP Per Capita', fontsize = 18)a
        
        ax.set_xlabel('GDP Per Capita (log scale)', fontsize = 32)
        csize = z 
        y1.append(x)
        y2.append(y)
        xx.append(csize)
        #if legend == True:

            #ax.scatter(csize,x, color='r', alpha = 0.7, label = 'Percentage of ASes')
            #ax.scatter(csize,y, color='b', alpha = 0.7, label = 'Percentage of Addresses')
            #legend = False
        #else:
            #ax.scatter(csize,x, color='r', alpha = 0.7)
            #ax.scatter(csize,y, color='b', alpha = 0.7)
    y1s = [x for _,x in sorted(zip(xx,y1))]
    y2s = [x for _,x in sorted(zip(xx,y2))]
    xxs = sorted(xx)
    ax.plot(xxs,y1s,color='r', marker = "o", markersize = 5, lw=2, alpha = 0.7, label = 'Percentage of ASes')
    ax.plot(xxs,y2s,color='b',marker = "o", markersize = 5, lw=2, alpha = 0.7, label = 'Percentage of Addresses')
    ax.set_title('AS and Address Concentration \n Across Countries vs. GDP Per Capita \nn=188 UN Members with GDP Per Capita Info', fontsize = 26)
    ax.set_xlim([200, 180000])
    ax.set_ylim([1, 102])
    ax.set_yscale('log')
    ax.legend(loc=3,fontsize=28)
    plt.tight_layout()
    ax.set_xscale('log')
    z = np.polyfit(xxs, y2s, 1)
    p = np.poly1d(z)
    ax.plot(xxs,p(xxs),"r--")
    # the line equation:
    print "y=%.6fx+(%.6f)"%(z[0],z[1])

    slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(xxs, y2s)
    print( 'r2 ' + str(r_value*r_value))
    print( 'p ' + str(p_value))
    print('stderr ' + str(std_err))
    print('slope' + str(slope))
    print( ' intercept ' + str(intercept))
    fig.savefig(plot_filename)
    #addie_reg = []
    #gdp_reg = []
    #for i in range(len(xxs)):
    #    addie_reg.append(np.log10(y2s[i]))
    #    gdp_reg.append(np.log10(y2s
    # calc the trendline

def read_ipmap(filename):
    #read ipmap lines into list
    ipmap_list = ''
    with open(filename, 'rb') as f: #import file
        ipmap_list = f.readlines()
    return ipmap_list

def main():

    input_country = str(sys.argv[1])

    input_snapshot = str(sys.argv[2])

    nationality_dict = read_nationality_file(input_country, input_snapshot)

    cc_dict = read_cc(cc_filename)
 
    asname_dict = read_asname() #read asnames into memory

    country_ases = dict()

    country_numbers = dict() #numbers to plot on scatter

    #for i in range(len(countries)):
    country = input_country
    #country_ases[country] = read_top_file(country)
    #country_addresses = country_dict[country]

    origin_dict, addy_dict = read_origin_file(country)
    try:

        dom_rel, int_rel, glo_rel, ove_rel = read_transit_file(country, nationality_dict, input_snapshot, origin_dict) #domestic, int'l, global, overall
    
    except IOError:
        print "something went wrong"
        exit()
    saving_line = input_snapshot + ',' + str(dom_rel) + ',' + str(int_rel) + ',' + str(glo_rel) + ',' + str(ove_rel) + '\n'

    output_file = "/project/mapkit/agamerog/country_asn_analysis/evolution/" + input_country + ".csv"
    with open(output_file, 'a+') as f:
        f.write(saving_line)
        print ("saving /project/mapkit/agamerog/country_asn_analysis/evolution/" + input_country + ".csv")

    
main()
#print ("saving " + plot_filename)

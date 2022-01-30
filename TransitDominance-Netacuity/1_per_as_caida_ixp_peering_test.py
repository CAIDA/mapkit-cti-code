import json
import csv
import sys
import bz2 
import json_lines

#usage python per_as_caida_ixp_peering_test.py <cti_month> <ixp_month> <pdb_month>
#usage python per_as_caida_ixp_peering_test.py 20200301 201910 202003
csv.field_size_limit(sys.maxsize) #thing so we can read Elverton's huge csv lines

current_snapshot = sys.argv[1]
current_ixp_month = sys.argv[2]
current_pdb_month = sys.argv[3]
asrel_filename = '/data/external/as-rank-ribs/' + current_snapshot + '/' + current_snapshot + '.as-rel.txt.bz2'

glob_file = open('individual_ases_failing_' + current_snapshot + '.csv','w+')

#country_list = ['AF','AM','AT','AZ','BO','BW','BG','BF','BI','CF','TD','CU','CZ','SV','ER','ET','GN','GW','GY','HU','IR','JO','KZ','KI','KG','LR','LI','MW','ML','MR','ME','MM','NI','KP','MK','PY','CG','RO','RW','RS','SL','SK','SI','PW','NU','CK','WS','TL','FM']

#country_list = ['AF','AL','DZ','AO','AG','AR','AM','AU','AT','AZ','BS','BH','BD','BB','BY','BE','BZ','BJ','BT','BO','BA','BW','BR','BN','BG','BF','BI','CV','KH','CM','CA','CF','TD','CL','CN','CO','KM','CG','CD','CR','CI','HR','CU','CY','CZ','DK','DJ','DM','DO','EC','EG','SV','GQ','ER','EE','ET','FJ','FI','FR','GA','GM','GE','DE','GH','GR','GD','GT','GN','GW','GY','HT','VA','HN','HU','IS','IN','ID','IR','IQ','IE','IL','IT','JM','JP','JO','KZ','KE','KI','KP','KR','KW','KG','LA','LV','LB','LS','LR','LY','LI','LT','LU','MK','MG','MW','MY','MV','ML','MT','MH','MR','MU','MX','FM','MD','MC','MN','ME','MA','MZ','MM','NA','NR','NP','NL','NZ','NI','NE','NG','NO','OM','PK','PW','PS','PA','PG','PY','PE','PH','PL','PT','QA','RO','RU','RW','KN','LC','VC','WS','SM','ST','SA','SN','RS','SC','SL','SG','SK','SI','SB','SO','ZA','SS','ES','LK','SD','SR','SZ','SE','CH','SY','TJ','TZ','TH','TL','TG','TO','TT','TN','TR','TM','TV','AE','UG','GB','UA','UY','UZ','VU','VE','VN','YE','ZM','ZW'] #all countries except the US

#country_list = ['SY','BO','CG','YE','ET','ER','SR','CU','MK','KG','CV','GN','CF','BF','GM','LS','ML','KI','BI','CM','MH','NR','OM','WS','SO','SL', 'FM', 'NE', 'PW', 'ST', 'SS', 'SB', 'TL','SD','TM','VU','TO','TJ','UZ','KM','BS','AF','BW','CD','BH','AL','BR','CR','BN','BD','DZ','CI','BB','AO','BG','HR','BY','AG','KH','CY','BE','AR','CA','CZ','BZ','AM','TD','DK','BJ','CL','AU','DJ','BT','CN','AT','DM','BA','AZ','CO','DO','GE','HN','JM','EC','DE','HU','JP','EG','GH','IS','JO','SV','GR'] #most of the countries where "False" is true for all three in 201803
#['BF','BI','CV','CM','CF','GM','GN','KI','KG','LS','MK','ML','MH','FM','NR','NE','OM','PW','WS','ST','SL','SB','SO','SS','SD','TJ','TL','TO','TM','UZ','VU'] second list
#['AF','AL','DZ','AO','AG','AR','AM','AU','AT','AZ','BS','BH','BD','BB','BY','BE','BZ','BJ','BT','BA','BW','BR','BN','BG','KH','CA','TD','CL','CN','CO','KM','CD','CR','CI','HR','CY','CZ','DK','DJ','DM','DO','EC','EG','SV','GQ','EE','FJ','FI','FR','GA','GE','DE','GH','GR','GD','GT','GW','GY','HT','VA','HN','HU','IS','IN','ID','IR','IQ','IE','IL','IT','JM','JP','JO','KZ','KE','KP','KR','KW','LA','LV','LB','LR','LY','LI','LT','LU','MG','MW','MY','MV','MT','MR','MU','MX','MD','MC','MN','ME','MA','MZ','MM','NA','NP','NL','NZ','NI','NG','NO','PK','PS','PA','PG','PY','PE','PH','PL','PT','QA','RO','RU','RW','KN','LC','VC','SM','SA','SN','RS','SC','SG','SK','SI','ZA','ES','LK','SZ','SE','CH','TZ','TH','TG','TT','TN','TR','TV','AE','UG','GB','UA','UY','VE','VN','ZM','ZW'] third list
in_list = set(['CF','BO','ET','PA','BJ','MR','VE','IR','TD','ER','MA','TT','ML','CM','NE','HT','PY','BF','SA','SV','CG','GT','PE','WS','GM','BD','RO','KW','GW','BE','BR','GH','GR','BS','BG','OM','GQ','BY','BA','JO','JP','BZ','BB','GY','HR','BN','RU','GE','HU','RW','BH','GD','HN','RS','BI','GB','PS','LT','BT','GA','PW','LU','JM','GN','PT','LR','BW','AO','LV','MN','FR','PG','MH','KZ','FI','PK','MK','ZW','FJ','PH','MU','FM','ES','PL','MT','NI','ME','ZM','MW','NL','MD','MV','EE','NO','MG','UG','EG','NA','UY','MY','ZA','NG','MC','MX','EC','UZ','NZ','AL','IL','MM','NP','CV','NR','KM','SB','CU','ST','CI','SG','SZ','SK','CH','SE','SY','SD','CO','KR','KG','DO','SI','KE','DM','KP','CN','SS','DJ','SO','CL','SR','DK','SN','CA','KI','DE','SM','CD','KH','YE','SL','CZ','KN','AT','SC','CY','DZ','TM','AF','CR','LB','TJ','IQ','AZ','LC','LS','IS','IE','LA','TH','AM','ID','TV','TG','IT','UA','TR','LY','VN','QA','LK','VA','AR','MZ','LI','VC','AU','TN','AE','VU','TO','AD','IN','TL','AG','TZ','US','AQ']) #every country except the US
#skip_list = set(['LI','LA','IL','AT','KR','AR','MV','US','AU','NG','GR','KZ','BD','SG','RW','DM','FI','SE','PG','BR','TN','DE','CY','BH','CA','MD','NZ','AO','PY','ZA','MX','PH','RS','KH','HU','VU','IS','RU','CR','BT','VA','EE','AG','SI','GD','BG','UA','HR','BJ','BW','DK','BN','KM','CH','LV','SN','NO','GB','SK','BE','IT','LT','SC','FR','CZ','RO','MU','GM','GH','UY','NL','PL','CG','MC','DO','LR','GA','AD','VN','NP','ID','JP','MY','KE','DZ','TZ','IE','TG','SR','MZ','MW','AE','BI','MG','DJ','AQ','CI','UG'])
skip_list = set()
country_list = list(in_list.difference(skip_list))
print(len(country_list))

def parse_caida_ixps():
   
    ixps_file = './ixs_' + current_ixp_month + '.jsonl'
    as_file = './ix-asns_' + current_ixp_month + '.jsonl'
    #ixps_file = '../20180301.asns.jsonl'
    ixp_to_country = dict()
    as_to_ixp = dict()
    ixp_to_as = dict()
    parsed_ids = set()
    with json_lines.open(ixps_file) as f:
        for data in f:
            #read country of IXP, add to dict with key the ix_id
            ix_id = str(data["ix_id"])
            try:
                ix_cc = u' '.join(data["country"]).encode('utf-8').strip()
            except KeyError:
                ix_cc = "XX" #country not known
                if ix_id in parsed_ids:
                    if ixp_to_country[ix_id] != "XX":
                        continue
                        #If this IXP was found in another dataset with a 
                        #nonempty country, keep that previous value
            parsed_ids.add(ix_id)
            ixp_to_country[ix_id] = ix_cc

    with json_lines.open(as_file) as f:
        for data in f:
            #create two dicts, one with IXP:[AS1,AS2,...] and another with AS:[IXP1,IXP2,...]
            ix_id = str(data["ix_id"])
            ix_as = str(data["asn"])
            if ix_id in ixp_to_as:
                ixp_to_as[ix_id].append(ix_as)
            else:
                ixp_to_as[ix_id] = [ix_as]

            if ix_as in as_to_ixp:
                as_to_ixp[ix_as].append(ix_id)
            else:
                as_to_ixp[ix_as] = [ix_id]
    if current_pdb_month == '201803': #don't use sql for that month (more recent months are all json)
        pdb_file = 'peeringdb_2_dump_' + current_pdb_month[:4] + '_' + current_pdb_month[4:] + '_11.json'
    else:
        pdb_file = 'peeringdb_2_dump_' + current_pdb_month[:4] + '_' + current_pdb_month[4:] + '_01.json'
    with open(pdb_file) as json_file:
        #add peeringdb private interconnection facilities as "IXPs"
        data = json.load(json_file)
        pdboffset = 1000000
        #offset private peering facility ID so it
        #doesn't conflict with IXP IDs (which go up to 960)
        #print(json.dumps(data['net'], indent=4))
        #print(data.keys())
        json_formatted_str = json.dumps(data, indent=2)
        for fac in data['fac']['data']:
            fac_id = str(pdboffset + int(fac['id']))
            try:
                fac_cc = u' '.join(fac["country"]).encode('utf-8').strip()
            except KeyError:
                fac_cc = "XX" #country not known
            ixp_to_country[fac_id] = fac_cc
        
        for autsys in data['netfac']['data']:
            ix_id = str(pdboffset + int(autsys["fac_id"]))
            ix_as = str(autsys["local_asn"])
            if ix_id in ixp_to_as:
                ixp_to_as[ix_id].append(ix_as)
            else:
                ixp_to_as[ix_id] = [ix_as]

            if ix_as in as_to_ixp:
                as_to_ixp[ix_as].append(ix_id)
            else:
                as_to_ixp[ix_as] = [ix_id]

    return ixp_to_as, as_to_ixp, ixp_to_country

def read_asrel_file():
    #Create a set of AS-AS strings
    #Separated by a ':' where one is an inferred transit provider of the other
    #'provider:customer' where provider and customer are both ASNumbers
    asrel_set = set() #transit providers
    peer_dict = dict()
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
                p2c = provider + ':' + customer
                #country_rel = provider + ':99999'
                #asrel_set.add(country_rel)
                asrel_set.add(p2c)
                p2c = customer + ':' + provider
                asrel_set.add(p2c)

            if int(row_rel) == 0:
                peerone = row.split('|')[0]
                peertwo = row.split('|')[1]
                if peerone in peer_dict:
                    peer_dict[peerone].append(peertwo)
                else:
                    peer_dict[peerone] = [peertwo]
                if peertwo in peer_dict:
                    peer_dict[peertwo].append(peerone)
                else:
                    peer_dict[peertwo] = [peerone]

            #make sure every transit provider of anyone is a transit provider of the country
        return asrel_set, peer_dict

def read_nat_row(nat_row, current_country): #for reading the multi-country lines

    nat_frac = float(nat_row.split(',')[2])
    #determine if origin AS is domestic or foreign
    if nat_frac >= 0.666666666666666666:
        return 'domestic'
    else:
        return 'foreign'

def read_nationality_file(current_country):
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
            names = row.split(',')[1]
            country_dict[code] = names
    return country_dict

global_country_dict = read_summary_file()

def read_origin_file(origin_filename, current_country):
    #Read file with number of addresses, and the percentage of the country
        #they represent, for each AS with addresses in this country
        #Returns a dictionary where the key is the AS Number and the value
        #is a list with two variables (#addresses and %country)
    country_cumsums = dict()
    origin_dict = dict()
    num_ases = 0
    try:
        #f = bz2.BZ2File(origin_filename, 'rb')
        f = open(origin_filename, 'r')
        try:
            ipmap_list = f.readlines()
        except:
            sys.stderr.write("\n something went wrong opening " + origin_filename + "\n")
            sys.exit()
        country_total = 0 #count for the entire country (for the '99999' all ASN origin)
        cumsum = 0.0
        for i in range(len(ipmap_list)):
            row = ipmap_list[i].strip('\n')
            if row.split(',')[0] == 'OriginASN':
                continue #skip header
            origin_perc = float(row.split(',')[2])
            if origin_perc < 0.05: #THIS THRESHOLD IS IMPORTANT TODO AGG
                continue #skip tiny ASes
            cumsum = cumsum + origin_perc
            asn = row.split(',')[0].split('-')[0]
            origin_dict[asn] = origin_perc

            num_ases = num_ases + 1
        country_cumsums[current_country] = [cumsum, num_ases]
        #if current_country == 'JP':
        #    print('JAPAN')
        #    print(num_ases)
        #    print(len(origin_dict))
    except IOError:
        print "no file for " + origin_filename
    return origin_dict, country_cumsums #return set of ASes that account for the country's 90% of addresses

def test_bgp_foreign(access, peers, nation_dict):
    test_output = 'False'
    peer_set = peers[access]
    for peer in peer_set:
        current_nat = 'domestic'
        try:
            current_nat = nation_dict[peer]
        except KeyError:
            continue #nationality unknown
        if current_nat != 'domestic':
            glob_file.write('test_bgp_foreign-' + str(access) + '-' + str(peer) + '\n')
            test_output = 'True' #at least one peer is not domestic
            break
    return test_output

def test_ixp_foreign(access, as_to_ixp, ixp_to_country, current_country):

    test_output = 'False'
    ixp_set = as_to_ixp[access]
    for ixp in ixp_set:
        if ixp in ixp_to_country:
            if str(ixp_to_country[ixp]).replace(' ','') == 'XX':
                continue #country of IXP unknown
            elif str(ixp_to_country[ixp]).replace(' ','') != current_country:
                test_output = 'True'
                glob_file.write('test_ixp_foreign-' + str(access) + '-' + str(ixp) + '-' + str(ixp_to_country[ixp]).replace(' ','') + '\n')
                break
        else:
            continue #no country for this IXP
    return test_output

def test_as_foreign(access, as_to_ixp, ixp_to_as, nation_dict, transits):
    test_output = 'False'
    ixp_set = as_to_ixp[access]
    for ixp in ixp_set:
        potential_peers = ixp_to_as[ixp] #fetch IXP members
        for pot_peer in potential_peers:
            p2c_test = pot_peer + ':' + access
            if p2c_test in transits:
                continue #this is a transit relationship
            if pot_peer in nation_dict:
                if nation_dict[pot_peer] == 'XX':
                    continue #country unknown
                elif nation_dict[pot_peer] != 'domestic':
                    glob_file.write('test_as_foreign-' + str(access) + '-' + str(ixp) + '-' + str(pot_peer) + '\n')
                    test_output = 'True' #foreign peer
                    break

    return test_output

def country_qualifier_peering(ixp_to_as, as_to_ixp, ixp_to_country, transits, peers):
    print('Country,Non-Peering Fraction,Number Ases,Fraction over Threshold')
    as_list_irb = []
    with open('./individual_as_country_qualifier_' + current_snapshot + '.csv','w+') as f:
        glob_file.write('Country_code,country_name,AS,test,reason\n')
        f.write('Country_code,country_name,country_perc_addresses,num_ases,as1-as2-...-asN\n')
        globalAScount = 0
        globalCountrycount = 0
        for i in range(len(country_list)):
            bgp_test_as = dict()
            foreign_ixp_test_as = dict()
            foreign_as_test_as = dict()
            country_non_peering_frac = 0.0
            foreign_ixp_test = 'False'
            foreign_as_test = 'False'
            bgp_test = 'False'
            current_country = country_list[i]
            origin_filename = "/project/mapkit/agamerog/country_asn_analysis/country_aspath/origin/" + current_snapshot + "/" + current_country + ".csv"

            access_ases, country_perc = read_origin_file(origin_filename, current_country)
            nation_dict = read_nationality_file(current_country)

            line = country_list[i] + ',' + global_country_dict[country_list[i]] + ','
            #BGP peer test
            glob_file.write('***,' + country_list[i] + ',' + global_country_dict[country_list[i]] + ',')
            for access in access_ases:
                if access in peers:
                    bgp_test = test_bgp_foreign(access, peers, nation_dict)        
                    if bgp_test == 'True':
                        bgp_test = 'True ' + access
                        bgp_test_as[access] = 'True' #build a dictionary with test results per AS
                    else:
                        bgp_test_as[access] = 'False'
                else: #AS has no peers
                        bgp_test_as[access] = 'False'

            #Peers at foreign IXP test
            for access in access_ases:
                if access in as_to_ixp:
                    foreign_ixp_test = test_ixp_foreign(access, \
                            as_to_ixp, ixp_to_country, current_country)
                    if foreign_ixp_test == 'True':
                        foreign_ixp_test = 'True ' + access 
                        foreign_ixp_test_as[access] = 'True'
                    else:
                        foreign_ixp_test_as[access] = 'False'
                else: #access does not peer at any IXP
                    foreign_ixp_test_as[access] = 'False' 
                        #break #

            for access in access_ases:
                if access in as_to_ixp:
                    foreign_as_test = test_as_foreign(access, \
                            as_to_ixp, ixp_to_as, nation_dict, transits)

                    if foreign_as_test == 'True':
                        foreign_as_test_as[access] = 'True'
                        foreign_as_test = 'True ' + access 
                    else:
                        foreign_as_test_as[access] = 'False'
                else: #access does not peer at any IXP
                    foreign_as_test_as[access] = 'False'
            #Now that we have run every test on every AS(origin > 0.1), see if the entire country 
            #has any set of ASes that add up to a non_peering perc > 50.0
            candidate_ases = set()
            for access in access_ases:

                test_str = foreign_as_test_as[access] + foreign_ixp_test_as[access] \
                    + bgp_test_as[access]
                if test_str == 'FalseFalseFalse': #If the AS simultaneously fails every test
                    country_non_peering_frac = country_non_peering_frac + access_ases[access] #this dict has perc originated
                    candidate_ases.add(access)
                    as_list_irb.append(access)
                    globalAScount = globalAScount + 1
            first_dash = True
            print(current_country + ',' + str(global_country_dict[current_country]) + ',' + str(country_non_peering_frac) + ',' + str(len(access_ases)) + ',' + str(country_perc[current_country][0]))
            if country_non_peering_frac > 25.0:
                globalCountrycount = globalCountrycount + 1
                line = line + str(country_non_peering_frac) + ',' + str(len(candidate_ases)) + ','
                for asn in access_ases:#this has been changed from candidate_ases
                    #as we would like to probe every AS that originates a fraciton bigger than the threshold

                    if first_dash: #don't write - at the beginning
                        line = line + asn
                        first_dash = False
                    else:
                        line = line + '-' + asn
                line = line + '\n'
                
            else:
                line = line + 'False,False,None\n'
                           
            #Peers at any IXP where at least one other peer is foreign (except transit providers or customers) test
            #Country_code,country_name,country_perc_addresses,num_ases,as1-as2-...-asN

            f.write(line)
    print('saving individual_as_country_qualifier_' + current_snapshot + '.csv')
    print('individual_ases_failing_' + current_snapshot + '.csv')
    print('number of ASes: ' + str(globalAScount))
    print('number of ASes: ' + str(globalCountrycount))
    print('number of ASes for IRB: ' + str(len(as_list_irb)))
def main():
    
    transits, peers = read_asrel_file()

    ixp_to_as, as_to_ixp, ixp_to_country = parse_caida_ixps()
    #print(as_to_ixp)
    country_qualifier_peering(ixp_to_as, as_to_ixp, ixp_to_country, transits, peers)
    
main()

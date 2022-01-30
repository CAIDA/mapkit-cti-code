import os
import sys
#import base dir and dates
baseDir = '/project/mapkit/agamerog/country_asn_analysis/dd_and_bd/'
year = str(sys.argv[1])
month = str(sys.argv[2])
#Download ip2as inputs
#Download and decompress RIR delegation files
dateString = year + month + "01" #first day of the month

os.system("cd /project/mapkit/agamerog/country_asn_analysis/dd_and_bd/; wget https://ftp.apnic.net/stats/afrinic/" + year + "/delegated-afrinic-extended-" + dateString)
os.system("cd /project/mapkit/agamerog/country_asn_analysis/dd_and_bd/; wget https://ftp.apnic.net/stats/lacnic/delegated-lacnic-extended-" + dateString)
os.system("cd /project/mapkit/agamerog/country_asn_analysis/dd_and_bd/; wget https://ftp.apnic.net/stats/arin/delegated-arin-extended-" + dateString)
os.system("cd /project/mapkit/agamerog/country_asn_analysis/dd_and_bd/; wget https://ftp.apnic.net/stats/apnic/" + year + "/delegated-apnic-extended-" + dateString + ".gz")
os.system("cd /project/mapkit/agamerog/country_asn_analysis/dd_and_bd/; wget https://ftp.ripe.net/pub/stats/ripencc/" + year + "/delegated-ripencc-extended-" + dateString + ".bz2")
os.system("bunzip2 /project/mapkit/agamerog/country_asn_analysis/dd_and_bd/delegated-ripencc-extended-" + dateString + ".bz2")
os.system("gunzip /project/mapkit/agamerog/country_asn_analysis/dd_and_bd/delegated-apnic-extended-" + dateString + ".gz")
os.system("cd /project/mapkit/agamerog/country_asn_analysis/dd_and_bd/; cat delegated-*extended-" + dateString + " > delegated-combined-extended-" + dateString)

# Archived as branch old-repo-sigcomm-submission-2019

## SIGCOMM 2019 Submission #274 ##

### Data ###
We conduct our analyses with a set of 230,848,218 IPv4 AS-level paths observed in BGP table dumps gathered by [CAIDA](http://www.caida.org)'s [AS-Rank](http://as-rank.caida.org/) from [RouteViews](http://www.routeviews.org/routeviews/) and RIPE [RIS](https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris) collectors during the first five days of March 2018.

The path file: [[download link](https://) will be added shortly]

#### Supporting Data ####
* RIR information (delegation file): IP prefix assignment
* ASN information (asn-to-country): ASN, country, prefix count and precentage, IP count and percentage
* BGP monitor geolocation: the country of BGP monitors that observe BGP path - `data/20180301.geo-collectors-final.json`
* AS relationship: specify the relationship between peers (c2p, p2c, p2p) - `data/20180301.as-rel.txt`
* Prefixes-to-country mapping: the IP geolocation results from Maxmind's free version - `data/pfx-to-country.2018-03-01.maxmind`


### Scripts ###
* transit_path_metric.py - computing the ATI value for each ASN in a country specified by country-code
```
$ transit_path_metric.py [two-letter-country-code]
```
* heavy-reliance analysis
* State-own ASes study

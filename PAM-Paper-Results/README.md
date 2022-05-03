# Welcome to the CTI Repo!
Main developer and researcher: Alexander Gamero-Garrido (GitHub:@gamero email:gamero@alum.mit.edu)

Contributors: Esteban Carisimo, Vishwesh Rege, Alistair King, Shuai Hao, Bradley Huffaker, Amogh Dhamdhere, Alex C. Snoeren and Alberto Dainotti

If you use the data in this repository, you must cite this paper:

- Gamero-Garrido, A., Carisimo, E., Hao, S., Huffaker, B., Snoeren, A.C., Dainotti, A. (2022). Quantifying Nations’ Exposure to Traffic Observation and Selective Tampering. In: Hohlfeld, O., Moura, G., Pelsser, C. (eds) Passive and Active Measurement. PAM 2022. Lecture Notes in Computer Science, vol 13210. Springer, Cham. https://doi.org/10.1007/978-3-030-98785-5_29

This project was awarded Best Dataset at the Passive and Active Measurement Conference (PAM 2022).

## Subdirectory: CTI

This directory contains the core results of the CTI paper (Sec. 5.1): the CTI values for each Autonomous System in each country.
Files are named as CC.csv, with CC being the 2-letter country code.
The first column is the AS Number - ASName, and the second column is the CTI value for that AS.

### Origin

We also publish the number of addresses originated by each AS in each country. Files are named as <CC>.csv, with <CC> being the 2-letter country code.
The columns are explained in the README.md file in the Origin subdirectory.

## Subdirectory: TransitDominance

We release our findings from the transit-dominance analysis in the CTI paper.
  
From Sec. 6.3: Country-level transit fractions T(C) for countries in our sample (column 5, transit_w_scaled):
https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/TransitDominance/country.transit.summary.202003.csv 
  
From Sec. 6.1: percentage of a country's address space originated by candidate ASes (column 3, country_perc_addresses).
https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/TransitDominance/individual_as_country_qualifier_20200301.csv
  
## Subdirectory: Misc
  
From Sec. 9.1: Comparison with Hegemony for each AS-Country pair:
  https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/Misc/combined_joint_cti_hegemony.csv
  
From Sec. 5.2: Submarine cable operators in each relevant country:
  https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/Misc/LIST_SUBMARINE_CABLE_OPERATORS_AND_ASES.xlsx
  
 From Sec. 5.3: State-owned ASes, Column 2 is CTIn, Column 3 is F(C), Column 4 is Directly Originated
  https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/Misc/state_owned_F_CTIn.csv
  
  

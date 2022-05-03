# Welcome to the CTI Repo!

Main developer and researcher: Alexander Gamero-Garrido (GitHub:@gamero email:gamero@alum.mit.edu)

Contributors: Esteban Carisimo, Vishwesh Rege, Alistair King, Shuai Hao, Bradley Huffaker, Amogh Dhamdhere, Alex C. Snoeren and Alberto Dainotti

If you use the data in this repository, you must cite this paper:

- Gamero-Garrido, A., Carisimo, E., Hao, S., Huffaker, B., Snoeren, A.C., Dainotti, A. (2022). Quantifying Nations’ Exposure to Traffic Observation and Selective Tampering. In: Hohlfeld, O., Moura, G., Pelsser, C. (eds) Passive and Active Measurement. PAM 2022. Lecture Notes in Computer Science, vol 13210. Springer, Cham. https://doi.org/10.1007/978-3-030-98785-5_29

This project was awarded Best Dataset at the Passive and Active Measurement Conference (PAM 2022).

## Subdirectory: CTI

This directory contains the core results of the CTI paper (Sec. 5.1): the CTI values for each Autonomous System in each country.

- https://github.com/CAIDA/mapkit-cti-code/tree/master/PAM-Paper-Results/CTI/CC.csv
- Files are named as CC.csv, with CC being the 2-letter country code.
- The first column is the AS Number - ASName, and the second column is the CTI value for that AS.

### Origin

We also publish the number of addresses originated by each AS in each country. 

- https://github.com/CAIDA/mapkit-cti-code/tree/master/PAM-Paper-Results/CTI/origin/CC.csv
- Files are named as CC.csv, with CC being the 2-letter country code.
- The columns are explained in the README.md file in the Origin subdirectory.

## Subdirectory: TransitDominance

We release our findings from the transit-dominance analysis in the CTI paper.
  
From Sec. 6.3: Country-level transit fractions T(C) for countries in our sample

- https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/TransitDominance/country.transit.summary.202003.csv 
- T(C) is in column 5, transit_w_scaled
  
From Sec. 6.1: percentage of a country's address space originated by candidate ASes 

- https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/TransitDominance/individual_as_country_qualifier_20200301.csv
- percentage of a country's address space originated by candidate ASes is in column 3, country_perc_addresses
  
## Subdirectory: Misc
  
From Sec. 9.1: Comparison with Hegemony

- https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/Misc/combined_joint_cti_hegemony.csv
- Data for all AS-Country pairs: ASN, CTI, Hegemony
  
From Sec. 5.2: Submarine cable operators

- https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/Misc/LIST_SUBMARINE_CABLE_OPERATORS_AND_ASES.xlsx
- Data for each non-landlocked country
- Column headers: Country Code,	Country Name,	CTI Rank,	Top AS, Top AS CTI, Submarine Cable(s)
 
From Sec. 5.3: State-owned ASes
    
- https://github.com/CAIDA/mapkit-cti-code/blob/master/PAM-Paper-Results/Misc/state_owned_F_CTIn.csv
- Column 2 is CTIn, Column 3 is F(C), Column 4 is Directly Originated
  
  

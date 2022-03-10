#!/bin/bash

for chr in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y M;
	do;
		echo MISTIC_chr"$chr".tsv 
		cat MISTIC_chr"$chr".tsv | tail -n+2 | bgzip >> MISTIC_GRCh38_dbNSFP_complete.tsv.gz
	done;

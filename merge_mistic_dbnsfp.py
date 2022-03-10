from platform import python_version
print(python_version())
import numpy as np
import subprocess
import pandas as pd

# path_mistic_cagi_grch38 = "/gstock/MISTIC/CAGI6/download/OUTPUT/MISTIC_GRCh38_dbNSFP_complete.tsv.gz"
# mistic_cagi_grch38 = pd.read_csv(path_mistic_cagi_grch38, compression="gzip", sep="\t", low_memory=False)
# mistic_cagi_grch38["#chr"] = mistic_cagi_grch38["#chr"].astype(str)
# mistic_cagi_grch38["pos(1-based)"] = mistic_cagi_grch38["pos(1-based)"].astype(int)
# mistic_cagi_grch38["ref"] = mistic_cagi_grch38["ref"].astype(str)
# mistic_cagi_grch38["alt"] = mistic_cagi_grch38["alt"].astype(str)
# print(mistic_cagi_grch38)

# dbnsfp_v4_lite_path = "/gstock/MISTIC/CAGI6/download/dbNSFP4_nsSNV.parquet"
# dbnsfp_v4_lite = pd.read_parquet(dbnsfp_v4_lite_path)
# dbnsfp_v4_lite = dbnsfp_v4_lite.loc[dbnsfp_v4_lite['pos(1-based)'] != 'pos(1-based)']
# dbnsfp_v4_lite["#chr"] = dbnsfp_v4_lite["#chr"].astype(str)
# dbnsfp_v4_lite["pos(1-based)"] = dbnsfp_v4_lite["pos(1-based)"].astype(int)
# dbnsfp_v4_lite["ref"] = dbnsfp_v4_lite["ref"].astype(str)
# dbnsfp_v4_lite["alt"] = dbnsfp_v4_lite["alt"].astype(str)
# print(dbnsfp_v4_lite)


# final_merge = pd.merge(
#     mistic_cagi_grch38,
#     dbnsfp_v4_lite,
#     on=['#chr', 'pos(1-based)', 'ref', 'alt']
# )
# print(final_merge)


# for chrom in sorted(final_merge['#chr'].unique().tolist()):
#     print(chrom)
#     final_merge.loc[final_merge['#chr'] == chrom].to_parquet("/gstock/MISTIC/MISTIC_dbNSFP_merge_{}.parquet".format(chrom))

from tqdm import tqdm
import sys,os


# chroms = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "X", "Y", "M",]
# # chroms = ["1"]
# common_cols = ['score', 'sd', 'pred', 'comments', 'aaref', 'aaalt', 'genename', 'cds_strand', 'refcodon', 'codonpos', 'Ensembl_geneid', 'Ensembl_transcriptid', 'Ensembl_proteinid', 'aapos']
# hg38_cols = ['#chr', 'pos(1-based)', 'ref', 'alt']
# hg19_cols = ['hg19_chr', 'hg19_pos(1-based)', 'ref', 'alt']
# hg18_cols = ['hg18_chr', 'hg18_pos(1-based)', 'ref', 'alt']


# tmp_list = list()
# for chrom in tqdm(chroms):
#     df = pd.read_parquet(str('/gstock/MISTIC/MISTIC_dbNSFP_merge_{}.parquet').format(chrom))
#     # df = pd.read_csv(str('/gstock/MISTIC/MISTIC_dbNSFP_merge_{}.tsv.gz').format(chrom), compression='gzip', sep='\t', nrows=1000)
#     df[hg38_cols + common_cols].to_csv('/gstock/MISTIC/MISTIC_chr{}_hg38.tsv.gz'.format(chrom), compression='gzip', sep='\t', index=False)
#     df[hg19_cols + common_cols].to_csv('/gstock/MISTIC/MISTIC_chr{}_hg19.tsv.gz'.format(chrom), compression='gzip', sep='\t', index=False)
#     df[hg18_cols + common_cols].to_csv('/gstock/MISTIC/MISTIC_chr{}_hg18.tsv.gz'.format(chrom), compression='gzip', sep='\t', index=False)
#     # df_38 = df[]
#     # df.to_csv('/gstock/MISTIC/MISTIC_dbNSFP_merge_{}.tsv.gz'.format(chrom), compression='gzip', sep='\t', index=False)
#     # if chrom == "1":
#     #     df.to_csv('/gstock/MISTIC/MISTIC_dbNSFP_merge_complete.tsv.gz'.format(chrom), compression='gzip', sep='\t', index=False, mode="w", header=True)
#     # else:
#     #     df.to_csv('/gstock/MISTIC/MISTIC_dbNSFP_merge_complete.tsv.gz'.format(chrom), compression='gzip', sep='\t', index=False, mode="a", header=False)

from sqlalchemy import create_engine
import sqlite3

assembly = sys.argv[1]
engine = create_engine('sqlite:////gstock/MISTIC/MISTIC_{}.db'.format(assembly), echo=False)

for j, df in tqdm(enumerate(pd.read_csv('/gstock/MISTIC/MISTIC_{}.tsv.gz'.format(assembly), compression='gzip', sep='\t', chunksize=10000))):
    if assembly != "hg38":
        df = df.loc[(df['{}_chr'.format(assembly)] != '.')]

    # if j == 0:
    df.to_sql('MISTIC_{}'.format(assembly), con=engine, if_exists='append', index=False)
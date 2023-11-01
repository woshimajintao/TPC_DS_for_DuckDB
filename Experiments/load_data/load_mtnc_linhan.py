import duckdb
import os
import time

# specify the scale factor
sc = "1"
con = duckdb.connect('sc_'+sc+'.db')

# specify the path for dataset
dat_folder_name = '/Users/wanglinhan/Desktop/BDMA/ULB/INFO-H419/Project/P1/all_codes/refresh_dataset/run_1'
dat_list = os.listdir(dat_folder_name)

for dat_file in dat_list:
    if dat_file.startswith("s_"):
        table_name = dat_file[:-6]
        copy_command = "COPY "+table_name+" FROM "+"'"+dat_folder_name+"/"\
            +dat_file+"'"+" (DELIMITER '|')"
        con.sql(copy_command)


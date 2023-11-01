import duckdb
import os
import time

# specify the scale factor
sc = "7"
con = duckdb.connect('sc_'+sc+'.db')

# specify the path for dataset
csv_folder_name = '/Users/wanglinhan/Desktop/BDMA/ULB/INFO-H419/Project/P1/data/sc_'+ sc + '/csv'
csv_list = os.listdir(csv_folder_name)

# set the timer start
start_time = time.time()
for csv_file in csv_list:
    table_name = csv_file[:-4]
    # load data into tables
    copy_command = "COPY "+table_name+" FROM "+"'"+csv_folder_name+"/"+format(csv_file,"")+"'"
    con.sql(copy_command)
# set the timer end
end_time = time.time()
time_taken = end_time - start_time
print(time_taken)
import duckdb
import time

delete_file = "/Users/wanglinhan/Desktop/BDMA/ULB/INFO-H419/Project/P1/all_codes/refresh_dataset/run_1/delete_1.dat"
sc = "1"

con = duckdb.connect('sc_'+sc+'.db')

with open(delete_file, 'r') as dat_file:
    for row in dat_file:
        row = [value.strip() for value in row.split('|')]
        start_date = row[0]
        end_date = row[1]
        start_time = time.time()
        print(f'The Deletion for DF_I started: {start_time} , for date {start_date} and {end_date}')

        con.sql(f" DELETE FROM inventory USING date_dim WHERE inventory.inv_date_sk = date_dim.d_date_sk AND date_dim.d_date BETWEEN CAST('{start_date}' AS DATE) AND CAST('{end_date}' AS DATE);")        

        end_time = time.time()
        print(f'The time for the delete (DF_I) took: {end_time-start_time} seconds')



con.close()
import duckdb
import time


# this function is to simulate failure
delete_file = "/home/pce/Pictures/duckdb_terminal/refresh_dataset/run_1/delete_1.dat"
sc = "1"

con = duckdb.connect('sc_'+sc+'.db')

 
             
con.sql(f"DELETE FROM web_sales USING date_dim_fd WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk ")             
con.sql(f" DELETE FROM web_returns USING date_dim_df WHERE web_returns.wr_returned_date_sk = date_dim.d_date_sk ")



con.close()
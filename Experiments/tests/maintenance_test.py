
import os
import subprocess
import time
import duckdb
import threading
import signal
import fnmatch



# Specify the root directory containing your SQL files
root_directory = "/Users/wanglinhan/Desktop/BDMA/ULB/INFO-H419/Project/P1/all_codes/refresh_dataset"

# specify the scale factor
sc = "7"
con = duckdb.connect("sc_"+sc+".db")


# simulate the database failure for the data accessibility test
def simulate_duckdb_failure():
    
    # Record the start time before triggering the failure
    
    failure_start_time = time.time()
    
    # Simulate a failure event by stopping DuckDB
    print("Simulating DuckDB failure during Data Maintenance Test 1...")
    
    # The following line sends the SIGKILL signal to the DuckDB process
    # os.kill(os.getpid(), signal.SIGKILL)
    # os._exit(1) 
    time.sleep(20)
    # Record the end time after triggering the failure
    
    failure_end_time = time.time()
    print(f"DuckDB failure time: {failure_end_time - failure_start_time:.2f} seconds")
   


# Function to run all maintenance functions in a folder
def run_sql_queries_in_folder(folder_path):
    global con
    if folder_path.endswith("2"):
        run = 2
    else:
        run = 1
    print("We are running in "+folder_path + " for refresh run", run)
    # Get a list of SQL files in the folder
    py_files = [file for file in sorted(os.listdir(folder_path)) if file.endswith(".py")]
    time_dict = {}
    # Loop through the SQL files and execute them
    for py_file in py_files:
        with open(os.path.join(folder_path, py_file), "r") as sql_script:
            t=time.time()
            py_query = sql_script.read()
        
            try:
                exec(py_query)
                print(f"successfully executed! {py_file} with time {time.time()-t} seconds")
                time_dict[py_file] = time.time() - t
                        
            except Exception as e:
                print(f"problem executing {py_file}! : {e}") 
                
                
                ttime = time.time()
                print(ttime)
    # Simulate DuckDB failure in a separate thread
                failure_thread = threading.Thread(target=simulate_duckdb_failure)
                failure_thread.start()
        
     # Wait for the failure to complete
                failure_thread.join()
                print("Durable media failure completed.")
                #Add end time
                tftime= time.time()
                time_dict["failure"] = tftime-ttime
                print(f'completed: {tftime-ttime}')
    return time_dict
        
   

# List all subdirectories in the root directory
b_time= time.time()
print(f"This is the beginning of the Data maintenance query {b_time}")

subdirectories = [d for d in os.listdir(root_directory) if os.path.isdir(os.path.join(root_directory, d))]
print(subdirectories)

# record the result for data maintenance test
result_run_1 = run_sql_queries_in_folder("/Users/wanglinhan/Desktop/BDMA/ULB/INFO-H419/Project/P1/all_codes/maintenance_functions")
result_run_2 = run_sql_queries_in_folder("/Users/wanglinhan/Desktop/BDMA/ULB/INFO-H419/Project/P1/all_codes/maintenance_functions_2")
print("All SQL queries executed successfully.")
e_time= time.time()
print(f"This is the end of the Data maintenance query: {e_time-b_time}")
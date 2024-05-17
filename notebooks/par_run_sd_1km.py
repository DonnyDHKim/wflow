import os
import time
import logging
import datetime as dt
import pandas as pd
from multiprocessing import Pool, cpu_count, set_start_method
from IPython.display import display, clear_output
from ipywidgets import Output
import importlib
import sys

## Change to the appropriate working directory
#os.chdir(os.path.join("D:\\", "GitHub", "wflow"))
#
## Pre-import and store module references
#model_modules = {
#    "wflow_tofuflex": importlib.import_module("wflow.wflow_tofuflex"),
#    "wflow_tofuflex_ns": importlib.import_module("wflow.wflow_tofuflex_ns"),
#    "wflow_topoflex_bm": importlib.import_module("wflow.wflow_topoflex_bm")
#}

def run_model(wflow_cloneMap, caseName, model_ver, runId, configfile):
    try:
        module_path = "D:\\GitHub\\wflow"  # Adjust this path to the actual path of the 'wflow' module
        if module_path not in sys.path:
            sys.path.insert(0, module_path)
        #os.chdir(os.path.join("D:\\", "GitHub", "wflow"))
        model_module = importlib.import_module(str("wflow."+ model_ver))
        WflowModel = getattr(model_module, 'WflowModel')
        wf_DynamicFramework = getattr(model_module, 'wf_DynamicFramework')
    
        myModel = WflowModel(wflow_cloneMap, caseName, runId, configfile)
        dynModelFw = wf_DynamicFramework(myModel, datetimestart=datetimestart, lastTimeStep=lastTimeStep, firstTimestep=firstTimestep, timestepsecs=timestepsecs, mode="steps")
    
        logfname = f"wflow_{os.getpid()}.log"
        dynModelFw.createRunId(NoOverWrite=False, logfname=logfname, level=logging.DEBUG, model=model_ver)
        
        dynModelFw._runInitial()
        dynModelFw._runResume()
        dynModelFw._runDynamic(firstTimestep, lastTimeStep)
        dynModelFw._runSuspend()
        dynModelFw._wf_shutdown()
    except Exception as e:
        logging.error(f"Error in run_model: {e}")
        raise

# Define the function that simulates your model running
def parallel_run_model(args):
    start_time = time.time()
    try:
        run_model(*args)
        runtime = time.time() - start_time
        return runtime
    except Exception as e:
        return f"Error: {str(e)}"

# Define the function to handle results and update the output widget
def update_progress(result):
    global completed_count
    completed_count += 1
    with out:
        out.clear_output(wait=True)
        print(f"Completed {completed_count}/{total_tasks} processes.")

# Setup the Output widget
out = Output()
display(out)

# Setup DataFrame and other parameters
df = pd.read_csv(os.path.join("D:\\", "wflow_models", "combined_samples.csv"))
df = df

wsName = "san-diego_california_20331196_11023340"
caseName = os.path.join("D:\\", "wflow_models", wsName)
wflow_cloneMap = 'wflow_subcatch.map' 
firstTimestep = 1
timestepsecs = 3600
datetimestart = dt.datetime(2013, 9, 1, 1)
datetimeend = dt.datetime(2018, 10, 1, 23)
step_diff = (datetimeend - datetimestart).total_seconds() / timestepsecs
lastTimeStep = firstTimestep + step_diff

# Prepare multiprocessing
if __name__ == "__main__":
    start_time = time.time()
    set_start_method('spawn')
    num_processes = 50  # Optimal number of processes
    args_list = []
    for i in range(len(df)):  # Assuming 'df' is your DataFrame with necessary data
        #args_list.append((
        #    wflow_cloneMap, 
        #    caseName, 
        #    "wflow_tofuflex", 
        #    os.path.join("output_BM", str(i)), 
        #    os.path.join("temp", str(i), f"wflow_tofuflex_{i}.ini")
        #))
        #args_list.append((
        #    wflow_cloneMap, 
        #    caseName, 
        #    "wflow_tofuflex_ns", 
        #    os.path.join("output_BM", f"ns_{i}"), 
        #    os.path.join("temp", str(i), f"wflow_tofuflex_ns_{i}.ini")
        #))
        args_list.append((
            wflow_cloneMap, 
            caseName, 
            "wflow_topoflex_bm", 
            os.path.join("output_BM_1km", f"bm_{i}"), 
            os.path.join("temp", str(i), f"wflow_topoflex_bm1k_{i}.ini")
        ))

    results = []
    completed_count = 0
    total_tasks = len(args_list)

    with Pool(num_processes) as pool:
        result_objects = pool.imap_unordered(parallel_run_model, args_list)
        for result in result_objects:
            results.append(result)
            update_progress(result)

    # Summarize and display final results
    total_runtime = time.time() - start_time
    #total_runtime = sum(results)
    avg_runtime = sum(results) / len(results) if results else 0
    max_runtime = max(results) if results else 0
    min_runtime = min(results) if results else 0
    with out:
        clear_output(wait=True)
        print(f"Completed all processes.")
        print(f"Total runtime: {total_runtime:.2f} seconds")
        print(f"Average iteration runtime: {avg_runtime:.2f} seconds")
        print(f"Max iteration runtime: {max_runtime:.2f} seconds")
        print(f"Min iteration runtime: {min_runtime:.2f} seconds")

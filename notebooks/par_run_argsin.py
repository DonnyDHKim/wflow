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
import argparse
import glob

def run_model(wflow_cloneMap, caseName, model_ver, runId, configfile, intbl_path, datetimestart, datetimeend):
    try:
        module_path = "D:\\GitHub\\wflow"  # Adjust this path to the actual path of the 'wflow' module
        if module_path not in sys.path:
            sys.path.insert(0, module_path)
        
        model_module = importlib.import_module(str("wflow." + model_ver))
        WflowModel = getattr(model_module, 'WflowModel')
        wf_DynamicFramework = getattr(model_module, 'wf_DynamicFramework')
    
        myModel = WflowModel(wflow_cloneMap, caseName, runId, configfile)
        
        timestepsecs = 3600
        firstTimestep = 1
        step_diff = (datetimeend - datetimestart).total_seconds() / timestepsecs
        lastTimeStep = firstTimestep + step_diff

        dynModelFw = wf_DynamicFramework(myModel, datetimestart=datetimestart, lastTimeStep=lastTimeStep, firstTimestep=firstTimestep, timestepsecs=timestepsecs, mode="steps")
    
        logfname = f"wflow_{os.getpid()}.log"
        
        ## Validate intbl_path
        #logging.info(f"Checking intbl_path: {intbl_path}")
        #if not os.path.exists(intbl_path):
        #    raise FileNotFoundError(f"intbl_path does not exist: {intbl_path}")
        #if not glob.glob(os.path.join(intbl_path, "*.tbl")):
        #    raise FileNotFoundError(f"No .tbl files found in intbl_path: {intbl_path}")
        
        dynModelFw.createRunId(intbl=intbl_path, NoOverWrite=False, logfname=logfname, level=logging.ERROR, model=model_ver)
        
        dynModelFw._runInitial()
        dynModelFw._runResume()
        dynModelFw._runDynamic(firstTimestep, lastTimeStep)
        dynModelFw._runSuspend()
        dynModelFw._wf_shutdown()
    except Exception as e:
        logging.error(f"Error in run_model: {e}")
        raise

def parallel_run_model(args):
    start_time = time.time()
    try:
        run_model(*args)
        runtime = time.time() - start_time
        return runtime
    except Exception as e:
        return f"Error: {str(e)}"

def update_progress(result):
    global completed_count
    completed_count += 1
    with out:
        out.clear_output(wait=True)
        print(f"Completed {completed_count}/{total_tasks} processes.")

# Setup the Output widget
out = Output()
display(out)

if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser(description="Run Wflow model in parallel")
    parser.add_argument('--outpath', type=str, required=True, help="Output path name")
    parser.add_argument('--wsName', type=str, required=True, help="Workspace name")
    parser.add_argument('--datetimestart', type=lambda s: dt.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'), required=True, help="Start date in 'YYYY-MM-DD HH:MM:SS' format")
    parser.add_argument('--datetimeend', type=lambda s: dt.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'), required=True, help="End date in 'YYYY-MM-DD HH:MM:SS' format")
    parser.add_argument('--model_ver', nargs='+', required=True, help="List of model versions to run")
    parser.add_argument('--sp_config', type=str, required=True, help="Spatial configuration string")
    parser.add_argument('--sample_size', type=str, required=True, help="Sample size, determined by the parameterset CSV file")
    parser.add_argument('--num_processes', type=int, required=True, help="Number of processes to use")

    args = parser.parse_args()

    out = Output()
    display(out)

    #df = pd.read_csv(args.csv_path)#.iloc[250:499, :] #adjust if needed?
    sample_size = int(args.sample_size)

    caseName = os.path.join("D:\\", "wflow_models", args.wsName)
    wflow_cloneMap = 'wflow_subcatch.map' 
    
    set_start_method('spawn')
    num_processes = args.num_processes
    args_list = []
#    for i in range(len(df)):
    for i in range(0, sample_size): # adjust if needed?
        for model in args.model_ver:
            if model == "wflow_tofuflex":
                args_list.append((
                    wflow_cloneMap, 
                    caseName, 
                    "wflow_tofuflex", 
                    os.path.join(str(args.outpath), f"{i}"), 
                    os.path.join("temp", str(i), f"wflow_tofuflex{args.sp_config}_{i}.ini"),
                    os.path.join("temp", str(i), "intbl"),
                    args.datetimestart,
                    args.datetimeend
                ))
            elif model == "wflow_tofuflex_ns":
                args_list.append((
                    wflow_cloneMap, 
                    caseName, 
                    "wflow_tofuflex_ns", 
                    os.path.join(str(args.outpath), f"ns_{i}"), 
                    os.path.join("temp", str(i), f"wflow_tofuflex_ns{args.sp_config}_{i}.ini"),
                    os.path.join("temp", str(i), "intbl"),
                    args.datetimestart,
                    args.datetimeend
                ))
                #print(glob.glob(os.path.join(caseName, os.path.join("temp", str(i), "intbl") + "/*.tbl")))
            elif model == "wflow_topoflex_bm":
                args_list.append((
                    wflow_cloneMap, 
                    caseName, 
                    "wflow_topoflex_bm", 
                    os.path.join(str(args.outpath), f"bm_{i}"), 
                    os.path.join("temp", str(i), f"wflow_topoflex_bm{args.sp_config}_{i}.ini"),
                    os.path.join("temp", str(i), "intbl"),
                    args.datetimestart,
                    args.datetimeend
                ))

    results = []
    completed_count = 0
    total_tasks = len(args_list)

    with Pool(num_processes) as pool:
        result_objects = pool.imap_unordered(parallel_run_model, args_list)
        for result in result_objects:
            results.append(result)
            update_progress(result)

    total_runtime = time.time() - start_time
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

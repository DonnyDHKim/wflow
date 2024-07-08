import os
import csv
import pandas as pd
import shutil
import re
import pcraster as pcr
import numpy as np
import gc


def modify_and_save_ini(file_path, index, new_file_path):
    # Read the existing content from the file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    
    # Modify lines containing 'intbl'
    modified_lines = []
    for line in lines:
        if 'intbl' in line:
            modified_line = f'intbl = temp/{index}/intbl\n'
            modified_lines.append(modified_line)
        else:
            modified_lines.append(line)
    
    # Save the modified content to a new file
    with open(new_file_path, 'w', encoding='utf-8') as new_file:
        new_file.writelines(modified_lines)




def modify_and_save_ini2(file_path, index, new_numbers, new_file_path):
    # Read the existing content from the file
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    
    # Modify lines containing 'intbl'
    modified_lines = []
    for line in lines:
        if 'intbl' in line:
            modified_line = f'intbl = temp/{index}/intbl\n'
            modified_lines.append(modified_line)
        elif 'Tf = [' in line:
            # Modify the line with the new numbers for Tf
            tf_values = [int(new_numbers[0]), int(new_numbers[2]), int(new_numbers[3])]
            modified_line = f'Tf = [{", ".join(map(str, tf_values))}]\n'
            modified_lines.append(modified_line)
        elif 'Tfa = [' in line:
            # Modify the line with the new numbers for Tfa
            tfa_values = [0, 0, int(new_numbers[1])]
            modified_line = f'Tfa = [{", ".join(map(str, tfa_values))}]\n'
            modified_lines.append(modified_line)
        else:
            modified_lines.append(line)
    
    # Save the modified content to a new file
    with open(new_file_path, 'w', encoding='utf-8') as new_file:
        new_file.writelines(modified_lines)




def modify_and_save_intbl(file_path, new_number, new_file_path):
    # Read the existing text
    with open(file_path, 'r', encoding='utf-8') as file:
        existing_text = file.read()
    
    # Use regular expression to find and replace the last number in the given pattern
    pattern = r'(\[0,> \[0,> \[0,> )\d+\.?\d*'
    replacement = r'\g<1>' + str(new_number)  # Convert new_number to string if not already
    modified_text = re.sub(pattern, replacement, existing_text)
    
    # Save the modified text to the new file path
    with open(new_file_path, 'w', encoding='utf-8') as new_file:
        new_file.write(modified_text)




def reset_pcraster_environment(new_clone_path):
    # Set the clone map to the new raster's clone
    pcr.setclone(new_clone_path)
    # Force garbage collection
    gc.collect()


def process_Kf_files(directory, identifiers, static_directory):
    """
    Process files in the specified directory with the given identifiers.
    
    Parameters:
    - directory: The path to the directory containing the files.
    - identifiers: A list of identifiers for the files to be processed.
    """
    reset_pcraster_environment(static_directory + "/TIA.map")
    TIA =  pcr.readmap(static_directory + "/TIA.map")


    for i in identifiers:          
        percent = pcr.readmap(static_directory + "/wflow_percent" + i + ".map")
        catchment = pcr.readmap(static_directory + "/wflow_ws.map")

        TIA_stat = pcr.areaaverage(TIA * percent, catchment)
        TIA_stat2 = pcr.pcr2numpy(TIA_stat, mv=np.nan)
        TIA_stat3 = np.nanmean(TIA_stat2)
        
        #print(str("Average TIA for " + i + " :"+ str(TIA_stat3)))
        
        if i == "W":
            #print("For W, not performing Kf modification")
            continue
                        
        # Construct the original and new file names
        original_file_name = f"Kf{i}_OG.tbl"
        new_file_name = f"Kf{i}.tbl"
        
        # Path to the original and new files
        original_file_path = os.path.join(directory, original_file_name)
        new_file_path = os.path.join(directory, new_file_name)
        
        # Open the original file
        with open(original_file_path, 'r') as file:
            # Read the line
            line = file.readline().strip()
            # Extract the last number
            parts = line.split(' ')
            last_number = float(parts[-1])
            
            # Perform the calculation
            new_number = round(last_number * (1 + 0.02/2 * TIA_stat3 * 100), 4)
            #new_number = last_number
            
            # Prepare the new line with the calculated number
            new_line = line.rsplit(' ', 1)[0] + f' {new_number}'
        
        # Write the new line to the new file
        with open(new_file_path, 'w') as file:
            file.write(new_line)

    del TIA




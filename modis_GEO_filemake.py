import os
import subprocess
import multiprocessing

# Define the directory containing the bz2 files
directory = f'/mnt/j/isee_remote_data/Trichodesmium/MODIS_data_L1A'

def process_file(filename):
    if filename.endswith('.L1A_LAC.x.hdf.bz2'):
        # Construct the full path to the bz2 file
        bz2_file_path = os.path.join(directory, filename)
        # Construct the output hdf file path
        hdf_file_path = bz2_file_path[:-4]  # Remove the '.bz2' extension
        
        # Decompress the bz2 file to get the hdf file
        decompress_command = f"bzip2 -dk {bz2_file_path}"
        subprocess.run(decompress_command, shell=True)
        
        # Construct the command to run modis_GEO on the hdf file
        geo_command = f"modis_GEO {hdf_file_path}"
        print(geo_command)
        
        # Execute the command
        subprocess.run(geo_command, shell=True)

# Create a pool of workers
pool = multiprocessing.Pool()

# Iterate over all bz2 files in the directory and add them to the pool
pool.map(process_file, os.listdir(directory))

# Close the pool and wait for the work to finish
pool.close()
pool.join()

import os
import subprocess
import multiprocessing

# Define the directory containing the files
directory_base = '/mnt/j/isee_remote_data/Trichodesmium'
DataSource = 'TERRA'
Date_name = '202007'

l2prod = r'Rrs_748,Rrs_859,Rrs_869,Rrs_vvv,rhos_748,rhos_859,rhos_869,rhos_vvv'
atmocor = 1
proc_ocean = 1
resolution = 250
oformat = r'netCDF4'
format_suffix = r'nc'
maskland = 1
maskglint = 1
maskcloud = 1
maskstlight = 1
maskhilt = 1

if DataSource == 'AQUA':
    directory_GEO = f'{directory_base}/MODIS_data_L1A_{Date_name}/AQUA_MODIS_GEO_hdf'
    directory_L1B_LAC = f'{directory_base}/MODIS_data_L1B_{Date_name}/AQUA_MODIS_L1B'
    directory_L2 = f'{directory_base}/MODIS_data_L2_{Date_name}/AQUA_MODIS_L2'
    startswith_l1b = 'AQUA_MODIS.'
elif DataSource == 'TERRA':
    directory_GEO = f'{directory_base}/MODIS_data_L1A_{Date_name}/TERRA_MODIS_GEO_hdf'
    directory_L1B_LAC = f'{directory_base}/MODIS_data_L1B_{Date_name}/TERRA_MODIS_L1B'
    directory_L2 = f'{directory_base}/MODIS_data_L2_{Date_name}/TERRA_MODIS_L2'
    startswith_l1b = 'TERRA_MODIS.'
else:
    raise ValueError(f"Invalid DataSource: {DataSource}")
    quit()

if not os.path.exists(directory_L2):
    os.makedirs(directory_L2)

# Get all the .GEO.hdf files and .L1B_LAC.hdf files and sort them by name
geo_files = sorted([f for f in os.listdir(directory_GEO) if f.endswith('.GEO.hdf')])
l1b_files = sorted([f for f in os.listdir(directory_L1B_LAC) if f.endswith('.L1B.hdf') and f.startswith(startswith_l1b)])
l2_files = []

# Check that the number of files is the same
for geo, l1b in zip(geo_files, l1b_files):
    geo_number = geo.split('.')[1]
    l1b_number = l1b.split('.')[1]
    geo_head = geo.split('.')[0]
    print(geo, l1b)
    if geo_number != l1b_number:
        raise ValueError(f"Error: {geo} {l1b}")
        quit()
    else:
        l2_file = f'{directory_L2}/{geo_head}.{geo_number}.L2.{format_suffix}'
        l2_files.append(l2_file)

def process_files(geo_file_path, l1b_file_path, l2_file_path):
    command = f"l2gen ifile={l1b_file_path} geofile={geo_file_path} ofile1={l2_file_path} l2prod={l2prod} atmocor={atmocor} proc_ocean={proc_ocean} resolution={resolution} maskland={maskland} maskglint={maskglint} maskcloud={maskcloud} maskstlight={maskstlight} maskhilt={maskhilt} oformat={oformat}"
    #command = f"l2gen ifile={l1b_file_path} geofile={geo_file_path} ofile1={l2_file_path} l2prod={l2prod} resolution={resolution} oformat={oformat}"
    print(command)
    subprocess.run(command, shell=True)

# Function to handle multiprocessing
def worker(file_tuple):
    geo_file_path, l1b_file_path, l2_file_path = file_tuple
    process_files(geo_file_path, l1b_file_path, l2_file_path)

#worker((os.path.join(directory_GEO, geo_files[0]), os.path.join(directory_L1B_LAC, l1b_files[0]), l2_files[0]))

#quit()

file_tuples = [(os.path.join(directory_GEO, geo), os.path.join(directory_L1B_LAC, l1b), l2) for geo, l1b, l2 in zip(geo_files, l1b_files, l2_files)]

if __name__ == "__main__":
    with multiprocessing.Pool() as pool:
        pool.map(worker, file_tuples)

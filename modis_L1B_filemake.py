import os
import subprocess
import datetime

# Define the directory containing the files
directory_base = '/mnt/j/isee_remote_data/Trichodesmium/MODIS_data_L1A'
DataSource = 'AQUA'

if DataSource == 'AQUA':
    directory_x_hdf = f'{directory_base}/A_x_hdf'
    directory_geo_hdf = f'{directory_base}/AQUA_MODIS_GEO_hdf'
elif DataSource == 'TERRA':
    directory_x_hdf = f'{directory_base}/T_x_hdf'
    directory_geo_hdf = f'{directory_base}/TERRA_MODIS_GEO_hdf'
else:
    raise ValueError(f"Invalid DataSource: {DataSource}")
    quit()

# Get all the .x.hdf files and .GEO.hdf files and sort them by name
l1a_files = sorted([f for f in os.listdir(directory_x_hdf) if f.endswith('LAC')])
l1b_files = sorted([f for f in os.listdir(directory_geo_hdf) if f.endswith('.GEO.hdf')])

# ファイルの一致の確認
for l1a, l1b in zip(l1a_files, l1b_files):
    l1a_number = l1a.split('.')[0]
    l1a_number_year, l1a_number_DOY, l1a_number_time = l1a_number[1:5], l1a_number[5:8], l1a_number[8:]
    l1a_number_year = int(l1a_number_year)
    l1a_number_DOY = int(l1a_number_DOY)
    # l1a_number_DOI, l1a_number_yearから日付を計算
    l1a_date = datetime.date(l1a_number_year, 1, 1) + datetime.timedelta(days=l1a_number_DOY - 1)

    l1b_number = l1b.split('.')[1]
    l1b_number_year, l1b_number_month, l1b_number_day, l1b_number_time = l1b_number[:4], l1b_number[4:6], l1b_number[6:8], l1b_number[9:]
    l1b_number_year = int(l1b_number_year)
    l1b_number_month = int(l1b_number_month)
    l1b_number_day = int(l1b_number_day)
    l1b_date = datetime.date(l1b_number_year, l1b_number_month, l1b_number_day)

    print(l1a_date, l1b_date, l1a_number_time, l1b_number_time)
    if l1a_date != l1b_date or l1a_number_time != l1b_number_time:
        print(f"Error: {l1a} {l1b}")
        quit()


def process_files(l1a_file_path, lib_file_path):
    command = f"modis_L1B {l1a_file_path} {lib_file_path}"
    print(command)
    subprocess.run(command, shell=True)
    
process_files(os.path.join(directory_x_hdf, l1a_files[0]), os.path.join(directory_geo_hdf, l1b_files[0]))

quit()
for l1a, l1b in zip(l1a_files, l1b_files):
    l1a_file_path = os.path.join(directory_x_hdf, l1a)
    l1b_file_path = os.path.join(directory_geo_hdf, l1b)
    process_files(l1a_file_path, l1b_file_path)
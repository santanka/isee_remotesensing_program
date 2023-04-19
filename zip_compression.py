import zipfile
import os
import datetime

folder_path = f'/mnt/j/isee_remote_data/himawari_AshRGB_GeoTIFF/202007'
output_path = f'/mnt/j/isee_remote_data/himawari_AshRGB_GeoTIFF/202007.zip'

def zip_compress(source_path, target_path, compression_level):
    file_list = []
    for root, dirs, files in os.walk(source_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(file_path)

    total_size = sum(os.path.getsize(f) for f in file_list)

    with zipfile.ZipFile(target_path, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=compression_level) as zipf:
        for file in file_list:
            zipf.write(file, os.path.relpath(file, source_path))
            now = str(datetime.datetime.now())
            print(f'Compressing... {os.path.relpath(file, source_path)} ({round(os.path.getsize(file) / total_size * 100)}%)   {now}')

    print(f'Compress finished: {target_path}')

zip_compress(folder_path, output_path, 9)

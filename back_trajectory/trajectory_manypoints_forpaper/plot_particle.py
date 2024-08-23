import os
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import datetime
import numpy as np
import requests
import re
from multiprocessing import Pool
import pyproj
import time
import ftplib
import xarray as xr
import socket
from PIL import Image
import tarfile
import urllib.request
import bz2
import concurrent.futures
import pandas as pd
import matplotlib.cm as cm
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor


# directory
back_or_forward = 'forward'
input_condition = 2
trajectory_plot = True

threshold_initial_distance = True
if threshold_initial_distance == True:
    vmin_threshold_initial_distance = 60
    vmax_threshold_initial_distance = 150

def file_name_input(back_or_forward, input_condition):
    dir_1 = f'/mnt/j/isee_remote_data/JST/'
    if back_or_forward == 'back':
        dir_2 = f'back_trajectory_manypoints_forpaper/back_trajectory_condition_{input_condition}/'
    elif back_or_forward == 'forward':
        dir_2 = f'forward_trajectory_manypoints_forpaper/forward_trajectory_condition_{input_condition}/'
    file_input = f'initial_condition_{input_condition}'
    file_name = dir_1 + dir_2 + file_input
    return file_name

make_input_data_name = file_name_input(back_or_forward, input_condition)
file_name_time = make_input_data_name + '_time.csv'
file_name_point = make_input_data_name + '_point.csv'
print(file_name_time, file_name_point)

# read csv file
# time file
start_time = np.loadtxt(file_name_time, delimiter=',', max_rows=1)
end_time = np.loadtxt(file_name_time, delimiter=',', skiprows=1, max_rows=1)

start_year_JST, start_month_JST, start_day_JST, start_hour_JST = int(start_time[0]), int(start_time[1]), int(start_time[2]), int(start_time[3])
start_time_JST = datetime.datetime(start_year_JST, start_month_JST, start_day_JST, start_hour_JST)
end_year_JST, end_month_JST, end_day_JST, end_hour_JST = int(end_time[0]), int(end_time[1]), int(end_time[2]), int(end_time[3])
end_time_JST = datetime.datetime(end_year_JST, end_month_JST, end_day_JST, end_hour_JST)

print(start_time_JST, end_time_JST)

# datetimeリスト
now_time = start_time_JST
datetime_list = []
while (now_time >= end_time_JST and back_or_forward == 'back') or (now_time <= end_time_JST and back_or_forward == 'forward'):
    datetime_list.append(now_time)
    if back_or_forward == 'back':
        now_time = now_time - datetime.timedelta(hours=6)
    elif back_or_forward == 'forward':
        now_time = now_time + datetime.timedelta(hours=6)

#input_datetime = datetime.datetime(start_year_JST, start_month_JST, start_day_JST, 0) + datetime.timedelta(days=8)
#input_datetime = datetime.datetime(2020, 7, 4, 0) + datetime.timedelta(days=2)
#input_datetime_list = [
#   input_datetime,
#   input_datetime + datetime.timedelta(hours=6),
#   input_datetime + datetime.timedelta(hours=12),
#   #input_datetime + datetime.timedelta(hours=18),
#   #input_datetime + datetime.timedelta(days=1)
#    ]
#input_datetime_list = [
#    input_datetime,
#    input_datetime - datetime.timedelta(hours=6),
#    #input_datetime - datetime.timedelta(hours=12),
#    #input_datetime - datetime.timedelta(hours=18)
#    ]

input_datetime_list = [
#    datetime.datetime(2020, 6, 20, 12),
    datetime.datetime(2020, 6, 28, 18),
    datetime.datetime(2020, 7, 1, 12),
    datetime.datetime(2020, 7, 4, 12), 
    #datetime.datetime(2020, 6, 16, 6),
    ]



input_datetime_num = len(input_datetime_list)

for time_check in input_datetime_list:
    if time_check not in datetime_list:
        print(f'Error!: {time_check} is not in datetime_list')
        quit()

# read initial point file
def read_point_file():
    point = np.loadtxt(file_name_point, delimiter=',')
    latitude = point[:, 1]
    longitude = point[:, 0]
    return latitude, longitude
initial_latitude, initial_longitude = read_point_file()


# trajectory file
def read_trajectory_file_for_width(now_time):
    file_name_trajectory = os.path.dirname(file_name_time) + f'/trajectory_chla_{now_time.year}_{now_time.month}_{now_time.day}_{now_time.hour}.csv'
    trajectory = np.loadtxt(file_name_trajectory, delimiter=',')
    latitude = trajectory[:, 2]
    longitude = trajectory[:, 1]
    max_latitude = np.max(latitude)
    min_latitude = np.min(latitude)
    max_longitude = np.max(longitude)
    min_longitude = np.min(longitude)
    return max_latitude, min_latitude, max_longitude, min_longitude


max_latitude_list = np.array([])
min_latitude_list = np.array([])
max_longitude_list = np.array([])
min_longitude_list = np.array([])

for now_time in datetime_list:
    max_latitude, min_latitude, max_longitude, min_longitude = read_trajectory_file_for_width(now_time)
    max_latitude_list = np.append(max_latitude_list, max_latitude)
    min_latitude_list = np.append(min_latitude_list, min_latitude)
    max_longitude_list = np.append(max_longitude_list, max_longitude)
    min_longitude_list = np.append(min_longitude_list, min_longitude)

max_latitude = np.max(max_latitude_list)
min_latitude = np.min(min_latitude_list)
max_longitude = np.max(max_longitude_list)
min_longitude = np.min(min_longitude_list)

print(max_latitude, min_latitude, max_longitude, min_longitude)


# Nishinoshima location
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

# Mukojima location
mukojima_lon = 142.14
mukojima_lat = 27.68

# shapefile
os.environ['SHAPE_RESTORE_SHX'] = 'YES'
shapefile_path = f'C23-06_13-g_Coastline.shp'
shape_data = gpd.read_file(shapefile_path)

# plot width
width_merge = 0.3
def adjust_plot_boundaries(max_lat, min_lat, max_lon, min_lon, nishinoshima_lat, nishinoshima_lon):
    plot_latitude_max = max_lat + width_merge if max_lat > nishinoshima_lat else nishinoshima_lat + width_merge
    plot_latitude_min = min_lat - width_merge if min_lat < nishinoshima_lat else nishinoshima_lat - width_merge
    plot_longitude_max = max_lon + width_merge if max_lon > nishinoshima_lon else nishinoshima_lon + width_merge
    plot_longitude_min = min_lon - width_merge if min_lon < nishinoshima_lon else nishinoshima_lon - width_merge

    return plot_latitude_max, plot_latitude_min, plot_longitude_max, plot_longitude_min

plot_latitude_max, plot_latitude_min, plot_longitude_max, plot_longitude_min = adjust_plot_boundaries(max_latitude, min_latitude, max_longitude, min_longitude, nishinoshima_lat, nishinoshima_lon)

#plot_latitude_max, plot_latitude_min, plot_longitude_max, plot_longitude_min = 28.2, 26.4, 142.5, 139.8

print(plot_latitude_max, plot_latitude_min, plot_longitude_max, plot_longitude_min)
latitude_width = plot_latitude_max - plot_latitude_min
longitude_width = plot_longitude_max - plot_longitude_min
ratio_lat_lon = latitude_width / longitude_width

# initial_latitude, initial_longitudeから、各点のnishinoshimaまでの距離(km)を計算
def calculate_distance(latitude, longitude):
    geod = pyproj.Geod(ellps='WGS84')
    azimuth1, azimuth2, distance = geod.inv(longitude, latitude, nishinoshima_lon, nishinoshima_lat)
    return distance / 1000

distance_list = np.array([])
for i in range(len(initial_latitude)):
    distance = calculate_distance(initial_latitude[i], initial_longitude[i])
    distance_list = np.append(distance_list, distance)

max_distance = np.max(distance_list)
min_distance = np.min(distance_list)

print(max_distance, min_distance)

# output
#download data
dir_data = f''
dir_figure = os.path.dirname(file_name_time) + f'/figure/'
if not os.path.exists(dir_figure):
    os.makedirs(dir_figure)
figure_name = dir_figure + f'figure_trajectory_{back_or_forward}_condition_{input_condition}_'
#ディレクトリ内のpngのファイルの数を数える
figure_number = len([name for name in os.listdir(dir_figure) if os.path.isfile(os.path.join(dir_figure, name)) and name.endswith('.png')])
figure_name = figure_name + f'{figure_number}.png'



#並列処理の数をPCの最大コア数に設定
parallel_number = os.cpu_count()
print(f'Number of parallel processes: {parallel_number}')

#日時の指定、string化
def time_and_date(year, month, day, hour):
    yyyy    = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month
    dd      = str(day).zfill(2)     #day
    hh      = str(hour).zfill(2)    #hour (UTC)
    mn      = '00'                  #minutes (UTC)
    return yyyy, mm, dd, hh, mn

#JSTの日時をUTCに変換する関数
def JST_to_UTC(year, month, day, hour):
    JST = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
    JST_time = datetime.datetime(year, month, day, hour, 0, 0, tzinfo=JST)
    UTC_time = JST_time.astimezone(datetime.timezone.utc)
    year_UTC = UTC_time.year
    month_UTC = UTC_time.month
    day_UTC = UTC_time.day
    hour_UTC = UTC_time.hour
    return year_UTC, month_UTC, day_UTC, hour_UTC

#ファイルの確認
def check_file_exists(filename):
    if os.path.isfile(filename):
        return True
    else:
        return False

######chla関連#####
#JAXAひまわりモニタからchlaのデータをダウンロード
#https://www.eorc.jaxa.jp/ptree/userguide_j.html
ftp_site        = 'ftp.ptree.jaxa.jp'                   # FTPサイトのURL
ftp_user        = 'koseki.saito_stpp.gp.tohoku.ac.jp'   # FTP接続に使用するユーザー名
ftp_password    = 'SP+wari8'                            # FTP接続に使用するパスワード
#1km日本域のデータを使用(24N-50N, 123E-150Eの矩形領域)
pixel_number_chla    = 2701
line_number_chla     = 2601
data_lon_min, data_lon_max, data_lat_min, data_lat_max  = 123E0, 150E0, 24E0, 50E0
#Chlorophyll-a濃度のプロット範囲
chla_vmin = 1E-1
chla_vmax = 1E0

#ダウンロードするファイルのパスの生成
def download_path(year, month, day, hour):
    yyyy, mm, dd, hh, mn = time_and_date(year=year, month=month, day=day, hour=hour)
    if(year < 2022):
        ver = '010'
        nn = '08'
    elif(year == 2022):
        if(month <= 9):
            ver = '010'
            nn = '08'
        elif(month >= 10):
            ver = '021'
            nn = '09'
    elif(year >= 2023):
        ver = '021'
        nn = '09'
    
    pixel = str(pixel_number_chla).zfill(5)
    line = str(line_number_chla).zfill(5)
    ftp_path = f'/pub/himawari/L3/CHL/{ver}/{yyyy}{mm}/{dd}/H{nn}_{yyyy}{mm}{dd}_{hh}{mn}_1H_rOC{ver}_FLDK.{pixel}_{line}.nc'
    return ftp_path

#ファイルのダウンロード、データを返す
def download_netcdf(year, month, day, hour):
    ftp_path = download_path(year=year, month=month, day=day, hour=hour)
    ftp_base = os.path.basename(ftp_path)
    local_path = os.path.join(dir_data, ftp_base)

    # ファイルが存在しない場合にダウンロードを試みる
    if not check_file_exists(local_path):
        while True:  # 無限に再試行
            try:
                # タイムアウトを設定（接続と読み取りのタイムアウト）
                with ftplib.FTP(ftp_site, timeout=30) as ftp:
                    ftp.login(user=ftp_user, passwd=ftp_password)  # ログイン
                    ftp.cwd('/')  # ルートディレクトリに移動

                    # ファイルをバイナリモードでダウンロード
                    with open(local_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {ftp_path}', f.write)
                        print(r'Download file is ' + local_path)
                        break  # ダウンロード成功したらループを抜ける
            #データがない場合は0のデータを返す
            #それ以外のエラーは10秒間待機して再試行
            except tuple(ftplib.all_errors) + (socket.timeout,) as e:
                print(f"Error downloading file: {e}")
                error_message = str(e)
                #dデータがない場合は0のデータを返す
                if '550' in error_message:
                    print("No data available.")
                    return np.zeros((line_number_chla, pixel_number_chla))
                else:
                    print("Wait for 10 seconds before retrying...")
                    time.sleep(10)  # 10秒間待機して再試行
    
    try:
        data = xr.open_dataset(local_path, engine='h5netcdf')
    except OSError as e:
        print(f"Error opening file: {e}")
        print(f"File path: {local_path}")
        data = None
    
    if data is None:
        return np.zeros((line_number_chla, pixel_number_chla))
    #os.remove(local_path)
    return data

def calculate_7hours_average(year, month, day, hour):
    data_sum = None
    count_zeros = None
    time_input = datetime.datetime(year, month, day, hour, 0, 0)
    time_base = time_input + datetime.timedelta(seconds=-3*60*60)

    for count_i in range(7):
        time_now = time_base + datetime.timedelta(seconds=60*60*count_i)
        data = download_netcdf(year=time_now.year, month=time_now.month, day=time_now.day, hour=time_now.hour)
        if data.all() != 0:
            try:
                data_filled = data['chlor_a'].fillna(0)  # 欠損値に0を代入
            except KeyError as e:
                print(f"Error: {e}")
                print(f"Dataset keys: {data.keys()}")
                data_filled = np.zeros((line_number_chla, pixel_number_chla))
            
            if data_sum is None:
                data_sum = data_filled
                count_zeros = (data_filled == 0).astype(int)  # 0の回数をカウントするためのマスクを作成
                #print(hour_int, r'count_zeros is initialized.', r'data exists.')
            else:
                data_sum += data_filled
                count_zeros += (data_filled == 0).astype(int)
                #print(hour_int, r'count_zeros is added.', r'data exists.')
        else:
            if data_sum is None:
                data_sum = np.zeros((line_number_chla, pixel_number_chla))
                count_zeros = np.ones((line_number_chla, pixel_number_chla)).astype(int)
                #print(hour_int, r'count_zeros is initialized.', r'data does not exist.')
            else:
                count_zeros += np.ones((line_number_chla, pixel_number_chla)).astype(int)
                #print(hour_int, r'count_zeros is added.', r'data does not exist.')
        
    divisor = 7 * np.ones((line_number_chla, pixel_number_chla)).astype(int) - count_zeros
    data_average = xr.where(divisor != 0, data_sum / divisor, 0)

    return data_average


#chlaのメディアンフィルタ&vmin, vmaxの設定
def median_filter_chla(data, filter_size):
    data = data.astype(np.float64)
    #data = data.where(data != 0, np.nan)
    data = data.rolling(longitude=filter_size, latitude=filter_size, center=True).median()

    #data = xr.where((data < chla_vmin) & (data != 0), chla_vmin, data)
    #data = xr.where(data > chla_vmax, chla_vmax, data)
    data = data.astype(float)
    data = data.where(data != 0, np.nan)
    return data


#####AshRGB関連#####
#千葉大学環境リモートセンシング研究センターから放射輝度のデータをダウンロード
# http://www.cr.chiba-u.jp/databases/GEO/H8_9/FD/index_jp_V20190123.html
#バンド帯の指定
band_11 = '09'
band_13 = '01'
band_14 = '02'
band_15 = '03'
#データのサイズ
pixel_number_ash = 6000
line_number_ash = 6000
#座標系の定義
lon_min_ash, lon_max_ash, lat_min_ash, lat_max_ash = 85, 205, -60, 60

#放射輝度への変換テーブルのダウンロード
cfname = "count2tbb_v103.tgz"
local_path_cfname = os.path.join(dir_data, cfname)
if (check_file_exists(cfname) == False):
    url_cfname = f"ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/support/{cfname}"
    while True:
        try:
            urllib.request.urlretrieve(url_cfname, local_path_cfname)
            break
        except urllib.error.URLError as e:
            print(f"Connection failed: {e}")
            now = str(datetime.datetime.now())
            print(f'{now}     Retrying in 60 seconds...')
            time.sleep(60)
    # tarファイルを解凍する
    with tarfile.open(local_path_cfname, 'r:gz') as tar:
        tar.extractall()

#放射輝度のデータのファイル名
def bz2_filename(yyyy, mm, dd, hh, mn, band):
    return f'{yyyy}{mm}{dd}{hh}{mn}.tir.{band}.fld.geoss.bz2'

#ファイルのURL
def bz2_url(yyyy, mm, fname):
    return f'ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/{yyyy}{mm}/TIR/{fname}'

#ファイルのダウンロード
def bz2_download(url, fname, band):
    local_path_fname = os.path.join(dir_data, fname)
    if not check_file_exists(fname):
        filename = os.path.basename(url)
        while True:
            # タイムアウトを60秒に設定
            try:
                with urllib.request.urlopen(url, timeout=60) as response:
                    content = response.read()
                    # ファイルに内容を書き込む
                    with open(local_path_fname, 'wb') as f:
                        print(f'Downloading {filename}...')
                        f.write(content)
                break
            except urllib.error.URLError as e:
                print(f"Connection failed: {e}")
                #if isinstance(e.reason, str):
                # FTPのエラーであり、550エラーの場合は再試行しない
                if '550' in str(e.reason):
                    time.sleep(5)
                    print('File not found. Retry aborted.')
                    return None
                else:
                    now = str(datetime.datetime.now())
                    print(f'{now}     Retrying in 10 seconds...')
                    time.sleep(10)

    _, tbb = np.loadtxt(f"count2tbb_v103/tir.{band}", unpack=True)
    try:
        with bz2.BZ2File(fname) as bz2file:
            # pixel_number_ashとline_number_ashは適切な値を設定する必要があります。
            dataDN = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number_ash, line_number_ash)
            data = np.float32(tbb[dataDN])
    except OSError as e:
        print(f"Error opening file: {e}")
        print(f"File path: {fname}")
        data = None
    #os.remove(local_path_fname)
    return data

#AshRGBのデータ作成(inverse=1で反転、0で反転しない)
def AshRGB_data(data, min_K, max_K, gamma, inverse):
    if(inverse == 0):
        #print(r'not inverse')
        data_RGB = (max_K - data) / (max_K - min_K)
    elif(inverse == 1):
        #print(r'inverse')
        data_RGB = (data - min_K) / (max_K - min_K)
    else:
        print(r'error!: inverse value')
        quit()
    #print(r'check 1')
    data_RGB[data_RGB < 0] = 0E0
    #print(r'check 2')
    data_RGB[data_RGB > 1] = 1E0
    #print(r'check 3')
    data_RGB = data_RGB**gamma
    #print(r'check 4')
    return data_RGB

def median_filter_ash(data, filter_size):
    data_df = pd.DataFrame(data)
    data_filtered = data_df.rolling(window=filter_size, min_periods=1, center=True).median()
    data_filtered = data_filtered.to_numpy()
    return data_filtered

#AshRGBのデータのダウンロード
def download_AshRGB(time_base, plus_hour):
    time_now = time_base + datetime.timedelta(seconds=60*60*plus_hour)
    yyyy_now, mm_now, dd_now, hh_now, mn_now = time_and_date(year=time_now.year, month=time_now.month, day=time_now.day, hour=time_now.hour)

    fname_11 = bz2_filename(yyyy=yyyy_now, mm=mm_now, dd=dd_now, hh=hh_now, mn=mn_now, band=band_11)
    fname_13 = bz2_filename(yyyy=yyyy_now, mm=mm_now, dd=dd_now, hh=hh_now, mn=mn_now, band=band_13)
    fname_14 = bz2_filename(yyyy=yyyy_now, mm=mm_now, dd=dd_now, hh=hh_now, mn=mn_now, band=band_14)
    fname_15 = bz2_filename(yyyy=yyyy_now, mm=mm_now, dd=dd_now, hh=hh_now, mn=mn_now, band=band_15)

    url_11 = bz2_url(yyyy=yyyy_now, mm=mm_now, fname=fname_11)
    url_13 = bz2_url(yyyy=yyyy_now, mm=mm_now, fname=fname_13)
    url_14 = bz2_url(yyyy=yyyy_now, mm=mm_now, fname=fname_14)
    url_15 = bz2_url(yyyy=yyyy_now, mm=mm_now, fname=fname_15)

    urls = [url_11, url_13, url_14, url_15]
    fnames = [fname_11, fname_13, fname_14, fname_15]
    bands = [band_11, band_13, band_14, band_15]

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(bz2_download, urls, fnames, bands))
    
    data_tbb_11, data_tbb_13, data_tbb_14, data_tbb_15 = results

    #data_tbb_11 = bz2_download(url=url_11, fname=fname_11, band=band_11)
    #data_tbb_13 = bz2_download(url=url_13, fname=fname_13, band=band_13)
    #data_tbb_14 = bz2_download(url=url_14, fname=fname_14, band=band_14)
    #data_tbb_15 = bz2_download(url=url_15, fname=fname_15, band=band_15)
    if data_tbb_11 is None or data_tbb_13 is None or data_tbb_14 is None or data_tbb_15 is None:
        error_count = 1
        return None, None, None, error_count
    
    data_diff_tbb_11_14 = np.float32(data_tbb_11 - data_tbb_14)
    data_diff_tbb_13_15 = np.float32(data_tbb_13 - data_tbb_15)

    #Himawari Ash RGBクイックガイド参照
    #https://www.data.jma.go.jp/mscweb/ja/prod/pdf/RGB_QG_Ash_jp.pdf
    data_red = AshRGB_data(data_diff_tbb_13_15, -3.0E0, 7.5E0, 1.0E0, 0)
    data_green = AshRGB_data(data_diff_tbb_11_14, -5.9E0, 5.1E0, 8.5E-1, 0)
    data_blue = AshRGB_data(data_tbb_13, 243.6E0, 303.2E0, 1.0E0, 1)

    error_count = 0

    return data_red, data_green, data_blue, error_count


#AshRGBのデータの7時間平均
def calculate_7hours_average_ash(year, month, day, hour):
    data_red_sum = np.zeros((line_number_ash, pixel_number_ash))
    data_green_sum = np.zeros((line_number_ash, pixel_number_ash))
    data_blue_sum = np.zeros((line_number_ash, pixel_number_ash))
    error_count = 0

#    time_input = datetime.datetime(year, month, day, hour, 0, 0)
#    time_base = time_input + datetime.timedelta(seconds=-3*60*60)
#
#    #各時間のデータを並列でダウンロード
#    with concurrent.futures.ProcessPoolExecutor() as executor:
#        results = executor.map(download_AshRGB, [time_base]*7, range(7))
#    
#    for result in results:
#        if result[3] == 1 and result[0] is None and result[1] is None and result[2] is None:
#            error_count += 1
#            continue
#        else:
#            data_red_sum += result[0]
#            data_green_sum += result[1]
#            data_blue_sum += result[2]
#    
#    data_red_average = data_red_sum / (7 - error_count)
#    data_green_average = data_green_sum / (7 - error_count)
#    data_blue_average = data_blue_sum / (7 - error_count)

    #平均なし
    time_input = datetime.datetime(year, month, day, hour, 0, 0)
    time_base = time_input
    data_red_average, data_green_average, data_blue_average, error_count = download_AshRGB(time_base, 0)

    #メディアンフィルタ
    data_red_average_median = median_filter_ash(data=data_red_average, filter_size=4)
    data_green_average_median = median_filter_ash(data=data_green_average, filter_size=4)
    data_blue_average_median = median_filter_ash(data=data_blue_average, filter_size=4)

    data_RGB = np.dstack((data_red_average_median, data_green_average_median, data_blue_average_median))
    return data_RGB


#backwardでは距離のcolorbarを与えない

# plot setting
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
font_size = 20
plt.rcParams["font.size"] = font_size

if back_or_forward == 'forward' and trajectory_plot == True:
    fig = plt.figure(figsize=(10, (input_datetime_num+0.2)*5*ratio_lat_lon))
    markersize = 15

    #using gridspec
    gs = fig.add_gridspec(input_datetime_num+2, 2, height_ratios=[ratio_lat_lon]*input_datetime_num+[ratio_lat_lon/10]+[ratio_lat_lon/10], width_ratios=[1, 1])
    # last row is colorbar
    ax_cbar = fig.add_subplot(gs[-2, :])
    ax_cbar_2 = fig.add_subplot(gs[-1, :])
    axes = []
    for i in range(input_datetime_num):
        axes.append(fig.add_subplot(gs[i, 0]))
        axes.append(fig.add_subplot(gs[i, 1]))

    # colorbar
    cbar_vmin = 1E-1
    cbar_vmax = 1E0
    cmap_color = cm.cool
    norm = LogNorm(vmin=cbar_vmin, vmax=cbar_vmax)
    sm = plt.cm.ScalarMappable(cmap=cmap_color, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, cax=ax_cbar, orientation='horizontal')
    cbar.set_label(r'Chlorophyll-a concentration [$\mathrm{mg / m^{3}}$]', fontsize=font_size*1.2)

    # colorbar for distance
    if threshold_initial_distance == True:
        cbar_vmin_distance = np.nanmax([min_distance, vmin_threshold_initial_distance])
        cbar_vmax_distance = np.nanmin([max_distance, vmax_threshold_initial_distance])
    else:
        cbar_vmin_distance = min_distance
        cbar_vmax_distance = max_distance
    cmap_color_distance = cm.turbo
    # cm.springを反転
    #cmap_color_distance = cmap_color_distance.reversed()
    norm_distance = mpl.colors.Normalize(vmin=cbar_vmin_distance, vmax=cbar_vmax_distance)
    sm_distance = plt.cm.ScalarMappable(cmap=cmap_color_distance, norm=norm_distance)
    sm_distance.set_array([])
    cbar_distance = plt.colorbar(sm_distance, cax=ax_cbar_2, orientation='horizontal')
    cbar_distance.set_label(r'Initial Distance from Nishinoshima [km]', fontsize=font_size*1.2)

elif back_or_forward == 'back' or trajectory_plot == False:
    fig = plt.figure(figsize=(10, (input_datetime_num+0.1)*5*ratio_lat_lon))
    markersize = 15

    #using gridspec
    gs = fig.add_gridspec(input_datetime_num+1, 2, height_ratios=[ratio_lat_lon]*input_datetime_num+[ratio_lat_lon/10], width_ratios=[1, 1])
    # last row is colorbar
    ax_cbar = fig.add_subplot(gs[-1, :])
    axes = []
    for i in range(input_datetime_num):
        axes.append(fig.add_subplot(gs[i, 0]))
        axes.append(fig.add_subplot(gs[i, 1]))

    # colorbar
    cbar_vmin = 1E-1
    cbar_vmax = 1E0
    cmap_color = cm.cool
    norm = LogNorm(vmin=cbar_vmin, vmax=cbar_vmax)
    sm = plt.cm.ScalarMappable(cmap=cmap_color, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, cax=ax_cbar, orientation='horizontal')
    cbar.set_label(r'Chlorophyll-a concentration [$\mathrm{mg / m^{3}}$]', fontsize=font_size*1.2)


def plot_map(ax, now_time, count_i, chl_or_ash):
    # load trajectory file
    file_name_trajectory = os.path.dirname(file_name_time) + f'/trajectory_chla_{now_time.year}_{now_time.month}_{now_time.day}_{now_time.hour}.csv'
    trajectory = np.loadtxt(file_name_trajectory, delimiter=',')
    latitude = trajectory[:, 2]
    longitude = trajectory[:, 1]

    # plot setting
    ax.set_xlim(plot_longitude_min, plot_longitude_max)
    ax.set_ylim(plot_latitude_min, plot_latitude_max)
    ax.minorticks_on()
    ax.grid(which='both', alpha=0.3)
    ax.set_aspect('equal')

    if chl_or_ash == 'chl':
        # UTC time
        now_time_JST = now_time
        now_time_UTC_year, now_time_UTC_month, now_time_UTC_day, now_time_UTC_hour = JST_to_UTC(year=now_time_JST.year, month=now_time_JST.month, day=now_time_JST.day, hour=now_time_JST.hour)
        now_time_UTC = datetime.datetime(now_time_UTC_year, now_time_UTC_month, now_time_UTC_day, now_time_UTC_hour)

        # download data
        chla_data = calculate_7hours_average(year=now_time_UTC.year, month=now_time_UTC.month, day=now_time_UTC.day, hour=now_time_UTC.hour)
        chla_data_filtered = median_filter_chla(data=chla_data, filter_size=4)

        # chla data is different from plot range, so cut the data
        chla_data_filtered_cut = chla_data_filtered.sel(longitude=slice(plot_longitude_min, plot_longitude_max), latitude=slice(plot_latitude_max, plot_latitude_min))
        chla_data_filtered_cut = chla_data_filtered_cut.transpose('latitude', 'longitude')
        chla_data_filtered_cut = chla_data_filtered_cut.fillna(0)
        chla_data_filtered_cut = chla_data_filtered_cut.where(chla_data_filtered_cut != 0, np.nan)
        chla_data_filtered_cut = chla_data_filtered_cut.values

        # plot chla
        im = ax.imshow(chla_data_filtered_cut, cmap=cmap_color, norm=norm, extent=[plot_longitude_min, plot_longitude_max, plot_latitude_min, plot_latitude_max])
    
    elif chl_or_ash == 'ash':
        # UTC time
        now_time_JST = now_time
        now_time_UTC_year, now_time_UTC_month, now_time_UTC_day, now_time_UTC_hour = JST_to_UTC(year=now_time_JST.year, month=now_time_JST.month, day=now_time_JST.day, hour=now_time_JST.hour)
        now_time_UTC = datetime.datetime(now_time_UTC_year, now_time_UTC_month, now_time_UTC_day, now_time_UTC_hour)

        # download data
        data_ash = calculate_7hours_average_ash(year=now_time_UTC.year, month=now_time_UTC.month, day=now_time_UTC.day, hour=now_time_UTC.hour)
        if data_ash.size == 0:
            raise ValueError('data_ash is empty')

        # ash data is different from plot range, so cut the data
        # ash data is 6000x6000 pixels and longitude is 85-205, latitude is -60-60
        # plot range is plot_longitude_min to plot_longitude_max, plot_latitude_min to plot_latitude_max
        # so, calculate the index of plot range
        index_lon_min = int(np.ceil((plot_longitude_min - lon_min_ash) / (lon_max_ash - lon_min_ash) * pixel_number_ash))
        index_lon_max = int(np.floor((plot_longitude_max - lon_min_ash) / (lon_max_ash - lon_min_ash) * pixel_number_ash))
        index_lat_min = int(np.ceil((lat_max_ash - plot_latitude_max) / (lat_max_ash - lat_min_ash) * line_number_ash))
        index_lat_max = int(np.floor((lat_max_ash - plot_latitude_min) / (lat_max_ash - lat_min_ash) * line_number_ash))

        if index_lon_min < 0 or index_lon_max > pixel_number_ash or index_lat_min < 0 or index_lat_max > line_number_ash:
            raise ValueError('index is out of range')

        data_ash_cut = data_ash[index_lat_min:index_lat_max, index_lon_min:index_lon_max, :]

        if data_ash_cut.size == 0:
            raise ValueError('data_ash_cut is empty')


        # plot ash
        im = ax.imshow(data_ash_cut, extent=[plot_longitude_min, plot_longitude_max, plot_latitude_min, plot_latitude_max])

    # Nishinoshima location
    ax.scatter(nishinoshima_lon, nishinoshima_lat, color='red', edgecolors='yellow', s=markersize*6, marker='^', label='Nishinoshima', linewidths=2)

    # Mukojima location
    ax.scatter(mukojima_lon, mukojima_lat, color='blue', edgecolors='yellow', s=markersize*6, marker='^', label='Mukojima', linewidths=2)

    # plot initial point
#    ax.scatter(initial_longitude, initial_latitude, color='white', edgecolors='k', s=markersize, marker='o', label='Initial point', alpha=0.5)

    # plot trajectory # colorをcbar_distanceに合わせる
    #ax.scatter(longitude, latitude, color='yellow', edgecolors='k', s=markersize, label='Trajectory', zorder=10)
    if back_or_forward == 'forward' and trajectory_plot == True and threshold_initial_distance == False:
        ax.scatter(longitude, latitude, c=distance_list, cmap=cmap_color_distance, edgecolors='k', s=markersize, label='Trajectory', zorder=10)
    elif back_or_forward == 'forward' and trajectory_plot == True and threshold_initial_distance == True:
        for count_i in range(len(distance_list)):
            if distance_list[count_i] >= vmin_threshold_initial_distance and distance_list[count_i] <= vmax_threshold_initial_distance:
                ax.scatter(longitude[count_i], latitude[count_i], c=cmap_color_distance(norm_distance(distance_list[count_i])), edgecolors='k', s=markersize, label='Trajectory', zorder=10)
    elif back_or_forward == 'back' and trajectory_plot == True:
        ax.scatter(longitude, latitude, color='yellow', edgecolors='k', s=markersize, label='Trajectory', zorder=10)


    # shapefile
    shape_data.plot(ax=ax, edgecolor='lime', facecolor='orangered', linewidth=0.3)

    # time
#    ax.text(0.05, 0.05, now_time.strftime('%Y-%m-%d %H:%M JST'), transform=ax.transAxes, fontsize=font_size, verticalalignment='bottom', horizontalalignment='left', bbox=dict(facecolor='white', alpha=0.8))
    ax.text(0.05, 0.90, now_time.strftime('%Y-%m-%d %H:%M JST'), transform=ax.transAxes, fontsize=font_size, verticalalignment='bottom', horizontalalignment='left', bbox=dict(facecolor='white', alpha=0.8))

    # legend
    #if count_i == 0 and chl_or_ash == 'chl':
    #    #legend = ax.legend(loc='lower left', fontsize=font_size*0.8, bbox_to_anchor=(0.00, 0.00))
    #    legend = ax.legend(loc='upper left', fontsize=font_size*0.8, bbox_to_anchor=(0.02, 0.9))
    #    legend.set_zorder(12)
    #    #ax.legend(loc='lower right', fontsize=font_size*0.8, bbox_to_anchor=(0.98, 0.02))

    return ax

for count_i, now_time in enumerate(input_datetime_list):
    ax_1 = plot_map(axes[count_i*2], now_time, count_i, 'chl')
    ax_2 = plot_map(axes[count_i*2+1], now_time, count_i, 'ash')

axes[0].set_title(r'Chlorophyll-a', fontsize=font_size*1.5)
axes[1].set_title(r'AshRGB', fontsize=font_size*1.5)

# 各axに(a)...のラベルを左上につける、chlaの列、ashの列の順に
for i in range(input_datetime_num):
    axes[i*2].text(-0.10, 1.0, f'({chr(97+i)})', transform=axes[i*2].transAxes)
    axes[i*2+1].text(-0.10, 1.0, f'({chr(97+input_datetime_num+i)})', transform=axes[i*2+1].transAxes)


latitude_label_ax_number = input_datetime_num - 1
if latitude_label_ax_number % 2 == 1:
    latitude_label_ax_number = latitude_label_ax_number - 1
axes[latitude_label_ax_number].set_ylabel(r'Latitude [deg]', fontsize=font_size*1.2)
ax_cbar.set_title(r'Longitude [deg]', fontsize=font_size*1.2)

plt.tight_layout(h_pad=0., w_pad=1)

plt.savefig(figure_name)
plt.savefig(figure_name.replace('.png', '.pdf'))
print(f'Save {figure_name}')
plt.close()
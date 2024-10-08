import os
import datetime
import numpy as np
import requests
import re
from multiprocessing import Pool
import pyproj
import time
import ftplib
import socket
import xarray as xr

# directory
back_or_forward = 'forward'
input_condition = 2

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

# download directory
dir_data = f''

# read csv file
# time file
start_time = np.loadtxt(file_name_time, delimiter=',', max_rows=1)
end_time = np.loadtxt(file_name_time, delimiter=',', skiprows=1, max_rows=1)

start_year_JST, start_month_JST, start_day_JST, start_hour_JST = int(start_time[0]), int(start_time[1]), int(start_time[2]), int(start_time[3])
start_time_JST = datetime.datetime(start_year_JST, start_month_JST, start_day_JST, start_hour_JST)
end_year_JST, end_month_JST, end_day_JST, end_hour_JST = int(end_time[0]), int(end_time[1]), int(end_time[2]), int(end_time[3])
end_time_JST = datetime.datetime(end_year_JST, end_month_JST, end_day_JST, end_hour_JST)

print(start_time_JST, end_time_JST)

# point file
initial_point = np.loadtxt(file_name_point, delimiter=',')
initial_point_lon, initial_point_lat = initial_point[:, 0], initial_point[:, 1]
initial_point_index = np.asarray(range(len(initial_point_lon)))

LAT_data = initial_point_lat.flatten()
LON_data = initial_point_lon.flatten()
INDEX_data = initial_point_index.flatten()

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
    

#####oceanic current関連#####
#緯度の範囲(X < Y)
def get_grid_range_latitude(X, Y):
    grid_size = 4251
    lat_range = 170
    lat_per_grid = lat_range / (grid_size - 1)

    x_grid = int((X + 80) / lat_per_grid)
    y_grid = int((Y + 80) / lat_per_grid) + 1

    min_grid = x_grid
    max_grid = y_grid

    return min_grid, max_grid

#経度の範囲(X < Y, 東経0-360)
def get_grid_range_longitude(X, Y):
    grid_size = 4501
    lon_range = 360
    lon_per_grid = lon_range / (grid_size - 1)

    x_grid = int(X / lon_per_grid)
    y_grid = int(Y / lon_per_grid) + 1

    min_grid = x_grid
    max_grid = y_grid

    return min_grid, max_grid

#時間の変換
def get_time_from_OPeNDAP():
    url_time = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z.ascii?time[0:1:12952]"
    while True:
        try:
            response_time = requests.get(url_time, timeout=10)
            response_time.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx
            break  # リクエストが成功したらループを抜ける
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)

        # エラーが発生した場合は10秒間待機して再試行
        print("Wait for 10 seconds...")
        time.sleep(10)
    
    ascii_time = response_time.text.split("\n")
    time_line = None
    for i, line in enumerate(ascii_time):
        if line.startswith("time"):
            time_line = ascii_time[i+1]
    time_values = [float(x) for x in time_line.split(',')]
    return time_values

time_values = get_time_from_OPeNDAP()
print(r"Time values are loaded.")

def get_time(time):
    time_origin = datetime.datetime(2000, 1, 1, 0, 0, 0)
    elapsed_time = time - time_origin
    hours_elapsed_time = elapsed_time.total_seconds() / 3600
    return hours_elapsed_time

def get_time_grid(time):
    hours_elapsed_time = get_time(time)
    dategrid_array = np.array(time_values)
    for count_i, element in enumerate(dategrid_array):
        if element == hours_elapsed_time:
            return count_i
    print(r"Error: time is out of range.")
    return None

#データのダウンロード
def make_url(time, lat_min, lat_max, lon_min, lon_max):
    min_lat_grid, max_lat_grid = get_grid_range_latitude(lat_min, lat_max)
    min_lon_grid, max_lon_grid = get_grid_range_longitude(lon_min, lon_max)
    time_grid = get_time_grid(time)
    if time_grid == None:
        return None
    url = f"https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z.ascii?lat[{min_lat_grid}:1:{max_lat_grid}],lon[{min_lon_grid}:1:{max_lon_grid}],water_u[{time_grid}:1:{time_grid}][0:1:0][{min_lat_grid}:1:{max_lat_grid}][{min_lon_grid}:1:{max_lon_grid}],water_v[{time_grid}:1:{time_grid}][0:1:0][{min_lat_grid}:1:{max_lat_grid}][{min_lon_grid}:1:{max_lon_grid}]"
    #print(url)
    return url

def get_data(year_int, month_int, day_int, hour_int, lat_min, lat_max, lon_min, lon_max):
    now = datetime.datetime.now()
    print(year_int, month_int, day_int, hour_int, lat_min, lat_max, lon_min, lon_max, now)

    #UTCに変換
    year_UTC, month_UTC, day_UTC, hour_UTC = JST_to_UTC(year=year_int, month=month_int, day=day_int, hour=hour_int)
    if lat_min != lat_min or lat_max != lat_max or lon_min != lon_min or lon_max != lon_max:
        print(r"Error: latitude or longitude is NaN.")
        return None, None, None, None
    data_url = make_url(datetime.datetime(year_UTC, month_UTC, day_UTC, hour_UTC, 0, 0), lat_min, lat_max, lon_min, lon_max)
    if data_url == None:
        return None, None, None, None
    
    #response = requests.get(data_url)をtry-exceptで囲み、エラーもしくは10秒で処理が進まなかったら30秒後再度リクエストを送るようにする
    while True:
        try:
            response = requests.get(data_url, timeout=10)
            response.raise_for_status()  # Raises an HTTPError if the status is 4xx, 5xx
            break  # リクエストが成功したらループを抜ける
        except requests.exceptions.HTTPError as errh:
            print("Http Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("OOps: Something Else", err)

        # エラーが発生した場合は10秒間待機して再試行
        print("Wait for 10 seconds...")
        time.sleep(10)

    ascii_data = response.text.split("\n")

    #print(ascii_data)

    lat_line = None
    lon_line = None
    water_u_lines = []
    water_v_lines = []

    for i, line in enumerate(ascii_data):
        if line.startswith("lat"):
            lat_line = ascii_data[i+1]
        elif line.startswith("lon"):
            lon_line = ascii_data[i+1]
        elif line.startswith("water_u.water_u"):
            water_u_start = i+1
        elif line.startswith("water_v.water_v"):
            water_v_start = i+1

    if lat_line == None or lon_line == None or water_u_start == None or water_v_start == None:
        print(year_int, month_int, day_int, hour_int)
        print(ascii_data)
        print(lat_line)
        print(lon_line)
        print(water_u_start)
        print(water_v_start)
        quit()

    lat_values = [float(x) for x in lat_line.split(',')]
    lon_values = [float(x) for x in lon_line.split(',')]

    water_u_lines = ascii_data[water_u_start:water_u_start+len(lat_values)]
    water_v_lines = ascii_data[water_v_start:water_v_start+len(lat_values)]

    water_u_values = []
    water_v_values = []

    for line in water_u_lines:
        values = [int(x) for x in re.findall(r"[-+]?\d+", line)][3:] # 最初の3つの値をスライスで省く
        water_u_values.append(values)

    for line in water_v_lines:
        values = [int(x) for x in re.findall(r"[-+]?\d+", line)][3:] # 最初の3つの値をスライスで省く
        water_v_values.append(values)

    lat_data = np.array(lat_values)
    lon_data = np.array(lon_values)
    water_u_data = np.array(water_u_values)
    water_v_data = np.array(water_v_values)
    
    water_u_data = np.where(water_u_data != -30000, water_u_data, np.nan)
    water_v_data = np.where(water_v_data != -30000, water_v_data, np.nan)
    water_u_data = water_u_data * 0.001
    water_v_data = water_v_data * 0.001
    
    return lat_data, lon_data, water_u_data, water_v_data

#指定の地点のデータを双線形補間で取得する関数
def ocean_current_bilinear_interporation(year_int, month_int, day_int, hour_int, latitude, longitude):
    diff_lat = 0.2
    diff_lon = 0.2

    lat_min = latitude - diff_lat
    lat_max = latitude + diff_lat
    lon_min = longitude - diff_lon
    lon_max = longitude + diff_lon

    lat_data, lon_data, water_u_data, water_v_data = get_data(year_int, month_int, day_int, hour_int, lat_min, lat_max, lon_min, lon_max)
    if lat_data is None or lon_data is None or water_u_data is None or water_v_data is None:
        return np.nan, np.nan

    #latitude, longitudeの座標を囲む4点の座標を取得
    lon_1 = lon_data[np.where(lon_data < longitude)][-1]
    lon_2 = lon_data[np.where(lon_data > longitude)][0]
    lat_1 = lat_data[np.where(lat_data < latitude)][0]
    lat_2 = lat_data[np.where(lat_data > latitude)][-1]

    #4点の座標を取得
    water_u_11 = water_u_data[np.where(lat_data == lat_1)[0][0]][np.where(lon_data == lon_1)[0][0]]
    water_u_12 = water_u_data[np.where(lat_data == lat_1)[0][0]][np.where(lon_data == lon_2)[0][0]]
    water_u_21 = water_u_data[np.where(lat_data == lat_2)[0][0]][np.where(lon_data == lon_1)[0][0]]
    water_u_22 = water_u_data[np.where(lat_data == lat_2)[0][0]][np.where(lon_data == lon_2)[0][0]]

    water_v_11 = water_v_data[np.where(lat_data == lat_1)[0][0]][np.where(lon_data == lon_1)[0][0]]
    water_v_12 = water_v_data[np.where(lat_data == lat_1)[0][0]][np.where(lon_data == lon_2)[0][0]]
    water_v_21 = water_v_data[np.where(lat_data == lat_2)[0][0]][np.where(lon_data == lon_1)[0][0]]
    water_v_22 = water_v_data[np.where(lat_data == lat_2)[0][0]][np.where(lon_data == lon_2)[0][0]]

    #双線形補間
    water_u_interpolation = (water_u_11 * (lon_2 - longitude) * (lat_2 - latitude) + water_u_21 * (longitude - lon_1) * (lat_2 - latitude) + water_u_12 * (lon_2 - longitude) * (latitude - lat_1) + water_u_22 * (longitude - lon_1) * (latitude - lat_1)) / ((lon_2 - lon_1) * (lat_2 - lat_1))
    water_v_interpolation = (water_v_11 * (lon_2 - longitude) * (lat_2 - latitude) + water_v_21 * (longitude - lon_1) * (lat_2 - latitude) + water_v_12 * (lon_2 - longitude) * (latitude - lat_1) + water_v_22 * (longitude - lon_1) * (latitude - lat_1)) / ((lon_2 - lon_1) * (lat_2 - lat_1))

    return water_u_interpolation, water_v_interpolation

#4次Runge-Kutta法(delta_t = 2*dt)
dt = 3 * 60 * 60 #時間刻み
def runge_kutta_method(year, month, day, hour, lon, lat, index):
    
    if back_or_forward == 'back':
        delta_t_int = -2 * dt
    elif back_or_forward == 'forward':
        delta_t_int = 2 * dt
    
    delta_t_double = np.double(delta_t_int)
    date_initial = datetime.datetime(year, month, day, hour, 0, 0)
    date_k1 = date_initial
    date_k2 = date_initial + datetime.timedelta(seconds=delta_t_int/2)
    date_k3 = date_initial + datetime.timedelta(seconds=delta_t_int/2)
    date_k4 = date_initial + datetime.timedelta(seconds=delta_t_int)

    lon_initial = lon
    lat_initial = lat

    #k1
    water_u_k1, water_v_k1 = ocean_current_bilinear_interporation(date_k1.year, date_k1.month, date_k1.day, date_k1.hour, lat_initial, lon_initial)
    if water_u_k1 != water_u_k1 or water_v_k1 != water_v_k1:
        return np.nan, np.nan
    dx_k1 = water_u_k1 * delta_t_double / 2
    dy_k1 = water_v_k1 * delta_t_double / 2
    dr_k1 = np.sqrt(dx_k1**2 + dy_k1**2)
    theta_k1 = np.arctan2(dy_k1, dx_k1)
    azimuth_k1 = (theta_k1 * 180 / np.pi - 90) * -1
    lon_k1, lat_k1, _ = pyproj.Geod(ellps='WGS84').fwd(lon_initial, lat_initial, azimuth_k1, dr_k1)

    #k2
    water_u_k2, water_v_k2 = ocean_current_bilinear_interporation(date_k2.year, date_k2.month, date_k2.day, date_k2.hour, lat_k1, lon_k1)
    if water_u_k2 != water_u_k2 or water_v_k2 != water_v_k2:
        return np.nan, np.nan
    dx_k2 = water_u_k2 * delta_t_double / 2
    dy_k2 = water_v_k2 * delta_t_double / 2
    dr_k2 = np.sqrt(dx_k2**2 + dy_k2**2)
    theta_k2 = np.arctan2(dy_k2, dx_k2)
    azimuth_k2 = (theta_k2 * 180 / np.pi - 90) * -1
    lon_k2, lat_k2, _ = pyproj.Geod(ellps='WGS84').fwd(lon_initial, lat_initial, azimuth_k2, dr_k2)

    #k3
    water_u_k3, water_v_k3 = ocean_current_bilinear_interporation(date_k3.year, date_k3.month, date_k3.day, date_k3.hour, lat_k2, lon_k2)
    if water_u_k3 != water_u_k3 or water_v_k3 != water_v_k3:
        return np.nan, np.nan
    dx_k3 = water_u_k3 * delta_t_double
    dy_k3 = water_v_k3 * delta_t_double
    dr_k3 = np.sqrt(dx_k3**2 + dy_k3**2)
    theta_k3 = np.arctan2(dy_k3, dx_k3)
    azimuth_k3 = (theta_k3 * 180 / np.pi - 90) * -1
    lon_k3, lat_k3, _ = pyproj.Geod(ellps='WGS84').fwd(lon_initial, lat_initial, azimuth_k3, dr_k3)

    #k4
    water_u_k4, water_v_k4 = ocean_current_bilinear_interporation(date_k4.year, date_k4.month, date_k4.day, date_k4.hour, lat_k3, lon_k3)
    if water_u_k4 != water_u_k4 or water_v_k4 != water_v_k4:
        return np.nan, np.nan
    
    water_u_slope = (water_u_k1 + 2 * water_u_k2 + 2 * water_u_k3 + water_u_k4) / 6E0
    water_v_slope = (water_v_k1 + 2 * water_v_k2 + 2 * water_v_k3 + water_v_k4) / 6E0

    dx = water_u_slope * delta_t_double
    dy = water_v_slope * delta_t_double
    dr = np.sqrt(dx**2 + dy**2)
    theta = np.arctan2(dy, dx)
    azimuth = (theta * 180 / np.pi - 90) * -1
    lon_new, lat_new, _ = pyproj.Geod(ellps='WGS84').fwd(lon_initial, lat_initial, azimuth, dr)

    return lon_new, lat_new, index


#####Chla関連#####
#JAXAひまわりモニタからchlaのデータをダウンロード
#https://www.eorc.jaxa.jp/ptree/userguide_j.html
ftp_site        = 'ftp.ptree.jaxa.jp'                   # FTPサイトのURL
ftp_user        = 'koseki.saito_stpp.gp.tohoku.ac.jp'   # FTP接続に使用するユーザー名
ftp_password    = 'SP+wari8'                            # FTP接続に使用するパスワード
#1km日本域のデータを使用(24N-50N, 123E-150Eの矩形領域)
pixel_number_chla    = 2701
line_number_chla     = 2601
data_lon_min, data_lon_max, data_lat_min, data_lat_max  = 123E0, 150E0, 24E0, 50E0
median_filter_size = 4

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
    os.remove(local_path)
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
    data = data.rolling(longitude=filter_size, latitude=filter_size, center=True).median()
    data = data.astype(float)
    data = data.where(data != 0, np.nan)
    return data


#chlaのデータを双線形補間で取得する関数
def chla_bilinear_interporation(chla_data, latitude, longitude):
    lat_data = chla_data.coords['latitude'].values
    lon_data = chla_data.coords['longitude'].values
    
    #latitude, longitudeの座標を囲む4点の座標を取得
    lon_1 = lon_data[np.where(lon_data < longitude)][-1]
    lon_2 = lon_data[np.where(lon_data > longitude)][0]
    lat_1 = lat_data[np.where(lat_data < latitude)][0]
    lat_2 = lat_data[np.where(lat_data > latitude)][-1]

    #4点の座標を取得
    chla_11 = chla_data.sel(latitude=lat_1, longitude=lon_1).values
    chla_12 = chla_data.sel(latitude=lat_1, longitude=lon_2).values
    chla_21 = chla_data.sel(latitude=lat_2, longitude=lon_1).values
    chla_22 = chla_data.sel(latitude=lat_2, longitude=lon_2).values

    #双線形補間
    if chla_11 != chla_11 or chla_12 != chla_12 or chla_21 != chla_21 or chla_22 != chla_22:
        return np.nan
    else:
        chla_interpolation = (chla_11 * (lon_2 - longitude) * (lat_2 - latitude) + chla_21 * (longitude - lon_1) * (lat_2 - latitude) + chla_12 * (lon_2 - longitude) * (latitude - lat_1) + chla_22 * (longitude - lon_1) * (latitude - lat_1)) / ((lon_2 - lon_1) * (lat_2 - lat_1))
        return chla_interpolation


# main
now_time = start_time_JST
# save
file_name = os.path.dirname(file_name_point) + f'/trajectory_chla_{now_time.year}_{now_time.month}_{now_time.day}_{now_time.hour}.csv'
now_time_UTC_year, now_time_UTC_month, now_time_UTC_day, now_time_UTC_hour = JST_to_UTC(now_time.year, now_time.month, now_time.day, now_time.hour)
chla_data = calculate_7hours_average(year=now_time_UTC_year, month=now_time_UTC_month, day=now_time_UTC_day, hour=now_time_UTC_hour)
chla_data = median_filter_chla(chla_data, filter_size=median_filter_size)
CHLA_data = np.array([chla_bilinear_interporation(chla_data, lat, lon) for lat, lon in zip(LAT_data, LON_data)])
#INDEX_dataのfmtはint, LON_data, LAT_data, CHLA_dataのfmtは%.5f
np.savetxt(file_name, np.c_[INDEX_data, LON_data, LAT_data, CHLA_data], delimiter=',', fmt='%d,%.5f,%.5f,%.5f')

while (now_time >= end_time_JST and back_or_forward == 'back') or (now_time <= end_time_JST and back_or_forward == 'forward'):
    
    # update
    if back_or_forward == 'back':
        now_time = now_time + datetime.timedelta(seconds=-2*dt)
    elif back_or_forward == 'forward':
        now_time = now_time + datetime.timedelta(seconds=2*dt)
    
    print(r"  ")
    print(r"Now Calculating: " + str(now_time) + " JST")

    now_time_UTC_year, now_time_UTC_month, now_time_UTC_day, now_time_UTC_hour = JST_to_UTC(now_time.year, now_time.month, now_time.day, now_time.hour)

    # backward trajectoryの計算
    LAT_data_new = np.array([])
    LON_data_new = np.array([])
    INDEX_data_new = np.array([])
    CHLA_data_new = np.array([])

    # chlaデータの取得
    chla_data = calculate_7hours_average(year=now_time_UTC_year, month=now_time_UTC_month, day=now_time_UTC_day, hour=now_time_UTC_hour)
    chla_data = median_filter_chla(chla_data, filter_size=median_filter_size)

    # parallel
    if __name__ == '__main__':
        with Pool(parallel_number) as p:
            results = p.starmap(runge_kutta_method, [(now_time_UTC_year, now_time_UTC_month, now_time_UTC_day, now_time_UTC_hour, lon, lat, index) for lon, lat, index in zip(LON_data, LAT_data, INDEX_data)])
            for result in results:
                if result[0] == result[0] and result[1] == result[1]:
                    LON_data_new = np.append(LON_data_new, result[0])
                    LAT_data_new = np.append(LAT_data_new, result[1])
                    INDEX_data_new = np.append(INDEX_data_new, result[2])
                    CHLA_data_new = np.append(CHLA_data_new, chla_bilinear_interporation(chla_data, result[1], result[0]))

    # save
    file_name = os.path.dirname(file_name_point) + f'/trajectory_chla_{now_time.year}_{now_time.month}_{now_time.day}_{now_time.hour}.csv'
    np.savetxt(file_name, np.c_[INDEX_data_new, LON_data_new, LAT_data_new, CHLA_data_new], delimiter=',', fmt='%d,%.5f,%.5f,%.5f')
        
    LAT_data = LAT_data_new
    LON_data = LON_data_new
    INDEX_data = INDEX_data_new

    


print(r'Finished.')
import ftplib
import os
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import datetime
import numpy as np
import cartopy.crs as ccrs
import requests
import re
from multiprocessing import Pool
import pyproj

# chlaのダウンロード
#以下で扱うデータはJAXAひまわりモニタから引用
#https://www.eorc.jaxa.jp/ptree/userguide_j.html
ftp_site        = 'ftp.ptree.jaxa.jp'                   # FTPサイトのURL
ftp_user        = 'koseki.saito_stpp.gp.tohoku.ac.jp'   # FTP接続に使用するユーザー名
ftp_password    = 'SP+wari8'                            # FTP接続に使用するパスワード

#1km日本域のデータを使用(24N-50N, 123E-150Eの矩形領域)
pixel_number    = 2701
line_number     = 2601
data_lon_min, data_lon_max, data_lat_min, data_lat_max  = 123E0, 150E0, 24E0, 50E0

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#プロットする範囲(西之島の座標+-width)
pm_number = 2
width_plot_1_lon    = pm_number
width_plot_1_lat    = pm_number

#始点の座標、時刻(JST)
start_year = 2020
start_month = 7
start_day = 31
start_hour = 12
start_lon = 141.7
start_lat = 27.5

#終点の時刻(JST)
end_year = 2020
end_month = 7
end_day = 1
end_hour = 12

#データファイルの保存先のディレクトリ (形式: hoge/hogehoge)
dir_data = f''
#プロットした図の保存先のディレクトリ (形式: hoge/hogehoge)
dir_figure = f'/mnt/j/isee_remote_data/JST/back_trajectory_test'

#Chlorophyll-a濃度のプロット範囲
chla_vmin = 1E-1
chla_vmax = 1E0

#図の書式の指定(設定ではLaTeX調になっています)
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25

#関数を定義
#プロットする範囲を指定する関数
def plot_width(width_lon, width_lat):
    lon_set_min = nishinoshima_lon - width_lon
    lon_set_max = nishinoshima_lon + width_lon
    lat_set_min = nishinoshima_lat - width_lat
    lat_set_max = nishinoshima_lat + width_lat
    return lon_set_min, lon_set_max, lat_set_min, lat_set_max

lon_1_min, lon_1_max, lat_1_min, lat_1_max = plot_width(width_lon=width_plot_1_lon, width_lat=width_plot_1_lat)

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
    
#フォルダの生成
def mkdir_folder(path_dir_name):
    try:
        os.makedirs(path_dir_name)
    except FileExistsError:
        pass
    return

#chla関連
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
    
    pixel = str(pixel_number).zfill(5)
    line = str(line_number).zfill(5)
    ftp_path = f'/pub/himawari/L3/CHL/{ver}/{yyyy}{mm}/{dd}/H{nn}_{yyyy}{mm}{dd}_{hh}{mn}_1H_rOC{ver}_FLDK.{pixel}_{line}.nc'
    return ftp_path

#ファイルのダウンロード、データを返す
def download_netcdf(year, month, day, hour):
    ftp_path = download_path(year=year, month=month, day=day, hour=hour)
    ftp_base = os.path.basename(ftp_path)
    local_path = os.path.join(dir_data, ftp_base)

    if (check_file_exists(local_path) == False):              #ファイルが存在しない場合、ダウンロードする
        try:
            with ftplib.FTP(ftp_site) as ftp:
                ftp.login(user=ftp_user, passwd=ftp_password)   #ログイン
                ftp.cwd('/')                                    #ルートディレクトリに移動

                with open(local_path, 'wb') as f:                 #ファイルをバイナリモードでダウンロード
                    ftp.retrbinary(f'RETR {ftp_path}', f.write)
                    print(r'Download file is ' + local_path)
        except Exception as e:
            print(f"Error downloading file: {e}")
            return np.zeros((line_number, pixel_number))
        
    data = xr.open_dataset(ftp_base)
    if data is None:
        return np.zeros((line_number, pixel_number))
    os.remove(local_path)
    return data

#chlaのデータを取得する関数
def get_chla(year, month, day, hour):
    #UTCに変換
    year_UTC, month_UTC, day_UTC, hour_UTC = JST_to_UTC(year=year, month=month, day=day, hour=hour)
    #データのダウンロード
    data = download_netcdf(year=year_UTC, month=month_UTC, day=day_UTC, hour=hour_UTC)
    #データの抽出
    if data.all() != 0:
        try:
            chla = data['CHL'].data
        except KeyError as e:
            print(f"Error: {e}")
            print(f"Dataset keys: {data.keys()}")
            chla = np.zeros((line_number, pixel_number))
    else:
        chla = np.zeros((line_number, pixel_number))

    return chla

#oceanic current関連
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
    response_time = requests.get(url_time)
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
    data_url = make_url(datetime.datetime(year_UTC, month_UTC, day_UTC, hour_UTC, 0, 0), lat_min, lat_max, lon_min, lon_max)
    if data_url == None:
        return None, None, None, None
    response = requests.get(data_url)
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
    
    #print(lat_line)
    #print(lon_line)

    if lat_line == None or lon_line == None:
        return None, None, None, None

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


#バックトラジェクトリー関連

dt = 3 * 60 * 60 #時間刻み

#Euler法(delta_t = dt)
def euler_method(year, month, day, hour, lon, lat):
    delta_t = np.double(dt)
    water_u, water_v = ocean_current_bilinear_interporation(year, month, day, hour, lat, lon)
    dx = water_u * - delta_t
    dy = water_v * - delta_t
    dr = np.sqrt(dx**2 + dy**2)
    theta = np.arctan2(dy, dx)
    azimuth = (theta * 180 / np.pi - 90) * -1
    lon_new, lat_new, _ = pyproj.Geod(ellps='WGS84').fwd(lon, lat, azimuth, dr)
    return lon_new, lat_new

#improved Euler法
def improved_euler_method(year, month, day, hour, lon, lat):
    delta_t = np.double(dt)
    water_u, water_v = ocean_current_bilinear_interporation(year, month, day, hour, lat, lon)
    dx = water_u * -delta_t
    dy = water_v * -delta_t
    dr = np.sqrt(dx**2 + dy**2)
    theta = np.arctan2(dy, dx)
    azimuth = (theta * 180 / np.pi - 90) * -1
    lon_new, lat_new, _ = pyproj.Geod(ellps='WGS84').fwd(lon, lat, azimuth, dr)

    water_u_new, water_v_new = ocean_current_bilinear_interporation(year, month, day, hour, lat_new, lon_new)
    dx_new = water_u_new * -delta_t
    dy_new = water_v_new * -delta_t
    dr_new = np.sqrt(dx_new**2 + dy_new**2)
    theta_new = np.arctan2(dy_new, dx_new)
    azimuth_new = (theta_new * 180 / np.pi - 90) * -1
    lon_new_2, lat_new_2, _ = pyproj.Geod(ellps='WGS84').fwd(lon, lat, azimuth_new, dr_new)

    lon_new = (lon_new + lon_new_2) / 2
    lat_new = (lat_new + lat_new_2) / 2

    return lon_new, lat_new

#4次Runge-Kutta法(delta_t = 2*dt)
def runge_kutta_method(year, month, day, hour, lon, lat):
    delta_t_int = 2 * dt
    delta_t_double = np.double(delta_t_int)
    date_initial = datetime.datetime(year, month, day, hour, 0, 0)
    date_k1 = date_initial
    date_k2 = date_initial + datetime.timedelta(seconds=-delta_t_int/2)
    date_k3 = date_initial + datetime.timedelta(seconds=-delta_t_int/2)
    date_k4 = date_initial + datetime.timedelta(seconds=-delta_t_int)

    lon_initial = lon
    lat_initial = lat

    #k1
    water_u_k1, water_v_k1 = ocean_current_bilinear_interporation(date_k1.year, date_k1.month, date_k1.day, date_k1.hour, lat_initial, lon_initial)
    dx_k1 = water_u_k1 * - delta_t_double / 2
    dy_k1 = water_v_k1 * - delta_t_double / 2
    dr_k1 = np.sqrt(dx_k1**2 + dy_k1**2)
    theta_k1 = np.arctan2(dy_k1, dx_k1)
    azimuth_k1 = (theta_k1 * 180 / np.pi - 90) * -1
    lon_k1, lat_k1, _ = pyproj.Geod(ellps='WGS84').fwd(lon_initial, lat_initial, azimuth_k1, dr_k1)

    #k2
    water_u_k2, water_v_k2 = ocean_current_bilinear_interporation(date_k2.year, date_k2.month, date_k2.day, date_k2.hour, lat_k1, lon_k1)
    dx_k2 = water_u_k2 * - delta_t_double / 2
    dy_k2 = water_v_k2 * - delta_t_double / 2
    dr_k2 = np.sqrt(dx_k2**2 + dy_k2**2)
    theta_k2 = np.arctan2(dy_k2, dx_k2)
    azimuth_k2 = (theta_k2 * 180 / np.pi - 90) * -1
    lon_k2, lat_k2, _ = pyproj.Geod(ellps='WGS84').fwd(lon_initial, lat_initial, azimuth_k2, dr_k2)

    #k3
    water_u_k3, water_v_k3 = ocean_current_bilinear_interporation(date_k3.year, date_k3.month, date_k3.day, date_k3.hour, lat_k2, lon_k2)
    dx_k3 = water_u_k3 * - delta_t_double
    dy_k3 = water_v_k3 * - delta_t_double
    dr_k3 = np.sqrt(dx_k3**2 + dy_k3**2)
    theta_k3 = np.arctan2(dy_k3, dx_k3)
    azimuth_k3 = (theta_k3 * 180 / np.pi - 90) * -1
    lon_k3, lat_k3, _ = pyproj.Geod(ellps='WGS84').fwd(lon_initial, lat_initial, azimuth_k3, dr_k3)

    #k4
    water_u_k4, water_v_k4 = ocean_current_bilinear_interporation(date_k4.year, date_k4.month, date_k4.day, date_k4.hour, lat_k3, lon_k3)
    
    water_u_slope = (water_u_k1 + 2 * water_u_k2 + 2 * water_u_k3 + water_u_k4) / 6E0
    water_v_slope = (water_v_k1 + 2 * water_v_k2 + 2 * water_v_k3 + water_v_k4) / 6E0

    dx = water_u_slope * - delta_t_double
    dy = water_v_slope * - delta_t_double
    dr = np.sqrt(dx**2 + dy**2)
    theta = np.arctan2(dy, dx)
    azimuth = (theta * 180 / np.pi - 90) * -1
    lon_new, lat_new, _ = pyproj.Geod(ellps='WGS84').fwd(lon_initial, lat_initial, azimuth, dr)

    return lon_new, lat_new


#バックトラジェクトリーの計算
latitude_euler = np.array([start_lat])
longitude_euler = np.array([start_lon])
latitude_improved_euler = np.array([start_lat])
longitude_improved_euler = np.array([start_lon])
latitude_runge_kutta = np.array([start_lat])
longitude_runge_kutta = np.array([start_lon])

if start_hour != 12:
    latitude_euler_12 = np.array([])
    longitude_euler_12 = np.array([])
    latitude_improved_euler_12 = np.array([])
    longitude_improved_euler_12 = np.array([])
    latitude_runge_kutta_12 = np.array([])
    longitude_runge_kutta_12 = np.array([])
else:
    latitude_euler_12 = np.array([start_lat])
    longitude_euler_12 = np.array([start_lon])
    latitude_improved_euler_12 = np.array([start_lat])
    longitude_improved_euler_12 = np.array([start_lon])
    latitude_runge_kutta_12 = np.array([start_lat])
    longitude_runge_kutta_12 = np.array([start_lon])

date_initial = datetime.datetime(start_year, start_month, start_day, start_hour, 0, 0)
date_end = datetime.datetime(end_year, end_month, end_day, end_hour, 0, 0)

date_euler = np.array([date_initial])
date_improved_euler = np.array([date_initial])
date_runge_kutta = np.array([date_initial])

#Euler法
def euler_calc(date_initial, date_end, longitude_euler, latitude_euler, date_euler, longitude_euler_12, latitude_euler_12):
    date_euler_now = date_initial
    while(date_euler_now > date_end):
        date_euler_now = date_euler_now + datetime.timedelta(seconds=-dt)
        year, month, day, hour = date_euler_now.year, date_euler_now.month, date_euler_now.day, date_euler_now.hour
        lon, lat = euler_method(year, month, day, hour, longitude_euler[-1], latitude_euler[-1])
        latitude_euler = np.append(latitude_euler, lat)
        longitude_euler = np.append(longitude_euler, lon)
        date_euler = np.append(date_euler, date_euler_now)
        if date_euler_now.hour == 12:
            latitude_euler_12 = np.append(latitude_euler_12, lat)
            longitude_euler_12 = np.append(longitude_euler_12, lon)
    return longitude_euler, latitude_euler, date_euler, longitude_euler_12, latitude_euler_12

##improved Euler法
def improved_euler_calc(date_initial, date_end, longitude_improved_euler, latitude_improved_euler, date_improved_euler, longitude_improved_euler_12, latitude_improved_euler_12):
    date_improved_euler_now = date_initial
    while(date_improved_euler_now > date_end):
        date_improved_euler_now = date_improved_euler_now + datetime.timedelta(seconds=-dt)
        year, month, day, hour = date_improved_euler_now.year, date_improved_euler_now.month, date_improved_euler_now.day, date_improved_euler_now.hour
        lon, lat = improved_euler_method(year, month, day, hour, longitude_improved_euler[-1], latitude_improved_euler[-1])
        latitude_improved_euler = np.append(latitude_improved_euler, lat)
        longitude_improved_euler = np.append(longitude_improved_euler, lon)
        date_improved_euler = np.append(date_improved_euler, date_improved_euler_now)
        if date_improved_euler_now.hour == 12:
            latitude_improved_euler_12 = np.append(latitude_improved_euler_12, lat)
            longitude_improved_euler_12 = np.append(longitude_improved_euler_12, lon)
    return longitude_improved_euler, latitude_improved_euler, date_improved_euler, longitude_improved_euler_12, latitude_improved_euler_12

#4次Runge-Kutta法
def runge_kutta_calc(date_initial, date_end, longitude_runge_kutta, latitude_runge_kutta, date_runge_kutta, longitude_runge_kutta_12, latitude_runge_kutta_12):
    date_runge_kutta_now = date_initial
    while(date_runge_kutta_now > date_end):
        date_runge_kutta_now = date_runge_kutta_now + datetime.timedelta(seconds=-2*dt)
        year, month, day, hour = date_runge_kutta_now.year, date_runge_kutta_now.month, date_runge_kutta_now.day, date_runge_kutta_now.hour
        lon, lat = runge_kutta_method(year, month, day, hour, longitude_runge_kutta[-1], latitude_runge_kutta[-1])
        latitude_runge_kutta = np.append(latitude_runge_kutta, lat)
        longitude_runge_kutta = np.append(longitude_runge_kutta, lon)
        date_runge_kutta = np.append(date_runge_kutta, date_runge_kutta_now)
        if date_runge_kutta_now.hour == 12:
            latitude_runge_kutta_12 = np.append(latitude_runge_kutta_12, lat)
            longitude_runge_kutta_12 = np.append(longitude_runge_kutta_12, lon)
    return longitude_runge_kutta, latitude_runge_kutta, date_runge_kutta, longitude_runge_kutta_12, latitude_runge_kutta_12


#プロット(軌道のみ)
#緯度経度の範囲
lon_1_min, lon_1_max, lat_1_min, lat_1_max = plot_width(width_lon=width_plot_1_lon, width_lat=width_plot_1_lat)

#図のサイズ
fig = plt.figure(figsize=(15, 15), dpi=100)
ax = fig.add_subplot(111, xlim=(lon_1_min, lon_1_max), ylim=(lat_1_min, lat_1_max), xlabel='Longitude', ylabel='Latitude')
ax.set_title('Start: ' + str(start_year) + '/' + str(start_month) + '/' + str(start_day) + ' ' + str(start_hour) + ':00' + ' JST, End: ' + str(end_year) + '/' + str(end_month) + '/' + str(end_day) + ' ' + str(end_hour) + ':00' + ' JST')

def main(args):
    if args == 0:
        longitude, latitude, date, longitude_12, latitude_12 = euler_calc(date_initial, date_end, longitude_euler, latitude_euler, date_euler, longitude_euler_12, latitude_euler_12)
    elif args == 1:
        longitude, latitude, date, longitude_12, latitude_12 = improved_euler_calc(date_initial, date_end, longitude_improved_euler, latitude_improved_euler, date_improved_euler, longitude_improved_euler_12, latitude_improved_euler_12)
    elif args == 2:
        longitude, latitude, date, longitude_12, latitude_12 = runge_kutta_calc(date_initial, date_end, longitude_runge_kutta, latitude_runge_kutta, date_runge_kutta, longitude_runge_kutta_12, latitude_runge_kutta_12)
    return longitude, latitude, date, longitude_12, latitude_12, args

if __name__ == '__main__':
    count_list = [0, 1, 2]

    with Pool(3) as p:
        results = p.map(main, count_list)
        
    for result in results:
        if result[5] == 0:
            longitude_euler, latitude_euler, date_euler, longitude_euler_12, latitude_euler_12 = result[0], result[1], result[2], result[3], result[4]
            ax.plot(longitude_euler, latitude_euler, color='hotpink', label='Euler', linewidth=2, marker='o', zorder=5, alpha=0.5)
            ax.scatter(longitude_euler_12, latitude_euler_12, marker='*', s=150, c='hotpink', label='Euler at 12:00', edgecolors='k', zorder=10)
        elif result[5] == 1:
            longitude_improved_euler, latitude_improved_euler, date_improved_euler, longitude_improved_euler_12, latitude_improved_euler_12 = result[0], result[1], result[2], result[3], result[4]
            ax.plot(longitude_improved_euler, latitude_improved_euler, color='deepskyblue', label='Improved Euler', linewidth=2, marker='o', zorder=5, alpha=0.5)
            ax.scatter(longitude_improved_euler_12, latitude_improved_euler_12, marker='*', s=150, c='deepskyblue', label='Improved Euler at 12:00', edgecolors='k', zorder=10)
        elif result[5] == 2:
            longitude_runge_kutta, latitude_runge_kutta, date_runge_kutta, longitude_runge_kutta_12, latitude_runge_kutta_12 = result[0], result[1], result[2], result[3], result[4]
            ax.plot(longitude_runge_kutta, latitude_runge_kutta, color='limegreen', label='4th Runge-Kutta', linewidth=2, marker='o', zorder=5, alpha=0.5)
            ax.scatter(longitude_runge_kutta_12, latitude_runge_kutta_12, marker='*', s=150, c='limegreen', label='4th Runge-Kutta at 12:00', edgecolors='k', zorder=10)

ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=200, c='lightgrey', label='Nishinoshima', edgecolors='k', zorder=1)
ax.scatter(longitude_euler[0], latitude_euler[0], marker='o', s=200, c='orange', label=r'Start point: ' + f'{start_lat:.1f}' + r'N, ' + f'{start_lon:.1f}' + r'E', edgecolors='k', zorder=1)
ax.legend()
ax.minorticks_on()
ax.grid(which='both', alpha=0.3)
fig.tight_layout()

#図の保存
mkdir_folder(dir_figure)
fig_name = f'{dir_figure}/back_trajectory_{start_year}{start_month:02}{start_day:02}{start_hour:02}_{end_year}{end_month:02}{end_day:02}{end_hour:02}_{start_lat:.1f}_{start_lon:.1f}.png'
fig.savefig(fig_name)
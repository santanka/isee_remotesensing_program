import os
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import netCDF4 as nc
import datetime
import requests
import re
from multiprocessing import Pool

pm_number = 5

#画像のフォルダ
figure_folder_path = f'/mnt/j/isee_remote_data/JST/hycom_GLBy0.08_expt_93.0_water_speed_daily_sum_pm{pm_number}/'

#開始日時(1日~31日まで回す)
year_input  = 2020
start_month = 8
end_month = 8

#データの縮小
average_num = int((pm_number+1) / 2)

#データの最大値、最小値
vmin = 0
vmax = 100

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#pm1: scale=25, headwidth=2, width=0.005, average_num=1
#pm2: scale=25, headwidth=2, width=0.005, average_num=1
#pm3: scale=25, headwidth=2, width=0.005, average_num=2
#pm5: scale=25, headwidth=2, width=0.005, average_num=3
#pm7.5: scale=25, headwidth=2, width=0.005, average_num=4
#pm10: scale=25, headwidth=2, width=0.005, average_num=5
#average_num = int(pm_number / 2)

#プロットする範囲
width_plot_1_lon = float(pm_number)
width_plot_1_lat = float(pm_number)

lon_1_max = nishinoshima_lon + width_plot_1_lon
lon_1_min = nishinoshima_lon - width_plot_1_lon
lat_1_max = nishinoshima_lat + width_plot_1_lat
lat_1_min = nishinoshima_lat - width_plot_1_lat

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

#JSTからUTCへの変換
def JST_to_UTC(year, month, day, hour):
    JST_time = datetime.datetime(year, month, day, hour)
    UTC_time = JST_time - datetime.timedelta(hours=9)
    year_UTC_int = UTC_time.year
    month_UTC_int = UTC_time.month
    day_UTC_int = UTC_time.day
    hour_UTC_int = UTC_time.hour
    #print(f'JST: {year}/{month}/{day} {hour}')
    #print(f'UTC: {year_UTC_int}/{month_UTC_int}/{day_UTC_int} {hour_UTC_int}')
    return year_UTC_int, month_UTC_int, day_UTC_int, hour_UTC_int

#データのダウンロード
def make_url(time):
    min_lat_grid, max_lat_grid = get_grid_range_latitude(lat_1_min, lat_1_max)
    min_lon_grid, max_lon_grid = get_grid_range_longitude(lon_1_min, lon_1_max)
    time_grid = get_time_grid(time)
    if time_grid == None:
        return None
    url = f"https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z.ascii?lat[{min_lat_grid}:1:{max_lat_grid}],lon[{min_lon_grid}:1:{max_lon_grid}],water_u[{time_grid}:1:{time_grid}][0:1:0][{min_lat_grid}:1:{max_lat_grid}][{min_lon_grid}:1:{max_lon_grid}],water_v[{time_grid}:1:{time_grid}][0:1:0][{min_lat_grid}:1:{max_lat_grid}][{min_lon_grid}:1:{max_lon_grid}]"
    print(url)
    return url

def get_data(year_int, month_int, day_int, hour_int):
    data_url = make_url(datetime.datetime(year_int, month_int, day_int, hour_int, 0, 0))
    if data_url == None:
        return None, None, None, None, None
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

def data_average(data, average_num):
    # 入力の検証
    if not isinstance(data, np.ndarray) or len(data.shape) not in [1, 2]:
        raise ValueError("Expected 'data' to be a 1-dimensional or 2-dimensional numpy array")

    # 1次元の場合
    if len(data.shape) == 1:
        averaged_data = np.zeros(data.shape[0] // average_num)
        for i in range(averaged_data.shape[0]):
            averaged_data[i] = np.nanmean(data[i * average_num : (i + 1) * average_num])

    # 2次元の場合
    elif len(data.shape) == 2:
        averaged_data = np.zeros((data.shape[0] // average_num, data.shape[1] // average_num))
        for i in range(averaged_data.shape[0]):
            for j in range(averaged_data.shape[1]):
                averaged_data[i, j] = np.nanmean(data[i * average_num : (i + 1) * average_num, j * average_num : (j + 1) * average_num])

    return averaged_data

#データの日平均
def get_data_daily(year_int, month_int, day_int):
    lat_data_daily = None
    lon_data_daily = None
    water_u_data_daily = None
    water_v_data_daily = None
    lat_data_min_grid, lat_data_max_grid = get_grid_range_latitude(lat_1_min, lat_1_max)
    lon_data_min_grid, lon_data_max_grid = get_grid_range_longitude(lon_1_min, lon_1_max)
    count_NaN = np.zeros((lat_data_max_grid - lat_data_min_grid + 1, lon_data_max_grid - lon_data_min_grid + 1))
    #count_NaN = np.zeros((102, 52))     #本来ならばlat_data, lon_dataから取得するべきだが、今回は固定値
    for hour_idx in range(8):

        #12時から翌日9時までのデータを取得
        JST_time = datetime.datetime(year_int, month_int, day_int, 12) + datetime.timedelta(hours=hour_idx*3)
        year_JST_int = JST_time.year
        month_JST_int = JST_time.month
        day_JST_int = JST_time.day
        hour_JST_int = JST_time.hour

        year_UTC_int, month_UTC_int, day_UTC_int, hour_UTC_int = JST_to_UTC(year_JST_int, month_JST_int, day_JST_int, hour_JST_int)

        lat_data, lon_data, water_u_data, water_v_data = get_data(year_UTC_int, month_UTC_int, day_UTC_int, hour_UTC_int)
        if water_u_data is not None:
            if water_u_data_daily is None:
                lat_data_daily = lat_data
                lon_data_daily = lon_data
                water_u_data_daily = water_u_data
                count_NaN[np.isnan(water_u_data_daily) == True] += 1
                water_u_data_daily = np.where(np.isnan(water_u_data_daily) == True, 0, water_u_data_daily)
                water_v_data_daily = water_v_data
                count_NaN[np.isnan(water_v_data_daily) == True] += 1
                water_v_data_daily = np.where(np.isnan(water_v_data_daily) == True, 0, water_v_data_daily)
            else:
                count_NaN[np.isnan(water_u_data) == True] += 1
                water_u_data = np.where(np.isnan(water_u_data) == True, 0, water_u_data)
                water_u_data_daily += water_u_data
                count_NaN[np.isnan(water_v_data) == True] += 1
                water_v_data = np.where(np.isnan(water_v_data) == True, 0, water_v_data)
                water_v_data_daily += water_v_data
        else:
            count_NaN += 1
    water_u_data_daily /= np.double(8 - count_NaN)
    water_v_data_daily /= np.double(8 - count_NaN)

    #データの縮小
    lat_data_daily_resize = data_average(lat_data_daily, average_num)
    lon_data_daily_resize = data_average(lon_data_daily, average_num)
    water_u_data_daily_resize = data_average(water_u_data_daily, average_num)
    water_v_data_daily_resize = data_average(water_v_data_daily, average_num)

    water_speed_daily = np.sqrt(water_u_data_daily_resize**2 + water_v_data_daily_resize**2)

    return lat_data_daily_resize, lon_data_daily_resize, water_u_data_daily_resize, water_v_data_daily_resize, water_speed_daily
    
#日時の確認(うるう年非対応)
def time_check(month, day):
    if (month == 2):
        if (day > 28):
            return False
        else:
            return True
    elif (month == 4 or month == 6 or month == 9 or month == 11):
        if (day > 30):
            return False
        else:
            return True
    else:
        if (day > 31):
            return False
        else:
            return True

#日時の指定、string化
def time_and_date(year, month, day, hour):
    yyyy    = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month
    dd      = str(day).zfill(2)     #day
    hh      = str(hour).zfill(2)    #hour (UTC)
    mn      = '00'                  #minutes (UTC)
    return yyyy, mm, dd, hh, mn

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

#データのプロット
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25

def main(args):
    year_int, month_int, day_int = args

    if (time_check(month_int, day_int) == False):
        return
    
    yyyy, mm, dd, hh, mn = time_and_date(year_int, month_int, day_int, 0)
    figure_name = f'{figure_folder_path}{yyyy}{mm}/{yyyy}{mm}{dd}.png'
    
    lat_data_daily, lon_data_daily, water_u_data_daily, water_v_data_daily, water_speed_daily = get_data_daily(year_int, month_int, day_int)

    if water_u_data_daily is None:
        return
    
    #各地点での1日移動量を計算
    water_u_data_daily_length_m = water_u_data_daily * 3600E0 * 24E0
    water_v_data_daily_length_m = water_v_data_daily * 3600E0 * 24E0
    water_daily_length_m = np.sqrt(water_u_data_daily_length_m**2 + water_v_data_daily_length_m**2)

    water_u_data_daily_length_km = water_u_data_daily_length_m * 1E-3
    water_v_data_daily_length_km = water_v_data_daily_length_m * 1E-3
    water_daily_length_km = water_daily_length_m * 1E-3




    fig = plt.figure(figsize=(15, 15), dpi=75)

    next_day = datetime.datetime(year_int, month_int, day_int, 12) + datetime.timedelta(days=1)
    year_next_day_int = next_day.year
    month_next_day_int = next_day.month
    day_next_day_int = next_day.day
    yyyy_next, mm_next, dd_next, hh_next, mn_next = time_and_date(year_next_day_int, month_next_day_int, day_next_day_int, 0)

    ax = fig.add_subplot(111, title=f'{yyyy}/{mm}/{dd} 12:00 JST - {yyyy_next}/{mm_next}/{dd_next} 12:00 JST')
    cmap = mpl.colormaps.get_cmap('nipy_spectral')
    ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=300, c='black')
    
    sm = ax.quiver(lon_data_daily, lat_data_daily, water_u_data_daily_length_km, water_v_data_daily_length_km, water_daily_length_km, cmap=cmap, scale=1200, headwidth=3, width=0.003)

    # 軸ラベルを設定する
    ax.set_xlabel('longitude')
    ax.set_ylabel('latitude')
    ax.set_xlim(lon_1_min, lon_1_max)
    ax.set_ylim(lat_1_min, lat_1_max)
    ax.minorticks_on()
    ax.grid(which='both', axis='both', alpha=0.5)

    # カラーバーを表示する
    cbar = fig.colorbar(sm, ax=ax, label=r'daily travel distance of ocean current [$\mathrm{km}$]')
    cbar.mappable.set_clim(vmin, vmax)

    #ディレクトリの生成
    mkdir_folder(f'{figure_folder_path}{yyyy}{mm}')

    # 画像を保存する
    plt.savefig(figure_name)
    plt.close()

    return

#main([year_input, 8, 1])
#quit()

if (__name__ == '__main__'):
    
    #プロセス数
    num_processes = 16

    #並列処理の指定
    with Pool(processes=num_processes) as pool:
        results = []
        for month_int in range(start_month, end_month+1):
            for day_int in range(1, 32):
                result = pool.apply_async(main, [(year_input, month_int, day_int)])
                results.append(result)
        for result in results:
            result.get()
        
    print(r'finish')
    quit()
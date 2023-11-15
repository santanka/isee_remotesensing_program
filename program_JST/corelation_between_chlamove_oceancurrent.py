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
from geopy.distance import geodesic


# chla質量中心座標を取得
year = 2020
month = 6

#西之島座標
nishinoshima_lat = 27.243889
nishinoshima_lon = 140.879722

def read_chla_central_point(year, month):
    filename = f'chla_central_point_JST_{year:04d}{month:02d}_2.csv'
    central_point = np.loadtxt(filename, delimiter=',', skiprows=1)
    central_point_day = central_point[:, 0]
    central_point_all_lon = central_point[:, 1]
    central_point_all_lat = central_point[:, 2]
    central_point_a_lon = central_point[:, 3]
    central_point_a_lat = central_point[:, 4]
    central_point_b_lon = central_point[:, 5]
    central_point_b_lat = central_point[:, 6]

    #lonとlatが西之島の座標と一致する場合はnanに置換
    for i in range(len(central_point_day)):
        if (central_point_all_lon[i] == nishinoshima_lon and central_point_all_lat[i] == nishinoshima_lat) or (central_point_all_lon[i] == 0E0 and central_point_all_lat[i] == 0E0):
            central_point_all_lon[i] = np.nan
            central_point_all_lat[i] = np.nan
        if (central_point_a_lon[i] == nishinoshima_lon and central_point_a_lat[i] == nishinoshima_lat) or (central_point_a_lon[i] == 0E0 and central_point_a_lat[i] == 0E0):
            central_point_a_lon[i] = np.nan
            central_point_a_lat[i] = np.nan
        if (central_point_b_lon[i] == nishinoshima_lon and central_point_b_lat[i] == nishinoshima_lat) or (central_point_b_lon[i] == 0E0 and central_point_b_lat[i] == 0E0):
            central_point_b_lon[i] = np.nan
            central_point_b_lat[i] = np.nan

    return central_point_day, central_point_all_lon, central_point_all_lat, central_point_a_lon, central_point_a_lat, central_point_b_lon, central_point_b_lat

#2点間の距離を取得
def get_distance(lat_1, lon_1, lat_2, lon_2):
    distance = geodesic((lat_1, lon_1), (lat_2, lon_2)).m
    return distance

# chla質量中心の移動方向を取得
def get_chla_move_direction(central_point_day, central_point_all_lon, central_point_all_lat, central_point_a_lon, central_point_a_lat, central_point_b_lon, central_point_b_lat):
    chla_u_move_all = np.zeros(len(central_point_day)-1)
    chla_v_move_all = np.zeros(len(central_point_day)-1)
    chla_u_move_a = np.zeros(len(central_point_day)-1)
    chla_v_move_a = np.zeros(len(central_point_day)-1)
    chla_u_move_b = np.zeros(len(central_point_day)-1)
    chla_v_move_b = np.zeros(len(central_point_day)-1)

    chla_u_move_all_unit = np.zeros(len(central_point_day)-1)
    chla_v_move_all_unit = np.zeros(len(central_point_day)-1)
    chla_u_move_a_unit = np.zeros(len(central_point_day)-1)
    chla_v_move_a_unit = np.zeros(len(central_point_day)-1)
    chla_u_move_b_unit = np.zeros(len(central_point_day)-1)
    chla_v_move_b_unit = np.zeros(len(central_point_day)-1)

    chla_move_all_distance = np.zeros(len(central_point_day)-1)
    chla_move_a_distance = np.zeros(len(central_point_day)-1)
    chla_move_b_distance = np.zeros(len(central_point_day)-1)

    for i in range(len(central_point_day)-1):
        if (central_point_all_lat[i] == central_point_all_lat[i] and central_point_all_lon[i] == central_point_all_lon[i]) and (central_point_all_lat[i+1] == central_point_all_lat[i+1] and central_point_all_lon[i+1] == central_point_all_lon[i+1]):
            chla_u_move_all[i] = central_point_all_lon[i+1] - central_point_all_lon[i]
            chla_v_move_all[i] = central_point_all_lat[i+1] - central_point_all_lat[i]
            chla_u_move_all_unit[i] = chla_u_move_all[i] / np.sqrt(chla_u_move_all[i]**2 + chla_v_move_all[i]**2)
            chla_v_move_all_unit[i] = chla_v_move_all[i] / np.sqrt(chla_u_move_all[i]**2 + chla_v_move_all[i]**2)
            chla_move_all_distance[i] = get_distance(central_point_all_lat[i], central_point_all_lon[i], central_point_all_lat[i+1], central_point_all_lon[i+1])
        
        else:
            chla_u_move_all_unit[i] = np.nan
            chla_v_move_all_unit[i] = np.nan
            chla_move_all_distance[i] = np.nan

        if (central_point_a_lat[i] == central_point_a_lat[i] and central_point_a_lon[i] == central_point_a_lon[i]) or (central_point_a_lat[i+1] == central_point_a_lat[i+1] and central_point_a_lon[i+1] == central_point_a_lon[i+1]):
            if (central_point_a_lat[i] != central_point_a_lat[i] and central_point_a_lon[i] != central_point_a_lon[i]) and (central_point_a_lat[i+1] == central_point_a_lat[i+1] and central_point_a_lon[i+1] == central_point_a_lon[i+1]):
                if (central_point_all_lat[i] == central_point_all_lat[i] and central_point_all_lon[i] == central_point_all_lon[i]):
                    chla_u_move_a[i] = central_point_a_lon[i+1] - central_point_all_lon[i]
                    chla_v_move_a[i] = central_point_a_lat[i+1] - central_point_all_lat[i]
                    chla_u_move_a_unit[i] = chla_u_move_a[i] / np.sqrt(chla_u_move_a[i]**2 + chla_v_move_a[i]**2)
                    chla_v_move_a_unit[i] = chla_v_move_a[i] / np.sqrt(chla_u_move_a[i]**2 + chla_v_move_a[i]**2)
                    chla_move_a_distance[i] = get_distance(central_point_all_lat[i], central_point_all_lon[i], central_point_a_lat[i+1], central_point_a_lon[i+1])
                else:
                    chla_u_move_a[i] = np.nan
                    chla_v_move_a[i] = np.nan
                    chla_u_move_a_unit[i] = np.nan
                    chla_v_move_a_unit[i] = np.nan
                    chla_move_a_distance[i] = np.nan
            elif (central_point_a_lat[i] == central_point_a_lat[i] and central_point_a_lon[i] == central_point_a_lon[i]) and (central_point_a_lat[i+1] != central_point_a_lat[i+1] and central_point_a_lon[i+1] != central_point_a_lon[i+1]):
                if (central_point_all_lat[i+1] == central_point_all_lat[i+1] and central_point_all_lon[i+1] == central_point_all_lon[i+1]):
                    chla_u_move_a[i] = central_point_all_lon[i+1] - central_point_a_lon[i]
                    chla_v_move_a[i] = central_point_all_lat[i+1] - central_point_a_lat[i]
                    chla_u_move_a_unit[i] = chla_u_move_a[i] / np.sqrt(chla_u_move_a[i]**2 + chla_v_move_a[i]**2)
                    chla_v_move_a_unit[i] = chla_v_move_a[i] / np.sqrt(chla_u_move_a[i]**2 + chla_v_move_a[i]**2)
                    chla_move_a_distance[i] = get_distance(central_point_a_lat[i], central_point_a_lon[i], central_point_all_lat[i+1], central_point_all_lon[i+1])
                else:
                    chla_u_move_a[i] = np.nan
                    chla_v_move_a[i] = np.nan
                    chla_u_move_a_unit[i] = np.nan
                    chla_v_move_a_unit[i] = np.nan
                    chla_move_a_distance[i] = np.nan
            elif (central_point_a_lat[i] == central_point_a_lat[i] and central_point_a_lon[i] == central_point_a_lon[i]) and (central_point_a_lat[i+1] == central_point_a_lat[i+1] and central_point_a_lon[i+1] == central_point_a_lon[i+1]):
                chla_u_move_a[i] = central_point_a_lon[i+1] - central_point_a_lon[i]
                chla_v_move_a[i] = central_point_a_lat[i+1] - central_point_a_lat[i]
                chla_u_move_a_unit[i] = chla_u_move_a[i] / np.sqrt(chla_u_move_a[i]**2 + chla_v_move_a[i]**2)
                chla_v_move_a_unit[i] = chla_v_move_a[i] / np.sqrt(chla_u_move_a[i]**2 + chla_v_move_a[i]**2)
                chla_move_a_distance[i] = get_distance(central_point_a_lat[i], central_point_a_lon[i], central_point_a_lat[i+1], central_point_a_lon[i+1])
            else:
                print('unknown error a')
                print(central_point_a_lat[i], central_point_a_lon[i], central_point_a_lat[i+1], central_point_a_lon[i+1])
                quit()
        else:
            chla_u_move_a[i] = np.nan
            chla_v_move_a[i] = np.nan
            chla_u_move_a_unit[i] = np.nan
            chla_v_move_a_unit[i] = np.nan
            chla_move_a_distance[i] = np.nan
        
        if (central_point_b_lat[i] == central_point_b_lat[i] and central_point_b_lon[i] == central_point_b_lon[i]) or (central_point_b_lat[i+1] == central_point_b_lat[i+1] and central_point_b_lon[i+1] == central_point_b_lon[i+1]):
            if (central_point_b_lat[i] != central_point_b_lat[i] and central_point_b_lon[i] != central_point_b_lon[i]) and (central_point_b_lat[i+1] == central_point_b_lat[i+1] and central_point_b_lon[i+1] == central_point_b_lon[i+1]):
                if (central_point_all_lat[i] == central_point_all_lat[i] and central_point_all_lon[i] == central_point_all_lon[i]):
                    chla_u_move_b[i] = central_point_b_lon[i+1] - central_point_all_lon[i]
                    chla_v_move_b[i] = central_point_b_lat[i+1] - central_point_all_lat[i]
                    chla_u_move_b_unit[i] = chla_u_move_b[i] / np.sqrt(chla_u_move_b[i]**2 + chla_v_move_b[i]**2)
                    chla_v_move_b_unit[i] = chla_v_move_b[i] / np.sqrt(chla_u_move_b[i]**2 + chla_v_move_b[i]**2)
                    chla_move_b_distance[i] = get_distance(central_point_all_lat[i], central_point_all_lon[i], central_point_b_lat[i+1], central_point_b_lon[i+1])
                else:
                    chla_u_move_b[i] = np.nan
                    chla_v_move_b[i] = np.nan
                    chla_u_move_b_unit[i] = np.nan
                    chla_v_move_b_unit[i] = np.nan
                    chla_move_b_distance[i] = np.nan
            elif (central_point_b_lat[i] == central_point_b_lat[i] and central_point_b_lon[i] == central_point_b_lon[i]) and (central_point_b_lat[i+1] != central_point_b_lat[i+1] and central_point_b_lon[i+1] != central_point_b_lon[i+1]):
                if (central_point_all_lat[i+1] == central_point_all_lat[i+1] and central_point_all_lon[i+1] == central_point_all_lon[i+1]):
                    chla_u_move_b[i] = central_point_all_lon[i+1] - central_point_b_lon[i]
                    chla_v_move_b[i] = central_point_all_lat[i+1] - central_point_b_lat[i]
                    chla_u_move_b_unit[i] = chla_u_move_b[i] / np.sqrt(chla_u_move_b[i]**2 + chla_v_move_b[i]**2)
                    chla_v_move_b_unit[i] = chla_v_move_b[i] / np.sqrt(chla_u_move_b[i]**2 + chla_v_move_b[i]**2)
                    chla_move_b_distance[i] = get_distance(central_point_b_lat[i], central_point_b_lon[i], central_point_all_lat[i+1], central_point_all_lon[i+1])
                else:
                    chla_u_move_b[i] = np.nan
                    chla_v_move_b[i] = np.nan
                    chla_u_move_b_unit[i] = np.nan
                    chla_v_move_b_unit[i] = np.nan
                    chla_move_b_distance[i] = np.nan
            elif (central_point_b_lat[i] == central_point_b_lat[i] and central_point_b_lon[i] == central_point_b_lon[i]) and (central_point_b_lat[i+1] == central_point_b_lat[i+1] and central_point_b_lon[i+1] == central_point_b_lon[i+1]):
                chla_u_move_b[i] = central_point_b_lon[i+1] - central_point_b_lon[i]
                chla_v_move_b[i] = central_point_b_lat[i+1] - central_point_b_lat[i]
                chla_u_move_b_unit[i] = chla_u_move_b[i] / np.sqrt(chla_u_move_b[i]**2 + chla_v_move_b[i]**2)
                chla_v_move_b_unit[i] = chla_v_move_b[i] / np.sqrt(chla_u_move_b[i]**2 + chla_v_move_b[i]**2)
                chla_move_b_distance[i] = get_distance(central_point_b_lat[i], central_point_b_lon[i], central_point_b_lat[i+1], central_point_b_lon[i+1])
            else:
                print('unknown error b')
                print(central_point_b_lat[i], central_point_b_lon[i], central_point_b_lat[i+1], central_point_b_lon[i+1])
                quit()
        else:
            chla_u_move_b[i] = np.nan
            chla_v_move_b[i] = np.nan
            chla_u_move_b_unit[i] = np.nan
            chla_v_move_b_unit[i] = np.nan
            chla_move_b_distance[i] = np.nan

    return chla_u_move_all_unit, chla_v_move_all_unit, chla_move_all_distance, chla_u_move_a_unit, chla_v_move_a_unit, chla_move_a_distance, chla_u_move_b_unit, chla_v_move_b_unit, chla_move_b_distance


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
    print(url)
    return url

def JST_to_UTC(year_int, month_int, day_int, hour_int):
    JST = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
    JST_time = datetime.datetime(year_int, month_int, day_int, hour_int, 0, 0, tzinfo=JST)
    UTC_time = JST_time.astimezone(datetime.timezone.utc)
    year_int_UTC = UTC_time.year
    month_int_UTC = UTC_time.month
    day_int_UTC = UTC_time.day
    hour_int_UTC = UTC_time.hour
    return year_int_UTC, month_int_UTC, day_int_UTC, hour_int_UTC

def get_data(year_int, month_int, day_int, hour_int, lat_min, lat_max, lon_min, lon_max):
    print(year_int, month_int, day_int, hour_int, lat_min, lat_max, lon_min, lon_max)

    data_url = make_url(datetime.datetime(year_int, month_int, day_int, hour_int, 0, 0), lat_min, lat_max, lon_min, lon_max)
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
    
    print(lat_line)
    print(lon_line)

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

#データの日平均
def get_data_daily(year_int, month_int, day_int, lat_min, lat_max, lon_min, lon_max):
    lat_data_daily = None
    lon_data_daily = None
    water_u_data_daily = None
    water_v_data_daily = None
    lat_data_min_grid, lat_data_max_grid = get_grid_range_latitude(lat_min, lat_max)
    lon_data_min_grid, lon_data_max_grid = get_grid_range_longitude(lon_min, lon_max)
    count_NaN = np.zeros((lat_data_max_grid - lat_data_min_grid + 1, lon_data_max_grid - lon_data_min_grid + 1))
    for hour_idx in range(8):
        #12時JSTから翌9時JSTまでのデータを取得
        year_int = int(year_int)
        month_int = int(month_int)
        day_int = int(day_int)
        jst_date = datetime.datetime(year_int, month_int, day_int, 12, 0, 0) + datetime.timedelta(hours=hour_idx * 3)
        year_int_UTC, month_int_UTC, day_int_UTC, hour_int_UTC = JST_to_UTC(jst_date.year, jst_date.month, jst_date.day, jst_date.hour)
        lat_data, lon_data, water_u_data, water_v_data = get_data(year_int_UTC, month_int_UTC, day_int_UTC, hour_int_UTC, lat_min, lat_max, lon_min, lon_max)
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

    water_speed_daily = np.sqrt(water_u_data_daily**2 + water_v_data_daily**2)

    return lat_data_daily, lon_data_daily, water_u_data_daily, water_v_data_daily, water_speed_daily


#指定の地点のデータを双線形補間で取得
def ocean_current_bilinear_interporation(year_int, month_int, day_int, latitude, longitude):

    if latitude != latitude or longitude != longitude:
        return np.nan, np.nan, np.nan, np.nan, np.nan

    diff_lat = 0.2
    diff_lon = 0.2

    lat_min = latitude - diff_lat
    lat_max = latitude + diff_lat
    lon_min = longitude - diff_lon
    lon_max = longitude + diff_lon

    lat_data_daily, lon_data_daily, water_u_data_daily, water_v_data_daily, water_speed_daily = get_data_daily(year_int, month_int, day_int, lat_min, lat_max, lon_min, lon_max)

    #latitude, longitudeの座標を囲む4点の座標を取得
    lon_1 = lon_data_daily[np.where(lon_data_daily < longitude)][-1]
    lon_2 = lon_data_daily[np.where(lon_data_daily > longitude)][0]
    lat_1 = lat_data_daily[np.where(lat_data_daily < latitude)][0]
    lat_2 = lat_data_daily[np.where(lat_data_daily > latitude)][-1]

    #latitude, longitudeの座標を囲む4点のデータを取得
    water_u_11 = water_u_data_daily[np.where(lat_data_daily == lat_1)[0][0], np.where(lon_data_daily == lon_1)[0][0]]
    water_u_12 = water_u_data_daily[np.where(lat_data_daily == lat_1)[0][0], np.where(lon_data_daily == lon_2)[0][0]]
    water_u_21 = water_u_data_daily[np.where(lat_data_daily == lat_2)[0][0], np.where(lon_data_daily == lon_1)[0][0]]
    water_u_22 = water_u_data_daily[np.where(lat_data_daily == lat_2)[0][0], np.where(lon_data_daily == lon_2)[0][0]]

    water_v_11 = water_v_data_daily[np.where(lat_data_daily == lat_1)[0][0], np.where(lon_data_daily == lon_1)[0][0]]
    water_v_12 = water_v_data_daily[np.where(lat_data_daily == lat_1)[0][0], np.where(lon_data_daily == lon_2)[0][0]]
    water_v_21 = water_v_data_daily[np.where(lat_data_daily == lat_2)[0][0], np.where(lon_data_daily == lon_1)[0][0]]
    water_v_22 = water_v_data_daily[np.where(lat_data_daily == lat_2)[0][0], np.where(lon_data_daily == lon_2)[0][0]]

    #双線形補間
    water_u_interpolation = (water_u_11 * (lon_2 - longitude) * (lat_2 - latitude) + water_u_21 * (longitude - lon_1) * (lat_2 - latitude) + water_u_12 * (lon_2 - longitude) * (latitude - lat_1) + water_u_22 * (longitude - lon_1) * (latitude - lat_1)) / ((lon_2 - lon_1) * (lat_2 - lat_1))
    water_v_interpolation = (water_v_11 * (lon_2 - longitude) * (lat_2 - latitude) + water_v_21 * (longitude - lon_1) * (lat_2 - latitude) + water_v_12 * (lon_2 - longitude) * (latitude - lat_1) + water_v_22 * (longitude - lon_1) * (latitude - lat_1)) / ((lon_2 - lon_1) * (lat_2 - lat_1))
    water_speed_interpolation = np.sqrt(water_u_interpolation**2 + water_v_interpolation**2)

    #単位ベクトル処理
    water_u_interpolation_unit = water_u_interpolation / water_speed_interpolation
    water_v_interpolation_unit = water_v_interpolation / water_speed_interpolation

    return water_u_interpolation, water_v_interpolation, water_speed_interpolation, water_u_interpolation_unit, water_v_interpolation_unit


#海流とchla質量中心移動の内積を取得
central_point_day, central_point_all_lon, central_point_all_lat, central_point_a_lon, central_point_a_lat, central_point_b_lon, central_point_b_lat = read_chla_central_point(year, month)
central_point_day = central_point_day.astype(np.int64)
chla_u_move_all_unit, chla_v_move_all_unit, chla_move_all_distance, chla_u_move_a_unit, chla_v_move_a_unit, chla_move_a_distance, chla_u_move_b_unit, chla_v_move_b_unit, chla_move_b_distance = get_chla_move_direction(central_point_day, central_point_all_lon, central_point_all_lat, central_point_a_lon, central_point_a_lat, central_point_b_lon, central_point_b_lat)

water_u_interpolation_all = np.zeros(len(central_point_day)-1)
water_v_interpolation_all = np.zeros(len(central_point_day)-1)
water_speed_interpolation_all = np.zeros(len(central_point_day)-1)
water_u_interpolation_all_unit = np.zeros(len(central_point_day)-1)
water_v_interpolation_all_unit = np.zeros(len(central_point_day)-1)
water_u_interpolation_a = np.zeros(len(central_point_day)-1)
water_v_interpolation_a = np.zeros(len(central_point_day)-1)
water_speed_interpolation_a = np.zeros(len(central_point_day)-1)
water_u_interpolation_a_unit = np.zeros(len(central_point_day)-1)
water_v_interpolation_a_unit = np.zeros(len(central_point_day)-1)
water_u_interpolation_b = np.zeros(len(central_point_day)-1)
water_v_interpolation_b = np.zeros(len(central_point_day)-1)
water_speed_interpolation_b = np.zeros(len(central_point_day)-1)
water_u_interpolation_b_unit = np.zeros(len(central_point_day)-1)
water_v_interpolation_b_unit = np.zeros(len(central_point_day)-1)
central_point_day_resize = np.zeros(len(central_point_day)-1)

def get_data_daily_parallel(args):
    count_i = args
    central_point_day_resize = central_point_day[count_i]
    water_u_interpolation_all, water_v_interpolation_all, water_speed_interpolation_all, water_u_interpolation_all_unit, water_v_interpolation_all_unit = ocean_current_bilinear_interporation(year, month, central_point_day[count_i], central_point_all_lat[count_i], central_point_all_lon[count_i])
    
    if chla_u_move_a_unit[count_i] != chla_u_move_a_unit[count_i] or chla_v_move_a_unit[count_i] != chla_v_move_a_unit[count_i]:
        water_u_interpolation_a = np.nan
        water_v_interpolation_a = np.nan
        water_speed_interpolation_a = np.nan
        water_u_interpolation_a_unit = np.nan
        water_v_interpolation_a_unit = np.nan
    else:
        water_u_interpolation_a, water_v_interpolation_a, water_speed_interpolation_a, water_u_interpolation_a_unit, water_v_interpolation_a_unit = ocean_current_bilinear_interporation(year, month, central_point_day[count_i], central_point_a_lat[count_i], central_point_a_lon[count_i])
    
    if chla_u_move_b_unit[count_i] != chla_u_move_b_unit[count_i] or chla_v_move_b_unit[count_i] != chla_v_move_b_unit[count_i]:
        water_u_interpolation_b = np.nan
        water_v_interpolation_b = np.nan
        water_speed_interpolation_b = np.nan
        water_u_interpolation_b_unit = np.nan
        water_v_interpolation_b_unit = np.nan
    else:
        water_u_interpolation_b, water_v_interpolation_b, water_speed_interpolation_b, water_u_interpolation_b_unit, water_v_interpolation_b_unit = ocean_current_bilinear_interporation(year, month, central_point_day[count_i], central_point_b_lat[count_i], central_point_b_lon[count_i])
    return central_point_day_resize, water_u_interpolation_all, water_v_interpolation_all, water_speed_interpolation_all, water_u_interpolation_all_unit, water_v_interpolation_all_unit, water_u_interpolation_a, water_v_interpolation_a, water_speed_interpolation_a, water_u_interpolation_a_unit, water_v_interpolation_a_unit, water_u_interpolation_b, water_v_interpolation_b, water_speed_interpolation_b, water_u_interpolation_b_unit, water_v_interpolation_b_unit
    
#非同期処理
if __name__ == "__main__":
    num_processes = 16
    args = range(len(central_point_day)-1)
    with Pool(num_processes) as p:
        results = p.map(get_data_daily_parallel, args)
        for result in results:
            count_i = result[0]-1
            central_point_day_resize[count_i] = result[0]
            water_u_interpolation_all[count_i] = result[1]
            water_v_interpolation_all[count_i] = result[2]
            water_speed_interpolation_all[count_i] = result[3]
            water_u_interpolation_all_unit[count_i] = result[4]
            water_v_interpolation_all_unit[count_i] = result[5]
            water_u_interpolation_a[count_i] = result[6]
            water_v_interpolation_a[count_i] = result[7]
            water_speed_interpolation_a[count_i] = result[8]
            water_u_interpolation_a_unit[count_i] = result[9]
            water_v_interpolation_a_unit[count_i] = result[10]
            water_u_interpolation_b[count_i] = result[11]
            water_v_interpolation_b[count_i] = result[12]
            water_speed_interpolation_b[count_i] = result[13]
            water_u_interpolation_b_unit[count_i] = result[14]
            water_v_interpolation_b_unit[count_i] = result[15]

#for count_j in range(len(central_point_day)-1):
#    central_point_day_resize[count_j], water_u_interpolation_all[count_j], water_v_interpolation_all[count_j], water_speed_interpolation_all[count_j], water_u_interpolation_all_unit[count_j], water_v_interpolation_all_unit[count_j], water_u_interpolation_a[count_j], water_v_interpolation_a[count_j], water_speed_interpolation_a[count_j], water_u_interpolation_a_unit[count_j], water_v_interpolation_a_unit[count_j], water_u_interpolation_b[count_j], water_v_interpolation_b[count_j], water_speed_interpolation_b[count_j], water_u_interpolation_b_unit[count_j], water_v_interpolation_b_unit[count_j] = get_data_daily_parallel([count_j])


#速度[m/s]を24時間距離に変換 [m/day]
water_speed_distance_interpolation_all = water_speed_interpolation_all * 3600E0 * 24E0
water_speed_distance_interpolation_a = water_speed_interpolation_a * 3600E0 * 24E0
water_speed_distance_interpolation_b = water_speed_interpolation_b * 3600E0 * 24E0

for count_i in range(len(central_point_day)-1):
    if (water_speed_distance_interpolation_a[count_i] != water_speed_distance_interpolation_a[count_i]) and (chla_move_a_distance[count_i] == chla_move_a_distance[count_i]):
        water_speed_distance_interpolation_a[count_i] = water_speed_distance_interpolation_all[count_i]
        water_u_interpolation_a_unit[count_i] = water_u_interpolation_all_unit[count_i]
        water_v_interpolation_a_unit[count_i] = water_v_interpolation_all_unit[count_i]
    if (water_speed_distance_interpolation_b[count_i] != water_speed_distance_interpolation_b[count_i]) and (chla_move_b_distance[count_i] == chla_move_b_distance[count_i]):
        water_speed_distance_interpolation_b[count_i] = water_speed_distance_interpolation_all[count_i]
        water_u_interpolation_b_unit[count_i] = water_u_interpolation_all_unit[count_i]
        water_v_interpolation_b_unit[count_i] = water_v_interpolation_all_unit[count_i]


chla_move_all_distance_coef = np.array([])
chla_move_a_distance_coef = np.array([])
chla_move_b_distance_coef = np.array([])
water_speed_distance_interpolation_all_coef = np.array([])
water_speed_distance_interpolation_a_coef = np.array([])
water_speed_distance_interpolation_b_coef = np.array([])

for count_i in range(len(central_point_day)-1):
    if chla_move_all_distance[count_i] == chla_move_all_distance[count_i]:
        chla_move_all_distance_coef = np.append(chla_move_all_distance_coef, chla_move_all_distance[count_i])
        water_speed_distance_interpolation_all_coef = np.append(water_speed_distance_interpolation_all_coef, water_speed_distance_interpolation_all[count_i])
    if chla_move_a_distance[count_i] == chla_move_a_distance[count_i]:
        chla_move_a_distance_coef = np.append(chla_move_a_distance_coef, chla_move_a_distance[count_i])
        water_speed_distance_interpolation_a_coef = np.append(water_speed_distance_interpolation_a_coef, water_speed_distance_interpolation_a[count_i])
    if chla_move_b_distance[count_i] == chla_move_b_distance[count_i]:
        chla_move_b_distance_coef = np.append(chla_move_b_distance_coef, chla_move_b_distance[count_i])
        water_speed_distance_interpolation_b_coef = np.append(water_speed_distance_interpolation_b_coef, water_speed_distance_interpolation_b[count_i])

print(chla_move_all_distance_coef * 1E-3)
print(water_speed_distance_interpolation_all_coef * 1E-3)
print(chla_move_a_distance_coef * 1E-3)
print(water_speed_distance_interpolation_a_coef * 1E-3)
print(chla_move_b_distance_coef * 1E-3)
print(water_speed_distance_interpolation_b_coef * 1E-3)

#chla移動距離と海流移動距離の相関係数を取得 [km]に変換
correlation_coefficient_all = np.corrcoef(chla_move_all_distance_coef * 1E-3, water_speed_distance_interpolation_all_coef * 1E-3)[0, 1]
correlation_coefficient_a = np.corrcoef(chla_move_a_distance_coef * 1E-3, water_speed_distance_interpolation_a_coef * 1E-3)[0, 1]
correlation_coefficient_b = np.corrcoef(chla_move_b_distance_coef * 1E-3, water_speed_distance_interpolation_b_coef * 1E-3)[0, 1]


#内積を取得
inner_product_all = np.zeros(len(central_point_day)-1)
inner_product_a = np.zeros(len(central_point_day)-1)
inner_product_b = np.zeros(len(central_point_day)-1)

for count_i in range(len(central_point_day)-1):
    inner_product_all[count_i] = chla_u_move_all_unit[count_i] * water_u_interpolation_all_unit[count_i] + chla_v_move_all_unit[count_i] * water_v_interpolation_all_unit[count_i]
    inner_product_a[count_i] = chla_u_move_a_unit[count_i] * water_u_interpolation_a_unit[count_i] + chla_v_move_a_unit[count_i] * water_v_interpolation_a_unit[count_i]
    inner_product_b[count_i] = chla_u_move_b_unit[count_i] * water_u_interpolation_b_unit[count_i] + chla_v_move_b_unit[count_i] * water_v_interpolation_b_unit[count_i]

#ベクトルの向きを算出
#北を0度として、時計回りに-180度から180度までの値を取る
chla_move_all_angle = np.ones(len(central_point_day)-1) * 1E10
chla_move_a_angle = np.ones(len(central_point_day)-1) * 1E10
chla_move_b_angle = np.ones(len(central_point_day)-1) * 1E10
ocean_current_all_angle = np.ones(len(central_point_day)-1) * 1E10
ocean_current_a_angle = np.ones(len(central_point_day)-1) * 1E10
ocean_current_b_angle = np.ones(len(central_point_day)-1) * 1E10

for count_i in range(len(central_point_day)-1):
    chla_move_all_angle[count_i]        = np.rad2deg(np.arctan2(chla_u_move_all_unit[count_i], chla_v_move_all_unit[count_i]))
    chla_move_a_angle[count_i]          = np.rad2deg(np.arctan2(chla_u_move_a_unit[count_i], chla_v_move_a_unit[count_i]))
    chla_move_b_angle[count_i]          = np.rad2deg(np.arctan2(chla_u_move_b_unit[count_i], chla_v_move_b_unit[count_i]))
    ocean_current_all_angle[count_i]    = np.rad2deg(np.arctan2(water_u_interpolation_all_unit[count_i], water_v_interpolation_all_unit[count_i]))
    ocean_current_a_angle[count_i]      = np.rad2deg(np.arctan2(water_u_interpolation_a_unit[count_i], water_v_interpolation_a_unit[count_i]))
    ocean_current_b_angle[count_i]      = np.rad2deg(np.arctan2(water_u_interpolation_b_unit[count_i], water_v_interpolation_b_unit[count_i]))

#1E10をnanに変換
chla_move_all_angle = np.where(chla_move_all_angle == 1E10, np.nan, chla_move_all_angle)
chla_move_a_angle = np.where(chla_move_a_angle == 1E10, np.nan, chla_move_a_angle)
chla_move_b_angle = np.where(chla_move_b_angle == 1E10, np.nan, chla_move_b_angle)
ocean_current_all_angle = np.where(ocean_current_all_angle == 1E10, np.nan, ocean_current_all_angle)
ocean_current_a_angle = np.where(ocean_current_a_angle == 1E10, np.nan, ocean_current_a_angle)
ocean_current_b_angle = np.where(ocean_current_b_angle == 1E10, np.nan, ocean_current_b_angle)

mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25

fig = plt.figure(figsize=(28, 28), dpi=100)
ax_1 = fig.add_subplot(321, xlabel=r'day', ylabel=r'inner product', xlim=(0, np.max(central_point_day_resize)+1))
ax_2 = fig.add_subplot(323, xlabel=r'day', ylabel=r'chla move angle [deg]', xlim=(0, np.max(central_point_day_resize)+1))
ax_3 = fig.add_subplot(325, xlabel=r'day', ylabel=r'ocean current angle [deg]', xlim=(0, np.max(central_point_day_resize)+1))
ax_4 = fig.add_subplot(322, xlabel=r'chla move distance [km]', ylabel=r'ocean current distance [km]')
ax_5 = fig.add_subplot(324, xlabel=r'day', ylabel=r'chla move distance [km]', xlim=(0, np.max(central_point_day_resize)+1))
ax_6 = fig.add_subplot(326, xlabel=r'day', ylabel=r'ocean current distance [km]', xlim=(0, np.max(central_point_day_resize)+1))

ax_1.plot(central_point_day_resize, inner_product_all, color='orange', linewidth=4, label=r'all', alpha=0.8)
if inner_product_a.all() != inner_product_a.all():
    ax_1.plot(central_point_day_resize, inner_product_a, color='blue', linewidth=4, label=r'Point a', alpha=0.8)
if inner_product_b.all() != inner_product_b.all():
    ax_1.plot(central_point_day_resize, inner_product_b, color='green', linewidth=4, label=r'Point b', alpha=0.8)

ax_2.plot(central_point_day_resize, chla_move_all_angle, color='orange', linewidth=4, label=r'all', alpha=0.8)
if chla_move_a_angle.all() != chla_move_a_angle.all():
    ax_2.plot(central_point_day_resize, chla_move_a_angle, color='blue', linewidth=4, label=r'Point a', alpha=0.8)
if chla_move_b_angle.all() != chla_move_b_angle.all():
    ax_2.plot(central_point_day_resize, chla_move_b_angle, color='green', linewidth=4, label=r'Point b', alpha=0.8)

ax_3.plot(central_point_day_resize, ocean_current_all_angle, color='orange', linewidth=4, label=r'all', alpha=0.8)
if ocean_current_a_angle.all() != ocean_current_a_angle.all():
    ax_3.plot(central_point_day_resize, ocean_current_a_angle, color='blue', linewidth=4, label=r'Point a', alpha=0.8)
if ocean_current_b_angle.all() != ocean_current_b_angle.all():
    ax_3.plot(central_point_day_resize, ocean_current_b_angle, color='green', linewidth=4, label=r'Point b', alpha=0.8)

ax_4.scatter(chla_move_all_distance_coef * 1E-3, water_speed_distance_interpolation_all_coef * 1E-3, color='orange', label=r'all: ' + f'{correlation_coefficient_all:.3f}', alpha=0.8)
if chla_move_a_distance.all() != chla_move_a_distance.all():
    ax_4.scatter(chla_move_a_distance_coef * 1E-3, water_speed_distance_interpolation_a_coef * 1E-3, color='blue', label=r'Point a: ' + f'{correlation_coefficient_a:.3f}', alpha=0.8)
if chla_move_b_distance.all() != chla_move_b_distance.all():
    ax_4.scatter(chla_move_b_distance_coef * 1E-3, water_speed_distance_interpolation_b_coef * 1E-3, color='green', label=r'Point b: ' + f'{correlation_coefficient_b:.3f}', alpha=0.8)
ax_4.set_xlim(0, np.max(chla_move_all_distance_coef * 1E-3)+1)
ax_4.set_ylim(0, np.max(water_speed_distance_interpolation_all_coef * 1E-3)+1)
print(np.max(chla_move_all_distance_coef * 1E-3))
print(np.max(water_speed_distance_interpolation_all_coef * 1E-3))

ax_5.plot(central_point_day_resize, chla_move_all_distance * 1E-3, color='orange', linewidth=4, label=r'all', alpha=0.8)
if chla_move_a_distance.all() != chla_move_a_distance.all():
    ax_5.plot(central_point_day_resize, chla_move_a_distance * 1E-3, color='blue', linewidth=4, label=r'Point a', alpha=0.8)
if chla_move_b_distance.all() != chla_move_b_distance.all():
    ax_5.plot(central_point_day_resize, chla_move_b_distance * 1E-3, color='green', linewidth=4, label=r'Point b', alpha=0.8)

ax_6.plot(central_point_day_resize, water_speed_distance_interpolation_all * 1E-3, color='orange', linewidth=4, label=r'all', alpha=0.8)
if water_speed_distance_interpolation_a.all() != water_speed_distance_interpolation_a.all():
    ax_6.plot(central_point_day_resize, water_speed_distance_interpolation_a * 1E-3, color='blue', linewidth=4, label=r'Point a', alpha=0.8)
if water_speed_distance_interpolation_b.all() != water_speed_distance_interpolation_b.all():
    ax_6.plot(central_point_day_resize, water_speed_distance_interpolation_b * 1E-3, color='green', linewidth=4, label=r'Point b', alpha=0.8)




ax_1.set_ylim(-1, 1)
ax_2.set_ylim(-180, 180)
ax_3.set_ylim(-180, 180)

axes = [ax_1, ax_2, ax_3, ax_4, ax_5, ax_6]

for ax in axes:
    ax.minorticks_on()
    ax.grid(which='both', alpha=0.3)
    ax.legend(loc = 'best')

plt.tight_layout()

dir_name = f'/mnt/j/isee_remote_data/JST/correlation_between_chla_ocean_current'
if not os.path.exists(dir_name):
    os.makedirs(dir_name)
figure_name = f'{dir_name}/{year:04d}{month:02d}.png'
plt.savefig(figure_name)
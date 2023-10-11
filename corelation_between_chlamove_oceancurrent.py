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


# chla質量中心座標を取得
year = 2020
month = 8

def read_chla_central_point(year, month):
    filename = f'chla_central_point_smooth_{year:04d}{month:02d}_2_edit.csv'
    central_point = np.loadtxt(filename, delimiter=',', skiprows=1)
    central_point_day = central_point[:, 0]
    central_point_all_lon = central_point[:, 1]
    central_point_all_lat = central_point[:, 2]
    central_point_a_lon = central_point[:, 3]
    central_point_a_lat = central_point[:, 4]
    central_point_b_lon = central_point[:, 5]
    central_point_b_lat = central_point[:, 6]
    return central_point_day, central_point_all_lon, central_point_all_lat, central_point_a_lon, central_point_a_lat, central_point_b_lon, central_point_b_lat

# chla質量中心の移動方向を取得
def get_chla_move_direction(central_point_day, central_point_all_lon, central_point_all_lat, central_point_a_lon, central_point_a_lat, central_point_b_lon, central_point_b_lat):
    chla_u_move_all = np.zeros(len(central_point_day-1))
    chla_v_move_all = np.zeros(len(central_point_day-1))
    chla_u_move_a = np.zeros(len(central_point_day-1))
    chla_v_move_a = np.zeros(len(central_point_day-1))
    chla_u_move_b = np.zeros(len(central_point_day-1))
    chla_v_move_b = np.zeros(len(central_point_day-1))

    chla_u_move_all_unit = np.zeros(len(central_point_day-1))
    chla_v_move_all_unit = np.zeros(len(central_point_day-1))
    chla_u_move_a_unit = np.zeros(len(central_point_day-1))
    chla_v_move_a_unit = np.zeros(len(central_point_day-1))
    chla_u_move_b_unit = np.zeros(len(central_point_day-1))
    chla_v_move_b_unit = np.zeros(len(central_point_day-1))

    for i in range(len(central_point_day)-1):
        chla_u_move_all[i] = central_point_all_lon[i+1] - central_point_all_lon[i]
        chla_v_move_all[i] = central_point_all_lat[i+1] - central_point_all_lat[i]
        chla_u_move_all_unit[i] = chla_u_move_all[i] / np.sqrt(chla_u_move_all[i]**2 + chla_v_move_all[i]**2)
        chla_v_move_all_unit[i] = chla_v_move_all[i] / np.sqrt(chla_u_move_all[i]**2 + chla_v_move_all[i]**2)

        if central_point_a_lat[i] != 0 and central_point_a_lon[i] != 0:
            if central_point_all_lat[i] != 0 and central_point_all_lon[i] != 0:
                chla_u_move_a[i] = central_point_a_lon[i+1] - central_point_all_lon[i]
                chla_v_move_a[i] = central_point_a_lat[i+1] - central_point_all_lat[i]
            else:
                chla_u_move_a[i] = central_point_a_lon[i+1] - central_point_a_lon[i]
                chla_v_move_a[i] = central_point_a_lat[i+1] - central_point_a_lat[i]
            chla_u_move_a_unit[i] = chla_u_move_a[i] / np.sqrt(chla_u_move_a[i]**2 + chla_v_move_a[i]**2)
            chla_v_move_a_unit[i] = chla_v_move_a[i] / np.sqrt(chla_u_move_a[i]**2 + chla_v_move_a[i]**2)
        else:
            chla_u_move_a_unit[i] = np.nan
            chla_v_move_a_unit[i] = np.nan
        
        if central_point_b_lat[i] != 0 and central_point_b_lon[i] != 0:
            if central_point_all_lat[i] != 0 and central_point_all_lon[i] != 0:
                chla_u_move_b[i] = central_point_b_lon[i+1] - central_point_all_lon[i]
                chla_v_move_b[i] = central_point_b_lat[i+1] - central_point_all_lat[i]
            else:
                chla_u_move_b[i] = central_point_b_lon[i+1] - central_point_b_lon[i]
                chla_v_move_b[i] = central_point_b_lat[i+1] - central_point_b_lat[i]
            chla_u_move_b_unit[i] = chla_u_move_b[i] / np.sqrt(chla_u_move_b[i]**2 + chla_v_move_b[i]**2)
            chla_v_move_b_unit[i] = chla_v_move_b[i] / np.sqrt(chla_u_move_b[i]**2 + chla_v_move_b[i]**2)
        else:
            chla_u_move_b_unit[i] = np.nan
            chla_v_move_b_unit[i] = np.nan

    return chla_u_move_all_unit, chla_v_move_all_unit, chla_u_move_a_unit, chla_v_move_a_unit, chla_u_move_b_unit, chla_v_move_b_unit


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

def get_data(year_int, month_int, day_int, hour_int, lat_min, lat_max, lon_min, lon_max):
    print(year_int, month_int, day_int, hour_int, lat_min, lat_max, lon_min, lon_max)
    data_url = make_url(datetime.datetime(year_int, month_int, day_int, hour_int, 0, 0), lat_min, lat_max, lon_min, lon_max)
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

#データの日平均
def get_data_daily(year_int, month_int, day_int, lat_min, lat_max, lon_min, lon_max):
    lat_data_daily = None
    lon_data_daily = None
    water_u_data_daily = None
    water_v_data_daily = None
    lat_data_min_grid, lat_data_max_grid = get_grid_range_latitude(lat_min, lat_max)
    lon_data_min_grid, lon_data_max_grid = get_grid_range_longitude(lon_min, lon_max)
    count_NaN = np.zeros((lat_data_max_grid - lat_data_min_grid + 1, lon_data_max_grid - lon_data_min_grid + 1))
    #count_NaN = np.zeros((102, 52))     #本来ならばlat_data, lon_dataから取得するべきだが、今回は固定値
    for hour_idx in range(8):
        hour_int = hour_idx * 3
        lat_data, lon_data, water_u_data, water_v_data = get_data(year_int, month_int, day_int, hour_int, lat_min, lat_max, lon_min, lon_max)
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
chla_u_move_all_unit, chla_v_move_all_unit, chla_u_move_a_unit, chla_v_move_a_unit, chla_u_move_b_unit, chla_v_move_b_unit = get_chla_move_direction(central_point_day, central_point_all_lon, central_point_all_lat, central_point_a_lon, central_point_a_lat, central_point_b_lon, central_point_b_lat)
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
chla_move_all_angle = np.zeros(len(central_point_day)-1)
chla_move_a_angle = np.zeros(len(central_point_day)-1)
chla_move_b_angle = np.zeros(len(central_point_day)-1)
ocean_current_all_angle = np.zeros(len(central_point_day)-1)
ocean_current_a_angle = np.zeros(len(central_point_day)-1)
ocean_current_b_angle = np.zeros(len(central_point_day)-1)

for count_i in range(len(central_point_day)-1):
    chla_move_all_angle[count_i] = np.rad2deg(np.arctan2(chla_v_move_all_unit[count_i], chla_u_move_all_unit[count_i]))
    chla_move_a_angle[count_i] = np.rad2deg(np.arctan2(chla_v_move_a_unit[count_i], chla_u_move_a_unit[count_i]))
    chla_move_b_angle[count_i] = np.rad2deg(np.arctan2(chla_v_move_b_unit[count_i], chla_u_move_b_unit[count_i]))
    ocean_current_all_angle[count_i] = np.rad2deg(np.arctan2(water_v_interpolation_all_unit[count_i], water_u_interpolation_all_unit[count_i]))
    ocean_current_a_angle[count_i] = np.rad2deg(np.arctan2(water_v_interpolation_a_unit[count_i], water_u_interpolation_a_unit[count_i]))
    ocean_current_b_angle[count_i] = np.rad2deg(np.arctan2(water_v_interpolation_b_unit[count_i], water_u_interpolation_b_unit[count_i]))
    

mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25

fig = plt.figure(figsize=(14, 28), dpi=100)
ax_1 = fig.add_subplot(311, xlabel=r'day', ylabel=r'inner product')
ax_2 = fig.add_subplot(312, xlabel=r'day', ylabel=r'chla move angle')
ax_3 = fig.add_subplot(313, xlabel=r'day', ylabel=r'ocean current angle')

ax_1.plot(central_point_day_resize, inner_product_all, color='orange', linewidth=4, label=r'all', alpha=0.8, zorder=10)
ax_1.plot(central_point_day_resize, inner_product_a, color='blue', linewidth=4, label=r'Point a', alpha=0.8, zorder=10)
ax_1.plot(central_point_day_resize, inner_product_b, color='green', linewidth=4, label=r'Point b', alpha=0.8, zorder=10)

ax_2.plot(central_point_day_resize, chla_move_all_angle, color='orange', linewidth=4, label=r'all', alpha=0.8, zorder=10)
ax_2.plot(central_point_day_resize, chla_move_a_angle, color='blue', linewidth=4, label=r'Point a', alpha=0.8, zorder=10)
ax_2.plot(central_point_day_resize, chla_move_b_angle, color='green', linewidth=4, label=r'Point b', alpha=0.8, zorder=10)

ax_3.plot(central_point_day_resize, ocean_current_all_angle, color='orange', linewidth=4, label=r'all', alpha=0.8, zorder=10)
ax_3.plot(central_point_day_resize, ocean_current_a_angle, color='blue', linewidth=4, label=r'Point a', alpha=0.8, zorder=10)
ax_3.plot(central_point_day_resize, ocean_current_b_angle, color='green', linewidth=4, label=r'Point b', alpha=0.8, zorder=10)



ax_1.set_ylim(-1, 1)
ax_2.set_ylim(-180, 180)
ax_3.set_ylim(-180, 180)

ax_1.set_xlim(0, np.max(central_point_day_resize)+1)
ax_2.set_xlim(0, np.max(central_point_day_resize)+1)
ax_3.set_xlim(0, np.max(central_point_day_resize)+1)

ax_1.minorticks_on()
ax_1.grid(which='both', alpha=0.3)

ax_2.minorticks_on()
ax_2.grid(which='both', alpha=0.3)

ax_3.minorticks_on()
ax_3.grid(which='both', alpha=0.3)

ax_1.legend(fontsize=20)
ax_2.legend(fontsize=20)
ax_3.legend(fontsize=20)

plt.tight_layout()

dir_name = f'/mnt/j/isee_remote_data/correlation_between_chla_ocean_current'
if not os.path.exists(dir_name):
    os.makedirs(dir_name)
figure_name = f'{dir_name}/{year:04d}{month:02d}.png'
plt.savefig(figure_name)
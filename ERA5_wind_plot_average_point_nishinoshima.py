import cdsapi
import netCDF4 as nc
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import os
from multiprocessing import Pool


dataset_short_name  = 'reanalysis-era5-pressure-levels'
product_type        = 'reanalysis'
variable_wind_EW    = 'u_component_of_wind'
variable_wind_NS    = 'v_component_of_wind'
year                = '2020'
time                = [ '00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                        '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00' ]
pressure_level      = ['1000', '975', '950', '925', '900', '875', '850', '825', '800', '775', '750', '700', '650', '600', '550', '500', '450', '400', '350']
file_format         = 'netcdf'

start_month = 8
end_month = 8

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#プロットする範囲
width_plot_1_lon = 3E0
width_plot_1_lat = 3E0
width_plot_2_lon = 2E0
width_plot_2_lat = 2E0
lat_1_max = nishinoshima_lat + width_plot_1_lat
lat_1_min = nishinoshima_lat - width_plot_1_lat
lon_1_max = nishinoshima_lon + width_plot_1_lon
lon_1_min = nishinoshima_lon - width_plot_1_lon
lat_2_max = nishinoshima_lat + width_plot_2_lat
lat_2_min = nishinoshima_lat - width_plot_2_lat
lon_2_max = nishinoshima_lon + width_plot_2_lon
lon_2_min = nishinoshima_lon - width_plot_2_lon

area = [lat_1_max, lon_1_min, lat_1_min, lon_1_max]

mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25

#ファイルの確認
def check_file_exists(filename):
    if os.path.isfile(filename):
        return True
    else:
        return False
    
#ファイル取得
def get_netcdf(variable, month, day, pressure_level):
    mm = str(month).zfill(2)
    dd = str(day).zfill(2)
    download_file_name = f'{year}{mm}{dd}_pressurelevel_{pressure_level}_{variable}.nc'
    if (check_file_exists(download_file_name) == False):
        c = cdsapi.Client()
        c.retrieve(dataset_short_name,
            {
                "variable":         variable,
                "pressure_level":   pressure_level,
                "product_type":     product_type,
                "year":             year,
                "month":            mm,
                "day":              dd,
                "time":             time,
                "format":           file_format,
                "area":             area
            },
            download_file_name)
    return download_file_name


#データ読み込み
def data_read(file_name, wind_kind):
    nc_file = nc.Dataset(file_name)
    lons = nc_file.variables['longitude'][:]
    lats = nc_file.variables['latitude'][:]
    time = nc_file.variables['time'][:]
    wind = nc_file.variables[wind_kind][:].squeeze()
    nc_file.close()
    return wind, lons, lats, time


#西之島周辺の4点のデータを取得&双線形補間を行う
def data_interpolation(wind, lons, lats):
    #西之島の座標を囲む4点の座標を取得
    lon_1 = lons[np.where(lons < nishinoshima_lon)][-1]
    lon_2 = lons[np.where(lons > nishinoshima_lon)][0]
    lat_1 = lats[np.where(lats < nishinoshima_lat)][0]
    lat_2 = lats[np.where(lats > nishinoshima_lat)][-1]
    #西之島の座標を囲む4点のデータを取得
    wind_11 = wind[np.where(lons == lon_1)[0][0], np.where(lats == lat_1)[0][0]]
    wind_12 = wind[np.where(lons == lon_1)[0][0], np.where(lats == lat_2)[0][0]]
    wind_21 = wind[np.where(lons == lon_2)[0][0], np.where(lats == lat_1)[0][0]]
    wind_22 = wind[np.where(lons == lon_2)[0][0], np.where(lats == lat_2)[0][0]]
    #双線形補間
    wind_interpolation = (wind_11 * (lon_2 - nishinoshima_lon) * (lat_2 - nishinoshima_lat) + wind_21 * (nishinoshima_lon - lon_1) * (lat_2 - nishinoshima_lat) + wind_12 * (lon_2 - nishinoshima_lon) * (nishinoshima_lat - lat_1) + wind_22 * (nishinoshima_lon - lon_1) * (nishinoshima_lat - lat_1)) / ((lon_2 - lon_1) * (lat_2 - lat_1))

    return wind_interpolation


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
        


#csvファイルの作成
def main(month, pressure_idx):
    path_dir_name = f'/mnt/j/isee_remote_data/{dataset_short_name}/wind_average_point_nishinoshima/'

    #画像データが既にあるかの確認
    csv_file_name = f'{year}{str(month).zfill(2)}_pressure_{pressure_idx}.csv'
    path_csv_name = f'{path_dir_name}{csv_file_name}'
    print(path_csv_name)

    if (check_file_exists(path_csv_name) == True):
        return

    if (check_file_exists(path_dir_name) == False):
        try:
            os.makedirs(path_dir_name)
        except FileExistsError:
            pass
    

    #データを作成してcsvファイルに書き込む
    with open(path_csv_name, mode='w') as f:

        f.write('day, EW_wind_average_interpolation, NS_wind_average_interpolation, wind_speed_average, wind_angle_average\n')

        for day in range(1, 32):

            if (time_check(month, day) == False):
                continue

            file_name_EW = get_netcdf(variable_wind_EW, month, day, pressure_idx)
            file_name_NS = get_netcdf(variable_wind_NS, month, day, pressure_idx)

            EW_wind, EW_lons, EW_lats, EW_time = data_read(file_name_EW, 'u')
            NS_wind, NS_lons, NS_lats, NS_time = data_read(file_name_NS, 'v')

            EW_wind_average = EW_wind[0] * 0E0
            NS_wind_average = NS_wind[0] * 0E0

            for time_idx in range(24):
                EW_wind_average += EW_wind[time_idx]
                NS_wind_average += NS_wind[time_idx]
        
            EW_wind_average /= 24E0
            NS_wind_average /= 24E0

            EW_wind_average_interpolation = data_interpolation(EW_wind_average, EW_lons, EW_lats)
            NS_wind_average_interpolation = data_interpolation(NS_wind_average, NS_lons, NS_lats)

            wind_speed_average = np.sqrt(EW_wind_average_interpolation**2E0 + NS_wind_average_interpolation**2E0)
            #北を0度として時計回りに増加
            wind_angle_average = np.arctan2(EW_wind_average_interpolation, NS_wind_average_interpolation) * 180E0 / np.pi

            f.write(f'{str(day).zfill(2)}, {EW_wind_average_interpolation}, {NS_wind_average_interpolation}, {wind_speed_average}, {wind_angle_average}\n')

            #ファイルの削除
            os.remove(file_name_EW)
            os.remove(file_name_NS)
    
    return


def pressure_loop(args):
    month = args
    for pressure_idx in range(len(pressure_level)):
        pressure = pressure_level[pressure_idx]

        main(month=month, pressure_idx=pressure)

    return



#pressure_loop((8, 1))
#quit()

if (__name__ == '__main__'):
    
    #プロセス数
    num_processes = 8

    #並列処理の指定
    with Pool(processes=num_processes) as pool:
        results = []
        for month_int in range(start_month, end_month+1):
            result = pool.apply_async(pressure_loop, [month_int])
            results.append(result)
        for result in results:
            result.get()
        
    print(r'finish')
    quit()
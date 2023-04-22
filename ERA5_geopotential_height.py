import cdsapi
import netCDF4 as nc
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import os
from multiprocessing import Pool


dataset_short_name  = 'reanalysis-era5-pressure-levels'
product_type        = 'reanalysis'
variable            = 'geopotential'
year                = '2020'
time                = [ '00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                        '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00' ]
pressure_level      = ['1000', '975', '950', '925', '900', '875', '850', '825', '800', '775', '750', '700', '650', '600', '550', '500', '450', '400', '350']
file_format         = 'netcdf'

start_month = 6
end_month = 9

gravitational_acceleration = 9.80665E0  #[m s-1]

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#プロットする範囲
width_plot_1_lon = 10E0
width_plot_1_lat = 10E0
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

area = [lat_2_max, lon_2_min, lat_2_min, lon_2_max]

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
def data_read(file_name):
    nc_file = nc.Dataset(file_name)
    lons = nc_file.variables['longitude'][:]
    lats = nc_file.variables['latitude'][:]
    time = nc_file.variables['time'][:]
    geo_altitude = nc_file.variables['z'][:].squeeze() / gravitational_acceleration
    nc_file.close()
    return geo_altitude, lons, lats, time

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
        
#画像作成
def main(args, pressure_idx):
    month, day = args
    if (time_check(month, day) == False):
        return
    
    path_dir_name = f'/mnt/j/isee_remote_data/{dataset_short_name}/geopotential_altitude/pressure_{pressure_idx}/{year}{str(month).zfill(2)}'

    #画像データが既にあるかの確認
    checker = False
    for time_idx in range(24):
        fig_name = f'{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(time_idx).zfill(2)}.png'
        path_fig_name = f'{path_dir_name}/{fig_name}'
        if (check_file_exists(path_fig_name) == False):
            checker = True
        else:
            try:
                with open(path_fig_name, 'rb') as f:
                    f.read()
            except (IOError, OSError):
                os.remove(path_fig_name)
                checker = True
    if (checker == False):
        return

    file_name = get_netcdf(variable=variable, month=month, day=day, pressure_level=pressure_idx)

    geo_altitude, lons, lats, time = data_read(file_name=file_name)

    if (check_file_exists(path_dir_name) == False):
        #ディレクトリの生成 (ディレクトリは要指定)
        try:
            os.makedirs(path_dir_name)
        except FileExistsError:
            pass
    
    for time_idx in range (24):
        fig_name = f'{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(time_idx).zfill(2)}.png'
        path_fig_name = f'{path_dir_name}/{fig_name}'

        if (check_file_exists(path_fig_name) == True):
            continue

        fig = plt.figure(figsize=(15, 15), dpi=75)
        ax = fig.add_subplot(111, title=f'{year}/{str(month).zfill(2)}/{str(day).zfill(2)} {str(time_idx).zfill(2)}:00 (UTC)     {pressure_idx}' + r' [$\mathrm{hPa}$]')
        cmap = plt.cm.get_cmap('nipy_spectral')
        cont = ax.contour(lons, lats, geo_altitude[time_idx], cmap=cmap)
        ax.clabel(cont, inline=True)
        ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=300, c='black')

        fig.colorbar(cont, label=r'geopotential altitude [$\mathrm{m}$]')

        fig.savefig(path_fig_name)
    
    os.remove(file_name)

    return

def pressure_loop(args):
    for pressure_idx in range(19):
        pressure = pressure_level[pressure_idx]

        main(args=args, pressure_idx=pressure)

    return

#pressure_loop([7, 1])

#quit()

if __name__ == '__main__':
    
    #プロセス数
    num_processes = 8

    #並列処理の指定
    with Pool(processes=num_processes) as pool:
        results = []
        for month_int in range(start_month, end_month+1):
            for day_int in range(1, 32):
                result = pool.apply_async(pressure_loop, [(month_int, day_int)])
                results.append(result)
        for result in results:
            result.get()
        
    print(r'finish')
    quit()
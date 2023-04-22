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

start_month = 6
end_month = 9

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
    
    path_dir_name = f'/mnt/j/isee_remote_data/{dataset_short_name}/wind/pressure_{pressure_idx}/{year}{str(month).zfill(2)}'

    #画像データが既にあるかの確認
    checker = False
    for time_idx in range(24):
        fig_name = f'{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(time_idx).zfill(2)}.png'
        path_fig_name = f'{path_dir_name}/{fig_name}'
        if (check_file_exists(path_fig_name) == False):
            checker = True
    if (checker == False):
        return

    file_name_EW = get_netcdf(variable_wind_EW, month, day, pressure_idx)
    file_name_NS = get_netcdf(variable_wind_NS, month, day, pressure_idx)

    EW_wind, EW_lons, EW_lats, EW_time = data_read(file_name_EW, 'u')
    NS_wind, NS_lons, NS_lats, NS_time = data_read(file_name_NS, 'v')
    wind_speed = np.sqrt(EW_wind**2E0 + NS_wind**2E0)

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
        
        #colorの最大値、最小値の設定
        fig = plt.figure(figsize=(15, 15), dpi=75)
        ax = fig.add_subplot(111, title=f'{year}/{str(month).zfill(2)}/{str(day).zfill(2)} {str(time_idx).zfill(2)}:00 (UTC)     {pressure_idx}' + r' [$\mathrm{hPa}$]')
        cmap = plt.cm.get_cmap('nipy_spectral')
        ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=300, c='black')
        color_Q = ax.quiver(EW_lons, EW_lats, EW_wind[time_idx], NS_wind[time_idx], wind_speed[time_idx], cmap=cmap, scale=150, headwidth=2, width=0.005)

        sm = plt.cm.ScalarMappable(cmap=cmap)
        sm.set_array(wind_speed[time_idx])
        sm.set_clim(0, 30)

        # 軸ラベルを設定する
        ax.set_xlabel('longitude')
        ax.set_ylabel('latitude')
        ax.set_xlim(lon_2_min, lon_2_max)
        ax.set_ylim(lat_2_min, lat_2_max)
        ax.minorticks_on()
        ax.grid(which='both', axis='both', alpha=0.5)

        # カラーバーを表示する
        fig.colorbar(sm, label=r'wind speed [$\mathrm{m} \, \mathrm{s}^{-1}$]')

        #画像の保存 (保存先は要指定)
        fig.savefig(path_fig_name)
        plt.close()

    #ファイルの削除
    os.remove(file_name_EW)
    os.remove(file_name_NS)

    return

def pressure_loop(args):
    for pressure_idx in range(19):
        pressure = pressure_level[pressure_idx]

        main(args=args, pressure_idx=pressure)

    return

if (__name__ == '__main__'):
    
    #プロセス数
    num_processes = 1

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
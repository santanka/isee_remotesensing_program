import urllib.request
import os
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import datetime
import numpy as np
import time
import cftime


#開始日時(1日~31日まで回す)
year_input  = 2020
start_month = 7
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

#データファイルの保存先のディレクトリ (形式: hoge/hogehoge)
dir_data = f'/mnt/j/isee_remote_data/hycom_GLBy0.08_expt_93.0/data/'
#プロットした図の保存先のディレクトリ (形式: hoge/hogehoge)
dir_figure = f'/mnt/j/isee_remote_data/hycom_GLBy0.08_expt_93.0/figure/'

mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25

#日時の指定
def time_and_date(year, month, day, hour):
    yyyy    = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month
    dd      = str(day).zfill(2)     #day
    hh      = str(hour).zfill(2)    #hour (UTC)
    mn      = '00'                  #minutes (UTC)
    return yyyy, mm, dd, hh, mn

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

#ファイルの確認
def check_file_exists(filename):
    if os.path.isfile(filename):
        return True
    else:
        return False

#以下で扱うデータはhycom GOFS3.1のものを使用
#https://www.hycom.org/dataserver/gofs-3pt1/analysis
ftp_site        = 'ftp.hycom.org'   # FTPサイトのURL
ftp_user        = 'anonymous'       # FTP接続に使用するユーザー名

def netCDF4_filename(yyyy, mm, dd, hh):
    filename = f'hycom_glby_930_{yyyy}{mm}{dd}12_t0{hh}_uv3z.nc'
    return filename

def url_name(yyyy, mm, dd, hh):
    url = f'ftp://{ftp_site}/datasets/GLBy0.08/expt_93.0/data/hindcasts/{yyyy}/{netCDF4_filename(yyyy, mm, dd, hh)}'
    return url

#データのダウンロード
def download_netCDF4(year, month, day, hour):
    yyyy, mm, dd, hh, mn = time_and_date(year=year, month=month, day=day, hour=hour)
    ftp_path = url_name(yyyy=yyyy, mm=mm, dd=dd, hh=hh)
    ftp_base = os.path.basename(ftp_path)
    local_path = os.path.join(dir_data, ftp_base)

    if (check_file_exists(local_path) == False):
        while True:
            try:
                urllib.request.urlretrieve(ftp_path, local_path)
                break
            except urllib.error.URLError as e:
                if isinstance(e.reason, str):
                # FTPのエラーであり、550エラーの場合は再試行しない
                    if '550' in str(e.reason):
                        time.sleep(5)
                        print('File not found. Retry aborted.')
                        return np.zeros(10)
                print(f"Connection failed: {e}")
                now = str(datetime.datetime.now())
                print(f'{now}     Retrying in 60 seconds...')
                time.sleep(60)

    try:
        data = xr.open_dataset(local_path, decode_times=True)
    except ValueError:
        data = xr.open_dataset(local_path, decode_times=False)

    # Trim the data
    data = data.sel(lat=slice(lat_1_min, lat_1_max), lon=slice(lon_1_min, lon_1_max))
    
    return data, local_path

#main function
def main(args):
    year_int, month_int, day_int, hour_int = args

    if (time_check(month=month_int, day=day_int) == False):
        return
    
    yyyy, mm, dd, hh, mn = time_and_date(year=year_int, month=month_int, day=day_int, hour=hour_int)

    path_dir_name = f'{dir_figure}{yyyy}{mm}/'
    fig_name = f'{path_dir_name}{yyyy}{mm}{dd}{hh}.png'
    if (check_file_exists(fig_name) == True):
        print(r'Figure already exists: ' + fig_name)
        return
    
    now = str(datetime.datetime.now())
    print(f'{now}     Now Downloading: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

    data, local_path = download_netCDF4(year=year_int, month=month_int, day=day_int, hour=hour_int)

    print(local_path)
    print(data)

    # Extract data
    water_EW = data['water_u'][0, 0, :, :]
    water_NS = data['water_v'][0, 0, :, :]
    water_speed = np.sqrt(water_EW**2 + water_NS**2)
    lon = data['lon'][:]
    lat = data['lat'][:]

    #ディレクトリの生成 (ディレクトリは要指定)
    try:
        os.makedirs(path_dir_name)
    except FileExistsError:
        pass

    fig = plt.figure(figsize=(15, 15))
    ax = fig.add_subplot(111, title=f'{yyyy}/{mm}/{dd} {hh}:{mn} (UTC)')
    cmap = plt.cm.get_cmap('nipy_spectral')
    ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=300, c='black')
    ax.quiver(lon, lat, water_NS, water_EW, water_speed, cmap=cmap, scale=40, headwidth=2, width=0.005)

    sm = plt.cm.ScalarMappable(cmap=cmap)
    sm.set_array(water_speed)
    sm.set_clim(0, 10)

    # Set axis labels
    ax.set_xlabel('longitude')
    ax.set_ylabel('latitude')

    # Set the plot axis range to the trimmed range
    ax.set_xlim(lon_2_min, lon_2_max)
    ax.set_ylim(lat_2_min, lat_2_max)

    ax.minorticks_on()
    ax.grid(which='both', axis='both', alpha=0.5)

    # カラーバーを表示する
    fig.colorbar(sm, label=r'water speed [$\mathrm{m/s}$]')

    #画像の保存 (保存先は要指定)
    fig.savefig(fig_name)
    plt.close()

    return


# Call the main function with individual arguments instead of a list
main([year_input, 8, 1, 0])




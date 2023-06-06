import ftplib
import os
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import datetime
import numpy as np
from multiprocessing import Pool

#JASMES SGLI標準データのChlorophyll-a濃度のデータをプロットするプログラム
#以下で扱うデータはJASMES SGLI標準データから引用
ftp_site        = 'apollo.eorc.jaxa.jp'                     # FTPサイトのURL

#プロットする日時
year_input  = 2020
start_month = 6
end_month   = 9

#データファイルの保存先のディレクトリ (形式: hoge/hogehoge)
dir_data = f''
#プロットした図の保存先のディレクトリ (形式: hoge/hogehoge)
dir_figure = f'/mnt/j/isee_remote_data/SGLI_NRT_CHLA/'

#Chlorophyll-a濃度のプロット範囲
vmin = 1E-2
vmax = 1E0

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#プロットする範囲(西之島の座標+-width)
width_plot_1_lon    = 2E0
width_plot_1_lat    = 2E0

#図の書式の指定(設定ではLaTeX調になっています)
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25

#関数を定義
#プロットする範囲
def plot_width(width_lon, width_lat):
    lon_set_min = nishinoshima_lon - width_lon
    lon_set_max = nishinoshima_lon + width_lon
    lat_set_min = nishinoshima_lat - width_lat
    lat_set_max = nishinoshima_lat + width_lat
    return lon_set_min, lon_set_max, lat_set_min, lat_set_max

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

#フォルダの生成
def mkdir_folder(path_dir_name):
    try:
        os.makedirs(path_dir_name)
        print(f'Folder is created: {path_dir_name}')
    except FileExistsError:
        pass
    return

#日時の指定、string化
def time_and_date(year, month, day, hour):
    yyyy    = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month
    dd      = str(day).zfill(2)     #day
    hh      = str(hour).zfill(2)    #hour (UTC)
    mn      = '00'                  #minutes (UTC)
    return yyyy, mm, dd, hh, mn

#ファイルのダウンロード
def download_netcdf_data(year, month, day):
    yyyy, mm, dd, _, _ = time_and_date(year, month, day, 0)
    download_folder = f'/pub/SGLI_NRT/L2_In-water_properties/CHLA/{yyyy}/{mm}/{dd}/'
    file_start = f'GC1SG1_{yyyy}{mm}{dd}'
    file_end = f'_CHLA_15.nc'

    downloaded_files = []

    with ftplib.FTP(ftp_site) as ftp:
        ftp.login()
        ftp.cwd(download_folder)
        files = ftp.nlst()
        for file in files:
            if file.startswith(file_start) and file.endswith(file_end):
                with open(f'{dir_data}{file}', 'wb') as f:
                    ftp.retrbinary(f'RETR {file}', f.write)
                downloaded_files.append(file)
    return downloaded_files

#ファイルの読み込み
def read_netcdf_data(filename):
    data = xr.open_dataset(filename)
    return data

#ファイルの加工
def process_netcdf_data(data):
    #プロットする範囲の指定
    lon_set_min, lon_set_max, lat_set_min, lat_set_max = plot_width(width_plot_1_lon, width_plot_1_lat)
    #プロットする範囲のデータを抽出
    data = data.sel(Longitude=slice(lon_set_min, lon_set_max), Latitude=slice(lat_set_max, lat_set_min))

    #Chlorophyll-a濃度のデータを抽出
    chla = data['CHLA']
    #データにadd_offsetを引く
    chla = chla - data.add_offset
    #データにscale_factorで割る
    chla = chla / data.scale_factor
    
    #これでChrolophyll-a濃度の初期データに戻せる

    #データは0-65533まで、それ以外は欠損値
    #0-65533以外をNaNに変換
    chla = chla.where((chla >= 0) & (chla <= 65533), np.nan)

    #Chlorophyll-a濃度の単位をmg/m^3に変換
    chla = chla * data.scale_factor + data.add_offset

    #データで、vmax以上のデータをvmaxに、vmin以下のデータをvminに変換、NaNはそのまま
    chla = chla.clip(vmin, vmax)

    #もし、データがすべてNaNだったら、配列を0で埋める
    if np.isnan(chla).all():
        chla = chla.fillna(0)
    
    #Lon, Latのデータを抽出
    lon = data['Longitude']
    lat = data['Latitude']

    return chla, lon, lat


#main program
def main(args):
    year_int, month_int, day_int = args

    yyyy, mm, dd, _, _ = time_and_date(year_int, month_int, day_int, 0)

    now = str(datetime.datetime.now())
    print(f'{now}     Start: {yyyy}/{mm}/{dd}')

    #日時の確認
    if time_check(month_int, day_int) == False:
        print(f'Error!: {yyyy}/{mm}/{dd} is not a valid date.')
        return
    
    #データの保存先のディレクトリを作成
    print(f'{dir_figure}{yyyy}{mm}/')
    mkdir_folder(f'{dir_figure}{yyyy}{mm}/')

    downloaded_filename = download_netcdf_data(year_int, month_int, day_int)

    for file_name in downloaded_filename:

        data_path = os.path.join(dir_data, file_name)

        #画像ファイル名
        figure_name = file_name.replace('.nc', '.png')
        print(figure_name)

        #画像ファイルがすでに存在する場合は、プロットしない
        #if check_file_exists(f'{dir_figure}{yyyy}{mm}/{figure_name}') == True:
        #    os.remove(data_path)
        #    continue

        data = read_netcdf_data(f'{dir_data}{file_name}')
        chla, lon, lat = process_netcdf_data(data)

        #chlaがすべて0だったら、プロットしない
        if np.all(chla == 0):
            os.remove(data_path)
            continue

        #ファイルの取得時間を取得
        figure_time_hour = figure_name[15:17]
        figure_time_minute = figure_name[17:19]
        figure_path_number = figure_name[20:23]

        #プロット
        fig = plt.figure(figsize=(15, 15), dpi=100)
        ax = fig.add_subplot(111, title=f'{yyyy}/{mm}/{dd} {figure_time_hour}:{figure_time_minute}(UTC) Path:{figure_path_number}', xlabel=r'longitude', ylabel=r'latitude')
        im = ax.imshow(chla, extent=[lon.min(), lon.max(), lat.min(), lat.max()], cmap='turbo', norm=LogNorm(vmin=vmin, vmax=vmax))
        ax.set_xlim(lon.min(), lon.max())
        ax.set_ylim(lat.min(), lat.max())
        ax.set_aspect('equal')
        ax.minorticks_on()
        ax.grid(which='both', axis='both', lw='0.5', alpha=0.5)
        ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=3, c='black')
        plt.colorbar(im, label=r'Chlorophyll-a [$\mathrm{mg / m^{3}}$]')

        #画像の保存
        fig.savefig(f'{dir_figure}{yyyy}{mm}/{figure_name}')
        now = str(datetime.datetime.now())
        print(f'{now}     Image is saved.: {yyyy}/{mm}/{dd}')
        plt.close()

        #データの削除
        os.remove(data_path)

    downloaded_filename = []

    return

#main((2020, 6, 1))
#quit()


#以下、非同期処理のためのコード、main関数を並列処理する
if (__name__ == '__main__'):
    
    #プロセス数
    num_processes = 8

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
import ftplib
import os
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import datetime
import numpy as np


#以下で扱うデータはJAXAひまわりモニタから引用
#https://www.eorc.jaxa.jp/ptree/userguide_j.html
ftp_site        = 'ftp.ptree.jaxa.jp'                   # FTPサイトのURL
ftp_user        = 'XXXX'   # FTP接続に使用するユーザー名
ftp_password    = 'YYYY'                            # FTP接続に使用するパスワード

#1km日本域のデータを使用(24N-50N, 123E-150Eの矩形領域)
pixel_number    = 2701
line_number     = 2601
data_lon_min, data_lon_max, data_lat_min, data_lat_max  = 123E0, 150E0, 24E0, 50E0

#プロットする日時
year_input  = 2020
month_input = 8
day_input   = 19
hour_input  = 3        #UTC

#データファイルの保存先のディレクトリ (形式: hoge/hogehoge)
dir_data = f''
#プロットした図の保存先のディレクトリ (形式: hoge/hogehoge)
dir_figure = f''

#Chlorophyll-a濃度のプロット範囲
vmin = 1E-2
vmax = 1E2

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

#日時の指定、string化
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

#フォルダの生成
def mkdir_folder(path_dir_name):
    try:
        os.makedirs(path_dir_name)
    except FileExistsError:
        pass
    return

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
        with ftplib.FTP(ftp_site) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_password)   #ログイン
            ftp.cwd('/')                                    #ルートディレクトリに移動

            with open(local_path, 'wb') as f:                 #ファイルをバイナリモードでダウンロード
                ftp.retrbinary(f'RETR {ftp_path}', f.write)
                print(r'Download file is ' + local_path)
        
    data = xr.open_dataset(ftp_base)
    return data


#main function
def main(year_int, month_int, day_int, hour_int):

    yyyy, mm, dd, hh, mn = time_and_date(year=year_int, month=month_int, day=day_int, hour=hour_int)
    fig_name    = f'{dir_figure}{yyyy}{mm}/{yyyy}{mm}{dd}{hh}{mn}.png'
    print(r'Figure name is ' + fig_name)

    #存在しない日時では、何もしない
    if (time_check(month=month_int, day=day_int) == False):
        print(f'Error!: {yyyy}/{mm}/{dd} is not existed.')
        return
    
    #ディレクトリの生成
    mkdir_folder(f'{dir_figure}{yyyy}{mm}')

    #図が存在する場合、何もしない
    if (check_file_exists(filename=fig_name) == True):
        print(f'Error!: {fig_name} is existed.')
        return
    
    now = str(datetime.datetime.now())
    print(f'{now}     Now Downloading: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

    #データ取得
    data = download_netcdf(year=year_int, month=month_int, day=day_int, hour=hour_int)  #chlorophyll-a [mg m-3]
    chlorophyll = data['chlor_a']

    #プロット範囲を制限
    plot_lon_min, plot_lon_max, plot_lat_min, plot_lat_max = plot_width(width_lon=width_plot_1_lon, width_lat=width_plot_1_lat)
    mask_region = np.logical_and(
                                np.logical_and(
                                                data['longitude'] >= plot_lon_min,
                                                data['longitude'] <= plot_lon_max
                                            ),
                                np.logical_and(
                                                data['latitude'] >= plot_lat_min,
                                                data['latitude'] <= plot_lat_max
                                            ))
    chlorophyll_mask = np.ma.masked_array(chlorophyll, ~mask_region)

    #プロット範囲にデータが存在する場合のみプロット
    if not np.isnan(chlorophyll_mask.all()):
        now = str(datetime.datetime.now())
        print(f'{now}     Now Plotting: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

        #値を[vmin, vmax]に指定
        chlorophyll_mask = xr.where(chlorophyll_mask < vmin, vmin, chlorophyll_mask)
        chlorophyll_mask = xr.where(chlorophyll_mask > vmax, vmax, chlorophyll_mask)
        chlorophyll_mask = chlorophyll_mask.astype(float)

        #プロット
        fig = plt.figure(figsize=(15, 15), dpi=100)
        ax = fig.add_subplot(111, title=f'{yyyy}/{mm}/{dd} {hh}:{mn} (UTC)', xlabel=r'longitude', ylabel=r'latitude')
        im = ax.imshow(chlorophyll_mask, extent=[data_lon_min, data_lon_max, data_lat_min, data_lat_max], cmap='turbo', norm=LogNorm(vmin=vmin, vmax=vmax))
        ax.set_xlim(plot_lon_min, plot_lon_max)
        ax.set_ylim(plot_lat_min, plot_lat_max)
        ax.minorticks_on()
        ax.grid(which='both', axis='both', lw='0.5', alpha=0.5)
        ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=3, c='black')
        plt.colorbar(im, label=r'Chlorophyll-a [$\mathrm{mg / m^{3}}$]')

        #画像の保存
        fig.savefig(fig_name)
        now = str(datetime.datetime.now())
        print(f'{now}     Image is saved.: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

        plt.close()

    else:
        now = str(datetime.datetime.now())
        print(f'{now}     Data contains only NaN.: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

    return



#実行
main(year_int=year_input, month_int=month_input, day_int=day_input, hour_int=hour_input)
print(r'Finish')
import ftplib
import os
import xarray as xr
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import datetime
from multiprocessing import Pool
import numpy as np

plt.rcParams.update({'mathtext.default': 'default', 'mathtext.fontset': 'stix'})

#以下で扱うデータの情報は千葉大学環境リモートセンシング研究センターのサイトを参照
# https://www.eorc.jaxa.jp/ptree/index_j.html

ftp_site = 'ftp.ptree.jaxa.jp'  # FTPサイトのURL
ftp_user = 'koseki.saito_stpp.gp.tohoku.ac.jp'  # FTP接続に使用するユーザー名
ftp_password = 'SP+wari8'  # FTP接続に使用するパスワード

pixel_number = 2701
line_number = 2601

lon_min, lon_max, lat_min, lat_max = 123, 150, 24, 50

year = 2020
start_month = 6
end_month = 9

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#プロットする範囲
width_plot_1_lon = 2E0
width_plot_1_lat = 2E0

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

#ダウンロードするファイルのパスの生成
def download_path(year, month, day, hour):
    yyyy, mm, dd, hh, mn = time_and_date(year, month, day, hour)
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

# FTPサイトに接続
def download_netcdf(year, month, day, hour):

    with ftplib.FTP(ftp_site) as ftp:
        ftp.login(user=ftp_user, passwd=ftp_password)  # ログイン
        ftp.cwd('/')  # ルートディレクトリに移動

        # ファイルをバイナリモードでダウンロード
        ftp_path = download_path(year, month, day, hour)
        ftp_base = os.path.basename(ftp_path)
        with open(ftp_base, 'wb') as f:
            ftp.retrbinary(f'RETR {ftp_path}', f.write)
        data = xr.open_dataset(ftp_base)
        os.remove(ftp_base)
        return data
    
#plotする範囲
def plot_width(width_lon, width_lat):
    lon_set_min = nishinoshima_lon - width_lon
    lon_set_max = nishinoshima_lon + width_lon
    lat_set_min = nishinoshima_lat - width_lat
    lat_set_max = nishinoshima_lat + width_lat
    return lon_set_min, lon_set_max, lat_set_min, lat_set_max

    
#main
def main_loop_function(args):
    year_int, month_int, day_int, hour_int = args
    yyyy, mm, dd, hh, mn = time_and_date(year_int, month_int, day_int, hour_int)

    if (time_check(month_int, day_int) == False):
        return
    if (check_file_exists(f'/mnt/j/isee_remote_data/himawari_chrolophyll/{yyyy}{mm}/{yyyy}{mm}{dd}{hh}{mn}.png') == True):
        return
    
    now = str(datetime.datetime.now())
    print(f'{now}     Now Downloading: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

    data = download_netcdf(year, month_int, day_int, hour_int) #chrolophyll-a [mg/m3]

    chrolophyll = data['chlor_a']

    #lon_range = slice(plot_width(width_plot_1_lon, width_plot_1_lat)[0], plot_width(width_plot_1_lon, width_plot_1_lat)[1])
    #lat_range = slice(plot_width(width_plot_1_lon, width_plot_1_lat)[2], plot_width(width_plot_1_lon, width_plot_1_lat)[3])
    #chrolophyll_mask = chrolophyll.sel(longitude = lon_range)
    #chrolophyll_mask = chrolophyll.sel(latitude = lat_range)
#
    #print(data)
    #print(chrolophyll_mask)
    
    width = np.logical_and( np.logical_and(data['longitude'] >= plot_width(width_plot_1_lon, width_plot_1_lat)[0],
                                            data['longitude'] <= plot_width(width_plot_1_lon, width_plot_1_lat)[1]),
                            np.logical_and(data['latitude'] >= plot_width(width_plot_1_lon, width_plot_1_lat)[2],
                                            data['latitude'] <= plot_width(width_plot_1_lon, width_plot_1_lat)[3]))
    chrolophyll_mask = np.ma.masked_array(chrolophyll, ~width)

    
    if not np.isnan(chrolophyll_mask).all():
        now = str(datetime.datetime.now())
        print(f'{now}     Now Plotting: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

        chrolophyll = xr.where(chrolophyll < 1E-2, 1E-2, chrolophyll)
        chrolophyll = xr.where(chrolophyll > 1E2, 1E2, chrolophyll)
        chrolophyll = chrolophyll.astype(float)


        fig = plt.figure(figsize=(10, 6), dpi=200)

        ax1 = fig.add_subplot(111, title=f'{yyyy}/{mm}/{dd} {hh}:{mn} (UTC)', xlabel=r'longitude', ylabel=r'latitude')
        #contourf_1 = ax1.contourf(data['longitude'], data['latitude'], chrolophyll, cmap='winter', vmin='-40', vmax='40')
        im = ax1.imshow(chrolophyll, extent=[lon_min, lon_max, lat_min, lat_max], cmap='jet', norm=LogNorm(vmin=1E-2, vmax=1E2))
        ax1.set_xlim(plot_width(width_plot_1_lon, width_plot_1_lat)[0], plot_width(width_plot_1_lon, width_plot_1_lat)[1])
        ax1.set_ylim(plot_width(width_plot_1_lon, width_plot_1_lat)[2], plot_width(width_plot_1_lon, width_plot_1_lat)[3])
        ax1.grid(which='both', axis='both', lw='0.5', alpha=0.5)
        ax1.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=3, c='black')
        plt.colorbar(im, label=r'Chlorophyll-a $[\mathrm{mg/m^{3}}]$')

        #ディレクトリの生成 (ディレクトリは要指定)
        try:
            os.makedirs(f'/mnt/j/isee_remote_data/himawari_chrolophyll/{yyyy}{mm}')
        except FileExistsError:
            pass
            
        #画像の保存 (保存先は要指定)
        fig.savefig(f'/mnt/j/isee_remote_data/himawari_chrolophyll/{yyyy}{mm}/{yyyy}{mm}{dd}{hh}{mn}.png')
        now = str(datetime.datetime.now())
        print(f'{now}     Image is saved.: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')
    
    else:
        now = str(datetime.datetime.now())
        print(f'{now}     Data contains only NaN.: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')
    return

#main_loop_function([2022, 10, 10, 1])

##並列処理
if __name__ == '__main__':
    
    #プロセス数
    num_processes = 1

    #並列処理の指定
    with Pool(processes=num_processes) as pool:
        pool.map(main_loop_function, 
                 [(year, month_int, day_int, hour_int) 
                  for month_int in range(start_month, end_month+1)
                  for day_int in range(1, 32)
                  for hour_int in range(0, 24)],
                  chunksize=1)
        
print('finish')
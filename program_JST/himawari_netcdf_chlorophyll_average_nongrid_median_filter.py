import ftplib
import os
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import datetime
import numpy as np
from multiprocessing import Pool


#以下で扱うデータはJAXAひまわりモニタから引用
#https://www.eorc.jaxa.jp/ptree/userguide_j.html
ftp_site        = 'ftp.ptree.jaxa.jp'                   # FTPサイトのURL
ftp_user        = 'koseki.saito_stpp.gp.tohoku.ac.jp'   # FTP接続に使用するユーザー名
ftp_password    = 'SP+wari8'                            # FTP接続に使用するパスワード

#1km日本域のデータを使用(24N-50N, 123E-150Eの矩形領域)
pixel_number    = 2701
line_number     = 2601
data_lon_min, data_lon_max, data_lat_min, data_lat_max  = 123E0, 150E0, 24E0, 50E0

#プロットする日時
year_input  = 2020
start_month = 6
end_month   = 8
#month_input = 8
#day_input   = 1

#データファイルの保存先のディレクトリ (形式: hoge/hogehoge)
dir_data = f''
#プロットした図の保存先のディレクトリ (形式: hoge/hogehoge)
dir_figure = f'/mnt/j/isee_remote_data/JST/himawari_chlorophyll_average_nongrid_median_filter_10_-1_0'

#Chlorophyll-a濃度のプロット範囲
vmin = 1E-1
vmax = 1E0

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#プロットする範囲(西之島の座標+-width)
width_plot_1_lon    = 2E0
width_plot_1_lat    = 2E0
width_plot_lat_list = [2E0, 1E0, 5E-1]
width_plot_lon_list = [2E0, 1E0, 5E-1]
width_plot_name_list = ['2', '1', '0.5']

#カラーマップの設定
#White to Black
cdicts = {  'red':  [[0.0, 1.0, 1.0],
                    [1.0, 0.0, 0.0]],
            'green':[[0.0, 1.0, 1.0],
                    [1.0, 0.0, 0.0]],
            'blue': [[0.0, 1.0, 1.0],
                    [1.0, 0.0, 0.0]]}
my_cmap = mpl.colors.LinearSegmentedColormap('whitetoblack', cdicts, 256)

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
        try:
            with ftplib.FTP(ftp_site) as ftp:
                ftp.login(user=ftp_user, passwd=ftp_password)   #ログイン
                ftp.cwd('/')                                    #ルートディレクトリに移動

                with open(local_path, 'wb') as f:                 #ファイルをバイナリモードでダウンロード
                    ftp.retrbinary(f'RETR {ftp_path}', f.write)
                    print(r'Download file is ' + local_path)
        except Exception as e:
            print(f"Error downloading file: {e}")
            return np.zeros((line_number, pixel_number))
        
    data = xr.open_dataset(ftp_base)
    if data is None:
        return np.zeros((line_number, pixel_number))
    os.remove(local_path)
    return data

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


def calculate_daily_mean(year_int, month_int, day_int):
    data_sum = None
    count_zeros = None

    for hour_int in range(24):

        #9時から15時までのデータを使用
        if (hour_int < 9) or (hour_int > 15):
            continue
        
        else:
            year_UTC_int, month_UTC_int, day_UTC_int, hour_UTC_int = JST_to_UTC(year=year_int, month=month_int, day=day_int, hour=hour_int)
            yyyy, mm, dd, hh, mn = time_and_date(year=year_UTC_int, month=month_UTC_int, day=day_UTC_int, hour=hour_UTC_int)
            now = str(datetime.datetime.now())
            print(f'{now}     Now Downloading: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

            data = download_netcdf(year_UTC_int, month_UTC_int, day_UTC_int, hour_UTC_int)
            if data.all() != 0:
                try:
                    data_filled = data['chlor_a'].fillna(0)  # 欠損値に0を代入
                except KeyError as e:
                    print(f"Error: {e}")
                    print(f"Dataset keys: {data.keys()}")
                    data = np.zeros((line_number, pixel_number))
                
                if data_sum is None:
                    data_sum = data_filled
                    count_zeros = (data_filled == 0).astype(int)  # 0の回数をカウントするためのマスクを作成
                    #print(hour_int, r'count_zeros is initialized.', r'data exists.')
                else:
                    data_sum += data_filled
                    count_zeros += (data_filled == 0).astype(int)
                    #print(hour_int, r'count_zeros is added.', r'data exists.')
            else:
                if hour_int == 9:
                    data_sum = np.zeros((line_number, pixel_number))
                    count_zeros = np.ones((line_number, pixel_number)).astype(int)
                    #print(hour_int, r'count_zeros is initialized.', r'data does not exist.')
                else:
                    count_zeros += np.ones((line_number, pixel_number)).astype(int)
                    #print(hour_int, r'count_zeros is added.', r'data does not exist.')

    divisor = 7 * np.ones((line_number, pixel_number)).astype(int) - count_zeros   # 7(9-15時)から0の回数を引く

    daily_mean = xr.where(divisor != 0, data_sum / divisor, 0)  # 各グリッドごとに24-count_iで割る、もし24-count_i=0の場合は0を与える

    return daily_mean

#main function
def main(args):

    year_int, month_int, day_int = args

    yyyy, mm, dd, hh, mn = time_and_date(year=year_int, month=month_int, day=day_int, hour=0)

    

    #存在しない日時では、何もしない
    if (time_check(month=month_int, day=day_int) == False):
        print(f'Error!: {yyyy}/{mm}/{dd} is not existed.')
        return
    


    #図が存在する場合、何もしない
    if (check_file_exists(f'{dir_figure}_{width_plot_name_list[0]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png') == True):
        if (check_file_exists(f'{dir_figure}_{width_plot_name_list[1]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png') == True):
            if (check_file_exists(f'{dir_figure}_{width_plot_name_list[2]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png') == True):
                print(f'Error!: {yyyy}/{mm}/{dd} is existed.')
                return
    
    #日平均値の計算
    chlorophyll_daily_mean = calculate_daily_mean(year_int, month_int, day_int)

    now = str(datetime.datetime.now())
    print(f'{now}     Now Plotting: {yyyy}/{mm}/{dd}')

    #メディアンフィルターをかける
    chlorophyll_daily_mean = chlorophyll_daily_mean.astype(float)
    chlorophyll_daily_mean = chlorophyll_daily_mean.where(chlorophyll_daily_mean != 0, np.nan)
    chlorophyll_daily_mean = chlorophyll_daily_mean.rolling(longitude=4, latitude=4, center=True).median()

    #値を[vmin, vmax]に指定
    chlorophyll_daily_mean = xr.where((chlorophyll_daily_mean < vmin) & (chlorophyll_daily_mean != 0), vmin, chlorophyll_daily_mean)
    chlorophyll_daily_mean = xr.where(chlorophyll_daily_mean > vmax, vmax, chlorophyll_daily_mean)
    chlorophyll_daily_mean = chlorophyll_daily_mean.astype(float)
    chlorophyll_daily_mean = chlorophyll_daily_mean.where(chlorophyll_daily_mean != 0, np.nan)

    #plot_lon_min, plot_lon_max, plot_lat_min, plot_lat_max = plot_width(width_lon=width_plot_1_lon, width_lat=width_plot_1_lat)
    ##プロット
    #fig = plt.figure(figsize=(15, 15), dpi=100)
    #ax = fig.add_subplot(111, title=f'{yyyy}/{mm}/{dd}', xlabel=r'longitude', ylabel=r'latitude')
    #im = ax.imshow(chlorophyll_daily_mean, extent=[data_lon_min, data_lon_max, data_lat_min, data_lat_max], cmap='turbo', norm=LogNorm(vmin=vmin, vmax=vmax))
    #ax.set_xlim(plot_lon_min, plot_lon_max)
    #ax.set_ylim(plot_lat_min, plot_lat_max)
    #ax.minorticks_on()
    #ax.grid(which='both', axis='both', lw='0.5', alpha=0.5)
    #ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=3, c='black')
    #plt.colorbar(im, label=r'Chlorophyll-a [$\mathrm{mg / m^{3}}$]')
    ##画像の保存
    #fig.savefig(fig_name)
    #now = str(datetime.datetime.now())
    #print(f'{now}     Image is saved.: {yyyy}/{mm}/{dd}')
    #plt.close()

    for count_i in range(len(width_plot_lon_list)):
        #ディレクトリの生成
        mkdir_folder(f'{dir_figure}_{width_plot_name_list[count_i]}/{yyyy}{mm}')
        fig_name    = f'{dir_figure}_{width_plot_name_list[count_i]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png'
        plot_lon_min, plot_lon_max, plot_lat_min, plot_lat_max = plot_width(width_lon=width_plot_lon_list[count_i], width_lat=width_plot_lat_list[count_i])
        #プロット
        fig = plt.figure(figsize=(15, 15), dpi=200, facecolor='black')
        ax = fig.add_subplot(111)
        im = ax.imshow(chlorophyll_daily_mean, extent=[data_lon_min, data_lon_max, data_lat_min, data_lat_max], cmap=my_cmap, norm=LogNorm(vmin=vmin, vmax=vmax))
        ax.set_xlim(plot_lon_min, plot_lon_max)
        ax.set_ylim(plot_lat_min, plot_lat_max)
        fig.subplots_adjust(left=0, bottom=0, right=1, top=1, wspace=None, hspace=None)
        fig.savefig(fig_name)
        plt.close()

    now = str(datetime.datetime.now())
    print(f'{now}     Image is saved.: {yyyy}/{mm}/{dd}')

    return



#実行
#main((year_input, 8, 1))
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
import urllib.request
import os
import bz2
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import tarfile
import datetime
from multiprocessing import Pool
import time

#以下で扱うデータの情報は千葉大学環境リモートセンシング研究センターのサイトを参照
# http://www.cr.chiba-u.jp/databases/GEO/H8_9/FD/index_jp_V20190123.html

#バンド帯の指定
band_11 = '09'
band_13 = '01'
band_14 = '02'
band_15 = '03'

#データのサイズ
pixel_number = 6000
line_number = 6000

#座標系の定義
lon_min, lon_max = 85, 205
lat_min, lat_max = -60, 60

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#プロットする範囲
width_plot_1_lon = 10E0
width_plot_1_lat = 10E0
width_plot_2_lon = 2E0
width_plot_2_lat = 2E0

width_plot_lat_list = [5E0, 2E0, 1E0, 5E-1]
width_plot_lon_list = [5E0, 2E0, 1E0, 5E-1]
width_plot_name_list = ['5', '2', '1', '0.5']

#開始日時(1日~31日まで回す)
year_int = 2020
start_month = 8
end_month = 8

#日時の指定
def time_and_date(year, month, day, hour):
    yyyy    = str(year).zfill(2)                #year
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

#ファイルの名前の指定
def bz2_filename(yyyy, mm, dd, hh, mn, band):
    return f'{yyyy}{mm}{dd}{hh}{mn}.tir.{band}.fld.geoss.bz2'

#ファイルのURLの指定
def url_name(yyyy, mm, fname):
    return f'ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/{yyyy}{mm}/TIR/{fname}'

#ファイルのダウンロード、解凍、データ取得、ファイル削除
def file_data_get(url, fname, band):
    if (check_file_exists(fname) == False):
        filename_url = os.path.basename(url)
        while True:
            try:
                urllib.request.urlretrieve(url, filename_url)
                time.sleep(1)
                break
            except urllib.error.URLError as e:
                print(f"Connection failed: {e}")
                #if isinstance(e.reason, str):
                # FTPのエラーであり、550エラーの場合は再試行しない
                if '550' in str(e.reason):
                    time.sleep(5)
                    print('File not found. Retry aborted.')
                    return np.zeros(10)
                else:
                    now = str(datetime.datetime.now())
                    print(f'{now}     Retrying in 30 seconds...')
                    time.sleep(30)
    _, tbb = np.loadtxt(f"count2tbb_v103/tir.{band}", unpack=True)
    try:
        with bz2.BZ2File(fname) as bz2file:
            dataDN = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number, line_number)
            data = np.float32(tbb[dataDN])
    except EOFError:
        print(f"Error: Compressed file ended before the end-of-stream marker was reached")
        return np.zeros(10)
    os.remove(fname)
    return data

#Ash RGB データ作成(inverse=1で反転、0で反転しない)
def AshRGB_data(data, min_K, max_K, gamma, inverse):
    if(inverse == 0):
        #print(r'not inverse')
        data_RGB = (max_K - data) / (max_K - min_K)
    elif(inverse == 1):
        #print(r'inverse')
        data_RGB = (data - min_K) / (max_K - min_K)
    else:
        print(r'error!: inverse value')
        quit()
    #print(r'check 1')
    data_RGB[data_RGB < 0] = 0E0
    #print(r'check 2')
    data_RGB[data_RGB > 1] = 1E0
    #print(r'check 3')
    data_RGB = data_RGB**gamma
    #print(r'check 4')
    return data_RGB

#plotする範囲
def plot_width(width_lon, width_lat):
    lon_set_min = nishinoshima_lon - width_lon
    lon_set_max = nishinoshima_lon + width_lon
    lat_set_min = nishinoshima_lat - width_lat
    lat_set_max = nishinoshima_lat + width_lat
    return lon_set_min, lon_set_max, lat_set_min, lat_set_max

mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25


#放射輝度への変換テーブルのダウンロード
cfname = "count2tbb_v103.tgz"
if (check_file_exists(cfname) == False):
    url_cfname = f"ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/support/{cfname}"
    filename_url_cfname = os.path.basename(url_cfname)
    while True:
        try:
            urllib.request.urlretrieve(url_cfname, filename_url_cfname)
            break
        except urllib.error.URLError as e:
            print(f"Connection failed: {e}")
            now = str(datetime.datetime.now())
            print(f'{now}     Retrying in 60 seconds...')
            time.sleep(60)
    # tarファイルを解凍する
    with tarfile.open(filename_url_cfname, 'r:gz') as tar:
        tar.extractall()

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

#ループ
def main_loop_function(args):
    month_int, day_int = args
    yyyy, mm, dd, hh, mn = time_and_date(year_int, month_int, day_int, 0)

    if (time_check(month_int, day_int) == False):
        return
    
    if (check_file_exists(f'/mnt/j/isee_remote_data/JST/himawari_AshRGB_enlarged_average_nongrid_{width_plot_name_list[0]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png') == True):
        if (check_file_exists(f'/mnt/j/isee_remote_data/JST/himawari_AshRGB_enlarged_average_nongrid_{width_plot_name_list[1]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png') == True):
            if (check_file_exists(f'/mnt/j/isee_remote_data/JST/himawari_AshRGB_enlarged_average_nongrid_{width_plot_name_list[2]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png') == True):
                if (check_file_exists(f'/mnt/j/isee_remote_data/JST/himawari_AshRGB_enlarged_average_nongrid_{width_plot_name_list[3]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png') == True):
                    return
    
    data_red_save = np.zeros((6000, 6000))
    data_green_save = np.zeros((6000, 6000))
    data_blue_save = np.zeros((6000, 6000))
    error_count = 0
    
    for hour_int in range(24):

        if (hour_int < 9) or (hour_int > 15):
            error_count += 1
            continue

        year_UTC_int, month_UTC_int, day_UTC_int, hour_UTC_int = JST_to_UTC(int(yyyy), int(mm), int(dd), hour_int)

        yyyy, mm, dd, hh, mn = time_and_date(year_UTC_int, month_UTC_int, day_UTC_int, hour_UTC_int)

        now = str(datetime.datetime.now())
        print(f'{now}     Now Downloading: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

        fname_11 = bz2_filename(yyyy, mm, dd, hh, mn, band_11)
        fname_13 = bz2_filename(yyyy, mm, dd, hh, mn, band_13)
        fname_14 = bz2_filename(yyyy, mm, dd, hh, mn, band_14)
        fname_15 = bz2_filename(yyyy, mm, dd, hh, mn, band_15)

        url_11 = url_name(yyyy, mm, fname_11)
        url_13 = url_name(yyyy, mm, fname_13)
        url_14 = url_name(yyyy, mm, fname_14)
        url_15 = url_name(yyyy, mm, fname_15)

        data_tbb_11 = file_data_get(url_11, fname_11, band_11)
        data_tbb_13 = file_data_get(url_13, fname_13, band_13)
        data_tbb_14 = file_data_get(url_14, fname_14, band_14)
        data_tbb_15 = file_data_get(url_15, fname_15, band_15)
        if (data_tbb_11.all() == 0 or data_tbb_13.all() == 0 or data_tbb_14.all() == 0 or data_tbb_15.all() == 0):
            error_count += 1
            continue

        now = str(datetime.datetime.now())
        print(f'{now}     Downloading is finished.: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

        data_diff_tbb_11_14 = np.float32(data_tbb_11 - data_tbb_14)
        data_diff_tbb_13_15 = np.float32(data_tbb_13 - data_tbb_15)

        #Himawari Ash RGBクイックガイドを参照 (https://www.data.jma.go.jp/mscweb/ja/prod/pdf/RGB_QG_Ash_jp.pdf)
        data_red    = AshRGB_data(data_diff_tbb_13_15,  -3.0E0,   7.5E0,  1.0E0, 0)
        data_green  = AshRGB_data(data_diff_tbb_11_14,  -5.9E0,   5.1E0, 8.5E-1, 0)
        data_blue   = AshRGB_data(        data_tbb_13, 243.6E0, 303.2E0,  1.0E0, 1)

        data_red_save += data_red
        data_green_save += data_green
        data_blue_save += data_blue

    data_red_save = data_red_save / (24 - error_count)

    data_green_save = data_green_save / (24 - error_count)

    data_blue_save = data_blue_save / (24 - error_count)

    try:
        data_rgb = np.dstack((data_red_save, data_green_save, data_blue_save))
    except Exception as e:
        print(f"An error occurred: {e}")
        quit()
    
    #作図
    now = str(datetime.datetime.now())
    print(f'{now}     Now Plotting: {yyyy}/{mm}/{dd}')


    for count_i in range(len(width_plot_lon_list)):
        fig = plt.figure(figsize=(15, 15), dpi=200)
        ax1 = fig.add_subplot(111)

        try:
            ax1.imshow(data_rgb, extent=[lon_min, lon_max, lat_min, lat_max])
        except Exception as e:
            print(f"An error occurred: {e}")
        ax1.set_xlim(plot_width(width_plot_lon_list[count_i], width_plot_lat_list[count_i])[0], plot_width(width_plot_lon_list[count_i], width_plot_lat_list[count_i])[1])
        ax1.set_ylim(plot_width(width_plot_lon_list[count_i], width_plot_lat_list[count_i])[2], plot_width(width_plot_lon_list[count_i], width_plot_lat_list[count_i])[3])
        #座標軸を表示しない
        ax1.axis('off')
        fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
        #ディレクトリの生成 (ディレクトリは要指定)
        try:
            os.makedirs(f'/mnt/j/isee_remote_data/JST/himawari_AshRGB_enlarged_average_nongrid_{width_plot_name_list[count_i]}/{yyyy}{mm}')
        except FileExistsError:
            pass
        #画像の保存 (保存先は要指定)
        fig.savefig(f'/mnt/j/isee_remote_data/JST/himawari_AshRGB_enlarged_average_nongrid_{width_plot_name_list[count_i]}/{yyyy}{mm}/{yyyy}{mm}{dd}.png')
        plt.close()
    
    now = str(datetime.datetime.now())
    print(f'{now}     Plotting is finished.: {yyyy}/{mm}/{dd}')

    return
    
#main_loop_function((8, 1))
#quit()


if (__name__ == '__main__'):
    # プロセス数
    num_processes = 8   #1推奨(要するに並列処理不可: ftp最大接続数10の制限の為)

    # 非同期処理の指定
    with Pool(processes=num_processes) as pool:
        results = []
        for month_int in range(start_month, end_month+1):
            for day_int in range(1, 32):
                result = pool.apply_async(main_loop_function, [(month_int, day_int)])
                results.append(result)
        # 全ての非同期処理の終了を待機
        for result in results:
            result.get()
        
        print(r'finish')
        quit()
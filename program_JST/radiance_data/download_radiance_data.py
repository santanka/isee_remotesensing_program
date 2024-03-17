import os
import datetime
import numpy as np
from multiprocessing import Pool
import pyproj
import time
import tarfile
import urllib.request
import bz2
import pandas as pd

#千葉大学環境リモートセンシング研究センターから放射輝度のデータをダウンロード
# http://www.cr.chiba-u.jp/databases/GEO/H8_9/FD/index_jp_V20190123.html
#バンド帯の指定
TIR_number = 5
#データのサイズ
pixel_number = 6000
line_number = 6000
#座標系の定義
lon_min_radiance, lon_max_radiance, lat_min_radiance, lat_max_radiance = 85, 205, -60, 60

#ダウンロードするデータの日付を指定(夜間のみ、JST)
#開始日
start_year = 2021
start_month = 1
#終了日
end_year = 2023
end_month = 12

#西之島を中心とした範囲のデータをダウンロード
#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889
#範囲の指定(km)
range_km = 30E0

#並列処理の数(10以下)
process_number = 10

#一時データファイルの保存先 (形式: hoge/hogehoge)
dir_temp = f''
#データの保存先 (形式: hoge/hogehoge)
path_dir = f'/mnt/j/isee_remote_data/JST/himawari8_radiance_data/TIR_{TIR_number:02}'

####以下、プログラム####

now_start_time = datetime.datetime.now()
print(f'Program start time: {now_start_time}')

#データの保存先のディレクトリを作成
if not os.path.exists(os.path.dirname(path_dir)):
    os.makedirs(os.path.dirname(path_dir))

#西之島を中心とした範囲の緯度経度を計算
#西之島から南西方向にrange_km*sqrt(2)km移動した点の座標
grs80 = pyproj.Geod(ellps='GRS80')
lon_min, lat_min, _ = grs80.fwd(nishinoshima_lon, nishinoshima_lat, 225, range_km*np.sqrt(2)*1000)
#西之島から北東方向にrange_km*sqrt(2)km移動した点の座標
lon_max, lat_max, _ = grs80.fwd(nishinoshima_lon, nishinoshima_lat, 45, range_km*np.sqrt(2)*1000)

#データの座標系を定義
lon_radiance_list = np.linspace(lon_min_radiance, lon_max_radiance, pixel_number)
lat_radiance_list = np.linspace(lat_max_radiance, lat_min_radiance, line_number)

#データの範囲内のindexを取得
nishinoshima_lat_index = np.argmin(np.abs(lat_radiance_list - nishinoshima_lat))
nishinoshima_lon_index = np.argmin(np.abs(lon_radiance_list - nishinoshima_lon))
lon_radiance_list_valid_index = np.where((lon_radiance_list >= lon_min) & (lon_radiance_list <= lon_max))[0]
lat_radiance_list_valid_index = np.where((lat_radiance_list >= lat_min) & (lat_radiance_list <= lat_max))[0]
print(f'lon_radiance_list_valid_index: {lon_radiance_list_valid_index[0]} - {lon_radiance_list_valid_index[-1]}')
print(f'lat_radiance_list_valid_index: {lat_radiance_list_valid_index[0]} - {lat_radiance_list_valid_index[-1]}')
print(f'nishinoshima_lon_index: {nishinoshima_lon_index}')
print(f'nishinoshima_lat_index: {nishinoshima_lat_index}')
#[lon_radiance_list_valid_index[0]:lon_radiance_list_valid_index[-1]+1, lat_radiance_list_valid_index[0]:lat_radiance_list_valid_index[-1]+1]
print(lon_radiance_list_valid_index[0], lon_radiance_list_valid_index[-1]+1, lat_radiance_list_valid_index[0], lat_radiance_list_valid_index[-1]+1)

#放射輝度への変換テーブルのダウンロード
cfname = "count2tbb_v103.tgz"
local_path_cfname = os.path.join(dir_temp, cfname)
if (not os.path.exists(local_path_cfname)):
    url_cfname = f"ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/support/{cfname}"
    while True:
        try:
            urllib.request.urlretrieve(url_cfname, local_path_cfname)
            break
        except urllib.error.URLError as e:
            print(f"Connection failed: {e}")
            now = str(datetime.datetime.now())
            print(f'{now}     Retrying in 60 seconds...')
            time.sleep(60)
    # tarファイルを解凍する
    with tarfile.open(local_path_cfname, 'r:gz') as tar:
        tar.extractall()

#放射輝度のデータのファイル名
def bz2_filename(yyyy, mm, dd, hh, mn, band):
    return f'{yyyy}{mm}{dd}{hh}{mn}.tir.{band:02}.fld.geoss.bz2'

#ファイルのURL
def bz2_url(yyyy, mm, fname):
    return f'ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/{yyyy}{mm}/TIR/{fname}'

#ファイルのダウンロード
def bz2_download(url, fname, band):
    local_path_fname = os.path.join(dir_temp, fname)
    if not os.path.exists(local_path_fname):
        filename = os.path.basename(url)
        while True:
            # タイムアウトを60秒に設定
            try:
                with urllib.request.urlopen(url, timeout=60) as response:
                    content = response.read()
                    # ファイルに内容を書き込む
                    with open(local_path_fname, 'wb') as f:
                        print(f'Downloading {filename}...')
                        f.write(content)
                break
            except urllib.error.URLError as e:
                print(f"Connection failed: {e}")
                #if isinstance(e.reason, str):
                # FTPのエラーであり、550エラーの場合は再試行しない
                if '550' in str(e.reason):
                    time.sleep(5)
                    print('File not found. Retry aborted.')
                    return None
                elif '530' in str(e.reason):
                    now = str(datetime.datetime.now())
                    print(f'{now}     Retrying in 30 minutes...')
                    time.sleep(1800)
                else:
                    now = str(datetime.datetime.now())
                    print(f'{now}     Retrying in 30 seconds...')
                    time.sleep(30)

    _, tbb = np.loadtxt(f"count2tbb_v103/tir.{band:02}", unpack=True)
    try:
        with bz2.BZ2File(fname) as bz2file:
            # pixel_number_ashとline_number_ashは適切な値を設定する必要があります。
            dataDN = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number, line_number)
            dataDN = np.array(dataDN, copy=True)
            # 放射輝度への変換
            valid_indices = dataDN < len(tbb)
            valid_dataDN = dataDN[valid_indices]
            valid_data = tbb[valid_dataDN]
            data = np.full((pixel_number, line_number), np.nan, np.float32)
            data[valid_indices] = valid_data
    except OSError as e:
        print(f"Error opening file: {e}")
        print(f"File path: {fname}")
        data = None
    os.remove(local_path_fname)
    return data

#ダウンロードしたデータをoutput_dataに格納
def download_data(date):
    date_UTC = date - datetime.timedelta(hours=9)
    yyyy = date_UTC.year
    mm = date_UTC.month
    dd = date_UTC.day
    hh = date_UTC.hour
    mn = date_UTC.minute
    fname = bz2_filename(yyyy, f'{mm:02}', f'{dd:02}', f'{hh:02}', f'{mn:02}', TIR_number)
    url = bz2_url(f'{yyyy}', f'{mm:02}', fname)
    data = bz2_download(url, fname, TIR_number)
    if data is None:
        data_mean = np.nan
        data_max = np.nan
    else:
        #lon_min, lat_min, lon_max, lat_maxの範囲のデータを抽出
        data = data[lat_radiance_list_valid_index[0]:lat_radiance_list_valid_index[-1]+1, lon_radiance_list_valid_index[0]:lon_radiance_list_valid_index[-1]+1]
        data_mean = np.nanmean(data)
        data_max = np.nanmax(data)
    
    now = str(datetime.datetime.now())
    print(f'{now}     {date}     {data_mean}     {data_max}     {date_list.index(date)+1}/{len(date_list)}')

    return data_mean, data_max


#start_year, start_monthからend_year, end_monthまでの月のリストを作成
#start_year, start_month == end_year, end_monthの場合は、start_year, start_monthのみのリストを作成
year_month_list = []
if start_year == end_year:
    for month in range(start_month, end_month+1):
        year_month_list.append([start_year, month])
else:
    for year in range(start_year, end_year+1):
        if year == start_year:
            for month in range(start_month, 13):
                year_month_list.append([year, month])
        elif year == end_year:
            for month in range(1, end_month+1):
                year_month_list.append([year, month])
        else:
            for month in range(1, 13):
                year_month_list.append([year, month])

for year_month in year_month_list:

    now_start_time_count = datetime.datetime.now()
    print(f'Start time of this loop: {now_start_time_count}')

    now_year = year_month[0]
    now_month = year_month[1]

    #データの日付をリスト化(21:00-3:00のみ)
    start_date = datetime.datetime(now_year, now_month, 1, 0, 0, 0)
    if now_month == 12:
        end_date = datetime.datetime(now_year+1, 1, 1, 0, 0, 0)
    else:
        end_date = datetime.datetime(now_year, now_month+1, 1, 0, 0, 0)
    
    date_list = []
    date = start_date
    while date < end_date:
        if date.hour >= 21 or date.hour <= 3:
            date_list.append(date)
        date += datetime.timedelta(hours=1)
    
    #データの保存先のディレクトリを作成
    path_data = f'{path_dir}/{now_year:04}{now_month:02}_JST.csv'

    output_data = np.zeros((len(date_list), 6))
    output_data[:, 0] = np.array([date.year for date in date_list])
    output_data[:, 1] = np.array([date.month for date in date_list])
    output_data[:, 2] = np.array([date.day for date in date_list])
    output_data[:, 3] = np.array([date.hour for date in date_list])

    #並列処理
    if __name__ == '__main__':
        with Pool(processes=process_number) as pool:
            results = pool.map(download_data, date_list)
        for result, i in zip(results, range(len(results))):
            output_data[i, 4] = result[0]
            output_data[i, 5] = result[1]
    
    #output_dataでnanを含む行を削除
    output_data = output_data[~np.isnan(output_data).any(axis=1)]

    #データをcsvファイルに保存
    df = pd.DataFrame(output_data, columns=['year', 'month', 'day', 'hour', 'mean_K', 'max_K'])
    #year, month, day, hourをint型に変換
    df[['year', 'month', 'day', 'hour']] = df[['year', 'month', 'day', 'hour']].astype(int)
    df.to_csv(path_data, index=False)

    now_end_time_count = datetime.datetime.now()
    print(f'End time of this loop: {now_end_time_count}')

now_end_time = datetime.datetime.now()
print(f'Program start time: {now_start_time}')
print(f'Program end time: {now_end_time}')
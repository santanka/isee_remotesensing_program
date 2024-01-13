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
TIR_number = 9
#データのサイズ
pixel_number_ash = 6000
line_number_ash = 6000
#座標系の定義
lon_min_radiance, lon_max_radiance, lat_min_radiance, lat_max_radiance = 85, 205, -60, 60

#ダウンロードするデータの日付を指定(夜間のみ、JST)
#開始日
start_year = 2020
start_month = 1
start_day = 1
#終了日
end_year = 2020
end_month = 12
end_day = 31

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
path_data = f'/mnt/j/isee_remote_data/JST/himawari8_radiance_data/TIR_{TIR_number:02}/{start_year:04}{start_month:02}{start_day:02}_{end_year:04}{end_month:02}{end_day:02}_JST.csv'

####以下、プログラム####

#データの保存先のディレクトリを作成
if not os.path.exists(os.path.dirname(path_data)):
    os.makedirs(os.path.dirname(path_data))

#西之島を中心とした範囲の緯度経度を計算
#西之島から南西方向にrange_km*sqrt(2)km移動した点の座標
grs80 = pyproj.Geod(ellps='GRS80')
lon_min, lat_min, _ = grs80.fwd(nishinoshima_lon, nishinoshima_lat, 225, range_km*np.sqrt(2)*1000)
#西之島から北東方向にrange_km*sqrt(2)km移動した点の座標
lon_max, lat_max, _ = grs80.fwd(nishinoshima_lon, nishinoshima_lat, 45, range_km*np.sqrt(2)*1000)

#データの座標系を定義
lon_radiance_list = np.linspace(lon_min_radiance, lon_max_radiance, pixel_number_ash)
lat_radiance_list = np.linspace(lat_min_radiance, lat_max_radiance, line_number_ash)

#データの日付をリスト化(21:00-3:00のみ)
start_date = datetime.datetime(start_year, start_month, start_day, 0, 0, 0)
end_date = datetime.datetime(end_year, end_month, end_day, 23, 0, 0)

date_list = []
date = start_date
while date <= end_date:
    if date.hour >= 21 or date.hour <= 3:
        date_list.append(date)
    date += datetime.timedelta(hours=1)

output_data = np.zeros((len(date_list), 6))
output_data[:, 0] = np.array([date.year for date in date_list])
output_data[:, 1] = np.array([date.month for date in date_list])
output_data[:, 2] = np.array([date.day for date in date_list])
output_data[:, 3] = np.array([date.hour for date in date_list])

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
                else:
                    now = str(datetime.datetime.now())
                    print(f'{now}     Retrying in 10 seconds...')
                    time.sleep(10)

    _, tbb = np.loadtxt(f"count2tbb_v103/tir.{band:02}", unpack=True)
    try:
        with bz2.BZ2File(fname) as bz2file:
            # pixel_number_ashとline_number_ashは適切な値を設定する必要があります。
            dataDN = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number_ash, line_number_ash)
            data = np.float32(tbb[dataDN])
    except OSError as e:
        print(f"Error opening file: {e}")
        print(f"File path: {fname}")
        data = None
    os.remove(local_path_fname)
    return data

#ダウンロードしたデータを放射輝度に変換してoutput_dataに格納
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
        lon_min_index = np.argmin(np.abs(lon_radiance_list - lon_min))
        lat_min_index = np.argmin(np.abs(lat_radiance_list - lat_min))
        lon_max_index = np.argmin(np.abs(lon_radiance_list - lon_max))
        lat_max_index = np.argmin(np.abs(lat_radiance_list - lat_max))
        data = data[lon_min_index:lon_max_index+1, lat_min_index:lat_max_index+1]
        data_mean = np.nanmean(data)
        data_max = np.nanmax(data)
    
    now = str(datetime.datetime.now())
    print(f'{now}     {date}     {data_mean}     {data_max}   {date_list.index(date)+1}/{len(date_list)}')

    return data_mean, data_max

#並列処理
if __name__ == '__main__':
    with Pool(processes=process_number) as pool:
        results = pool.map(download_data, date_list)
    for result, i in zip(results, range(len(results))):
        output_data[i, 4] = result[0]
        output_data[i, 5] = result[1]

#データをcsvファイルに保存
df = pd.DataFrame(output_data, columns=['year', 'month', 'day', 'hour', 'mean_K', 'max_K'])
#year, month, day, hourをint型に変換
df[['year', 'month', 'day', 'hour']] = df[['year', 'month', 'day', 'hour']].astype(int)
df.to_csv(path_data, index=False)
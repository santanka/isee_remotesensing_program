import os
import ftplib
import socket
import time
import numpy as np
import xarray as xr
import datetime
from multiprocessing import Pool
import tqdm


# time range
start_year = 2020
start_month = 6
start_day = 1

end_year = 2020
end_month = 8
end_day = 31

# Mukojima location
mukojima_lon = 142.14
mukojima_lat = 27.68

# location range
width_range = 0.3
lon_min = mukojima_lon - width_range
lon_max = mukojima_lon + width_range
lat_min = mukojima_lat - width_range
lat_max = mukojima_lat + width_range

# directory
dir_name = f'/mnt/j/isee_remote_data/JST/chla_daily_average/'
file_name = f'chla_daily_average_{mukojima_lon}_{mukojima_lat}_{start_year}_{start_month}_{start_day}_{end_year}_{end_month}_{end_day}'
file_suffix = '.csv'

print(f'Output file: {dir_name}{file_name}{file_suffix}')

dir_data = f''




#並列処理の数をPCの最大コア数に設定
parallel_number = os.cpu_count()
print(f'Number of parallel processes: {parallel_number}')

# daily list
start_time = datetime.datetime(start_year, start_month, start_day)
end_time = datetime.datetime(end_year, end_month, end_day)
delta = datetime.timedelta(days=1)
daily_list = []
while start_time <= end_time:
    daily_list.append(start_time)
    start_time += delta

# data list
data_list = []
for daily in daily_list:
    data_list.append([daily.year, daily.month, daily.day, None])



#日時の指定、string化
def time_and_date(year, month, day, hour):
    yyyy    = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month
    dd      = str(day).zfill(2)     #day
    hh      = str(hour).zfill(2)    #hour (UTC)
    mn      = '00'                  #minutes (UTC)
    return yyyy, mm, dd, hh, mn

#JSTの日時をUTCに変換する関数
def JST_to_UTC(year, month, day, hour):
    JST = datetime.timezone(datetime.timedelta(hours=+9), 'JST')
    JST_time = datetime.datetime(year, month, day, hour, 0, 0, tzinfo=JST)
    UTC_time = JST_time.astimezone(datetime.timezone.utc)
    year_UTC = UTC_time.year
    month_UTC = UTC_time.month
    day_UTC = UTC_time.day
    hour_UTC = UTC_time.hour
    return year_UTC, month_UTC, day_UTC, hour_UTC

#ファイルの確認
def check_file_exists(filename):
    if os.path.isfile(filename):
        return True
    else:
        return False

######chla関連#####
#JAXAひまわりモニタからchlaのデータをダウンロード
#https://www.eorc.jaxa.jp/ptree/userguide_j.html
ftp_site        = 'ftp.ptree.jaxa.jp'                   # FTPサイトのURL
ftp_user        = 'koseki.saito_stpp.gp.tohoku.ac.jp'   # FTP接続に使用するユーザー名
ftp_password    = 'SP+wari8'                            # FTP接続に使用するパスワード
#1km日本域のデータを使用(24N-50N, 123E-150Eの矩形領域)
pixel_number_chla    = 2701
line_number_chla     = 2601
data_lon_min, data_lon_max, data_lat_min, data_lat_max  = 123E0, 150E0, 24E0, 50E0
#Chlorophyll-a濃度のプロット範囲
chla_vmin = 1E-1
chla_vmax = 1E0

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
    
    pixel = str(pixel_number_chla).zfill(5)
    line = str(line_number_chla).zfill(5)
    ftp_path = f'/pub/himawari/L3/CHL/{ver}/{yyyy}{mm}/{dd}/H{nn}_{yyyy}{mm}{dd}_{hh}{mn}_1H_rOC{ver}_FLDK.{pixel}_{line}.nc'
    return ftp_path

#ファイルのダウンロード、データを返す
def download_netcdf(year, month, day, hour):
    ftp_path = download_path(year=year, month=month, day=day, hour=hour)
    ftp_base = os.path.basename(ftp_path)
    local_path = os.path.join(dir_data, ftp_base)

    # ファイルが存在しない場合にダウンロードを試みる
    if not check_file_exists(local_path):
        while True:  # 無限に再試行
            try:
                # タイムアウトを設定（接続と読み取りのタイムアウト）
                with ftplib.FTP(ftp_site, timeout=30) as ftp:
                    ftp.login(user=ftp_user, passwd=ftp_password)  # ログイン
                    ftp.cwd('/')  # ルートディレクトリに移動

                    # ファイルをバイナリモードでダウンロード
                    with open(local_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {ftp_path}', f.write)
                        #print(r'Download file is ' + local_path)
                        break  # ダウンロード成功したらループを抜ける
            #データがない場合は0のデータを返す
            #それ以外のエラーは10秒間待機して再試行
            except tuple(ftplib.all_errors) + (socket.timeout,) as e:
                print(f"Error downloading file: {e}")
                error_message = str(e)
                #dデータがない場合は0のデータを返す
                if '550' in error_message:
                    print("No data available.")
                    return np.zeros((line_number_chla, pixel_number_chla))
                else:
                    print("Wait for 10 seconds before retrying...")
                    time.sleep(10)  # 10秒間待機して再試行
    
    try:
        data = xr.open_dataset(local_path, engine='h5netcdf')
    except OSError as e:
        print(f"Error opening file: {e}")
        print(f"File path: {local_path}")
        data = None
    
    if data is None:
        return np.zeros((line_number_chla, pixel_number_chla))
    os.remove(local_path)
    return data

def calculate_average(year_JST, month_JST, day_JST):
    data_sum = None
    count_zeros = None
    
    for hour in range(24):
        year_UTC, month_UTC, day_UTC, hour_UTC = JST_to_UTC(year_JST, month_JST, day_JST, hour)
        data = download_netcdf(year=year_UTC, month=month_UTC, day=day_UTC, hour=hour_UTC)
        if data.all() != 0:
            try:
                data_filled = data['chlor_a'].fillna(0)  # 欠損値に0を代入
            except KeyError as e:
                print(f"Error: {e}")
                print(f"Dataset keys: {data.keys()}")
                data_filled = np.zeros((line_number_chla, pixel_number_chla))
            
            if data_sum is None:
                data_sum = data_filled
                count_zeros = (data_filled == 0).astype(int)  # 0の回数をカウントするためのマスクを作成
            else:
                data_sum += data_filled
                count_zeros += (data_filled == 0).astype(int)
        else:
            if data_sum is None:
                data_sum = np.zeros((line_number_chla, pixel_number_chla))
                count_zeros = np.ones((line_number_chla, pixel_number_chla)).astype(int)
            else:
                count_zeros += np.ones((line_number_chla, pixel_number_chla)).astype(int)
    
    divisor = 24 * np.ones((line_number_chla, pixel_number_chla)) - count_zeros
    data_average = xr.where(divisor != 0, data_sum / divisor, 0)

    return data_average


# chlaのメディアンフィルタ
def median_filter_chla(data, filter_size):
    data = data.astype(np.float64)
    data = data.rolling(longitude=filter_size, latitude=filter_size, center=True).median()

    data = xr.where((data < chla_vmin) & (data != 0), chla_vmin, data)
    data = xr.where(data > chla_vmax, chla_vmax, data)
    data = data.astype(np.float64)
    data = data.where(data != 0, np.nan)
    return data


# main (data_listの更新)
def main_daily(count_number):
    year_JST, month_JST, day_JST, _ = data_list[count_number]
    data_average = calculate_average(year_JST, month_JST, day_JST)
    data_average = median_filter_chla(data_average, filter_size=3)
    data_average = data_average.sel(longitude=slice(lon_min, lon_max), latitude=slice(lat_max, lat_min))
    data_area_average = data_average.mean(dim=('longitude', 'latitude'))
    data_area_average = data_area_average.astype(np.float64).values
    return data_area_average

if not os.path.exists(dir_name):
    os.makedirs(dir_name)

#並列処理(tqdmを使用) (data_listの更新に注意)
if __name__ == '__main__':
    with Pool(parallel_number) as p:
        for result in tqdm.tqdm(p.imap(main_daily, range(len(data_list))), total=len(data_list)):
            count_number = data_list.index([data for data in data_list if data[3] is None][0])
            data_list[count_number][3] = result

#データの保存
with open(f'{dir_name}{file_name}{file_suffix}', 'w') as f:
    f.write('year,month,day,chla\n')
    for data in data_list:
        year, month, day, data_area_average = data
        f.write(f'{year},{month},{day},{data_area_average}\n')

print('Finish')
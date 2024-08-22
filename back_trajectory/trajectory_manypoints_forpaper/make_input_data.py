import os
import numpy as np
import datetime
import ftplib
import xarray as xr
import socket
import time

# directory
back_or_forward = 'forward'
input_condition = 2

def file_name_input(back_or_forward, input_condition):
    dir_1 = f'/mnt/j/isee_remote_data/JST/'
    if back_or_forward == 'back':
        dir_2 = f'back_trajectory_manypoints_forpaper/back_trajectory_condition_{input_condition}/'
    elif back_or_forward == 'forward':
        dir_2 = f'forward_trajectory_manypoints_forpaper/forward_trajectory_condition_{input_condition}/'
    file_input = f'initial_condition_{input_condition}'
    file_name = dir_1 + dir_2 + file_input
    return file_name

make_input_data_name = file_name_input(back_or_forward, input_condition)
file_name_time = make_input_data_name + '_time.csv'
file_name_point = make_input_data_name + '_point.csv'
print(file_name_time, file_name_point)
os.makedirs(os.path.dirname(file_name_time), exist_ok=True)

if os.path.isfile(file_name_time):
    print('file exists')
    quit()


# start time
start_year = 2020
start_month = 6
start_day = 28
start_hour = 18

# end time
end_year = 2020
end_month = 7
end_day = 6
end_hour = 12

start_time = datetime.datetime(start_year, start_month, start_day, start_hour)
end_time = datetime.datetime(end_year, end_month, end_day, end_hour)

print(start_time)
print(end_time)

if start_time > end_time and back_or_forward == 'forward':
    print('start time is later than end time')
    quit()
if start_time < end_time and back_or_forward == 'back':
    print('start time is earlier than end time')
    quit()

# Nishinoshima location
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

# chla region
chla_lon_min = 140.9
chla_lon_max = 142.6
chla_lat_min = 27.3
chla_lat_max = 28.9

chla_grid_interval = 0.05
chla_vmin_input = 0.15
nan_input = True
all_input = False

chla_lon = np.arange(chla_lon_min, chla_lon_max+1E-6, chla_grid_interval)
chla_lat = np.arange(chla_lat_min, chla_lat_max+1E-6, chla_grid_interval)
CHLA_LON, CHLA_LAT = np.meshgrid(chla_lon, chla_lat)


# time fileに書き込む
with open(file_name_time, 'w') as f:
    f.write(f'{start_year},{start_month},{start_day},{start_hour}\n')
    f.write(f'{end_year},{end_month},{end_day},{end_hour}\n')
    f.write(f'{chla_lon_min},{chla_lon_max},{chla_lat_min},{chla_lat_max}\n')
    f.write(f'{chla_grid_interval},{chla_vmin_input},{nan_input},{all_input}\n')

#JAXAひまわりモニタからchlaのデータをダウンロード
#https://www.eorc.jaxa.jp/ptree/userguide_j.html
ftp_site        = 'ftp.ptree.jaxa.jp'                   # FTPサイトのURL
ftp_user        = 'koseki.saito_stpp.gp.tohoku.ac.jp'   # FTP接続に使用するユーザー名
ftp_password    = 'SP+wari8'                            # FTP接続に使用するパスワード
#1km日本域のデータを使用(24N-50N, 123E-150Eの矩形領域)
pixel_number_chla    = 2701
line_number_chla     = 2601
data_lon_min, data_lon_max, data_lat_min, data_lat_max  = 123E0, 150E0, 24E0, 50E0
data_lon_array = np.linspace(data_lon_min, data_lon_max, pixel_number_chla)
data_lat_array = np.linspace(data_lat_min, data_lat_max, line_number_chla)
interval_lon = data_lon_array[1] - data_lon_array[0]
interval_lat = data_lat_array[1] - data_lat_array[0]
#Chlorophyll-a濃度のプロット範囲
chla_vmin = 1E-1
chla_vmax = 1E0

dir_data = f''



######chla関連#####

#ファイルの確認
def check_file_exists(filename):
    if os.path.isfile(filename):
        return True
    else:
        return False

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
                        print(r'Download file is ' + local_path)
                        break  # ダウンロード成功したらループを抜ける
            #データがない場合は0のデータを返す
            #それ以外のエラーは10秒間待機して再試行
            except tuple(ftplib.all_errors) + (socket.timeout,) as e:
                print(f"Error downloading file: {e}")
                #dデータがない場合は0のデータを返す
                if e.args[0].startswith('550'):
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

def calculate_7hours_average(year, month, day, hour):
    data_sum = None
    count_zeros = None
    time_input = datetime.datetime(year, month, day, hour, 0, 0)
    time_base = time_input + datetime.timedelta(seconds=-3*60*60)

    for count_i in range(7):
        time_now = time_base + datetime.timedelta(seconds=60*60*count_i)
        data = download_netcdf(year=time_now.year, month=time_now.month, day=time_now.day, hour=time_now.hour)
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
                #print(hour_int, r'count_zeros is initialized.', r'data exists.')
            else:
                data_sum += data_filled
                count_zeros += (data_filled == 0).astype(int)
                #print(hour_int, r'count_zeros is added.', r'data exists.')
        else:
            if data_sum is None:
                data_sum = np.zeros((line_number_chla, pixel_number_chla))
                count_zeros = np.ones((line_number_chla, pixel_number_chla)).astype(int)
                #print(hour_int, r'count_zeros is initialized.', r'data does not exist.')
            else:
                count_zeros += np.ones((line_number_chla, pixel_number_chla)).astype(int)
                #print(hour_int, r'count_zeros is added.', r'data does not exist.')
        
    divisor = 7 * np.ones((line_number_chla, pixel_number_chla)).astype(int) - count_zeros
    data_average = xr.where(divisor != 0, data_sum / divisor, 0)

    return data_average


#chlaのメディアンフィルタ&vmin, vmaxの設定
def median_filter_chla(data, filter_size):
    data = data.astype(np.float64)
    data = data.rolling(longitude=filter_size, latitude=filter_size, center=True).median()
    data = data.astype(float)
    data = data.where(data != 0, np.nan)
    return data

#chlaのデータを双線形補間で取得する関数
def chla_bilinear_interporation(chla_data, latitude, longitude):
    lat_data = chla_data.coords['latitude'].values
    lon_data = chla_data.coords['longitude'].values
    
    #latitude, longitudeの座標を囲む4点の座標を取得
    lon_1 = lon_data[np.where(lon_data < longitude)][-1]
    lon_2 = lon_data[np.where(lon_data > longitude)][0]
    lat_1 = lat_data[np.where(lat_data < latitude)][0]
    lat_2 = lat_data[np.where(lat_data > latitude)][-1]

    #4点の座標を取得
    chla_11 = chla_data.sel(latitude=lat_1, longitude=lon_1).values
    chla_12 = chla_data.sel(latitude=lat_1, longitude=lon_2).values
    chla_21 = chla_data.sel(latitude=lat_2, longitude=lon_1).values
    chla_22 = chla_data.sel(latitude=lat_2, longitude=lon_2).values

    #双線形補間
    if chla_11 != chla_11 or chla_12 != chla_12 or chla_21 != chla_21 or chla_22 != chla_22:
        return np.nan
    else:
        chla_interpolation = (chla_11 * (lon_2 - longitude) * (lat_2 - latitude) + chla_21 * (longitude - lon_1) * (lat_2 - latitude) + chla_12 * (lon_2 - longitude) * (latitude - lat_1) + chla_22 * (longitude - lon_1) * (latitude - lat_1)) / ((lon_2 - lon_1) * (lat_2 - lat_1))
        return chla_interpolation


#chlaのデータを取得
time_start_UTC_year, time_start_UTC_month, time_start_UTC_day, time_start_UTC_hour = JST_to_UTC(start_year, start_month, start_day, start_hour)
data_chla = calculate_7hours_average(year=time_start_UTC_year, month=time_start_UTC_month, day=time_start_UTC_day, hour=time_start_UTC_hour)
data_chla = median_filter_chla(data_chla, 4)

def main(lon_main, lat_main):
    #1on_main, lat_mainを小数第二位で四捨五入
    lon = round(lon_main, 2)
    lat = round(lat_main, 2)

    if all_input == True:
        return lon, lat, chla_bilinear_interporation(data_chla, lat, lon)
    
    else:

        #lon, latに対応するchlaの値を取得
        chla = chla_bilinear_interporation(data_chla, lat, lon)
        print(lon, lat, chla)

        if nan_input == False:
            if chla > chla_vmin_input:
                return lon, lat, chla
            else:
                return None, None, None
        else:
            if chla != chla:
                return lon, lat, chla
            else:
                return None, None, None

#point fileに書き込む
with open(file_name_point, 'w') as f:
    for lon in chla_lon:
        for lat in chla_lat:
            if chla_vmin_input == chla_vmin_input:
                lon_point, lat_point, chla = main(lon, lat)
            else:
                if nan_input == False:
                    lon_point, lat_point = round(lon, 2), round(lat, 2)
                    chla = chla_bilinear_interporation(data_chla, lat_point, lon_point)
                else:
                    lon_point, lat_point, chla = main(lon, lat)

            if lon_point is not None:
                f.write(f'{lon_point},{lat_point},{chla}\n')

print('finish')
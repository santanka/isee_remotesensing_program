import ftplib
import os
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import datetime
import numpy as np
import cartopy.crs as ccrs
import requests
import re
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

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#プロットする範囲(西之島の座標+-width)
pm_number = 2
width_plot_1_lon    = pm_number
width_plot_1_lat    = pm_number

#プロットする年月
year_input  = 2020
month_input = 8

#データファイルの保存先のディレクトリ (形式: hoge/hogehoge)
dir_data = f''
#プロットした図の保存先のディレクトリ (形式: hoge/hogehoge)
dir_figure = f'/mnt/j/isee_remote_data/chla_oceanic_current_Ashvector_plot_widespace_redesign_2'

#Chlorophyll-a濃度のプロット範囲
chla_vmin = 1E-1
chla_vmax = 1E0

#海流の速度のプロット範囲
current_vmin = 0
current_vmax = 1

#データの縮小
#average_num = int((pm_number+1) / 2)
average_num = 2


#カラーマップの設定
#White to Black
cdicts = {  'red':  [[0.0, 0.0, 0.0],
                    [1.0, 0.0, 0.0]],
            'green':[[0.0, 0.0, 0.0],
                    [1.0, 0.0, 0.0]],
            'blue': [[0.0, 0.0, 0.0],
                    [1.0, 0.0, 0.0]]}
my_cmap = mpl.colors.LinearSegmentedColormap('whitetoblack', cdicts, 256)


#図の書式の指定(設定ではLaTeX調になっています)
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 25



#関数を定義
#プロットする範囲を指定する関数
def plot_width(width_lon, width_lat):
    lon_set_min = nishinoshima_lon - width_lon
    lon_set_max = nishinoshima_lon + width_lon
    lat_set_min = nishinoshima_lat - width_lat
    lat_set_max = nishinoshima_lat + width_lat
    return lon_set_min, lon_set_max, lat_set_min, lat_set_max

lon_1_min, lon_1_max, lat_1_min, lat_1_max = plot_width(width_lon=width_plot_1_lon, width_lat=width_plot_1_lat)

#日時の指定、string化
def time_and_date(year, month, day, hour):
    yyyy    = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month
    dd      = str(day).zfill(2)     #day
    hh      = str(hour).zfill(2)    #hour (UTC)
    mn      = '00'                  #minutes (UTC)
    return yyyy, mm, dd, hh, mn

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

#chla関連
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

#1日の平均値を計算する関数
def calculate_daily_mean(year_int, month_int, day_int):
    data_sum = None
    count_zeros = None

    for hour_int in range(24):
        yyyy, mm, dd, hh, mn = time_and_date(year=year_int, month=month_int, day=day_int, hour=hour_int)
        now = str(datetime.datetime.now())
        print(f'{now}     Now Downloading: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

        data = download_netcdf(year_int, month_int, day_int, hour_int)
        if data.all() != 0:
            try:
                data_filled = data['chlor_a'].fillna(0)  # 欠損値に0を代入
            except KeyError as e:
                print(f"Error: {e}")
                print(f"Dataset keys: {data.keys()}")
                data = np.zeros((line_number, pixel_number))
                continue  # データセットに 'chlor_a' が存在しない場合、次のループへ
            if data_sum is None:
                data_sum = data_filled
                if (hour_int == 0) and (data.all() == 0):
                    count_zeros = np.ones(data_filled.shape).astype(int)  # count_zeros 配列を全て1に設定
                else:
                    count_zeros = (data_filled == 0).astype(int)  # 0の回数をカウントするためのマスクを作成
            else:
                data_sum += data_filled
                count_zeros += (data_filled == 0).astype(int)  # 各グリッドの0の回数を更新
        else:
            if hour_int == 0:
                data_sum = np.zeros((line_number, pixel_number))
                count_zeros = np.ones((line_number, pixel_number)).astype(int)
            else:
                count_zeros += np.ones((line_number, pixel_number)).astype(int)  # 各グリッドの0の回数を更新

    divisor = 24 - count_zeros  # 24から0の回数を引く
    daily_mean = xr.where(divisor != 0, data_sum / divisor, 0)  # 各グリッドごとに24-count_iで割る、もし24-count_i=0の場合は0を与える

    return daily_mean

#central_pointのデータを読み込む
def read_central_point(year, month):
    yyyy = str(year).zfill(4)
    mm = str(month).zfill(2)
    central_point_csv_path = f'chla_central_point_smooth_{yyyy}{mm}_2_edit.csv'
    #1行目をスキップして読み込む
    central_point = np.loadtxt(central_point_csv_path, delimiter=',', skiprows=1)
    central_point_day = central_point[:, 0]
    central_point_all_longitude = central_point[:, 1]
    central_point_all_latitude = central_point[:, 2]
    central_point_a_longitude = central_point[:, 3]
    central_point_a_latitude = central_point[:, 4]
    central_point_b_longitude = central_point[:, 5]
    central_point_b_latitude = central_point[:, 6]
    return central_point_day, central_point_all_longitude, central_point_all_latitude, central_point_a_longitude, central_point_a_latitude, central_point_b_longitude, central_point_b_latitude


#Ash angleのデータを読み込む
def read_ash_angle(year, month):
    yyyy = str(year).zfill(4)
    mm = str(month).zfill(2)
    ash_angle_csv_path = f'ash_angle_{yyyy}{mm}.csv'
    #1行目をスキップして読み込む
    ash_data = np.loadtxt(ash_angle_csv_path, delimiter=',', skiprows=1)
    ash_day = ash_data[:, 0]
    ash_longitude = ash_data[:, 1]
    ash_latitude = ash_data[:, 2]
    #ash_longitudeとash_latitudeの値がどちらも0の場合はnanに変換する
    for count_i in range(len(ash_longitude)):
        if ash_longitude[count_i] == 0 and ash_latitude[count_i] == 0:
            ash_longitude[count_i] = np.nan
            ash_latitude[count_i] = np.nan
    #ash_angleを計算する
    ash_angle = np.zeros(len(ash_longitude))
    for count_j in range(len(ash_longitude)):
        if ash_longitude[count_j] != np.nan or ash_latitude[count_j] != np.nan:
            difference_longitude = ash_longitude[count_j] - nishinoshima_lon
            difference_latitude = ash_latitude[count_j] - nishinoshima_lat
            ash_angle[count_j] = np.arctan2(difference_latitude, difference_longitude) #[rad]
    
    return ash_day, ash_longitude, ash_latitude, ash_angle


#oceanic current関連
#緯度の範囲(X < Y)
def get_grid_range_latitude(X, Y):
    grid_size = 4251
    lat_range = 170
    lat_per_grid = lat_range / (grid_size - 1)

    x_grid = int((X + 80) / lat_per_grid)
    y_grid = int((Y + 80) / lat_per_grid) + 1

    min_grid = x_grid
    max_grid = y_grid

    return min_grid, max_grid

#経度の範囲(X < Y, 東経0-360)
def get_grid_range_longitude(X, Y):
    grid_size = 4501
    lon_range = 360
    lon_per_grid = lon_range / (grid_size - 1)

    x_grid = int(X / lon_per_grid)
    y_grid = int(Y / lon_per_grid) + 1

    min_grid = x_grid
    max_grid = y_grid

    return min_grid, max_grid

#時間の変換
def get_time_from_OPeNDAP():
    url_time = "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z.ascii?time[0:1:12952]"
    response_time = requests.get(url_time)
    ascii_time = response_time.text.split("\n")
    time_line = None
    for i, line in enumerate(ascii_time):
        if line.startswith("time"):
            time_line = ascii_time[i+1]
    time_values = [float(x) for x in time_line.split(',')]
    return time_values

time_values = get_time_from_OPeNDAP()
print(r"Time values are loaded.")

def get_time(time):
    time_origin = datetime.datetime(2000, 1, 1, 0, 0, 0)
    elapsed_time = time - time_origin
    hours_elapsed_time = elapsed_time.total_seconds() / 3600
    return hours_elapsed_time

def get_time_grid(time):
    hours_elapsed_time = get_time(time)
    dategrid_array = np.array(time_values)
    for count_i, element in enumerate(dategrid_array):
        if element == hours_elapsed_time:
            return count_i
    print(r"Error: time is out of range.")
    return None

#データのダウンロード
def make_url(time):
    min_lat_grid, max_lat_grid = get_grid_range_latitude(lat_1_min, lat_1_max)
    min_lon_grid, max_lon_grid = get_grid_range_longitude(lon_1_min, lon_1_max)
    time_grid = get_time_grid(time)
    if time_grid == None:
        return None
    url = f"https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/uv3z.ascii?lat[{min_lat_grid}:1:{max_lat_grid}],lon[{min_lon_grid}:1:{max_lon_grid}],water_u[{time_grid}:1:{time_grid}][0:1:0][{min_lat_grid}:1:{max_lat_grid}][{min_lon_grid}:1:{max_lon_grid}],water_v[{time_grid}:1:{time_grid}][0:1:0][{min_lat_grid}:1:{max_lat_grid}][{min_lon_grid}:1:{max_lon_grid}]"
    print(url)
    return url

def get_data(year_int, month_int, day_int, hour_int):
    data_url = make_url(datetime.datetime(year_int, month_int, day_int, hour_int, 0, 0))
    if data_url == None:
        return None, None, None, None, None
    response = requests.get(data_url)
    ascii_data = response.text.split("\n")

    #print(ascii_data)

    lat_line = None
    lon_line = None
    water_u_lines = []
    water_v_lines = []

    for i, line in enumerate(ascii_data):
        if line.startswith("lat"):
            lat_line = ascii_data[i+1]
        elif line.startswith("lon"):
            lon_line = ascii_data[i+1]
        elif line.startswith("water_u.water_u"):
            water_u_start = i+1
        elif line.startswith("water_v.water_v"):
            water_v_start = i+1

    lat_values = [float(x) for x in lat_line.split(',')]
    lon_values = [float(x) for x in lon_line.split(',')]

    water_u_lines = ascii_data[water_u_start:water_u_start+len(lat_values)]
    water_v_lines = ascii_data[water_v_start:water_v_start+len(lat_values)]

    water_u_values = []
    water_v_values = []

    for line in water_u_lines:
        values = [int(x) for x in re.findall(r"[-+]?\d+", line)][3:] # 最初の3つの値をスライスで省く
        water_u_values.append(values)

    for line in water_v_lines:
        values = [int(x) for x in re.findall(r"[-+]?\d+", line)][3:] # 最初の3つの値をスライスで省く
        water_v_values.append(values)

    lat_data = np.array(lat_values)
    lon_data = np.array(lon_values)
    water_u_data = np.array(water_u_values)
    water_v_data = np.array(water_v_values)
    
    water_u_data = np.where(water_u_data != -30000, water_u_data, np.nan)
    water_v_data = np.where(water_v_data != -30000, water_v_data, np.nan)
    water_u_data = water_u_data * 0.001
    water_v_data = water_v_data * 0.001
    
    return lat_data, lon_data, water_u_data, water_v_data

def data_average(data, average_num):
    # 入力の検証
    if not isinstance(data, np.ndarray) or len(data.shape) not in [1, 2]:
        raise ValueError("Expected 'data' to be a 1-dimensional or 2-dimensional numpy array")

    # 1次元の場合
    if len(data.shape) == 1:
        averaged_data = np.zeros(data.shape[0] // average_num)
        for i in range(averaged_data.shape[0]):
            averaged_data[i] = np.nanmean(data[i * average_num : (i + 1) * average_num])

    # 2次元の場合
    elif len(data.shape) == 2:
        averaged_data = np.zeros((data.shape[0] // average_num, data.shape[1] // average_num))
        for i in range(averaged_data.shape[0]):
            for j in range(averaged_data.shape[1]):
                averaged_data[i, j] = np.nanmean(data[i * average_num : (i + 1) * average_num, j * average_num : (j + 1) * average_num])

    return averaged_data

#データの日平均
def get_data_daily(year_int, month_int, day_int):
    lat_data_daily = None
    lon_data_daily = None
    water_u_data_daily = None
    water_v_data_daily = None
    lat_data_min_grid, lat_data_max_grid = get_grid_range_latitude(lat_1_min, lat_1_max)
    lon_data_min_grid, lon_data_max_grid = get_grid_range_longitude(lon_1_min, lon_1_max)
    count_NaN = np.zeros((lat_data_max_grid - lat_data_min_grid + 1, lon_data_max_grid - lon_data_min_grid + 1))
    #count_NaN = np.zeros((102, 52))     #本来ならばlat_data, lon_dataから取得するべきだが、今回は固定値
    for hour_idx in range(8):
        hour_int = hour_idx * 3
        lat_data, lon_data, water_u_data, water_v_data = get_data(year_int, month_int, day_int, hour_int)
        if water_u_data is not None:
            if water_u_data_daily is None:
                lat_data_daily = lat_data
                lon_data_daily = lon_data
                water_u_data_daily = water_u_data
                count_NaN[np.isnan(water_u_data_daily) == True] += 1
                water_u_data_daily = np.where(np.isnan(water_u_data_daily) == True, 0, water_u_data_daily)
                water_v_data_daily = water_v_data
                count_NaN[np.isnan(water_v_data_daily) == True] += 1
                water_v_data_daily = np.where(np.isnan(water_v_data_daily) == True, 0, water_v_data_daily)
            else:
                count_NaN[np.isnan(water_u_data) == True] += 1
                water_u_data = np.where(np.isnan(water_u_data) == True, 0, water_u_data)
                water_u_data_daily += water_u_data
                count_NaN[np.isnan(water_v_data) == True] += 1
                water_v_data = np.where(np.isnan(water_v_data) == True, 0, water_v_data)
                water_v_data_daily += water_v_data
        else:
            count_NaN += 1
    water_u_data_daily /= np.double(8 - count_NaN)
    water_v_data_daily /= np.double(8 - count_NaN)

    #データの縮小
    lat_data_daily_resize = data_average(lat_data_daily, average_num)
    lon_data_daily_resize = data_average(lon_data_daily, average_num)
    water_u_data_daily_resize = data_average(water_u_data_daily, average_num)
    water_v_data_daily_resize = data_average(water_v_data_daily, average_num)

    water_speed_daily = np.sqrt(water_u_data_daily_resize**2 + water_v_data_daily_resize**2)

    return lat_data_daily_resize, lon_data_daily_resize, water_u_data_daily_resize, water_v_data_daily_resize, water_speed_daily


def main(args):
    day_int = args

    central_point_day, central_point_all_longitude, central_point_all_latitude, central_point_a_longitude, central_point_a_latitude, central_point_b_longitude, central_point_b_latitude = read_central_point(year=year_input, month=month_input)

    central_point_day = central_point_day.astype(np.int64)

    yyyy, mm, dd, hh, mn = time_and_date(year=year_input, month=month_input, day=day_int, hour=0)

    #図が存在する場合、何もしない
    figure_name = f'{dir_figure}/{yyyy}{mm}/{yyyy}{mm}{dd}.png'
    #if (check_file_exists(figure_name) == True):
    #    return
    
    mkdir_folder(f'{dir_figure}/{yyyy}{mm}')
    

    #chla
    
    #日平均値の計算
    chlorophyll_daily_mean = calculate_daily_mean(year_input, month_input, day_int)

    #メディアンフィルターをかける
    chlorophyll_daily_mean = chlorophyll_daily_mean.astype(float)
    chlorophyll_daily_mean = chlorophyll_daily_mean.where(chlorophyll_daily_mean != 0, np.nan)
    chlorophyll_daily_mean = chlorophyll_daily_mean.rolling(longitude=4, latitude=4, center=True).median()

    #値を[vmin, vmax]に指定
    chlorophyll_daily_mean = xr.where((chlorophyll_daily_mean < chla_vmin) & (chlorophyll_daily_mean != 0), chla_vmin, chlorophyll_daily_mean)
    chlorophyll_daily_mean = xr.where(chlorophyll_daily_mean > chla_vmax, chla_vmax, chlorophyll_daily_mean)
    chlorophyll_daily_mean = chlorophyll_daily_mean.astype(float)
    chlorophyll_daily_mean = chlorophyll_daily_mean.where(chlorophyll_daily_mean != 0, np.nan)


    #海流の流速

    lat_data_daily, lon_data_daily, water_u_data_daily, water_v_data_daily, water_speed_daily = get_data_daily(year_input, month_input, day_int)
    if water_u_data_daily is None:
        return
    
    #Ash angle
    ash_day, ash_longitude, ash_latitude, ash_angle = read_ash_angle(year=year_input, month=month_input)


    now = str(datetime.datetime.now())
    print(f'{now}     Now Plotting: {yyyy}/{mm}/{dd}')
    
    fig = plt.figure(figsize=(15, 15), dpi=100)
    ax = fig.add_subplot(111, title=f'{yyyy}/{mm}/{dd}'+r'   with ocean flow (vector)', xlabel=r'longitude', ylabel=r'latitude')
    im = ax.imshow(chlorophyll_daily_mean, extent=[data_lon_min, data_lon_max, data_lat_min, data_lat_max], cmap='cool', norm=LogNorm(vmin=chla_vmin, vmax=chla_vmax))

    #西之島の座標をプロット
    ax.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=200, c='lightgrey', label='Nishinoshima', edgecolors='k')

    #day_intと同じ値のcentral_pointのデータの行番号を取得
    central_point_index = np.where(central_point_day == day_int)[0][0]
    #central_pointのデータをプロット
    ax.scatter(central_point_all_longitude[central_point_index], central_point_all_latitude[central_point_index], marker='D', s=200, c='lightgrey', label='Central Point', edgecolors='k') #s=70, c='red'
    if (central_point_a_latitude[central_point_index] != 0) and (central_point_a_longitude[central_point_index] != 0):
        ax.scatter(central_point_a_longitude[central_point_index], central_point_a_latitude[central_point_index], marker='<', s=200, c='lightgrey', label='Central Point A', edgecolors='k')
    if (central_point_b_latitude[central_point_index] != 0) and (central_point_b_longitude[central_point_index] != 0):
        ax.scatter(central_point_b_longitude[central_point_index], central_point_b_latitude[central_point_index], marker='>', s=200, c='lightgrey', label='Central Point B', edgecolors='k')

    #Ash angleを西之島中心に単位ベクトルでプロット
    if (ash_angle[central_point_index] != np.nan):
        ax.quiver(nishinoshima_lon, nishinoshima_lat, np.cos(ash_angle[central_point_index]), np.sin(ash_angle[central_point_index]), scale=10, headwidth=4, width=0.005, color='red', label='Ash') #darkorange
    
    ax.legend(loc='upper right', fontsize=25)

    #海流の流速をベクトルでプロット、色は黒
    sm = ax.quiver(lon_data_daily, lat_data_daily, water_u_data_daily, water_v_data_daily, water_speed_daily, scale=15, headwidth=2, width=0.005, cmap=my_cmap, alpha=0.7) #元々scale=25

    ax.set_xlim(lon_1_min, lon_1_max)
    ax.set_ylim(lat_1_min, lat_1_max)
    ax.minorticks_on()
    ax.grid(which='both', axis='both', lw='0.5', alpha=0.5)

    plt.colorbar(im, label=r'Chlorophyll-a [$\mathrm{mg / m^{3}}$]')
    plt.subplots_adjust()
    plt.tight_layout()

    #画像の保存
    #plt.show()
    fig.savefig(figure_name)
    now = str(datetime.datetime.now())
    print(f'{now}     Image is saved.: {yyyy}/{mm}/{dd}')
    plt.close()

    return

#main(1)
#quit()

#並列処理
if __name__ == '__main__':

    #プロセス数
    num_processes = 16

    day_list = read_central_point(year=year_input, month=month_input)[0]
    day_list = day_list.astype(int)

    #非同期処理
    with Pool(num_processes) as p:
        results = []
        for day_int in day_list:
            result = p.apply_async(main, [(day_int)])
            results.append(result)
        for result in results:
            result.get()

    print(r'finish')
    quit()
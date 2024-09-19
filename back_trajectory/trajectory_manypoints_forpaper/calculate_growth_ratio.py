import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import datetime
import pyproj
import geopandas as gpd
import matplotlib.cm as cm
from scipy.stats import linregress
from scipy.optimize import curve_fit


# directory
back_or_forward = 'forward'
input_condition = 2

growth_ratio_selection = 'linear'
#(power要調整)

cutoff_date = datetime.datetime(2020, 7, 4, 18)
#cutoff_date = datetime.datetime(2020, 7, 1, 0)

# Initial distance from Nishinoshima/Mukojima
vmin = 0
vmax = 150

# Nishinoshima location
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

# Mukojima location
mukojima_lon = 142.14
mukojima_lat = 27.68

def calculate_distance(latitude, longitude):
    geod = pyproj.Geod(ellps='WGS84')
    if back_or_forward == 'forward':
        azimuth1, azimuth2, distance = geod.inv(longitude, latitude, nishinoshima_lon, nishinoshima_lat)
    elif back_or_forward == 'back':
        azimuth1, azimuth2, distance = geod.inv(longitude, latitude, mukojima_lon, mukojima_lat)
    return distance / 1000

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

# trajectory file
def read_trajectory_file_for_width(now_time):
    file_name_trajectory = os.path.dirname(file_name_time) + f'/trajectory_chla_{now_time.year}_{now_time.month}_{now_time.day}_{now_time.hour}.csv'
    trajectory = np.loadtxt(file_name_trajectory, delimiter=',')
    index_number = trajectory[:, 0]
    latitude = trajectory[:, 2]
    longitude = trajectory[:, 1]
    chla = trajectory[:, 3]
    return index_number, latitude, longitude, chla

# plot setting
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
font_size = 30
plt.rcParams["font.size"] = font_size

# read time file
start_time = np.loadtxt(file_name_time, delimiter=',', max_rows=1)
end_time = np.loadtxt(file_name_time, delimiter=',', skiprows=1, max_rows=1)

start_year_JST, start_month_JST, start_day_JST, start_hour_JST = int(start_time[0]), int(start_time[1]), int(start_time[2]), int(start_time[3])
start_time_JST = datetime.datetime(start_year_JST, start_month_JST, start_day_JST, start_hour_JST)
end_year_JST, end_month_JST, end_day_JST, end_hour_JST = int(end_time[0]), int(end_time[1]), int(end_time[2]), int(end_time[3])
end_time_JST = datetime.datetime(end_year_JST, end_month_JST, end_day_JST, end_hour_JST)

# datetimeリスト
now_time = start_time_JST
datetime_list = []
while (now_time >= end_time_JST and back_or_forward == 'back') or (now_time <= end_time_JST and back_or_forward == 'forward'):
    datetime_list.append(now_time)
    if back_or_forward == 'back':
        now_time = now_time - datetime.timedelta(hours=6)
    elif back_or_forward == 'forward':
        now_time = now_time + datetime.timedelta(hours=6)

# read trajectory file
trajectory_initial_point = np.loadtxt(file_name_point, delimiter=',')
trajectory_number = len(trajectory_initial_point)

data_array = np.zeros((trajectory_number, len(datetime_list), 5))

for i in range(len(datetime_list)):
    now_time = datetime_list[i]
    index_number, latitude, longitude, chla = read_trajectory_file_for_width(now_time)
    for j in range(trajectory_number):
        initial_distance = calculate_distance(trajectory_initial_point[j, 1], trajectory_initial_point[j, 0])
        data_array[j, i, 0] = index_number[j]
        data_array[j, i, 1] = latitude[j]
        data_array[j, i, 2] = longitude[j]
        data_array[j, i, 3] = chla[j]
        data_array[j, i, 4] = initial_distance

def calculate_growth_ratio_linear(datetime_list_def, data_array_def):
    #datetime_list_defとdata_array_defの長さは同じ
    if back_or_forward == 'forward':
        valid_indices = ~np.isnan(data_array_def) & (np.array(datetime_list_def) < cutoff_date)
    elif back_or_forward == 'back':
        valid_indices = ~np.isnan(data_array_def) & (np.array(datetime_list_def) > cutoff_date)
    valid_dates = np.array([dt for i, dt in enumerate(datetime_list_def) if valid_indices[i]])
    valid_data = data_array_def[valid_indices]

    # 最初の日付を基準とした経過時間を計算
    base_date = valid_dates[0]
    elapsed_time_seconds = np.array([(dt - base_date).total_seconds() for dt in valid_dates])

    # 6時間ごとの成長率を求めるため、時間を6時間単位に変換
    elapsed_time_hours = elapsed_time_seconds / 3600  # 秒から時間に変換
    elapsed_time_6hour_units = elapsed_time_hours / 6  # 6時間単位に変換

    print(valid_dates)

    # 線形回帰を実行
    slope, intercept, r_value, p_value, std_err = linregress(elapsed_time_6hour_units, valid_data)

    return slope, intercept, r_value, p_value, std_err

def power_law(x, a, b):
    # xが0や負の値を持たないように、最低値を制限
    x = np.where(x > 0, x, 1e-10)  # 負の値やゼロを避ける
    return a * x ** b

def calculate_growth_ratio_power(datetime_list_def, data_array_def):
    # datetime_list_defとdata_array_defの長さは同じであることを前提
    if back_or_forward == 'forward':
        valid_indices = ~np.isnan(data_array_def) & (np.array(datetime_list_def) < cutoff_date)
    elif back_or_forward == 'back':
        valid_indices = ~np.isnan(data_array_def) & (np.array(datetime_list_def) > cutoff_date)

    valid_dates = np.array([dt for i, dt in enumerate(datetime_list_def) if valid_indices[i]])
    valid_data = data_array_def[valid_indices]

    # 最初の日付を基準とした経過時間を計算
    base_date = valid_dates[0]
    elapsed_time_seconds = np.array([(dt - base_date).total_seconds() for dt in valid_dates])

    # 6時間ごとの成長率を求めるため、時間を6時間単位に変換
    elapsed_time_hours = elapsed_time_seconds / 3600  # 秒から時間に変換
    elapsed_time_6hour_units = elapsed_time_hours / 6  # 6時間単位に変換

    print(valid_dates)

    # べき乗フィッティングを実行
    try:
        popt, pcov = curve_fit(power_law, elapsed_time_6hour_units, valid_data, maxfev=10000)
        a, b = popt

        # 予測値を計算して決定係数を求める
        predicted_data = power_law(elapsed_time_6hour_units, *popt)
        residuals = valid_data - predicted_data
        ss_res = np.sum(residuals**2)
        ss_tot = np.sum((valid_data - np.mean(valid_data))**2)
        r_value = np.sqrt(1 - (ss_res / ss_tot))

        # 標準誤差の計算（1つの値を取るための調整）
        std_err = np.sqrt(np.diag(pcov))
        std_err = std_err[0] if std_err.size > 0 else np.nan  # std_errが配列なら最初の要素を使用
    except RuntimeError as e:
        print(f"Fit did not converge: {e}")
        a, b = np.nan, np.nan
        r_value, p_value, std_err = np.nan, np.nan, np.nan

    # 線形フィッティングの出力形式に合わせて返す
    return b, a, r_value, np.nan, std_err

linregress_array = np.zeros((trajectory_number, 7))


for count_i in range(trajectory_number):
    if growth_ratio_selection == 'linear':
        slope, intercept, r_value, p_value, std_err = calculate_growth_ratio_linear(datetime_list, data_array[count_i, :, 3])
    elif growth_ratio_selection == 'power':
        slope, intercept, r_value, p_value, std_err = calculate_growth_ratio_power(datetime_list, data_array[count_i, :, 3])
    linregress_array[count_i, 0] = count_i
    linregress_array[count_i, 1] = data_array[count_i, 0, 4]
    linregress_array[count_i, 2] = slope
    linregress_array[count_i, 3] = intercept
    linregress_array[count_i, 4] = r_value**2E0
    linregress_array[count_i, 5] = p_value
    linregress_array[count_i, 6] = std_err


# plot setting
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
font_size = 30
plt.rcParams["font.size"] = font_size

# plot
fig = plt.figure(figsize=(20, 10))
gs = fig.add_gridspec(1, 2, width_ratios=[1, 0.05])

ax_cbar = fig.add_subplot(gs[1])
vmin = 0
vmax = 1
cbar = mpl.colorbar.ColorbarBase(ax_cbar, cmap=cm.turbo, orientation='vertical', norm=mpl.colors.Normalize(vmin=vmin, vmax=vmax))
cbar.set_label(r'R$^2$')

ax = fig.add_subplot(gs[0])

ax.errorbar(linregress_array[:, 1], linregress_array[:, 2], yerr=linregress_array[:, 6], fmt='o', 
            ecolor='gray', elinewidth=1, capsize=3, capthick=1, color='none')

sc = ax.scatter(linregress_array[:, 1], linregress_array[:, 2], c=linregress_array[:, 4], cmap=cm.turbo, vmin=vmin, vmax=vmax)

ax.minorticks_on()
ax.grid(which='both', alpha=0.3)

if back_or_forward == 'forward':
    ax.set_xlabel(r'Initial distance from Nishinoshima [km]')
elif back_or_forward == 'back':
    ax.set_xlabel(r'Initial distance from Mukojima [km]')
if growth_ratio_selection == 'linear':
    ax.set_ylabel(r'Growth rate of Chl-a concentration (linear) [mg/m$^3$/6 hour]')
elif growth_ratio_selection == 'power':
    ax.set_ylabel(r'Growth rate of Chl-a concentration (power) [mg/m$^3$/6 hour]')
plt.show()
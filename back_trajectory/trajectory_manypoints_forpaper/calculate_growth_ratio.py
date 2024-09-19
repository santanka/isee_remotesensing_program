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

if back_or_forward == 'forward':
    cutoff_date = datetime.datetime(2020, 7, 4, 18)
elif back_or_forward == 'back':
    cutoff_date = datetime.datetime(2020, 7, 1, 0)

# Initial distance from Nishinoshima/Mukojima
distance_plot = True
vmin = 0
vmax = 300

# Angle from Nishinoshima
angle_plot = False
angle_date_JST = datetime.datetime(2020, 6, 28, 18)

if (distance_plot == False and angle_plot == False) or (distance_plot == True and angle_plot == True):
    raise ValueError('Please set either distance_plot or angle_plot to True')

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

def calculate_angle(latitude, longitude, latitude_base, longitude_base):
    geod = pyproj.Geod(ellps='WGS84')
    azimuth1, azimuth2, distance = geod.inv(longitude_base, latitude_base, longitude, latitude)
    return azimuth1

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

data_array = np.zeros((trajectory_number, len(datetime_list), 6))

angle_base_data = np.zeros((trajectory_number, 4))
angle_index_number, angle_latitude, angle_longitude, angle_chla = read_trajectory_file_for_width(angle_date_JST)
angle_base_data[:, 0] = angle_index_number
angle_base_data[:, 1] = angle_longitude
angle_base_data[:, 2] = angle_latitude
for i in range(trajectory_number):
    angle_base_data[i, 3] = calculate_angle(angle_base_data[i, 2], angle_base_data[i, 1], nishinoshima_lat, nishinoshima_lon)

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
        data_array[j, i, 5] = angle_base_data[j, 3]
        if i == 0:
            print(latitude[j], longitude[j], chla[j], initial_distance, angle_base_data[j, 3])

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
linregress_array = np.zeros((trajectory_number, 7))


for count_i in range(trajectory_number):
    if growth_ratio_selection == 'linear':
        slope, intercept, r_value, p_value, std_err = calculate_growth_ratio_linear(datetime_list, data_array[count_i, :, 3])
    linregress_array[count_i, 0] = count_i
    if distance_plot == True:
        linregress_array[count_i, 1] = data_array[count_i, 0, 4]
    elif angle_plot == True:
        linregress_array[count_i, 1] = data_array[count_i, 0, 5]
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

ax.errorbar(linregress_array[:, 1], linregress_array[:, 2], yerr=linregress_array[:, 6], fmt='o', ecolor='gray', elinewidth=1, capsize=3, capthick=1, color='none')
sc = ax.scatter(linregress_array[:, 1], linregress_array[:, 2], c=linregress_array[:, 4], cmap=cm.turbo, vmin=vmin, vmax=vmax)

ax.minorticks_on()
ax.grid(which='both', alpha=0.3)

if back_or_forward == 'forward' and distance_plot == True:
    ax.set_xlabel(r'Initial distance from Nishinoshima [km]')
elif back_or_forward == 'back' and distance_plot == True:
    ax.set_xlabel(r'Initial distance from Mukojima [km]')
elif angle_plot == True:
    charactor_angle_date_JST = angle_date_JST.strftime('%Y-%m-%d %H:%M JST')
    ax.set_xlabel(f'Angle from Nishinoshima [deg]\n({charactor_angle_date_JST})')
if growth_ratio_selection == 'linear':
    ax.set_ylabel(f'Growth rate of Chl-a concentration\n(linear) ' + r'[mg/m$^3$/6 hour]')

# 図の左上に(j)を表示
ax.text(-0.1, 1.0, '(h)', transform=ax.transAxes, fontsize=font_size, verticalalignment='top')

fig.tight_layout()

def file_name():
    dir_1 = f'/mnt/j/isee_remote_data/JST/'

    if back_or_forward == 'back':
        dir_2 = f'back_trajectory_manypoints_forpaper/back_trajectory_condition_{input_condition}/figure/'
    elif back_or_forward == 'forward':
        dir_2 = f'forward_trajectory_manypoints_forpaper/forward_trajectory_condition_{input_condition}/figure/'
    
    dir_3 = f'calculate_growth_ratio/'
    
    dir_name = dir_1 + dir_2 + dir_3
    os.makedirs(dir_name, exist_ok=True)

    if distance_plot == True and growth_ratio_selection == 'linear':
        file_name = dir_name + f'linear_distance'
    elif angle_plot == True and growth_ratio_selection == 'linear':
        file_name = dir_name + f'linear_angle'
    
    return file_name

file_name = file_name()
fig.savefig(file_name + '.png')
fig.savefig(file_name + '.pdf')

plt.close()

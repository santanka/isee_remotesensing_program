import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import datetime
import pyproj
import geopandas as gpd
import matplotlib.cm as cm
import matplotlib.dates as mdates


# directory
back_or_forward = 'forward'
input_condition = 2

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

def calculate_angle(latitude, longitude, latitude_base, longitude_base):
    geod = pyproj.Geod(ellps='WGS84')
    azimuth1, azimuth2, distance = geod.inv(longitude_base, latitude_base, longitude, latitude)
    return azimuth1

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


# plot
fig = plt.figure(figsize=(20, 10))
gs = fig.add_gridspec(1, 2, width_ratios=[1, 0.05])

if distance_plot == True:
    ax_cbar = fig.add_subplot(gs[1])
    distance_min = np.min(data_array[:, :, 4])
    distance_max = np.max(data_array[:, :, 4])
    cbar_min = np.nanmax([vmin, distance_min])
    cbar_max = np.nanmin([vmax, distance_max])
    cmap = cm.turbo
    norm = mpl.colors.Normalize(vmin=cbar_min, vmax=cbar_max)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=ax_cbar, orientation='vertical')
    if back_or_forward == 'forward':
        cbar.set_label(r'Initial Distance from Nishinoshima [km]')
    elif back_or_forward == 'back':
        cbar.set_label(r'Initial Distance from Mukojima [km]')

elif angle_plot == True:
    ax_cbar = fig.add_subplot(gs[1])
    angle_min = np.nanmin(data_array[:, :, 5])
    angle_max = np.nanmax(data_array[:, :, 5])
    cbar_min = angle_min
    cbar_max = angle_max
    cmap = cm.turbo
    norm = mpl.colors.Normalize(vmin=cbar_min, vmax=cbar_max)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=ax_cbar, orientation='vertical')
    charactor_angle_date_JST = angle_date_JST.strftime('%Y-%m-%d %H:%M JST')
    cbar.set_label(f'Angle from Nishinoshima [deg]\n({charactor_angle_date_JST})')

ax = fig.add_subplot(gs[0])

for i in range(len(datetime_list)):
    print(f'{datetime_list[i]}: {data_array[0, i, 3]}')
formatted_date = [date.strftime('%m-%d %H') for date in datetime_list]
print(formatted_date)

for i in range(trajectory_number):
    if distance_plot == True:
        if data_array[i, 0, 4] >= cbar_min and data_array[i, 0, 4] <= cbar_max:
            print(data_array[i, 0, 0:5])
            ax.plot(datetime_list, data_array[i, :, 3], label=f'Trajectory {i+1}', linewidth=2, alpha=0.5, c=cmap(norm(data_array[i, 0, 4])))
        #else:
        #    ax.plot(formatted_date, data_array[i, :, 3], label=f'Trajectory {i+1}', linewidth=2, alpha=0.1, c='gray')
    elif angle_plot == True:
        ax.plot(datetime_list, data_array[i, :, 3], label=f'Trajectory {i+1}', linewidth=2, alpha=0.5, c=cmap(norm(data_array[i, 0, 5])))

for time in datetime_list:
    if time.hour == 0:
        ax.axvline(time, color='black', linestyle='-', alpha=0.2)
    elif time.hour == 12:
        ax.axvline(time, color='black', linestyle='--', alpha=0.2)

# カスタムフォーマッタを作成
def custom_date_formatter(x, pos):
    dt = mdates.num2date(x)  # X軸の値をdatetimeに変換
    label = ""
    
    # pos == 0 の場合、左端なので月を強制的に表示
    if pos == 0 or (dt.day == 1 and dt.hour == 0):
        label += f"{dt.strftime('%m')}\n"
    else:
        label += "\n"
    
    # 日の変わり目に日を表示
    if pos == 0 or dt.hour == 0:
        label += f"{dt.strftime('%d')}\n"
    else:
        label += "\n"
    
    # 時間は00, 06, 12, 18の時だけ表示
    #if dt.hour in [0, 6, 12, 18]:
    if dt.hour in [0, 12]:
        label += f"{dt.strftime('%H')}"
    else:
        label += "\n"
    
    return label

# ラベルの間隔を6時間ごとにし、00, 06, 12, 18時を表示
ax.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))

# x軸のラベルをカスタムフォーマットに変更
ax.xaxis.set_major_formatter(plt.FuncFormatter(custom_date_formatter))

# x軸の範囲を06/20 12:00から表示
if back_or_forward == 'back':
    ax.set_xlim([datetime_list[-1] - datetime.timedelta(hours=6), datetime_list[0] + datetime.timedelta(hours=6)])
elif back_or_forward == 'forward':
    ax.set_xlim([datetime_list[0] - datetime.timedelta(hours=6), datetime_list[-1] + datetime.timedelta(hours=6)])

plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, fontsize=font_size)

ax.set_xlabel(r'Time (JST) [month/day/hour]')
ax.set_ylabel(r'Chlorophyll-a [$\mathrm{mg/m^3}$]')

#y軸の目盛りを0.1刻みに
ylim_max = np.nanmax(data_array[:, :, 3])
ylim_max_round = np.ceil(ylim_max * 10) / 10
ax.set_ylim(0, ylim_max_round)
ax.set_yticks(np.arange(0, ylim_max_round, 0.1))
#yだけminor ticksを表示、xはmajor ticksのみ
ax.yaxis.minorticks_on()
ax.grid(axis='x', which='major', alpha=0.3)
ax.grid(axis='y', which='both', alpha=0.3)
ax.set_ylim(0, 0.5)
#ax.set_yscale('log')

# 図の左上に(i)を表示
ax.text(-0.075, 1.0, '(g)', transform=ax.transAxes, fontsize=font_size, verticalalignment='top')

fig.tight_layout()

def file_name():
    dir_1 = f'/mnt/j/isee_remote_data/JST/'

    if back_or_forward == 'back':
        dir_2 = f'back_trajectory_manypoints_forpaper/back_trajectory_condition_{input_condition}/figure/'
    elif back_or_forward == 'forward':
        dir_2 = f'forward_trajectory_manypoints_forpaper/forward_trajectory_condition_{input_condition}/figure/'
    
    dir_3 = f'chla_on_trajectory/'
    
    dir_name = dir_1 + dir_2 + dir_3
    os.makedirs(dir_name, exist_ok=True)

    if distance_plot == True:
        file_name = dir_name + f'distance'
    elif angle_plot == True:
        file_name = dir_name + f'angle'
    
    return file_name

file_name = file_name()
fig.savefig(file_name + '.png')
fig.savefig(file_name + '.pdf')

plt.close()


import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import datetime
import pyproj
import geopandas as gpd


# directory
back_or_forward = 'forward'
input_condition = 2

# Nishinoshima location
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

def calculate_distance(latitude, longitude):
    geod = pyproj.Geod(ellps='WGS84')
    azimuth1, azimuth2, distance = geod.inv(longitude, latitude, nishinoshima_lon, nishinoshima_lat)
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


# plot
fig = plt.figure(figsize=(20, 10))
gs = fig.add_gridspec(1, 2, width_ratios=[1, 0.05])

ax_cbar = fig.add_subplot(gs[1])
cbar_min = np.min(data_array[:, :, 4])
cbar_max = np.max(data_array[:, :, 4])
cmap = plt.get_cmap('turbo')
norm = mpl.colors.Normalize(vmin=cbar_min, vmax=cbar_max)
sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
sm.set_array([])
cbar = fig.colorbar(sm, cax=ax_cbar, orientation='vertical')
cbar.set_label(r'Initial Distance from Nishinoshima [km]')

ax = fig.add_subplot(gs[0])

for i in range(len(datetime_list)):
    print(f'{datetime_list[i]}: {data_array[0, i, 3]}')
formatted_date = [date.strftime('%m-%d %H') for date in datetime_list]

for i in range(trajectory_number):
    print(data_array[i, 0, 4])
    ax.plot(formatted_date, data_array[i, :, 3], label=f'Trajectory {i+1}', linewidth=2, alpha=0.5, c=cmap(norm(data_array[i, 0, 4])))

ax.set_xlabel(r'Time (JST)')
rotation = 90
ax.set_xticklabels(formatted_date, rotation=rotation)
ax.set_ylabel(r'Chlorophyll-a [$\mathrm{mg/m^3}$]')

#ax.minorticks_on()
ax.grid(which='both', alpha=0.3)
ax.set_ylim(0, 0.5)
#ax.set_yscale('log')

fig.tight_layout()
plt.show()


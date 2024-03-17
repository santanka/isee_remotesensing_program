import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import datetime
import os

TIR_number = 5

start_year = 2019
start_month = 11

end_year = 2020
end_month = 9

path_dir = f'/mnt/j/isee_remote_data/JST/himawari8_radiance_data/TIR_{TIR_number:02}/'
path_figure_dir = f'{path_dir}figure/'
if not os.path.exists(os.path.dirname(path_figure_dir)):
    os.makedirs(os.path.dirname(path_figure_dir))

path_figure = f'{path_figure_dir}{start_year:04}{start_month:02}_{end_year:04}{end_month:02}_JST'

#図の書式の指定(設定ではLaTeX調になっています)
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 35

#データの読み込み
#{start_year:04}{start_month:02}_JST.csvから{end_year:04}{end_month:02}_JST.csvまでのデータを読み込む
#start_yearとend_yearが同じ場合も考慮
data_array = []
for year in range(start_year, end_year+1):
    if year == start_year:
        start_month_loop = start_month
    else:
        start_month_loop = 1
    if year == end_year:
        end_month_loop = end_month
    else:
        end_month_loop = 12
    for month in range(start_month_loop, end_month_loop+1):
        path_data = f'{path_dir}{year:04}{month:02}_JST.csv'
        data = np.loadtxt(path_data, delimiter=',', skiprows=1)
        data_array.append(data)
data_array = np.concatenate(data_array, axis=0)

data_date = []
for count_i in range(len(data_array)):
    data_date.append(datetime.datetime(int(data_array[count_i, 0]), int(data_array[count_i, 1]), int(data_array[count_i, 2]), int(data_array[count_i, 3]), 0, 0))
data_mean_K = data_array[:, 4]
data_max_K = data_array[:, 5]

vmin = 280
vmax = 420

#横軸: 日付、縦軸: 平均輝度温度のグラフを作成
fig = plt.figure(figsize=(15, 10))
ax = fig.add_subplot(111, xlabel='Date', ylabel='Brightness temperature [K]')
ax.scatter(data_date, data_max_K, s=50, c='orange', label='Max', alpha=0.8)
ax.scatter(data_date, data_mean_K, s=50, c='blue', label='Mean', alpha=0.8)

ax.legend(loc='upper left')
ax.minorticks_on()
ax.grid(which='both', alpha=0.5, axis='y')
ax.grid(which='both', alpha=0.5, axis='x')

#3ヶ月ごとにminor locatorを設定、形式: 1行目は空白、2行目は月
#1月1日にmajor locatorを設定、start_monthが1以外の場合はstart_month月1日にmajor locatorを設定
start_date = data_date[0]
start_day = start_date.day
print(start_day)

# Generate a list of major locator ticks
major_locator_tick = []
# Check if start_month is January; if not, add the first tick for the start_month of the start_year
major_locator_tick.append(datetime.datetime(start_year, start_month, start_day))
# Add January 1st of each year from start_year to end_year (inclusive)
for year in range(start_year + 1, end_year + 1):
    major_locator_tick.append(datetime.datetime(year, 1, 1))
print(major_locator_tick)
# Convert datetime objects to matplotlib date format
major_locator_tick = [mdates.date2num(date) for date in major_locator_tick]

ax.xaxis.set_major_locator(mticker.FixedLocator(major_locator_tick))
ax.xaxis.set_minor_locator(mpl.dates.MonthLocator(interval=1))
ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%m \\\ \\\ %Y'))
ax.xaxis.set_minor_formatter(mpl.dates.DateFormatter('%m'))

#x軸の目盛りを45度回転
#plt.xticks(rotation=45)

ax.set_xlim([data_date[0], data_date[-1]])
ax.set_ylim([vmin, vmax])

#plt.tight_layout()
#plt.show()
plt.savefig(f'{path_figure}.png', bbox_inches='tight', pad_inches=0.05)
plt.savefig(f'{path_figure}.pdf', bbox_inches='tight', pad_inches=0.05)
plt.close()
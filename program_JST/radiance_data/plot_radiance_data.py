import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import datetime
import os

TIR_number = 5

start_year = 2015
start_month = 7

end_year = 2023
end_month = 12

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
plt.rcParams["font.size"] = 25

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
ax.grid(which='both', alpha=0.3, axis='y')
ax.grid(which='both', alpha=0.3, axis='x')

ax.tick_params(axis='x', which='major', labelrotation=45)

ax.set_xlim([data_date[0], data_date[-1]])
ax.set_ylim([vmin, vmax])

#plt.show()
plt.savefig(f'{path_figure}.png', bbox_inches='tight', pad_inches=0.05)
plt.savefig(f'{path_figure}.pdf', bbox_inches='tight', pad_inches=0.05)
plt.close()
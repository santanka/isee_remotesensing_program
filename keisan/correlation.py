import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 35

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

year = 2020
month = 8
end_day = 15

yyyy = str(year).zfill(4)
mm = str(month).zfill(2)

ash_data = np.genfromtxt(f'ash_angle_{yyyy}{mm}.csv', delimiter=',', skip_header=1)
ash_day = ash_data[:end_day, 0]
ash_longitude = ash_data[:end_day, 1]
#値が0のところはnanに変換
ash_longitude[ash_longitude == 0] = np.nan
ash_latitude = ash_data[:end_day, 2]
ash_latitude[ash_latitude == 0] = np.nan

chla_data = np.genfromtxt(f'chla_central_point_smooth_{yyyy}{mm}_2.csv', delimiter=',', skip_header=1)
chla_longitude = chla_data[:end_day, 0]
chla_latitude = chla_data[:end_day, 1]

diff_ash_longitude = ash_longitude - nishinoshima_lon
diff_ash_latitude = ash_latitude - nishinoshima_lat
diff_ash_angle = np.arctan2(diff_ash_longitude, diff_ash_latitude)
diff_ash_angle_deg = np.rad2deg(diff_ash_angle)

diff_chla_longitude = chla_longitude - nishinoshima_lon
diff_chla_latitude = chla_latitude - nishinoshima_lat
diff_chla_angle = np.arctan2(diff_chla_longitude, diff_chla_latitude)
diff_chla_angle_deg = np.rad2deg(diff_chla_angle)

def cross_correlation(x, y):
    return np.correlate(x , y, 'full')

#相互相関を計算
correlations = cross_correlation(diff_ash_angle, diff_chla_angle)
estimated_dalay = np.argmax(correlations) - len(diff_ash_angle) + 1
print(estimated_dalay)

#相互相関をプロット
lags = np.arange(-len(diff_ash_angle) + 1, len(diff_ash_angle))

fig = plt.figure(figsize=(24, 14), dpi=100)
ax = fig.add_subplot(121)
ax.plot(lags, correlations)
ax.set_xlabel('Lag')
ax.set_ylabel('Cross-correlation')
ax.set_xlim(0, len(diff_ash_angle) - 1)
ax.set_xticks(np.arange(0, len(diff_ash_angle), 2))
ax.grid(which='major', alpha=0.3)

ax1 = fig.add_subplot(122)
ax1.plot(ash_day, diff_ash_angle_deg, label='ash_angle')
ax1.plot(ash_day, diff_chla_angle_deg, label='chla_angle')
ax1.set_xlabel('Day')
ax1.set_ylabel('Angle')
ax1.set_xlim(1, len(diff_ash_angle))
ax1.set_xticks(np.arange(1, len(diff_ash_angle)+1, 2))
ax1.grid(which='major', alpha=0.3)
ax1.legend()

plt.tight_layout()
plt.show()
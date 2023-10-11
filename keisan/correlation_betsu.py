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
diff_ash_abs = np.sqrt(diff_ash_longitude**2 + diff_ash_latitude**2)
diff_ash_longitude = diff_ash_longitude / diff_ash_abs
diff_ash_latitude = diff_ash_latitude / diff_ash_abs

diff_chla_longitude = chla_longitude - nishinoshima_lon
diff_chla_latitude = chla_latitude - nishinoshima_lat
diff_chla_abs = np.sqrt(diff_chla_longitude**2 + diff_chla_latitude**2)
diff_chla_longitude = diff_chla_longitude / diff_chla_abs
diff_chla_latitude = diff_chla_latitude / diff_chla_abs

def cross_correlation(x, y):
    return np.correlate(x - np.mean(x) , y - np.mean(y), 'full')

#相互相関を計算
correlations_ash_lon_chla_lon = cross_correlation(diff_ash_longitude, diff_chla_longitude)
correlations_ash_lon_chla_lat = cross_correlation(diff_ash_longitude, diff_chla_latitude)
correlations_ash_lat_chla_lon = cross_correlation(diff_ash_latitude, diff_chla_longitude)
correlations_ash_lat_chla_lat = cross_correlation(diff_ash_latitude, diff_chla_latitude)

estimated_dalay_ash_lon_chla_lon = np.argmax(correlations_ash_lon_chla_lon) - len(diff_ash_longitude) + 1
estimated_dalay_ash_lon_chla_lat = np.argmax(correlations_ash_lon_chla_lat) - len(diff_ash_longitude) + 1
estimated_dalay_ash_lat_chla_lon = np.argmax(correlations_ash_lat_chla_lon) - len(diff_ash_latitude) + 1
estimated_dalay_ash_lat_chla_lat = np.argmax(correlations_ash_lat_chla_lat) - len(diff_ash_latitude) + 1

print(estimated_dalay_ash_lon_chla_lon)
print(estimated_dalay_ash_lon_chla_lat)
print(estimated_dalay_ash_lat_chla_lon)
print(estimated_dalay_ash_lat_chla_lat)

#相互相関をプロット
lags = np.arange(-len(diff_ash_longitude) + 1, len(diff_ash_longitude))

fig = plt.figure(figsize=(24, 24), dpi=100)

ax1 = fig.add_subplot(221)
ax1.plot(lags, correlations_ash_lon_chla_lon)
ax1.set_xlabel('Lag')
ax1.set_ylabel('Cross-correlation')
ax1.set_xlim(0, len(diff_ash_longitude) - 1)
ax1.set_xticks(np.arange(0, len(diff_ash_longitude), 2))
ax1.grid(which='major', alpha=0.3)
ax1.set_title(r'Ash Longitude - Chla Longitude')

ax2 = fig.add_subplot(222)
ax2.plot(lags, correlations_ash_lon_chla_lat)
ax2.set_xlabel('Lag')
ax2.set_ylabel('Cross-correlation')
ax2.set_xlim(0, len(diff_ash_longitude) - 1)
ax2.set_xticks(np.arange(0, len(diff_ash_longitude), 2))
ax2.grid(which='major', alpha=0.3)
ax2.set_title(r'Ash Longitude - Chla Latitude')

ax3 = fig.add_subplot(223)
ax3.plot(lags, correlations_ash_lat_chla_lon)
ax3.set_xlabel('Lag')
ax3.set_ylabel('Cross-correlation')
ax3.set_xlim(0, len(diff_ash_latitude) - 1)
ax3.set_xticks(np.arange(0, len(diff_ash_latitude), 2))
ax3.grid(which='major', alpha=0.3)
ax3.set_title(r'Ash Latitude - Chla Longitude')

ax4 = fig.add_subplot(224)
ax4.plot(lags, correlations_ash_lat_chla_lat)
ax4.set_xlabel('Lag')
ax4.set_ylabel('Cross-correlation')
ax4.set_xlim(0, len(diff_ash_latitude) - 1)
ax4.set_xticks(np.arange(0, len(diff_ash_latitude), 2))
ax4.grid(which='major', alpha=0.3)
ax4.set_title(r'Ash Latitude - Chla Latitude')

plt.tight_layout()
plt.show()
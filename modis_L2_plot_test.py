import netCDF4 as nc
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

# netCDF4ファイルを開く
file_path = '/mnt/j/isee_remote_data/Trichodesmium/MODIS_data_L2/AQUA_MODIS_L2/AQUA_MODIS.20071017T035500.L2.nc'
data = nc.Dataset(file_path, 'r')


# navigation_dataグループを開く
nav_data = data.groups['navigation_data']
geo_data = data.groups['geophysical_data']

# 変数を読み込む
latitude = nav_data.variables['latitude'][:]
longitude = nav_data.variables['longitude'][:]

Rrs_678 = geo_data.variables['Rrs_678'][:]
scale_factor = geo_data.variables['Rrs_678'].scale_factor if 'scale_factor' in geo_data.variables['Rrs_678'].ncattrs() else 1.0
offset = geo_data.variables['Rrs_678'].add_offset if 'add_offset' in geo_data.variables['Rrs_678'].ncattrs() else 0.0
Rrs_678_scaled = Rrs_678 #* scale_factor #+ offset

rhos_531 = geo_data.variables['rhos_531'][:]
rhos_645 = geo_data.variables['rhos_645'][:]
rhos_748 = geo_data.variables['rhos_748'][:]
rhos_859 = geo_data.variables['rhos_859'][:]

# プロットするデータの範囲を設定
lat_min, lat_max = 20, 40
lon_min, lon_max = 130, 150

# Nishinoshima location
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

# Mukojima location
mukojima_lon = 142.14
mukojima_lat = 27.68

# plot setting
mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
font_size = 20
plt.rcParams["font.size"] = font_size

# 範囲内のデータを抽出
#mask = (latitude >= lat_min) & (latitude <= lat_max) & (longitude >= lon_min) & (longitude <= lon_max) & (Rrs_678 < 0) & (rhos_748 < rhos_859) & (rhos_645 < rhos_531)
#mask = (Rrs_678_scaled < 0) #& (rhos_748 < rhos_859) & (rhos_645 < rhos_531)

#lat_subset = latitude[mask]
#lon_subset = longitude[mask]
#Rrs_678_subset = Rrs_678_scaled[mask]
lat_subset = latitude
lon_subset = longitude
Rrs_678_subset = Rrs_678

print(np.nanmax(Rrs_678_subset), np.nanmin(Rrs_678_subset))
print(scale_factor, offset)
quit()

# プロットを作成
plt.figure()
sc = plt.scatter(lon_subset, lat_subset, c=Rrs_678_subset, cmap='jet', s=10)
#plt.scatter(nishinoshima_lon, nishinoshima_lat, color='red', edgecolors='yellow', s=100, marker='^', label='Nishinoshima', linewidths=2)
#plt.scatter(mukojima_lon, mukojima_lat, color='blue', edgecolors='yellow', s=100, marker='^', label='Mukojima', linewidths=2)
plt.colorbar(sc, label='Rrs_678')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Rrs_678 Data within the specified geographic range')
plt.minorticks_on()
plt.grid(which='both', alpha=0.3)
plt.show()

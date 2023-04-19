import netCDF4 as nc
import bz2
import matplotlib.pyplot as plt

# NetCDFファイルを開く
filename_bz2 = '/home/satanka/Documents/isee_remotesensing/data/HIMAWARI-8/NC_H08_20200620_0100_B15_JP01_R20.nc.bz2'
with open(filename_bz2, 'rb') as f:
    decompressed = bz2.decompress(f.read())

data = nc.Dataset('/home/satanka/Documents/isee_remotesensing/data/HIMAWARI-8/NC_H08_20200620_0100_B15_JP01_R20.nc', mode='r', memory=decompressed)

# 変数のリストを取得する
var_names = data.variables.keys()
print('変数のリスト:', var_names)

# 各変数の寸法を取得する
for var_name in var_names:
    var = data.variables[var_name]
    print(f'変数名: {var_name}, 寸法: {var.dimensions}')

# 変数を読み取る
lat = data.variables['latitude'][:]
lon = data.variables['longitude'][:]
tbb = data.variables['tbb'][:]

# プロットする
plt.contourf(lon, lat, tbb, cmap='jet')
plt.colorbar()
plt.xlabel('longitude')
plt.ylabel('latitude')
plt.title('tbb')
plt.show()
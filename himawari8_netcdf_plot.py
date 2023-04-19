import urllib.request
import os
import bz2
import numpy as np
import matplotlib.pyplot as plt
import tarfile
import xarray as xr
import cartopy.crs as ccrs
import matplotlib.ticker as mticker

#時刻の指定
yyyy    = '2020'    #year
mm      = '06'      #month
dd      = '12'      #day
hh      = '13'      #hour (UTC)
mn      = '00'      #minutes (UTC)
band_11 = '09'
band_13 = '01'
band_14 = '02'
band_15 = '03'
fname_11 = f'{yyyy}{mm}{dd}{hh}{mn}.tir.{band_11}.fld.geoss.bz2'
fname_13 = f'{yyyy}{mm}{dd}{hh}{mn}.tir.{band_13}.fld.geoss.bz2'
fname_14 = f'{yyyy}{mm}{dd}{hh}{mn}.tir.{band_14}.fld.geoss.bz2'
fname_15 = f'{yyyy}{mm}{dd}{hh}{mn}.tir.{band_15}.fld.geoss.bz2'

#ファイルのダウンロード
url_tbb_11 = f'ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/{yyyy}{mm}/TIR/{fname_11}'
print('url_tbb_11: ' + url_tbb_11)
filename_url_tbb_11 = os.path.basename(url_tbb_11)
urllib.request.urlretrieve(url_tbb_11, filename_url_tbb_11)

url_tbb_13 = f'ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/{yyyy}{mm}/TIR/{fname_13}'
print('url_tbb_13: ' + url_tbb_13)
filename_url_tbb_13 = os.path.basename(url_tbb_13)
urllib.request.urlretrieve(url_tbb_13, filename_url_tbb_13)

url_tbb_14 = f'ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/{yyyy}{mm}/TIR/{fname_14}'
print('url_tbb_14: ' + url_tbb_14)
filename_url_tbb_14 = os.path.basename(url_tbb_14)
urllib.request.urlretrieve(url_tbb_14, filename_url_tbb_14)

url_tbb_15 = f'ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/{yyyy}{mm}/TIR/{fname_15}'
print('url_tbb_15: ' + url_tbb_15)
filename_url_tbb_15 = os.path.basename(url_tbb_15)
urllib.request.urlretrieve(url_tbb_15, filename_url_tbb_15)

#放射輝度への変換テーブルのダウンロード
cfname = "count2tbb_v103.tgz"
url_cfname = f"ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/support/{cfname}"
filename_url_cfname = os.path.basename(url_cfname)
urllib.request.urlretrieve(url_cfname, filename_url_cfname)

# tarファイルを解凍する
with tarfile.open(filename_url_cfname, 'r:gz') as tar:
    tar.extractall()
_, tbb_11 = np.loadtxt(f"count2tbb_v103/tir.{band_11}", unpack=True)
_, tbb_13 = np.loadtxt(f"count2tbb_v103/tir.{band_13}", unpack=True)
_, tbb_14 = np.loadtxt(f"count2tbb_v103/tir.{band_14}", unpack=True)
_, tbb_15 = np.loadtxt(f"count2tbb_v103/tir.{band_15}", unpack=True)

#座標系の定義
lon_min, lon_max = 85, 205
lat_min, lat_max = -60, 60
resolution = 0.02
lon = np.arange(lon_min, lon_max, resolution)
lat = np.arange(lat_max, lat_min, -resolution)

#データファイルを読んで等価黒体温度に変換
pixel_number = 6000
line_number = 6000
with bz2.BZ2File(fname_11) as bz2file:
    dataDN_11 = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number, line_number)
    data_tbb_11 = np.float32(tbb_11[dataDN_11])

with bz2.BZ2File(fname_13) as bz2file:
    dataDN_13 = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number, line_number)
    data_tbb_13 = np.float32(tbb_13[dataDN_13])

with bz2.BZ2File(fname_14) as bz2file:
    dataDN_14 = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number, line_number)
    data_tbb_14 = np.float32(tbb_14[dataDN_14])

with bz2.BZ2File(fname_15) as bz2file:
    dataDN_15 = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number, line_number)
    data_tbb_15 = np.float32(tbb_15[dataDN_15])

#xr_tbb = xr.DataArray(np.float32(data_tbb_11), name="tbb",
#                      coords = {
#                          'lat':('lat', lat, {'units': 'degrees_north'}),
#                          'lon':('lon', lon, {'units': 'degrees_east'})},
#                      dims = ['lat', 'lon'])
#
#xr_tbb.loc[60:-60, 85:205].plot.imshow(vmin=200, vmax=300,
#                  interpolation="None", cmap="turbo", figsize=[9,7])
#plt.show()
#
#quit()

#輝度温度
data_dif_tbb_13_15 = np.float32(data_tbb_13 - data_tbb_15)
data_dif_tbb_11_13 = np.float32(data_tbb_11 - data_tbb_13)
data_dif_tbb_11_14 = np.float32(data_tbb_11 - data_tbb_14)

# 範囲を設定する
# [140.879722, 27.243889]に西之島
nishinoshima_lat = 27.243889
nishinoshima_lon = 140.879722
#nishinoshima_lat = 3.17
#nishinoshima_lon = 98.391944
lon_set_min = nishinoshima_lon - 5E0
lon_set_max = nishinoshima_lon + 5E0
lat_set_min = nishinoshima_lat - 5E0
lat_set_max = nishinoshima_lat + 5E0
lon_set_min_enlarged = nishinoshima_lon - 2E0
lon_set_max_enlarged = nishinoshima_lon + 2E0
lat_set_min_enlarged = nishinoshima_lat - 2E0
lat_set_max_enlarged = nishinoshima_lat + 2E0

lon_set_2_5_min = np.ceil(lon_set_min / 2.5) * 2.5
lon_set_2_5_max = np.floor(lon_set_max / 2.5) * 2.5
lat_set_2_5_min = np.ceil(lat_set_min / 2.5) * 2.5
lat_set_2_5_max = np.floor(lat_set_max / 2.5) * 2.5
lon_set_2_5_min_enlarged = np.ceil(lon_set_min_enlarged / 2.5) * 2.5
lon_set_2_5_max_enlarged = np.floor(lon_set_max_enlarged / 2.5) * 2.5
lat_set_2_5_min_enlarged = np.ceil(lat_set_min_enlarged / 2.5) * 2.5
lat_set_2_5_max_enlarged = np.floor(lat_set_max_enlarged / 2.5) * 2.5

# latの範囲[25, 30]、lonの範囲[135, 145]に対応する配列を抽出する
lat_idx = np.where((lat >= lat_set_min) & (lat <= lat_set_max))[0]
lon_idx = np.where((lon >= lon_set_min) & (lon <= lon_set_max))[0]

# スライスで配列を抽出する
data_dif_tbb_13_15_sliced = data_dif_tbb_13_15[lon_idx[0]:lon_idx[-1]+1, lat_idx[0]:lat_idx[-1]+1]
data_dif_tbb_15_13_sliced = - data_dif_tbb_13_15[lon_idx[0]:lon_idx[-1]+1, lat_idx[0]:lat_idx[-1]+1]
data_dif_tbb_11_13_sliced = data_dif_tbb_11_13[lon_idx[0]:lon_idx[-1]+1, lat_idx[0]:lat_idx[-1]+1]
data_dif_tbb_13_11_sliced = - data_dif_tbb_11_13[lon_idx[0]:lon_idx[-1]+1, lat_idx[0]:lat_idx[-1]+1]
data_dif_tbb_11_14_sliced = data_dif_tbb_11_14[lon_idx[0]:lon_idx[-1]+1, lat_idx[0]:lat_idx[-1]+1]
data_tbb_13_sliced = data_tbb_13[lon_idx[0]:lon_idx[-1]+1, lat_idx[0]:lat_idx[-1]+1]

#ASH RGB (https://www.jma.go.jp/jma/jma-eng/satellite/VLab/RGB-Ash.pdf)
#data_red = ((data_dif_tbb_15_13_sliced - (-4E0)) / (2E0 - (-4E0)))
#data_red[data_red < 0] = 0
#data_red[data_red > 1] = 1
#data_red = data_red**1E0
#data_green = ((data_dif_tbb_13_11_sliced - (-4E0)) / (5E0 - (-4E0)))
#data_green[data_green < 0] = 0
#data_green[data_green > 1] = 1
#data_green = data_green**1E0
#data_blue = ((243E0 - data_tbb_13_sliced) / (243E0 - 208E0))**1.0E0
#data_blue[data_blue < 0] = 0
#data_blue[data_blue > 1] = 1
#data_blue = data_blue**1E0

#ASH RGB (https://www.data.jma.go.jp/mscweb/ja/prod/pdf/RGB_QG_Ash_jp.pdf) Green B11-B13
#data_red = (7.5E0 - data_dif_tbb_13_15_sliced) / (7.5E0 - (-3E0))
#data_red[data_red < 0] = 0
#data_red[data_red > 1] = 1
#data_red = data_red**1E0
#data_green = (4.9E0 - data_dif_tbb_11_13_sliced) / (4.9E0 - (-1.6E0))
#data_green[data_green < 0] = 0
#data_green[data_green > 1] = 1
#data_green = data_green**1.2E0
#data_blue = (data_tbb_13_sliced - 243.6E0) / (303.2E0 - 243.6E0)
#data_blue[data_blue < 0] = 0
#data_blue[data_blue > 1] = 1
#data_blue = data_blue**1E0

#ASH RGB (https://www.data.jma.go.jp/mscweb/ja/prod/pdf/RGB_QG_Ash_jp.pdf) Green B11-B14
data_red = (7.5E0 - data_dif_tbb_13_15) / (7.5E0 - (-3E0))
data_red[data_red < 0] = 0
data_red[data_red > 1] = 1
data_red = data_red**1E0
data_green = (5.1E0 - data_dif_tbb_11_14) / (5.1E0 - (-5.9E0))
data_green[data_green < 0] = 0
data_green[data_green > 1] = 1
data_green = data_green**0.85E0
data_blue = (data_tbb_13 - 243.6E0) / (303.2E0 - 243.6E0)
data_blue[data_blue < 0] = 0
data_blue[data_blue > 1] = 1
data_blue = data_blue**1E0


rgb = np.dstack((data_red, data_green, data_blue))

fig = plt.figure(figsize=(12, 6), dpi=100)
ax1 = fig.add_subplot(121, title=f'{yyyy}/{mm}/{dd} {hh}:{mn} (UTC)', xlabel=r'longitude', ylabel=r'latitude')
#projection=ccrs.PlateCarree(), 

#gl = ax.gridlines(crs=ccrs.PlateCarree(), linewidth=1, color=r'white', alpha=0.3)
#gl.xlocator = mticker.FixedLocator(np.arange(lon_set_2_5_min, lon_set_2_5_max+0.0001, 2.5))
#gl.ylocator = mticker.FixedLocator(np.arange(lat_set_2_5_min, lat_set_2_5_max+0.0001, 2.5))
#ax.set_xticks(np.arange(lon_set_2_5_min, lon_set_2_5_max+0.0001, 2.5), crs=ccrs.PlateCarree())
#ax.set_yticks(np.arange(lat_set_2_5_min, lat_set_2_5_max+0.0001, 2.5), crs=ccrs.PlateCarree())
#
##海岸線をプロット
#ax.coastlines(resolution='10m', color=r'white', alpha=0.5)

#ax.imshow(rgb, extent=[lon_set_min, lon_set_max, lat_set_min, lat_set_max])
ax1.imshow(rgb, extent=[lon_min, lon_max, lat_min, lat_max])

ax1.set_xlim(lon_set_min, lon_set_max)
ax1.set_ylim(lat_set_min, lat_set_max)

ax1.grid(which='both', axis='both', lw='0.5', alpha=0.5)
ax1.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=3, c='white')

ax2 = fig.add_subplot(122, title=r'enlarged', xlabel=r'longitude', ylabel=r'latitude')
ax2.imshow(rgb, extent=[lon_min, lon_max, lat_min, lat_max])
ax2.set_xlim(lon_set_min_enlarged, lon_set_max_enlarged)
ax2.set_ylim(lat_set_min_enlarged, lat_set_max_enlarged)
ax2.grid(which='both', axis='both', lw='0.5', alpha=0.5)
ax2.scatter(nishinoshima_lon, nishinoshima_lat, marker='o', s=3, c='white')

#fig.savefig(f'/home/satanka/Documents/isee_remotesensing/plot/himawari_ashrgb/{yyyy}{mm}{dd}{hh}{mn}.png')

plt.show()


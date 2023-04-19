import urllib.request
import os
import bz2
import numpy as np
from osgeo import gdal, osr
import tarfile
import datetime
from multiprocessing import Pool

#以下で扱うデータの情報は千葉大学環境リモートセンシング研究センターのサイトを参照
# http://www.cr.chiba-u.jp/databases/GEO/H8_9/FD/index_jp_V20190123.html

#バンド帯の指定
band_11 = '09'
band_13 = '01'
band_14 = '02'
band_15 = '03'

#データのサイズ
pixel_number = 6000
line_number = 6000

#座標系の定義
lon_min, lon_max = 85, 205
lat_min, lat_max = -60, 60

#西之島の座標
nishinoshima_lon = 140.879722
nishinoshima_lat = 27.243889

#開始日時(1日~31日まで回す)
year = 2020
start_month = 7
end_month = 7

#日時の指定
def time_and_date(year, month, day, hour):
    yyyy    = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month
    dd      = str(day).zfill(2)     #day
    hh      = str(hour).zfill(2)    #hour (UTC)
    mn      = '00'                  #minutes (UTC)
    return yyyy, mm, dd, hh, mn

#日時の確認(うるう年非対応)
def time_check(month, day):
    if (month == 2):
        if (day > 28):
            return False
        else:
            return True
    elif (month == 4 or month == 6 or month == 9 or month == 11):
        if (day > 30):
            return False
        else:
            return True
    else:
        if (day > 31):
            return False
        else:
            return True
        
#ファイルの確認
def check_file_exists(filename):
    if os.path.isfile(filename):
        return True
    else:
        return False

#ファイルの名前の指定
def bz2_filename(yyyy, mm, dd, hh, mn, band):
    return f'{yyyy}{mm}{dd}{hh}{mn}.tir.{band}.fld.geoss.bz2'

#ファイルのURLの指定
def url_name(yyyy, mm, fname):
    return f'ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/{yyyy}{mm}/TIR/{fname}'

#ファイルのダウンロード、解凍、データ取得、ファイル削除
def file_data_get(url, fname, band):
    filename_url = os.path.basename(url)
    urllib.request.urlretrieve(url, filename_url)
    _, tbb = np.loadtxt(f"count2tbb_v103/tir.{band}", unpack=True)
    with bz2.BZ2File(fname) as bz2file:
        dataDN = np.frombuffer(bz2file.read(), dtype=">u2").reshape(pixel_number, line_number)
        data = np.float32(tbb[dataDN])
    os.remove(fname)
    return data

#Ash RGB データ作成(inverse=1で反転、0で反転しない)
def AshRGB_data(data, min_K, max_K, gamma, inverse):
    if(inverse == 0):
        data_RGB = (max_K - data) / (max_K - min_K)
    elif(inverse == 1):
        data_RGB = (data - min_K) / (max_K - min_K)
    else:
        print(r'error!: inverse value')
        quit()
    data_RGB[data_RGB < 0] = 0E0
    data_RGB[data_RGB > 1] = 1E0
    data_RGB = data_RGB**gamma
    return data_RGB


#放射輝度への変換テーブルのダウンロード
cfname = "count2tbb_v103.tgz"
url_cfname = f"ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/support/{cfname}"
filename_url_cfname = os.path.basename(url_cfname)
urllib.request.urlretrieve(url_cfname, filename_url_cfname)

# tarファイルを解凍する
with tarfile.open(filename_url_cfname, 'r:gz') as tar:
    tar.extractall()

#ループ
def main_loop_function(args):
    year, month_int, day_int, hour_int = args
    yyyy, mm, dd, hh, mn = time_and_date(year, month_int, day_int, hour_int)

    if (time_check(month_int, day_int) == False):
        return
    
    if (check_file_exists(f'/mnt/j/isee_remote_data/himawari_AshRGB_GeoTIFF/{yyyy}{mm}/{yyyy}{mm}{dd}/{yyyy}{mm}{dd}{hh}{mn}.tif') == True):
        return
    
    now = str(datetime.datetime.now())
    print(f'{now}     Now Downloading: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

    fname_11 = bz2_filename(yyyy, mm, dd, hh, mn, band_11)
    fname_13 = bz2_filename(yyyy, mm, dd, hh, mn, band_13)
    fname_14 = bz2_filename(yyyy, mm, dd, hh, mn, band_14)
    fname_15 = bz2_filename(yyyy, mm, dd, hh, mn, band_15)

    url_11 = url_name(yyyy, mm, fname_11)
    url_13 = url_name(yyyy, mm, fname_13)
    url_14 = url_name(yyyy, mm, fname_14)
    url_15 = url_name(yyyy, mm, fname_15)

    data_tbb_11 = file_data_get(url_11, fname_11, band_11)
    data_tbb_13 = file_data_get(url_13, fname_13, band_13)
    data_tbb_14 = file_data_get(url_14, fname_14, band_14)
    data_tbb_15 = file_data_get(url_15, fname_15, band_15)

    now = str(datetime.datetime.now())
    print(f'{now}     Downloading is finished.: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

    data_diff_tbb_11_14 = np.float32(data_tbb_11 - data_tbb_14)
    data_diff_tbb_13_15 = np.float32(data_tbb_13 - data_tbb_15)

    #now = str(datetime.datetime.now())
    #print(f'{now}     Now Making Ash RGB data: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')

    #Himawari Ash RGBクイックガイドを参照 (https://www.data.jma.go.jp/mscweb/ja/prod/pdf/RGB_QG_Ash_jp.pdf)
    data_red    = AshRGB_data(data_diff_tbb_13_15,  -3.0E0,   7.5E0,  1.0E0, 0)
    data_green  = AshRGB_data(data_diff_tbb_11_14,  -5.9E0,   5.1E0, 8.5E-1, 0)
    data_blue   = AshRGB_data(        data_tbb_13, 243.6E0, 303.2E0,  1.0E0, 1)

    #ディレクトリの生成 (ディレクトリは要指定)
    try:
        os.makedirs(f'/mnt/j/isee_remote_data/himawari_AshRGB_GeoTIFF/{yyyy}{mm}/{yyyy}{mm}{dd}')
    except FileExistsError:
        pass

    #GeoTIFFファイルの作成
    now = str(datetime.datetime.now())
    print(f'{now}     Making GeoTIFF file: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')
    driver = gdal.GetDriverByName("GTiff")
    filename = f'/mnt/j/isee_remote_data/himawari_AshRGB_GeoTIFF/{yyyy}{mm}/{yyyy}{mm}{dd}/{yyyy}{mm}{dd}{hh}{mn}.tif'
    dataset = driver.Create(filename, pixel_number, line_number, 3, gdal.GDT_Float32)

    #RGBデータをGeoTIFFファイルに書き込む
    dataset.GetRasterBand(1).WriteArray(data_red)
    dataset.GetRasterBand(2).WriteArray(data_green)
    dataset.GetRasterBand(3).WriteArray(data_blue)

    # GeoTIFFファイルに座標情報を追加する
    lon_resolution = (lon_max - lon_min) / pixel_number
    lat_resolution = (lat_max - lat_min) / line_number
    geotransform = (lon_min, lon_resolution, 0, lat_max, 0, -lat_resolution)
    dataset.SetGeoTransform(geotransform)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326) #参考(https://www.spf.org/global-data/opri/visual/rep02_wvis_search.pdf)
    dataset.SetProjection(srs.ExportToWkt())

    #ディレクトリの生成 (ディレクトリは要指定)
    try:
        os.makedirs(f'/mnt/j/isee_remote_data/himawari_AshRGB_GeoTIFF/{yyyy}{mm}/{yyyy}{mm}{dd}')
    except FileExistsError:
        pass
    
    #GeoTIFFファイルの保存 (保存先は要指定)
    dataset.FlushCache()
    dataset = None
    now = str(datetime.datetime.now())
    print(f'{now}     GeoTIFF file is saved.: {yyyy}/{mm}/{dd} {hh}:{mn} UTC')
    return

#main_loop_function([year, 7, 1, 14])

#for month_int in range(start_month, end_month+1):
#    for day_int in range(1, 32):
#        for hour_int in range(0, 24):
#            main_loop_function([month_int, day_int, hour_int])

##並列処理
if __name__ == '__main__':
    
    #プロセス数
    num_processes = 8

    #並列処理の指定
    with Pool(processes=num_processes) as pool:
        pool.map(main_loop_function, 
                 [(year, month_int, day_int, hour_int) 
                  for month_int in range(start_month, end_month+1)
                  for day_int in range(1, 32)
                  for hour_int in range(0, 24)],
                  chunksize=1)
        
print('finish')
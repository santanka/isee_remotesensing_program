from osgeo import gdal
import datetime
from multiprocessing import Pool

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
        
def main(args):
    year_int, month_int, day_int, hour_int = args
    yyyy, mm, dd, hh, mn = time_and_date(year_int, month_int, day_int, hour_int)

    if (time_check(month_int, day_int) == False):
        return
    
    # 入力ファイル名と出力ファイル名を定義する
    input_file = f'/mnt/j/isee_remote_data/himawari_AshRGB_GeoTIFF/{yyyy}{mm}/{yyyy}{mm}{dd}/{yyyy}{mm}{dd}{hh}{mn}.tif'
    output_file = f'/mnt/j/isee_remote_data/himawari_AshRGB_GeoTIFF/{yyyy}{mm}/{yyyy}{mm}{dd}/{yyyy}{mm}{dd}{hh}{mn}.tif'

    # GDALで入力ファイルを開く
    input_dataset = gdal.Open(input_file)

    # 出力ファイルを作成する
    driver = gdal.GetDriverByName("GTiff")
    output_dataset = driver.CreateCopy(output_file, input_dataset, 0)

    # 圧縮設定を定義する
    options = ['COMPRESS=DEFLATE', 'PREDICTOR=2', 'ZLEVEL=9']

    # 圧縮を実行する
    output_dataset.BuildOverviews("AVERAGE", options=options)

    # データセットを解放する
    input_dataset = None
    output_dataset = None

##並列処理
if __name__ == '__main__':
    
    #プロセス数
    num_processes = 16

    #並列処理の指定
    with Pool(processes=num_processes) as pool:
        pool.map(main, 
                 [(year, month_int, day_int, hour_int) 
                  for month_int in range(start_month, end_month+1)
                  for day_int in range(1, 32)
                  for hour_int in range(0, 24)],
                  chunksize=1)
        

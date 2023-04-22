import cv2
import os
import datetime

year = 2020

#ファイルの確認
def check_file_exists(filename):
    if os.path.isfile(filename):
        return True
    else:
        return False
    

for month in range (6, 10):
    for pressure_idx in range(19):
        yyyy = str(year).zfill(4)    #year
        mm      = str(month).zfill(2)   #month

        # 画像が含まれるフォルダのパス
        folder_path = f'/mnt/j/isee_remote_data/himawari_AshRGB_enlarged/{yyyy}{mm}/'

        # 動画ファイルの設定
        video_filename = f'{folder_path}{yyyy}{mm}.mp4'

        if(check_file_exists(video_filename) == True):
            continue
        else:
            now = str(datetime.datetime.now())
            print(f'{now}     Making video.: {video_filename}')

        fps = 10  # フレームレート

        # 画像を読み込む
        images = []
        for filename in sorted(os.listdir(folder_path)):
            if filename.endswith('.jpg') or filename.endswith('.png'):
                filepath = os.path.join(folder_path, filename)
                image = cv2.imread(filepath)
                images.append(image)

        # 動画ファイルに保存する
        height, width, channels = images[0].shape
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # 出力フォーマット
        video_writer = cv2.VideoWriter(video_filename, fourcc, fps, (width, height))

        for image in images:
            video_writer.write(image)

        video_writer.release()

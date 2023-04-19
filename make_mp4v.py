import cv2
import os

year = 2022

for month in range (9, 12):
    yyyy = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month

    # 画像が含まれるフォルダのパス
    folder_path = f'/mnt/j/isee_remote_data/himawari_AshRGB/{yyyy}{mm}/'

    # 動画ファイルの設定
    video_filename = f'/mnt/j/isee_remote_data/himawari_AshRGB/{yyyy}{mm}/{yyyy}{mm}.mp4'
    fps = 5  # フレームレート

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

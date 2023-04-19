from PIL import Image
import os

year = 2022

for month in range (9, 12):
    yyyy = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month

    # 画像が含まれるフォルダのパス
    folder_path = f'/mnt/j/isee_remote_data/himawari_AshRGB/{yyyy}{mm}/'

    # 画像を読み込む
    images = []
    for filename in sorted(os.listdir(folder_path)):
        if filename.endswith('.jpg') or filename.endswith('.png'):
            filepath = os.path.join(folder_path, filename)
            image = Image.open(filepath)
            images.append(image)

    # GIFファイルに保存する
    gif_filepath = f'/mnt/j/isee_remote_data/himawari_AshRGB/{yyyy}{mm}/{yyyy}{mm}.gif'
    images[0].save(gif_filepath, save_all=True, append_images=images[1:], duration=10, loop=0)
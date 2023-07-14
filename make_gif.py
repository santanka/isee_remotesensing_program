from PIL import Image
import os

year = 2020

for month in range (8, 9):
    yyyy = str(year).zfill(4)    #year
    mm      = str(month).zfill(2)   #month

    # 画像が含まれるフォルダのパス
    folder_path = f'/mnt/j/isee_remote_data/himawari_AshRGB_enlarged_average_central_point/{yyyy}{mm}/'

    # 画像を読み込む
    images = []
    for filename in sorted(os.listdir(folder_path)):
        if filename.endswith('.jpg') or filename.endswith('.png'):
            filepath = os.path.join(folder_path, filename)
            image = Image.open(filepath)
            images.append(image)

    # GIFファイルに保存する
    gif_filepath = f'/mnt/j/isee_remote_data/himawari_AshRGB_enlarged_average_central_point/{yyyy}{mm}/{yyyy}{mm}.gif'
    images[0].save(gif_filepath, save_all=True, append_images=images[1:], duration=500, loop=0)
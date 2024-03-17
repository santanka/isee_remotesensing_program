import pandas as pd

# 元のCSVファイルを読み込む
dir_path = f'/mnt/j/isee_remote_data/JST/himawari8_radiance_data/TIR_05'
file_path = f'{dir_path}/old/20150101_20151231_JST.csv'
df = pd.read_csv(file_path)

# 'year'と'month'の列を組み合わせて新しい'year_month'列を作成
df['year_month'] = df['year'].astype(str) + df['month'].astype(str).str.zfill(2)

# 月ごとにデータを分割し、それぞれの月に対応する新しいCSVファイルに保存
for month in df['year_month'].unique():
    # 特定の月のデータを抽出
    month_df = df[df['year_month'] == month]

    # 新しいファイル名を作成 (例: 201507_JST.csv)
    new_file_name = f'{dir_path}/{month}_JST.csv'

    # year_month以外の列をcsvファイルとして保存
    month_df.drop('year_month', axis=1).to_csv(new_file_name, index=False)
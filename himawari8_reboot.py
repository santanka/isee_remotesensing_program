import time
import subprocess

while True:
    start_time = time.time()
    print(str(start_time) + r'     Reboot')
    while True:
        try:
            p = subprocess.Popen(["python3", "/home/satanka/Documents/isee_remotesensing/program/himawari8_bz2_plot_auto.py"])  # 実行したいプログラムを指定
            break
        except:
            time.sleep(1)  # エラーが表示された場合、1秒待機してから再起動

    while True:
        time.sleep(1)  # プログラムの実行中は1秒毎に監視する
        elapsed_time = time.time() - start_time
        if elapsed_time > 600:  # 10分経過したらプログラムを終了させる
            p.terminate()
            break
        elif p.poll() is not None:  # プログラムが終了した場合
            if p.poll() == 0:  # プログラムが正常に終了した場合、10分間待機
                time.sleep(600 - elapsed_time)
                break
            else:  # プログラムが異常終了した場合に再起動
                break

    if elapsed_time > 600:  # プログラムが異常終了した場合に再起動
        continue
    else:
        break  # プログラムが正常に終了した場合に終了

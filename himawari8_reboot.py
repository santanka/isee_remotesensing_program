import time
import subprocess

while True:
    start_time = time.time()
    print(str(start_time) + r'     Reboot')
    p = subprocess.call(["python3", "/home/satanka/Documents/isee_remotesensing/program/himawari8_netcdf_plot_auto.py"])  # 実行したいプログラムを指定
    
    while p.poll() is None:
        time.sleep(1)  # プログラムの実行中は1秒毎に監視する
        elapsed_time = time.time() - start_time
        if elapsed_time > 600:  # 10分経過したらプログラムを終了させる
            p.terminate()
    time.sleep(600 - elapsed_time)  # 10分毎に実行するためのウェイト

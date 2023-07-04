import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['text.usetex'] = True
mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Computer Modern Roman']
mpl.rcParams['mathtext.fontset'] = 'cm'
plt.rcParams["font.size"] = 20

data = np.genfromtxt('/home/satanka/Documents/isee_remotesensing/data/GL820_01_2023-03-07_10-39-49_small.csv', delimiter=',', skip_header=23, encoding='Shift-JIS')

no = data[:, 0]
time = data[:, 1]
ms = data[:, 2]
degree_CH2 = data[:, 3] #A1
degree_CH3 = data[:, 4] #A2
degree_CH5 = data[:, 5] #A3
degree_CH6 = data[:, 6] #B1
degree_CH7 = data[:, 7] #B2
degree_CH9 = data[:, 8] #B3

timer = np.float16(no-no[0])/3600

plt.plot(timer, degree_CH2, label=r'CH2')
plt.plot(timer, degree_CH3, label=r'CH3')
plt.plot(timer, degree_CH5, label=r'CH5')
plt.plot(timer, degree_CH6, label=r'CH6')
plt.plot(timer, degree_CH7, label=r'CH7')
plt.plot(timer, degree_CH9, label=r'CH9')
plt.xlabel(r'hour')
plt.ylabel(r'temperature [${}^{\circ}$C]')
plt.minorticks_on()
plt.grid(which='both', alpha=0.3)
plt.legend()
plt.show()
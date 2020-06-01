# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 13:28:14 2019
@author: admtableau
"""
#carga paquetes
import os
from datetime import datetime
path='C:/users/admtableau/desktop/tabpy/'
os.chdir(path)
from fx_runflow import fx_origen_a_csv
from fx_runflow import fx_tad
from fx_runflow import fx_runflow
#tarea programada en task scheduler del nodo 172.24.10.167 cada una hora a las XXh.30'
if datetime.now().weekday() <=4 and datetime.now().hour == 5:
    fx_origen_a_csv([])
if datetime.now().weekday() <=4 and datetime.now().hour == 6:    
    fx_runflow([])
    fx_runflow([0])
if datetime.now().weekday() <=4 and datetime.now().hour == 15:    
    fx_runflow([35,38])
if datetime.now().weekday() <=4 and datetime.now().hour in list(range(8,19)):
    #fx_origen_a_csv([])
    fx_tad()
    fx_runflow([27,0])

'''
ejecucion a demanda ATL
fx_origen_a_csv([30,31,32])
fx_runflow([37])

ejecucion a demanda CM
fx_origen_a_csv([0])
fx_runflow([11,16,17,18,21,29])

fx_runflow([27,29])

fx_runflow([18,21,27,29])

'''
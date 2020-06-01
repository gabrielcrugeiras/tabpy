# -*- coding: utf-8 -*-
"""
Created on Thu May 28 09:45:48 2020
@author: admtableau

import pandas as pd
df=pd.read_csv('C:/Users/admtableau/Downloads/Output_plazoX3_out.csv',sep=';')
df.to_csv('C:/Users/admtableau/Downloads/Output_plazoX3_in.csv')
"""
#funcion calculo dias habiles
def BusinessDays(df):
    import pandas as pd
    import numpy as np
    from datetime import datetime, date, time, timedelta
    #lista feriados
    cal_srt=np.busdaycalendar(holidays=[
        #feriados 2017
        '2017-02-27','2017-02-28','2017-03-24','2017-04-13','2017-04-14','2017-05-01','2017-05-25','2017-06-20','2017-06-27','2017-08-21','2017-10-16','2017-11-20','2017-12-08','2017-12-25',
        #feriados 2018
        '2018-01-01','2018-02-12','2018-02-13','2018-03-29','2018-03-30','2018-04-02','2018-04-30','2018-05-01','2018-05-25','2018-06-20','2018-06-27','2018-07-09','2018-08-20','2018-10-15','2018-11-19','2018-12-24','2018-12-25','2018-12-31',
        #feriados 2019
        '2019-01-01','2019-03-04','2019-03-05','2019-03-31','2019-04-18','2019-04-19','2019-05-01','2019-06-17','2019-06-20','2019-06-27','2019-07-08','2019-07-09','2019-08-19','2019-10-14','2019-11-18','2019-12-25',
        #feriados 2020
        '2020-01-01','2020-02-24','2020-02-25','2020-03-23','2020-03-24','2020-03-31','2020-04-09','2020-04-10','2020-04-24','2020-05-01','2020-05-25','2020-06-15','2020-07-09','2020-07-10','2020-08-17','2020-10-12','2020-11-23','2020-12-07','2020-12-08','2020-12-25',
        #cuarentena COVID19
        #2020-03
        '2020-03-20',
        '2020-03-25','2020-03-26','2020-03-27',
        '2020-03-30',
        #2020-04
        '2020-04-01','2020-04-02','2020-04-03',
        '2020-04-06','2020-04-07','2020-04-08',
        '2020-04-13','2020-04-14','2020-04-15','2020-04-16','2020-04-17',
        '2020-04-20','2020-04-21','2020-04-22','2020-04-23',
        '2020-04-27','2020-04-28','2020-04-29','2020-04-30',                                        
        #2020-05
        '2020-05-04','2020-05-05','2020-05-06','2020-05-07','2020-05-08',
        '2020-05-11','2020-05-12','2020-05-13','2020-05-14','2020-05-15',
        '2020-05-18','2020-05-19','2020-05-20','2020-05-21','2020-05-22',
        '2020-05-25','2020-05-26','2020-05-27','2020-05-28','2020-05-29',        
        ])
    #plazos a calcular
    plazos={'Plazo BOA':['Fecha caratulacion','kfin EDA'], #aplica Circuito=15 & 3+Motivo=R
            'Plazo LOOP':['kini LOOP','kfin LOOP'], #no aplica Circuito=5
            'Plazo citacion CMJ':['kini citacion','kfin citacion'], #no aplica Circuito=5
            'Plazo dictamen CMJ':['kfin citacion','kfin dictamen'], #no aplica Circuito=5
            'Plazo SH':['Fecha ingreso SH','kfin SH'], #aplica Circuito=15
            'Plazo BO':['Plazo BOA','Plazo LOOP'],
            'Plazo TM':['Plazo citacion CMJ','Plazo dictamen CMJ','Plazo SH'],
            'Plazo completo':['Plazo BO','Plazo TM'],
            'Plazo Ley 27348':['Plazo TM'], #aplica Circuito=15
            'Plazo Loop+Ley 27348':['Plazo LOOP','Plazo Ley 27348'], #aplica Circuito=15
            }
    #conversion a datetime
    for c in df.columns.drop(['Expediente Nro','Circuito','Motivo expediente (grupo)']):
        df[c] = pd.to_datetime(df[c],format='%Y-%m-%dZ', dayfirst=True, infer_datetime_format=True, exact=True, errors='raise')
        df[c] = df[c].dt.date
    #calculo de plazos
    for i in plazos.keys():
        print('-- Ejecutando',i)
        inicio=datetime.now()
        if i=='Plazo BOA':
            df2=( df[df.Circuito.isin(['1','5']) | (df.Circuito.isin(['3']) & df['Motivo expediente (grupo)'].isin(['R']))] #filtra filas
                [[plazos[i][0],plazos[i][1],'Fecha actualizacion']] #filtra columnas
                .rename(columns={plazos[i][0]:'date1',plazos[i][1]:'date2'}) ) #renombra
            df2['date2']=df2['date2'].fillna(df2['Fecha actualizacion']) #completa nulos
            df[i]=df2.apply(lambda x:np.busday_count(x['date1'],x['date2'],busdaycal=cal_srt),axis=1) #calcula plazo
        elif i=='Plazo LOOP':
            df2=( df[~df.Circuito.isin(['5']) & df[plazos[i][0]].notnull()]
                [[plazos[i][0],plazos[i][1],'Fecha actualizacion']] 
                .rename(columns={plazos[i][0]:'date1',plazos[i][1]:'date2'}) ) 
            df2['date2']=df2['date2'].fillna(df2['Fecha actualizacion'])
            df[i]=df2.apply(lambda x:np.busday_count(x['date1'],x['date2'],busdaycal=cal_srt),axis=1)
        elif i=='Plazo citacion CMJ':
            df2=( df[~df.Circuito.isin(['5']) & df[plazos[i][0]].notnull()]
                [[plazos[i][0],plazos[i][1],'Fecha actualizacion']] 
                .rename(columns={plazos[i][0]:'date1',plazos[i][1]:'date2'}) ) 
            df2['date2']=df2['date2'].fillna(df2['Fecha actualizacion']) 
            df[i]=df2.apply(lambda x:np.busday_count(x['date1'],x['date2'],busdaycal=cal_srt),axis=1)
        elif i=='Plazo dictamen CMJ':
            df2=( df[~df.Circuito.isin(['5']) & df[plazos[i][0]].notnull()]
                [[plazos[i][0],plazos[i][1],'Fecha actualizacion']] 
                .rename(columns={plazos[i][0]:'date1',plazos[i][1]:'date2'}) ) 
            df2['date2']=df2['date2'].fillna(df2['Fecha actualizacion']) 
            df[i]=df2.apply(lambda x:np.busday_count(x['date1'],x['date2'],busdaycal=cal_srt),axis=1)
        elif i=='Plazo SH':
            df2=( df[df.Circuito.isin(['1','5']) & df[plazos[i][0]].notnull()]
                [[plazos[i][0],plazos[i][1],'Fecha actualizacion']] 
                .rename(columns={plazos[i][0]:'date1',plazos[i][1]:'date2'}) ) 
            df2['date2']=df2['date2'].fillna(df2['Fecha actualizacion'])
            df[i]=df2.apply(lambda x:np.busday_count(x['date1'],x['date2'],busdaycal=cal_srt),axis=1)
        elif i=='Plazo BO':
            df2=( df[df[plazos[i][0]].notnull() | df[plazos[i][1]].notnull()] )
            df[i]=df2[plazos[i][0]].fillna(0) + df2[plazos[i][1]].fillna(0) 
        elif i=='Plazo TM':
            df2=( df[df[plazos[i][0]].notnull() | df[plazos[i][1]].notnull() | df[plazos[i][2]].notnull()] )
            df[i]=df2[plazos[i][0]].fillna(0) + df2[plazos[i][1]].fillna(0) + df2[plazos[i][2]].fillna(0) 
        elif i=='Plazo completo':
            df2=df[[plazos[i][0],plazos[i][1]]]
            df[i]=( df[plazos[i][0]].fillna(0) + df[plazos[i][1]].fillna(0) ) 
        elif i=='Plazo Ley 27348': 
            df[i]=( df[df.Circuito.isin(['1','5'])][plazos[i][0]] )
        elif i=='Plazo Loop+Ley 27348': 
            df2=( df[df.Circuito.isin(['1','5']) & df[plazos[i][1]].notnull()] )
            df[i]=df2[plazos[i][0]].fillna(0) + df2[plazos[i][1]].fillna(0)
        fin=datetime.now()
        time_run=fin-inicio
        print(i,': avg:',round(df[i].mean(),2),' - Tiempo carga:',round(time_run.total_seconds(),2),'segundos',' - Registros:',len(df2))    
    df=df[['Expediente Nro','Plazo BOA','Plazo LOOP','Plazo citacion CMJ','Plazo dictamen CMJ','Plazo SH','Plazo BO','Plazo TM','Plazo completo','Plazo Ley 27348','Plazo Loop+Ley 27348',]]
    return df 
#funcion tableauprep flow in
def GetBusinessDays(df):
    df=BusinessDays(df)
    return df
 #funcion tableauprep flow out
def get_output_schema():
    return pd.DataFrame({'Expediente Nro': prep_string(),
                         'Plazo BOA': prep_int(),
                         'Plazo LOOP': prep_int(),
                         'Plazo citacion CMJ': prep_int(),
                         'Plazo dictamen CMJ': prep_int(),
                         'Plazo SH': prep_int(),
                         'Plazo BO': prep_int(),
                         'Plazo TM': prep_int(),
                         'Plazo completo': prep_int(),
                         'Plazo Ley 27348': prep_int(),
                         'Plazo Loop+Ley 27348': prep_int(),
                         })    
        
'''
Run plazoX3.py 31/05/2020
-- Ejecutando Plazo BOA
Plazo BOA : avg: 10.82  - Tiempo carga: 4.09 segundos  - Registros: 48182
-- Ejecutando Plazo LOOP
Plazo LOOP : avg: 3.35  - Tiempo carga: 4.97 segundos  - Registros: 62634
-- Ejecutando Plazo citacion CMJ
Plazo citacion CMJ : avg: 10.38  - Tiempo carga: 3.18 segundos  - Registros: 43102
-- Ejecutando Plazo dictamen CMJ
Plazo dictamen CMJ : avg: 9.68  - Tiempo carga: 2.29 segundos  - Registros: 28735
-- Ejecutando Plazo SH
Plazo SH : avg: 3.87  - Tiempo carga: 0.38 segundos  - Registros: 3993
-- Ejecutando Plazo BO
Plazo BO : avg: 9.83  - Tiempo carga: 0.17 segundos  - Registros: 74430
-- Ejecutando Plazo TM
Plazo TM : avg: 16.98  - Tiempo carga: 0.06 segundos  - Registros: 43627
-- Ejecutando Plazo completo
Plazo completo : avg: 19.78  - Tiempo carga: 0.02 segundos  - Registros: 74430
-- Ejecutando Plazo Ley 27348
Plazo Ley 27348 : avg: 17.17  - Tiempo carga: 0.04 segundos  - Registros: 74430
-- Ejecutando Plazo Loop+Ley 27348
Plazo Loop+Ley 27348 : avg: 21.49  - Tiempo carga: 0.03 segundos  - Registros: 19371
'''
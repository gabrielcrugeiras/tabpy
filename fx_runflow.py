# -*- codin2g: utf-8 -*-
"""
Created on Wed Nov 27 11:14:28 2019
@author: gcrugeiras
Funcion para actualizacion flujos tableau prep
"""
# =============================================================================
# fx_origen_a_csv([48,49])
# =============================================================================
def fx_origen_a_csv(lista_archivos):
    import pandas as pd
    import os
    import hashlib
    df_hash=pd.read_csv('//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Fuentes/Metafuentes/CM_ENLACE_CM.csv')
    df_hash=pd.DataFrame(df_hash.Provincia.unique().tolist())
    df_hash.columns=['Provincia']
    df_hash['hash']=df_hash['Provincia'].str.encode ('utf-8').apply(lambda x: (hashlib.sha1 (x) .hexdigest ()))
    os.chdir('C:/users/admtableau/desktop')
    path_hyper='//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Fuentes/Metafuentes/'
    archivos={ 0:['CM_ENLACE_CM','//srt-fs01/_fortalecimiento_cm$/Enlace CM.xlsx','enlace CM'],
               1:['ADM_CASPM_LOTE','//srt-fs01/_adm_finanzas$/lote caspm.xlsx','Hoja1'],
               2:['ADM_COMPRAS_PAC','//srt-fs01/_adm_finanzas$/administracion/compras/proceso compras.xlsx','PAC'],
               3:['ADM_COMPRAS_OC','//srt-fs01/_adm_finanzas$/administracion/compras/detalle_ordenes_de_compra_crosstab.xlsx','Detalle_ordenes_de_compra_cross'],
               4:['ADM_COMPRAS_PLIEGOS','//srt-fs01/_adm_finanzas$/administracion/compras/hoja_detalle_tiempos_crosstab.xlsx','Hoja_Detalle_tiempos_crosstab'],
               5:['ADM_COMPRAS_SIN_AREA','//srt-fs01/_adm_finanzas$/administracion/compras/pliegos_sin_area_en_comprar.xlsx','Hoja1'],
               6:['ADM_EJECUCION_CREDITO','//srt-fs01/_adm_finanzas$/credito y ejecucion srt.xlsx','Sheet1'],
               7:['ADM_ATL_CONVENIOS','//srt-fs01/_adm_finanzas$/convenios por provincias/atl administracion.xlsx','convenios x rubro anual'],
               8:['ADM_OBRAS_AVANCE','//fileserversrt/sinfraestructura$/obras comisiones medicas/copia de avance obras cm.xlsx','avance'],
               9:['ADM_PAGOS_PENDIENTES','//srt-fs01/_adm_finanzas$/administracion/consulta de esidif pagos pendientes de autorizacion.xls','Hoja1'],
              10:['ADM_RECUPERO_CM','//srt-fs01/_adm_finanzas$/administracion/solicitud recupero gastos cm.xlsx','Hoja1'],
              11:['ADM_RECUPERO_MATRIZ','//srt-fs01/_adm_finanzas$/administracion/solicitud recupero gastos cm.xlsx','Hoja2'],
              12:['ADM_BALANCE_OBJETIVOS','//srt-fs01/_adm_finanzas$/administracion/Seguimiento de objetivos - Area Balance.xlsx','Fuente de datos'],
              13:['ADM_FINANZAS_DETALLE','//srt-fs01/_adm_finanzas$/administracion/indicadores financieros/indicadores financieros.xlsx','Detalle'],
              14:['ADM_FINANZAS_INDICADORES','//srt-fs01/_adm_finanzas$/administracion/indicadores financieros/indicadores financieros.xlsx','Indicadores'],
              15:['ADM_FINANZAS_CARTERA','//srt-fs01/_adm_finanzas$/administracion/indicadores financieros/indicadores financieros.xlsx','Cartera total'],
              16:['ADM_INGRESOS_FG','//srt-fs01/_adm_finanzas$/Rendimiento de las inversiones.xlsx','Hoja1'],
              17:['SRT_ART','//srt-fs01/_fortalecimiento_cm$/Enlace ART.xlsx','ART'],
              18:['SRT_ART_BAJAS','//srt-fs01/_fortalecimiento_cm$/Enlace ART.xlsx','sin PE_CONTRATOS'],
              19:['SRT_PROVINCIAS_ADHERIDAS','//fileserversrt/gpieycg$/02. SUBGCIA PLANIFICACION/Provincias adheridas a la ley 27348.xlsx','Hoja1'],
              20:['SRT_INDICADORES_GG','//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Fuentes/SRT/Indicadores GG.xlsx','Hoja1'],
              21:['PF_CUILES','//fileserversrt/SGEVALARTYCMS$/Repositorio BI Fraude/Cuiles ingresados a CCMM.xlsx','Hoja1'],
              22:['PRV_EVAL_CONDUCTAS_RPT','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Evaluación de Conductas.xlsx','REPORTE'],
              23:['PRV_EVAL_CONDUCTAS_CTRL','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Evaluación de Conductas.xlsx','Evaluación de Puntos de control'],
              24:['PRV_EVAL_CONDUCTAS_GESTION','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Gestión Expedientes Evaluación de Conductas.xlsx','Hoja1'],
              25:['PRV_CYMAT_CBA','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Consolidado CyMAT 2.xlsx','Córdoba'],
              26:['PRV_CYMAT_BA','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Consolidado CyMAT 2.xlsx','Buenos Aires'],
              27:['PRV_CYMAT_MZA','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Consolidado CyMAT 2.xlsx','Mendoza'],
              28:['PRV_CYMAT_LIT','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Consolidado CyMAT 2.xlsx','Litoral'],
              29:['PRV_ACTAS_AG','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Actas total sistema (2013-2018).xlsx','Hoja1'],
              30:['ATL_CUMPLIMIENTO_OPERATIVO','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Cumplimiento Operativos Estacionales - Solicitudes urgentes de intervencion.xlsx','Cumplimiento Operativos'],
              31:['ATL_METAS','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Metas.xlsx','ATL'], #/Metas ATL 2018-2019.xlsx','Hoja1'
              32:['ATL_ENVIO_ROAM','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/ENVIOS DE ROAM ATL.xlsx','Hoja1'], #'PRV_ROAM_ACCIDENTES':'//fileserversrt/FS-TABLEAU$/Prevencion/ROAM Unificado - 2018-2019.xlsx','Detalle Accidentes'
              33:['CPM_PERSONAL','//fileserversrt/SGControldePrestacionesMedicas$/LISTAS_TABLEAU/SCPM personal-sector.xlsx','personal'],
              34:['CPM_ART_OUT','//fileserversrt/SGControldePrestacionesMedicas$/LISTAS_TABLEAU/SCPM personal-sector.xlsx','ART inactivas'],
              35:['JUR_PROYECCION','//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Fuentes/JUR/Planificacion PROC_SANC.xlsx','proyeccion anual'],
              36:['JUD_XLS_MULTAS_NO_SIM','//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Fuentes/JUR/multas- art- empleadores.xlsx','ART+Emp'],
              37:['JUD_XLS_MULTAS_ADM','//srt-fs01/_adm_finanzas$/administracion/multas srt/expedientes srt.xlsx','INVENTARIO'],
              38:['RH_DOTACION','//fileserversrt/drrhh$/TABLEAU/Archivos Tablero RRHH/BASE GENERAL DOTACION PARA TABLEAU.xlsx','Datos'],
              39:['RH_PUESTOS','//fileserversrt/drrhh$/TABLEAU/Archivos Tablero RRHH/TABLAS VARIAS PARA DOTACION.xlsx','Agrup. Categorías'],
              40:['RH_FORMACION','//fileserversrt/drrhh$/TABLEAU/Archivos Tablero RRHH/TABLAS VARIAS PARA DOTACION.xlsx','FORMACION'],
              41:['RH_SUELDOS','//fileserversrt/drrhh$/TABLEAU/Archivos Tablero RRHH/TABLAS VARIAS PARA DOTACION.xlsx','Remuneración (Liq. 25 Mes)'],
              42:['PRV_METAS_INSPECTORES','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Metas.xlsx','Por inspector'],
              43:['PRV_METAS_GERENCIA','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Metas.xlsx','Metas Gerencia'],
              44:['PRV_PRONAPRE_REUNIONES','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/PRONAPRE.xlsx','Reuniones'],
              45:['PRV_MED_AMB_GENERAL','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Mediciones Ambientales.xlsx','General'],
              46:['PRV_MED_AMB_AGENTE','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Mediciones Ambientales.xlsx','Por agente'],
              47:['PRV_AUDITORIA_EN_SEDE','//fileserversrt/FS-TABLEAU$/Prevencion/Fuentes de tableros publicados/Auditorías en sede.xlsx','Hoja1'],
              48:['RH_DOTACION_AUXILIAR','//fileserversrt/drrhh$/TABLEAU/Archivos Tablero RRHH/TABLAS VARIAS PARA DOTACION.xlsx','Remuneración (Liq. 25 Mes)'],
              49:['RH_CATEGORIAS','//fileserversrt/drrhh$/TABLEAU/Archivos Tablero RRHH/Historial de Categoria para Tableau.xlsx','Base Resumen %'],

              ##
              97:['PRV_INSPECTORES_COORDINACION','//fileserversrt/FS-TABLEAU$/Prevencion/Archivos anteriores/7-5-19/Coordinaciones por Inspectores (con validación de datos)_1.xlsx','INSPECTORES X COORDINACIONES'],
              98:['PRV_METAS_COORDINACIONES','//fileserversrt/FS-TABLEAU$/Prevencion/Archivos anteriores/7-5-19/Metas por coordinaciones.xlsx','Hoja1'],
              99:['PRV_INSPECTORES_X_COORDINACION','//fileserversrt/FS-TABLEAU$/Prevencion/Archivos anteriores/7-5-19/Inspec x coord.xlsx','Hoja1'],
              ##
              }
    if lista_archivos == []:
        arch=list(archivos.keys())
    else :
        archivos_manual=[]
        for a in lista_archivos:
            archivos_manual.append(a)
        arch=archivos_manual
    print('Archivos a ejecutar:',len(arch))
    print(arch)
    def importar_origen_xls(filein,sheetin,fileout):
        df=pd.read_excel(filein, sheet_name=sheetin)
        #condicional para ajustarse a particularidades de distintos archivos
        if fileout == 'ADM_EJECUCION_CREDITO':
            col=df[1:2]
            col_names=[]
            for i in col.iterrows():
                col_names.append(i)
            col=list(col_names[0][1])
            df = df.set_axis([col], axis=1, inplace=False)
            df=df[2:]
            del col_names, col
        elif fileout == 'CM_ENLACE_CM':
            df=df.merge(df_hash, how='left')
        df.to_csv(path_hyper+fileout+'.csv',index=False)
    def importar_origen_csv(filein,fileout):
        df=pd.read_csv(filein,header=0,encoding='utf-8',sep=';')
        df.to_csv(path_hyper+fileout+'.csv',index=False)
    print('---')
    for a in arch:
        print(a,':',archivos[a][0])
        if archivos[a][1].find('xls')>0:
            importar_origen_xls(archivos[a][1],archivos[a][2],archivos[a][0])
        elif archivos[f][1].find('csv')>0:
            importar_origen_csv(archivos[a][1],archivos[a][0])
        print('---')
# =============================================================================
# fx_tad()
# =============================================================================
def fx_tad():
    import pandas as pd
    import os
    import csv
    path='//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Fuentes/JUR/'
    os.chdir(path)
    col=['Número Expediente','Fecha','Tarea/Estado','Fecha Últ. Modif.','Código Trámite','Descripción del Trámite','Motivo','Motivo Pase',
         'Fecha G. Temp.','Fecha Archivo','Mod. Origen','Reparticion','Usuario Anterior','Usuario Generador','update']
    df=pd.DataFrame(columns=col)
    df.to_csv('TAD_csv/TAD.csv',index=False,header=True,encoding='utf-8',quoting=csv.QUOTE_ALL)
    for f in os.walk(path+'TAD_xlsx'): 
        files=f[2]
    files.remove('Thumbs.db')
    df_files=pd.DataFrame(files)
    df_files.columns=['file']
    df_files=df_files[df_files['file'].str.find('.XLSX')>=0]
    df_files['update']=df_files['file'].str[7:26]
    for f in files:
        print(f)
        df_new=pd.read_excel('TAD_xlsx/'+f,sheet_name='Expedientes',sort=False,encoding='utf-8')
        df_new['update']=f[7:26].replace('_','')
        df_new=df_new[col]
        df=pd.concat([df,df_new])    
    df = df.sort_values('Fecha Últ. Modif.').groupby('Número Expediente').last() 
    df = df.reset_index()
    df.to_csv('TAD_csv/TAD.csv',index=False,header=False,mode='a',encoding='utf-8',quoting=csv.QUOTE_ALL)
# =============================================================================
# fx_runflow([])
# =============================================================================
def fx_runflow(lista_flujos):
    #carga de librerias
    import pandas as pd
    import numpy as np
    import os
    from os import remove
    import subprocess
    from time import time, strftime
    from datetime import date, datetime
    import csv, operator
    #ruta de trabajo
    os.chdir('C:/Windows/system32')
    #parametros
    path_prep_20193='"C:/Program Files/Tableau/Tableau Prep Builder 2019.3/scripts/"'
    path_prep_20201='"C:/Program Files/Tableau/Tableau Prep Builder 2020.1/scripts/"'#2019.3
    path_bat='//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Flujos/'
    path_json='"//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Flujos/'
    path_tfl='"//srt-fs01/_gerencia_tecnica$/TABLEAU/PREP/Flujos/'
    json='credenciales.json'
    #lista de flujos 
    flujos_default={
                    0:'00 TAB',
                    #1:'01 CM_XLSX_A_META_FUENTE',
                    #2:'02 ADM_XLSX_A_META_FUENTE',
                    #3:'03 PRV_XLSX_A_META_FUENTE',
                    #4:'04 SRT_XLSX_A_META_FUENTE',
                    11:'11 CM_CMC_BD_A_META_FUENTE',
                    #12:'12 CM_CMJ_PREP1_BD_A_META_FUENTE',
                    #13:'13 CM_CMJ_PREP2_META_FUENTE',
                    #14:'14 CM_CMJ_PREP3_META_FUENTE',
                    #15:'15 PRV_BD_A_META_FUENTE',
                    16:'16 CM_CMJ_15_A_META_FUENTE',
                    17:'17 CM_CMJ_23_A_META_FUENTE',
                    18:'18 CM_CMJ_4_A_META_FUENTE',
                    #19:'19 CM_CMJ_BD_A_META_FUENTE',
                    21:'21 CM',
                    22:'22 ADM',
                    23:'23 PRV',
                    24:'24 TKT',
                    25:'25 PE',
                    26:'26 GG',
                    27:'27 JUR',
                    #28:'28 MULTAS',
                    29:'29 PF',
                    30:'30 ADM2',
                    31:'31 SIST',
                    #32:'32 CUOTA OMITIDA',
                    #33:'33 CON',
                    34:'34 RH',
                    35:'35 CP',
                    #36:'36 RH',
                    37:'37 ATL',
                    38:'38 EST',
                    }
    if lista_flujos == []:
        tfl=list(flujos_default.values())
    else :
        flujos_manual=[]
        for i in lista_flujos:
            flujos_manual.append(flujos_default[i])
        tfl=flujos_manual
    print('Flujos a ejecutar:',len(tfl))
    print(tfl)
    #construccion de scrips de actualizacion y guardado como .bat
    bat=[]
    for t in tfl:
        #ajuste de distintas versiones de prep para correr algunos flujos
        script=str( path_prep_20193+'tableau-prep-cli.bat -c '+path_json+json+'" -t '+path_tfl+t+'.tfl"' if (
                    t[0:2]=='16' or t[0:2]=='17' or t[0:2]=='18' or t[0:2]=='31') else (
                    path_prep_20201+'tableau-prep-cli.bat -c '+path_json+json+'" -t '+path_tfl+t+'.tfl"'))
#        script=str(path_exe+'tableau-prep-cli.bat -c '+path_json+json+'" -t '+path_tfl+t+'.tfl"')
        file = open(path_bat+t+'.bat', "w")
        file.write(script+os.linesep)
        file.close()
        bat.append(t)
    #ejecucion de los .bat y almacenamiento en log
    inicio=datetime.now()
    print('================')
    print('Inicio run_flow:',inicio.isoformat())
    print('================')
    output=[]
    cabecera=[]
    #indice de corrida
    with open(path_bat+"log_actualizacion_cabecera.csv", newline='') as f1:  
        r1=f1.read().splitlines()
    ind=str(int(r1[-2][:r1[-2].find(',')])+1)
    cabecera.append(ind)#indice corrida
    cabecera.append(inicio.isoformat())#inicio corrida
    #iteracion de flujos
    for b in bat:
        out=[]
        print(b)
        out.append(ind)#indice corrida
        start_time=datetime.now()
        out.append(str(b))#flujo
        out.append(start_time.isoformat())#inicio
        x=subprocess.run(path_bat+b+'.bat', capture_output=True)
        out.append(list(str(x).replace("\\\\",'/').split('\\r\\n'))[0:-1][-1])#detalle
        print(out[3])
        end_time=datetime.now()
        time_run=end_time-start_time
        print("Tiempo de ejecución: %0.2f segundos." % time_run.total_seconds())
        print('----')
        out.append(str(end_time.isoformat()))#fin
        out.append(time_run.total_seconds())#tiempo_ejecucion
        out.append(1 if out[3]=='Finished running the flow successfully.' else 0)#resultado flujo
        output.append(out)
        del x
        remove(path_bat+b+'.bat')
    #guardado detalle corridas en csv
    with open(path_bat+'log_actualizacion_flujos.csv', 'a') as f2:
        w2 = csv.writer(f2)
        for i in range(len(output)):
            w2.writerow(output[i])
    fin=datetime.now()
    time_run_total=fin-inicio
    #guardado cabecera en csv
    cabecera.append(fin.isoformat())#fin corrida
    cabecera.append(time_run_total.total_seconds())#tiempo ejecucion corrida
    cabecera.append(len(output))#flujos corridos
    ok=0
    for f in range(len(output)):
        ok+=output[f][-1]
    cabecera.append(ok)#resultado corrida
    with open(path_bat+"log_actualizacion_cabecera.csv",'a') as f1: 
        w1 = csv.writer(f1)
        for i in range(1):
            w1.writerow(cabecera)
    print('=============')
    print('Fin run_flow:',fin.isoformat())
    print('=============')
    print("Tiempo de ejecución total:",round(float(str(time_run_total.seconds)+'.'+str(time_run_total.microseconds)),2),'segundos')
    print('=============')
def sum_acum(df):
    df=df.sort_values('Periodo')
    df['Index']=df.index
    df['Solicitud recupero acumulado'] = df['Solicitud recupero'].cumsum(skipna = False) 
    return df

def get_output_schema():
    return pd.DataFrame({
        'Periodo': prep_datetime(),
        'Solicitud recupero': prep_decimal(),
        'Solicitud recupero acumulado': prep_decimal(),
        'Index': prep_int(),
    })
    

#df['cumsum'] = df['monto'].cumsum(skipna = False) 
#month_parts = (df["month"] < df["month"].shift()).cumsum()


def etl_transform(url):
    import pandas as pd
    from sklearn.impute import KNNImputer
    import ssl
    import tempfile

    temp_dir=tempfile.mkdtemp()
   
    df=pd.read_csv(url)
    df.drop('Unnamed: 0',inplace=True, axis=1)

    #-----------------------------------------------------

    df.drop_duplicates(inplace = True) # eliminamos las filas duplicadas
    indicadores = df['ID_INDICADOR'].unique()
    indicadores.sort()
    Ind_out = [16] # sacamos el indicador 16 tambien por la redundancia en indicadores
    for i in indicadores:
        x = (df[df['ID_INDICADOR'] == i].VALOR.isnull().sum()/len(df[df['ID_INDICADOR'] == i])) * 100
        if x > 20:
            Ind_out.append(i)
    for i in Ind_out:
        df = df[df['ID_INDICADOR'] != i] # Sacamos los indicadores dentro de Ind_Out

    #-----------------------------------------------------
    imputer = KNNImputer(n_neighbors=2, weights='distance') #Reemplazamos los Valores faltantes con KNNImputer
    after = imputer.fit_transform(df) #Creamos un Data Frame usando 'after' que tiene los datos imputados.
    columnas = df.columns.values
    df_limpio = pd.DataFrame(after, columns=columnas) 

    #-----------------------------------------------------
    df_limpio.ID_PAIS=df_limpio.ID_PAIS.astype(int)
    df_limpio.ID_INCOME=df_limpio.ID_INCOME.astype(int)
    df_limpio.ID_CONTINENTE=df_limpio.ID_CONTINENTE.astype(int)
    df_limpio.ANIO=df_limpio.ANIO.astype(int)
    df_limpio.ID_INDICADOR=df_limpio.ID_INDICADOR.astype(int)
    df_limpio.VALOR=round(df_limpio.VALOR,2)

    #-----------------------------------------------------


    df_limpio.to_csv(temp_dir +'/EV_limpio_buenaso.csv', index=False)
    sql = f"PUT file://{temp_dir}/EV_limpio_buenaso.csv @DATA_STAGE auto_compress=true"
    return sql
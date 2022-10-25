import os
from datetime import datetime
from urllib.parse import _ParseResultBase
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import snowflake.connector
import tempfile

conn = snowflake.connector.connect(
    user='grupods03',
    password='Henry2022#',
    account='nr28668.sa-east-1.aws',
    database='prueba',
    warehouse='dw_prueba',
    schema='public')

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

temp_dir=tempfile.TemporaryDirectory()

def extract(ti):
    import pandas as pd
    import wbgapi as wb
    from sklearn import preprocessing
    import pickle
    import ssl

    ssl._create_default_https_context = ssl._create_unverified_context
    # --------------------------------------------------# 
    #                                                   #
    #       Cargamos datos al csv de hechos crudo       #
    #                                                   #
    # --------------------------------------------------#
    # ----------------------------------------------#
    #   Ingestamos datos de 30años a un dataframe   #
    #       sin los indicadores de los KPI's.       #
    # ----------------------------------------------#

    Nation_code= ['USA','CAN','MEX','CRI','PAN','BRA','ARG','CHL','URY','BOL','PER','EGY','LBY','ZAF','NGA','MAR','AUS','CHN','IND','THA','JPN','KOR','ISR','SAU','MYS','IDN','RUS','TUR','ESP','BGR','FRA','ITA','DEU','GBR','NOR','SWE','GRC']
    Indicador_code_INCOMPLETE = ['AG.LND.PRCP.MM',
    'EN.ATM.CO2E.KT',
    'SP.POP.GROW',
    'SE.PRM.CUAT.FE.ZS',
    'SE.TER.ENRR',
    'SE.ADT.LITR.ZS',
    'CSV1 from WHO',
    'SH.IMM.IDPT',
    'SH.IMM.MEAS',
    'SH.HIV.INCD.TL.P3',
    'SH.DTH.0509',
    'SH.DTH.IMRT.IN',
    'CSV24 from WHO',
    'SH.PRV.SMOK.MA',
    'SH.PRV.SMOK.FE',
    'SH.DYN.MORT',
    'SP.DYN.AMRT.MA',
    'SP.DYN.AMRT.FE',
    'SH.ALC.PCAP.LI',
    'EG.CFT.ACCS.RU.ZS',
    'SE.XPD.CPRM.ZS',
    'SH.XPD.CHEX.GD.ZS',
    'SL.UEM.TOTL.NE.ZS',
    'SP.DYN.LE00.MA.IN',
    'SP.DYN.LE00.FE.IN',
    'SE.XPD.TOTL.GD.ZS',
    'EN.POP.SLUM.UR.ZS',
    'SP.URB.TOTL.IN.ZS',
    'GB.XPD.RSDV.GD.ZS',
    'SP.POP.SCIE.RD.P6',
    'SI.POV.MDIM',
    'BG.GSR.NFSV.GD.ZS'
    ]
    df2=wb.data.DataFrame(Indicador_code_INCOMPLETE, 
                    Nation_code, 
                    mrv= 30,
                    columns='series',
                    numericTimeKeys=True)
    df2=df2.reset_index()
    df2=df2.melt(id_vars=['economy','time'], var_name="ID_INDICADOR", value_name="VALOR")
    # ----------------------------------------------------------#
    #      Ingesta de los indicadores de KPI's a dataframe      #
    #       diferenciando desde el año que existen datos        #
    # ----------------------------------------------------------#
    #Población rural desde 1960
    PR62=wb.data.DataFrame('SP.RUR.TOTL.ZS', 
                    Nation_code, 
                    mrv= 62,
                    columns='series',
                    numericTimeKeys=True
                    )
    PR62=PR62.reset_index()

    #Esperanza de vida desde 1960
    EV61=wb.data.DataFrame('SP.DYN.LE00.IN', 
                    Nation_code, 
                    mrv= 61,
                    columns='series',
                    numericTimeKeys=True
                    )
    EV61=EV61.reset_index()
    leftmerge=pd.merge(PR62,EV61, how="left", on=['economy', 'time'] )

    #Ingreso per capita desde 1960
    IPC62=wb.data.DataFrame('NY.GDP.PCAP.KD', 
                    Nation_code, 
                    mrv= 62,
                    columns='series',
                    numericTimeKeys=True
                    )
    IPC62=IPC62.reset_index()
    leftmerge=pd.merge(leftmerge,IPC62, how="left", on=['economy', 'time'] )

    #Mortalidad materna desde 2000
    MM18=wb.data.DataFrame('SH.STA.MMRT', 
                    Nation_code, 
                    mrv= 18,
                    columns='series',
                    numericTimeKeys=True
                    )
    MM18=MM18.reset_index()
    leftmerge=pd.merge(leftmerge,MM18, how="left", on=['economy', 'time'] )

    #Inversion publica en salud desde 1990
    GS31=wb.data.DataFrame('GC.XPN.TOTL.GD.ZS', 
                    Nation_code, 
                    mrv= 31,
                    columns='series',
                    numericTimeKeys=True
                    )
    GS31=GS31.reset_index()
    leftmerge=pd.merge(leftmerge,GS31, how="left", on=['economy', 'time'] )

    #Mortalidad infantil desde 1969
    MC52=wb.data.DataFrame('SH.DYN.MORT', 
                    Nation_code, 
                    mrv= 52,
                    columns='series',
                    numericTimeKeys=True
                    )
    MC52=MC52.reset_index()
    leftmerge=pd.merge(leftmerge,MC52, how="left", on=['economy', 'time'] )

    #Hacemos el melt de los datos de KPI's para poder concatenar luego a la tabla de hechos
    leftmerge=leftmerge.melt(id_vars=['economy','time'], var_name="ID_INDICADOR", value_name="VALOR")

    #Juntamos los indicadores de KPI's a la tabla de hechos
    hechos=pd.concat([df2,leftmerge])

    #Renombramos las columnas economy y time
    hechos=hechos.rename(columns={'economy':'CODIGO_PAIS','time':'ANIO'})

    #Cargamos codificación guardada anteriormente para indicador
    file = open("le_indicador.obj",'rb')
    le_loaded = pickle.load(file)
    file.close()
    #transformamos ID_INDICADOR
    hechos['ID_INDICADOR'] = le_loaded.transform(hechos['ID_INDICADOR'])

    # Creamos la columna Id_continente a partir de los países

    lista_america=['ARG','CAN','MEX','USA','PAN','BRA','ARG','CHL','URY','BOL','PER','CRI']
    lista_africa=['EGY','LBY','ZAF','NGA','MAR']
    lista_asia=['CHN','IND','THA','JPN','KOR','ISR','SAU','MYS','IDN','RUS','TUR']
    lista_europa=['ESP','BGR','FRA','ITA','DEU','GBR','NOR','SWE','GRC']
    lista_oceania=['AUS']

    def categoria_pais(row):
        if row['CODIGO_PAIS'] in lista_america:
            return "America"
        if row['CODIGO_PAIS'] in lista_europa:
            return "Europa"
        if row['CODIGO_PAIS'] in lista_asia:
            return "Asia"
        if row['CODIGO_PAIS'] in lista_africa:
            return "Africa"
        if row['CODIGO_PAIS'] in lista_oceania:
            return "Oceania"

    hechos['CONTINENTE'] = hechos.apply(lambda row: categoria_pais(row), axis=1)

    #Abrimos encoder guardado
    file = open("le_conti.obj",'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna CONTINENTE como ID_CONTINENTE
    hechos['ID_CONTINENTE'] = le_loaded.transform(hechos['CONTINENTE'])

    # Creamos la columna ID_INGRESO a partir de una lista y del nombre del pais
    lista_alto=['CAN','USA','PAN','CHL','URY','AUS','JPN','KOR','ISR','SAU','ESP','FRA','ITA','DEU','GBR','NOR','SWE','GRC']
    lista_medioalto=['ARG','MEX','BRA','ARG','CRI','PER','BGR','TUR','LBY','ZAF','RUS']
    lista_mediobajo=['CHN','THA','MYS','BOL','EGY','NGA','MAR','IND','IDN']


    def ingreso_pais(row):
        if row['CODIGO_PAIS'] in lista_alto:
            return 'H'
        if row['CODIGO_PAIS'] in lista_medioalto:
            return 'M'
        if row['CODIGO_PAIS'] in lista_mediobajo:
            return 'LM'
        
    hechos['CODIGO_INCOME'] = hechos.apply(lambda row: ingreso_pais(row), axis=1)

    #Abrimos encoder guardado
    file = open("le_income.obj",'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna CONTINENTE como ID_CONTINENTE
    hechos['ID_INCOME'] = le_loaded.transform(hechos['CODIGO_INCOME'])

    # Codificamos la columna ID_PAIS

    #Abrimos encoder guardado
    file = open("le_nation.obj",'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna CODIGO_PAIS como ID_PAIS
    hechos['ID_PAIS'] = le_loaded.transform(hechos['CODIGO_PAIS'])

    #Borramos las columnas que no necesitamos
    hechos=hechos.drop(columns=['CODIGO_PAIS','CONTINENTE', 'CODIGO_INCOME'])

    # ---------------------------------------------------------#
    #      Cargamos lo indicadores "Hepatitis B" y "Polio"     #
    #       desde un CSV descargado de la página de WHO        #
    # ---------------------------------------------------------#


    #Ingestamos la tabla hepatitis
    Hepa=pd.read_csv('https://raw.githubusercontent.com/grupohenryds03/esperanza_vida/main/datasets/HepatitisB.csv')

    Hepatitis=pd.DataFrame({2000:[],2001:[],2002:[],2003:[]})
    for n in Nation_code:
        Hepa2=Hepa[['Location','Period', 'Value','SpatialDimValueCode']]
        Hepa2=Hepa2.loc[Hepa2['SpatialDimValueCode']==n]
        Hepa2=Hepa2.sort_values(by=['Period'])
        Hepa2=Hepa2.set_index('Period').T
        Hepa2=Hepa2.drop(['Location','SpatialDimValueCode'])
        Hepa2=Hepa2.rename(index={'Value':n})
        Hepatitis=pd.concat([Hepatitis,Hepa2])

    Hepatitis=Hepatitis.reset_index()
    Hepatitis=Hepatitis.melt(id_vars=['index'], var_name="ANIO", value_name="VALOR")
    Hepatitis['ID_INDICADOR']=2
    Hepatitis=Hepatitis.rename(columns={"index":"CODIGO_PAIS"})

    #Ingestamos la tabla Polio
    Polio=pd.read_csv('https://raw.githubusercontent.com/grupohenryds03/esperanza_vida/main/datasets/Polio.csv')

    Poliodf=pd.DataFrame({2000:[],2001:[],2002:[],2003:[]})
    for n in Nation_code:
        Polio2=Polio[['Location','Period', 'Value', 'SpatialDimValueCode']]
        Polio2=Polio2.loc[Polio2['SpatialDimValueCode']==n]
        Polio2=Polio2.sort_values(by=['Period'])
        Polio2=Polio2.set_index('Period').T
        Polio2=Polio2.drop(['Location', 'SpatialDimValueCode' ])
        Polio2=Polio2.rename(index={'Value':n})
        Poliodf=pd.concat([Poliodf,Polio2])
        
    Poliodf=Poliodf.reset_index()
    Poliodf=Poliodf.melt(id_vars=['index'], var_name="ANIO", value_name="VALOR")
    Poliodf['ID_INDICADOR']=3
    Poliodf=Poliodf.rename(columns={"index":"CODIGO_PAIS"})

    # Concatenamos las tablas del WHO
    WHO=pd.concat([Hepatitis,Poliodf])

    # Agregamos las columnas ID_CONTINENTE, ID_INCOME y ID_PAIS con el mismo criterio de la tabla Hechos

    #ID_CONTINENTE
    lista_america=['ARG','CAN','MEX','USA','PAN','BRA','ARG','CHL','URY','BOL','PER','CRI']
    lista_africa=['EGY','LBY','ZAF','NGA','MAR']
    lista_asia=['CHN','IND','THA','JPN','KOR','ISR','SAU','MYS','IDN','RUS','TUR']
    lista_europa=['ESP','BGR','FRA','ITA','DEU','GBR','NOR','SWE','GRC']
    lista_oceania=['AUS']

    def categoria_pais(row):
        if row['CODIGO_PAIS'] in lista_america:
            return "America"
        if row['CODIGO_PAIS'] in lista_europa:
            return "Europa"
        if row['CODIGO_PAIS'] in lista_asia:
            return "Asia"
        if row['CODIGO_PAIS'] in lista_africa:
            return "Africa"
        if row['CODIGO_PAIS'] in lista_oceania:
            return "Oceania"

    WHO['CONTINENTE'] = WHO.apply(lambda row: categoria_pais(row), axis=1)

    #Abrimos encoder guardado
    file = open("le_conti.obj",'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna CONTINENTE como ID_CONTINENTE
    WHO['ID_CONTINENTE'] = le_loaded.transform(WHO['CONTINENTE'])

    #ID_INCOME
    lista_alto=['CAN','USA','PAN','CHL','URY','AUS','JPN','KOR','ISR','SAU','ESP','FRA','ITA','DEU','GBR','NOR','SWE','GRC']
    lista_medioalto=['ARG','MEX','BRA','ARG','CRI','PER','BGR','TUR','LBY','ZAF','RUS']
    lista_mediobajo=['CHN','THA','MYS','BOL','EGY','NGA','MAR','IND','IDN']

    def ingreso_pais(row):
        if row['CODIGO_PAIS'] in lista_alto:
            return 'H'
        if row['CODIGO_PAIS'] in lista_medioalto:
            return 'M'
        if row['CODIGO_PAIS'] in lista_mediobajo:
            return 'LM'
        
    WHO['CODIGO_INCOME'] = WHO.apply(lambda row: ingreso_pais(row), axis=1)

    #Abrimos encoder guardado
    file = open("le_income.obj",'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna INCOME como ID_INCOME
    WHO['ID_INCOME'] = le_loaded.transform(WHO['CODIGO_INCOME'])

    #Abrimos encoder guardado
    file = open("le_nation.obj",'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna CODIGO_PAIS como ID_PAIS
    WHO['ID_PAIS'] = le_loaded.transform(WHO['CODIGO_PAIS'])

    #Borramos las columnas que no vamos a necesitar
    WHO=WHO.drop(columns=['CODIGO_PAIS','CONTINENTE','CODIGO_INCOME'])


    # Concatenamos la tabla del WHO con la tabla de hechos de World Bank

    hechos=pd.concat([hechos,WHO])
    ti.xcom_push(key='hechos',value=hechos)

#-----------------------------------------------------------------

def file_to_stage(ti):
    import tempfile
    sql="remove @DATA_STAGE pattern='.*.csv.gz'"
    execute_query(conn, sql)
    hechos=ti.xcom_pull(key='hechos', task_ids=tast_file_to_temp)
    with tempfile.TemporaryDirectory() as temp_dir:
        hechos.to_csv(temp_dir +'/EV_completo.csv', index=False)
        sql = f"PUT file://{temp_dir+'/EV_completo.csv'} @DATA_STAGE auto_compress=true"
        execute_query(conn, sql)

with DAG(
    dag_id='prueba_ETL',
    schedule_interval='@yearly',
    start_date=datetime(year=2022, month=10, day=22),
    catchup=False
) as dag:
   
    tast_file_to_temp=PythonOperator(
        task_id='file_to_temp',
        python_callable=extract)
    tast_file_to_stage=PythonOperator(
        task_id='file_to_stage',
        python_callable=file_to_stage)

tast_file_to_temp >> tast_file_to_stage
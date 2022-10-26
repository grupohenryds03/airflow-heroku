
def etl_extract() ->str:
    import pandas as pd
    import wbgapi as wb
    from sklearn import preprocessing
    import snowflake.connector
    import pickle
    import tempfile
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context


    temp_dir=tempfile.mkdtemp()

    # --------------------------------------------------# 
    #                                                   #
    #         We create the Snowflake connection        #
    #                                                   #
    # --------------------------------------------------#

    #We create the connection with Snowflake
    conn = snowflake.connector.connect(
        user='grupods03',
        password='Henry2022#',
        account='nr28668.sa-east-1.aws')

    # We start connection 
    def execute_query(connection, query):
        cursor = connection.cursor() #Creates a cursor object. Each statement will be executed in a new cursor object.
        cursor.execute(query)
        cursor.close()

    # --------------------------------------------------# 
    #                                                   #
    #           We use database and warehouse           #
    #                                                   #
    # --------------------------------------------------#

    # Since the database and warehouse are already created, we only need to start using them:
    query = "Use database LAKE" # Initialize database
    execute_query(conn, query)

    query = "Use warehouse DW_EV" # Initialize datawarehouse
    execute_query(conn, query)

    # --------------------------------------------------# 
    #                                                   #
    #       We load data to the dimension tables        #
    #                                                   #
    # --------------------------------------------------#

    #*********************************#
    # Loading data to CONTINENT table #
    #*********************************#
    Continentes=['America','Europa','Asia','Africa','Oceania']
    df=pd.DataFrame({"CONTINENTE":Continentes})

    #Se codifica ID_PAIS a partir de CODIGO_PAIS
    le_conti = LabelEncoder()
    le_conti.fit(df['CONTINENTE'])
    df['ID_CONTINENTE'] = le_conti.transform(df['CONTINENTE'])

    #Se guarda la codificación
    
    dir_conti=temp_dir+"le_conti.obj"
    filehandler = open(dir_conti,"wb")
    pickle.dump(le_conti,filehandler)
    filehandler.close()
    
    

    #****************************#
    # Loading data to PAIS table #
    #****************************#
    Nation = ['United States', 'Canada','Mexico','Costa Rica','Panama','Brazil','Argentina','Chile','Uruguay','Bolivia','Peru','Egypt, Arab Rep.','Libya','South Africa','Nigeria','Morocco','Australia','China','India','Thailand','Japan','Korea, Rep.','Israel','Saudi Arabia','Malaysia','Indonesia','Russian Federation','Turkiye','Spain','Bulgaria','France','Italy','Germany','United Kingdom','Norway','Sweden','Greece']
    Nation_code= ['USA','CAN','MEX','CRI','PAN','BRA','ARG','CHL','URY','BOL','PER','EGY','LBY','ZAF','NGA','MAR','AUS','CHN','IND','THA','JPN','KOR','ISR','SAU','MYS','IDN','RUS','TUR','ESP','BGR','FRA','ITA','DEU','GBR','NOR','SWE','GRC']
    df=pd.DataFrame({'CODIGO_PAIS':Nation_code,'NOMBRE': Nation}) #Creamos el dataframe para PAIS

    #Se codifica ID_PAIS a partir de CODIGO_PAIS
    le_nation = LabelEncoder()
    le_nation.fit(df['CODIGO_PAIS'])
    df['ID_PAIS'] = le_nation.transform(df['CODIGO_PAIS'])

    #Se guarda la codificación
    dir_nation=temp_dir+"le_nation.obj"
    filehandler = open(dir_nation,"wb")
    pickle.dump(le_nation,filehandler)
    filehandler.close()

    

    #******************************#
    # Loading data to INCOME table #
    #******************************#
    Income=['Alto ingreso','Ingreso medio alto','Ingreso medio bajo']
    CODIGO_INC=['H','M','LM']
    df = pd.DataFrame({'INCOME':Income, 'CODIGO_INCOME':CODIGO_INC}) #Se crea el dataframe para INCOME

    #Se codifica ID_INCOME a partir de Codigo_income
    le_income = LabelEncoder()
    le_income.fit(df['CODIGO_INCOME'])
    df['ID_INCOME'] = le_income.transform(df['CODIGO_INCOME'])

    #Se guarda la codificación
    
    dir_income=temp_dir+"le_income.obj"
    filehandler = open(dir_income,"wb")
    pickle.dump(le_income,filehandler)
    filehandler.close()


    #*********************************#
    # Loading data to INDICADOR table #
    #*********************************#
    Indicador = ['Average precipitation in depth (mm per year)',
    'emisiones de CO2 (kt)',
    'crecimiento de la poblacion (% anual)',
    'Educational attainment, at least completed primary, population 25+ years, female (%) (cumulative)',
    'School enrollment, tertiary (% gross)',
    'Tasa de alfabetizacion, total adultos (% de personas)',
    'Hepatitis B (HepB3) immunization coverage among 1-year-olds (%)',
    'Immunization, DPT (% of children ages 12-23 months)',
    'Immunization, measles (% of children ages 12-23 months)',
    'Incidence of HIV, all (per 1,000 uninfected population)',
    'Mortality rate, under-5 (per 1,000 live births)',
    'Number of deaths ages 5-9 years',
    'Number of infant deaths (per 1,000 live births)',
    'Polio (Pol3) immunization covergae among 1-year-olds (%)',
    'prevalencia del consumo de tabaco, hombres',
    'prevalencia del consumo de tabaco, mujeres',
    'tasa de mortalidad materna (cada 100.000 nacidos vivos)',
    'tasa de mortalidad menores de 5 años (por 1000nacidos vivos)',
    'tasa de mortalidad, adultos hombres (por cada 1000 adultos)',
    'tasa de mortalidad, adultos mujeres (por cada 1000 adultos)',
    'Total alcohol consumption per capita (liters of pure alcohol, projected estimates, 15+ years of age)',
    'Access to clean fuels and technologies for cooking, rural (% of rural population)',
    'Current education expenditure, primary (% of total expenditure in primary public institutions)',
    'Current health expenditure (% of GDP)',
    'desempleo total (% de la poblacion laboral)',
    'esperanza de vida al nacer, hombres (años)',
    'esperanza de vida al nacer, muejres (años)',
    'esperanza de vida al nacer, Total (años)',
    'gasto publico (% del pib)',
    'gasto publico en educación, total (% del pbi)',
    'GDP per capita (constant 2015 US$)',
    'poblacion que vive en barrios marginales (% de la poblacion urbana)',
    'Población Rural (% de la poblacion total)',
    'poblacion urbana (%poblacion total)',
    'Research and development expenditure (% of GDP)',
    'Researchers in R&D (per million people)',
    'tasa de recuento de la pobreza, multidimensional (%de la poblacion total)',
    'Trade in services (% of GDP)'
    ]
    Code_Indicador = ['AG.LND.PRCP.MM',
    'EN.ATM.CO2E.KT',
    'SP.POP.GROW',
    'SE.PRM.CUAT.FE.ZS',
    'SE.TER.ENRR',
    'SE.ADT.LITR.ZS',
    'CSV1 from WHO',
    'SH.IMM.IDPT',
    'SH.IMM.MEAS',
    'SH.HIV.INCD.TL.P3',
    'SH.DYN.MORT',
    'SH.DTH.0509',
    'SH.DTH.IMRT.IN',
    'CSV2 from WHO',
    'SH.PRV.SMOK.MA',
    'SH.PRV.SMOK.FE',
    'SH.STA.MMRT',
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
    'SP.DYN.LE00.IN',
    'GC.XPN.TOTL.GD.ZS',
    'SE.XPD.TOTL.GD.ZS',
    'NY.GDP.PCAP.KD',
    'EN.POP.SLUM.UR.ZS',
    'SP.RUR.TOTL.ZS',
    'SP.URB.TOTL.IN.ZS',
    'GB.XPD.RSDV.GD.ZS',
    'SP.POP.SCIE.RD.P6',
    'SI.POV.MDIM',
    'BG.GSR.NFSV.GD.ZS'
    ]
    #Se crea el df para la tabla INDICADOR
    df=pd.DataFrame({'CODIGO':Code_Indicador,'DESCRIPCION':Indicador}) 

    #Se codifica ID_INDICADOR a partir de CODIGO
    le_indicador = LabelEncoder()
    le_indicador.fit(df['CODIGO'])
    df['ID_INDICADOR'] = le_indicador.transform(df['CODIGO'])

    #Se guarda la codificación
    
    dir_indicador=temp_dir+"le_indicador.obj"
    filehandler = open(dir_indicador,"wb")
    pickle.dump(le_indicador,filehandler)
    filehandler.close()


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
                    numericTimeKeys=True
                    )
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
    
    file = open(dir_indicador,'rb')
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
    
    file = open(dir_conti,'rb')
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
    
    file = open(dir_income,'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna CONTINENTE como ID_CONTINENTE
    hechos['ID_INCOME'] = le_loaded.transform(hechos['CODIGO_INCOME'])

    # Codificamos la columna ID_PAIS

    #Abrimos encoder guardado

    file = open(dir_nation,'rb')
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
    import tempfile
    dir=temp_dir+"le_conti.obj"
    file = open(dir,'rb')
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
    import tempfile
    dir=temp_dir+"le_income.obj"
    file = open(dir,'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna INCOME como ID_INCOME
    WHO['ID_INCOME'] = le_loaded.transform(WHO['CODIGO_INCOME'])

    #Abrimos encoder guardado
    dir_nation=temp_dir+"le_nation.obj"
    file = open(dir_nation,'rb')
    le_loaded = pickle.load(file)
    file.close()

    #Codificamos columna CODIGO_PAIS como ID_PAIS
    WHO['ID_PAIS'] = le_loaded.transform(WHO['CODIGO_PAIS'])

    #Borramos las columnas que no vamos a necesitar
    WHO=WHO.drop(columns=['CODIGO_PAIS','CONTINENTE','CODIGO_INCOME'])


    # Concatenamos la tabla del WHO con la tabla de hechos de World Bank

    hechos=pd.concat([hechos,WHO])

    #Hacemos el csv
    hechos.to_csv(temp_dir +'/EV.csv', index=False)
    sql = f"PUT file://{temp_dir}/EV.csv @DATA_STAGE auto_compress=true"
    return sql
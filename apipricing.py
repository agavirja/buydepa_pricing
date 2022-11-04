import streamlit as st
import re
import json
import mysql.connector as sql
import pandas as pd
import numpy as np
import math as mt
import unicodedata
import requests
import pytz
import random
import string
from bs4 import BeautifulSoup
from price_parser import Price
from multiprocessing.dummy import Pool
from fuzzywuzzy import fuzz
from datetime import datetime 
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine 
from sqlalchemy.types import VARCHAR,INT,DATETIME
from sqlalchemy.dialects.mysql import DOUBLE

# anaconda promt: streamlit run D:\Dropbox\Empresa\Buydepa\COLOMBIA\DESARROLLO\streamlit\pricing\apipricing.py
# https://streamlit.io/
# En anaconda: pipreqs --encoding utf-8 "D:\Dropbox\Empresa\Buydepa\COLOMBIA\DESARROLLO\streamlit\pricing"
# https://docs.streamlit.io/library/api-reference/control-flow/


#-----------------------------------------------------------------------------#
# Forecat ANN
#-----------------------------------------------------------------------------#
def pricingforecast(inputvar):
    
    user     = st.secrets["buydepauser"]
    password = st.secrets["buydepapass"]
    host     = st.secrets["buydepahost"]
    database = st.secrets["buydepadatabase"]
    
    mpio_ccdgo   = inputvar['mpio_ccdgo']
    tipoinmueble = inputvar['tipoinmueble']
    tiponegocio  = inputvar['tiponegocio']
    
    delta         = 0
    db_connection = sql.connect(user=user, password=password, host=host, database=database)
    salida        = pd.read_sql(f'SELECT salida FROM {database}.model_outcome WHERE mpio_ccdgo="{mpio_ccdgo}" AND tipoinmueble="{tipoinmueble}" AND tiponegocio="{tiponegocio}"' , con=db_connection)
    db_connection.close()
    salida        = json.loads(salida['salida'].iloc[0])
    options       = salida['options']
    varlist       = salida['varlist']
    coef          = salida['coef']
    minmax        = salida['minmax']
    variables     = pd.DataFrame(0, index=np.arange(1), columns=varlist)
    
    for i in inputvar:
        value = inputvar[i]
        idd   = [z==elimina_tildes(i) for z in varlist]
        if sum(idd)==0:
            try:
                idd = [re.findall(elimina_tildes(i)+'#'+str(int(value)), z)!=[] for z in varlist]
            except:
                try:
                    idd = [re.findall(elimina_tildes(i)+'#'+elimina_tildes(value), z)!=[] for z in varlist]
                except:
                    pass
            value = 1                   
        if sum(idd)>0:
            row                = [j for j, x in enumerate(idd) if x]
            varname            = varlist[row[0]]
            variables[varname] = value
            
    # Transform MinMax
    a = variables.iloc[0]
    a = a[a!=0]
    for i in a.index:
        mini         = minmax[i]['min']
        maxi         = minmax[i]['max']
        variables[i] = (variables[i]-mini)/(maxi-mini)
        
    x     = variables.values
    value = ForecastFun(coef,x.T,options)
    if options['ytrans']=='log':
        value = np.exp(value)
        
    value         = value*(1-delta)
    valorestimado = np.round(value, int(-(mt.floor(mt.log10(value)) - 2))) 
    valuem2       = value/inputvar['areaconstruida']
    valortotal = np.round(value, int(-(mt.floor(mt.log10(value)) - 2))) 
    valuem2    = valortotal/inputvar['areaconstruida']
    return {'valorestimado': valorestimado[0][0], 'valorestimado_mt2':valuem2[0][0]}

def ForecastFun(coef,x,options):

    hiddenlayers = options['hiddenlayers']
    lambdavalue  = options['lambdavalue']
    biasunit     = options['biasunit']
    tipofun      = options['tipofun']
    numvar       = x.shape[0]
    nodos        = [numvar]
    for i in hiddenlayers:
        nodos.append(i)
    nodos.append(1)
        
    k          = len(nodos)
    suma       = 0
    theta      = [[] for i in range(k-1)]
    lambdac    = [[] for i in range(k-1)]
    lambdavect = np.nan
    for i in range(k-1):
        theta[i]   = np.reshape(coef[0:(nodos[i]+suma)*nodos[i+1]], (nodos[i]+suma, nodos[i+1]), order='F').T
        lambdac[i] =lambdavalue*np.ones(theta[i].shape)
        coef       = coef[(nodos[i]+suma)*nodos[i+1]:]
        if biasunit=='on':
            suma = 1
            lambdac[i][:,0] = 0
        [fil,col]  = lambdac[i].shape
        lambdavect = np.c_[lambdavect,np.reshape(lambdac[i],(fil*col,1)).T ]
        
    lambdac = lambdavect[:,1:].T
        
    # Forward Propagation
    a    = [[] for i in range(k)]
    z    = [[] for i in range(k)]
    g    = [[] for i in range(k)]
    a[0] = x
    numN = x.shape[1]
    for i in range(k-1):
        z[i+1]      = np.dot(theta[i],a[i])
        [ai,g[i+1]] = ANNFun(z[i+1],tipofun)
        if ((i+1)!=(k-1)) & (biasunit=='on'):
            a[i+1] = np.vstack((np.ones((1,numN)),ai))
        else:
            a[i+1] = ai
    return a[-1]

def ANNFun(z, tipofun):
    z = np.asarray(z)
    if tipofun=='lineal':
        f = z
        g = 1
    if tipofun=='logistica':
        f = 1/(1+mt.exp(-z))
        g = f*(1-f)
    if tipofun=='exp':
        f = np.exp(z)
        g = np.exp(z)
    if tipofun=='cuadratica':
        f = z + 0.5*(z*z)
        g = 1 + z
    if tipofun=='cubica':
        f = z + 0.5*(z*z)+(1/3.0)*(z*z*z)
        g = 1 + z + z*z
    return [f,g]

#-----------------------------------------------------------------------------#
# elimina_tildes
#-----------------------------------------------------------------------------#
def elimina_tildes(s):
    s = s.replace(' ','').lower().strip()
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

#-----------------------------------------------------------------------------#
# precio_compra
#-----------------------------------------------------------------------------#
def precio_compra(inputvar):
    #inputvar = {'precio_venta':400000000,'areaconstruida':85,'admon':320000,'ganancia':0.06,'comision_compra':0.003,'comision_venta':0.003,'nmonths':6,'provisionmt2':100000,'pinturamt2':13000}
    
    ganancia        = 0.06 # (6%)
    comision_compra = 0.003 # (0.3%)
    comision_venta  = 0.003 # (0.3%)
    nmonths         = 6
    provisionmt2    = 100000  # Para reparaciones / colchon financiero
    pinturamt2      = 13000
    IVA             = 0.19
    p1              = None
    admon           = None
    areaconstruida  = None
    
    if 'precio_venta' in inputvar:
        p1 = inputvar['precio_venta']
    if 'ganancia' in inputvar and inputvar['ganancia']>0 and inputvar['ganancia']<100: 
        ganancia = inputvar['ganancia']
    if 'areaconstruida' in inputvar:
        areaconstruida = inputvar['areaconstruida']
    if 'nmonths' in inputvar: 
        nmonths = inputvar['nmonths']
    if 'admon' in inputvar and inputvar['admon']>0: 
        admon = inputvar['admon']*1.1 # Es usual que reporten un menor valor de la administracion
    else: 
        admon = 5500*areaconstruida
    if 'pinturamt2' in inputvar: 
        pinturamt2 = inputvar['pinturamt2']
    if 'provisionmt2' in inputvar: 
        provisionmt2 = inputvar['provisionmt2']
    
    PRECIO_GANANCIA  = p1/(1+ganancia)
    GN_VENTA         = 164000+0.0033*p1  # (regresion)
    COMISION_VENTA   = comision_venta*p1
    PINTURA          = pinturamt2*(1+IVA)*areaconstruida
    ADMON            = admon*nmonths
    PROVISION        = provisionmt2*areaconstruida
    X                = PRECIO_GANANCIA-GN_VENTA-COMISION_VENTA-PINTURA-ADMON-PROVISION
    preciocompra     = (X-57000)/(1+(0.0262+comision_compra))
    preciocompra     = np.round(preciocompra, int(-(mt.floor(mt.log10(preciocompra))-2)))
    gn_compra        = 57000+0.0262*preciocompra
    gn_compra        = np.round(gn_compra, int(-(mt.floor(mt.log10(gn_compra))-2)))
    gn_venta         = np.round(GN_VENTA, int(-(mt.floor(mt.log10(GN_VENTA))-2)))
    COMISION_COMPRA  = (preciocompra*comision_compra)
    retorno_bruto_esperado = p1/preciocompra-1
    retorno_neto_esperado  = (p1-COMISION_COMPRA-COMISION_VENTA-PINTURA-ADMON-PROVISION)/preciocompra-1
    return {'precio_venta':p1,'preciocompra':preciocompra,'retorno_bruto_esperado':retorno_bruto_esperado,'retorno_neto_esperado':retorno_neto_esperado,'gn_compra':gn_compra,'gn_venta':gn_venta,'comisiones':COMISION_VENTA+COMISION_COMPRA,'otros_gastos':PINTURA+ADMON+PROVISION}

#-----------------------------------------------------------------------------#
# dir2coddir
#-----------------------------------------------------------------------------#
def coddir(x):
    result = x
    try: result = prefijo(x) + getnewdir(x)
    except:pass
    return result

def getdirformat(x):
    # x    = 'carrera 19a # 103A - 62'
    result = ''
    x      = x.lower()
    x      = re.sub(r'[^0-9a-zA-Z]',' ', x).split(' ')
    for u in range(len(x)):
        i=x[u]
        try: i = i.replace(' ','').strip().lower()
        except: pass
        try:
            float(re.sub(r'[^0-9]',' ', i))
            result = result +'+'+i
        except:
            if i!='': result = result + i
        try:
            if len(re.sub(r'[^+]','',result))>=3:
                try:
                    if 'sur'  in x[u+1]:  result= result + 'sur'
                    if 'este' in x[u+1]:  result= result + 'este'
                except: pass
                break
        except: pass
    return result

def getnewdir(x):
    result = None
    try:
        x      = getdirformat(x).split('+')[1:]
        result = ''
        for i in x:
            result = result + '+' + re.sub(r'[^0-9]','', i)+''.join([''.join(sorted(re.sub(r'[^a-zA-Z]','', i)))])
    except: pass
    if result=='': result = None
    return result

def prefijo(x):
    result = None
    m      = re.search("\d", x).start()
    x      = x[:m].strip()
    prefijos = {'D':{'d','diagonal','dg', 'diag', 'dg.', 'diag.', 'dig'},
                'T':{'t','transv', 'tranv', 'tv', 'tr', 'tv.', 'tr.', 'tranv.', 'transv.', 'transversal', 'tranversal'},
                'C':{'c','avenida calle','avenida cll','avenida cl','calle', 'cl', 'cll', 'cl.', 'cll.', 'ac', 'a calle', 'av calle', 'av cll', 'a cll'},
                'AK':{'avenida carrera','avenida cr','avenida kr','ak', 'av cr', 'av carrera', 'av cra'},
                'K':{'k','carrera', 'cr', 'cra', 'cr.', 'cra.', 'kr', 'kr.', 'kra.', 'kra'},
                'A':{'av','avenida'}}
    for key, values in prefijos.items():
        if x.lower() in values:
            result = key
            break
    return result

#-----------------------------------------------------------------------------#
# Seleccionar datos del mercado y del conjunto de acuerdo a las caractetisticas
# del inmueble
#-----------------------------------------------------------------------------#  
def data_reference(inputvar):
    
    # Caracteristicas del inmueble
    metros                = 300
    area                  = inputvar['areaconstruida']
    areamin               = area*0.95
    areamax               = area*1.05
    habitaciones          = inputvar['habitaciones']
    banos                 = inputvar['banos']
    garajes               = inputvar['garajes']
    estrato               = inputvar['estrato']
    tipoinmueble          = inputvar['tipoinmueble']
    todaynum              = datetime.now(tz=pytz.timezone('America/Bogota'))
    fechainicial_conjunto = todaynum+relativedelta(months=-12)
    fechainicial_conjunto = fechainicial_conjunto.strftime("%Y-%m-%d")
    fcoddir               = coddir(inputvar['direccion'])
    fechainicial_market   = todaynum+relativedelta(months=-6)
    fechainicial_market   = fechainicial_market.strftime("%Y-%m-%d")
    inputvar['coddir']    = fcoddir
    
    # Bases de datos
    user          = st.secrets["prinanmasteruser"]
    password      = st.secrets["prinanpass"]
    host          = st.secrets["prinanhost"]
    database      = st.secrets["prinandatabase"]
    db_connection = sql.connect(user=user, password=password, host=host, database=database)
    datacatastro  = pd.read_sql(f"SELECT count(distinct(prechip)) as conjunto_unidades, min(prevetustz) as antiguedad_min, max(prevetustz) as antiguedad_max, latitud, longitud FROM {database}.data_bogota_catastroV3 WHERE coddir='{fcoddir}' AND predirecc LIKE '%AP%'" , con=db_connection)
    if datacatastro.empty is False:
        inputvar.update(datacatastro.iloc[0].to_dict())
    if 'latitud' not in inputvar or inputvar['latitud'] is None or 'longitud' not in inputvar or inputvar['longitud'] is None:
        inputvar = georreferenciacion(inputvar)
    latitud        = inputvar['latitud']
    longitud       = inputvar['longitud']
    dane           = pd.read_sql(f"SELECT dpto_ccdgo,mpio_ccdgo,setu_ccnct,secu_ccnct FROM {database}.SAE_dane WHERE  st_contains(geometry, POINT({longitud}, {latitud}))", con=db_connection)
    databarrio     = pd.read_sql(f"SELECT scacodigo,scanombre FROM {database}.data_bogota_barriocatastral WHERE st_contains(geometry, POINT({longitud}, {latitud}))", con=db_connection)
    consultabarrio = ''
    if dane.empty is False:
        inputvar.update(dane.iloc[0].to_dict())
        mpio_ccdgo = inputvar['mpio_ccdgo']
        setu_ccnct = inputvar['setu_ccnct']
        consultabarrio = f" mpio_ccdgo='{mpio_ccdgo}' AND setu_ccnct='{setu_ccnct}' AND "
    if databarrio.empty is False:
        inputvar.update(databarrio.iloc[0].to_dict())
        
    datastock = [pd.read_sql(f"SELECT areaconstruida,descripcion,direccion,estrato,fecha_inicial,fuente,garajes,habitaciones,id_tabla,latitud,longitud,tiempodeconstruido,tipoinmueble,tiponegocio,url,valorarriendo,valorventa FROM {database}.4M_stockdata WHERE tipoinmueble='{tipoinmueble}' AND coddir='{fcoddir}' AND fecha_inicial>='{fechainicial_conjunto}' AND  (areaconstruida>={areamin} AND areaconstruida<={areamax}) AND  url like '%bogota%'" , con=db_connection),
                 pd.read_sql(f"SELECT areaconstruida,descripcion,direccion,estrato,fecha_inicial,fuente,garajes,habitaciones,id_tabla,latitud,longitud,tiempodeconstruido,tipoinmueble,tiponegocio,url,valorarriendo,valorventa FROM {database}.4M_stockdata WHERE  {consultabarrio} tipoinmueble='{tipoinmueble}' AND (areaconstruida>={areamin} AND areaconstruida<={areamax}) AND estrato={estrato} AND habitaciones={habitaciones} AND banos={banos} AND garajes={garajes} AND fecha_inicial>='{fechainicial_market}' AND ST_Distance_Sphere(geometry, POINT({longitud},{latitud}))<={metros}" , con=db_connection)]    
    dataconjunto             = datastock[0]
    dataconjunto['tipodata'] = 'Conjunto'    
    dataconjunto['latitud']  = latitud
    dataconjunto['longitud'] = longitud
    datamarket               = datastock[1]
    datamarket['tipodata']   = 'Market'
    data                     = dataconjunto.append(datamarket)
    if data.empty is False:
        data['id']               = range(len(data))
        dataupdate               = urlupdate(data)
        data                     = data.merge(dataupdate,on='id',how='left',validate='1:1')
        data.drop(columns=[ 'areaconstruida_new','imagenes_new'],inplace=True)
        data['valormt2_venta']   = data['valorventa']/data['areaconstruida']
        data['valormt2_renta']   = data['valorarriendo']/data['areaconstruida']
    
        dataconjunto = pd.DataFrame()
        if sum(data['tipodata']=='Conjunto')>0:
            dataconjunto = duplicated_description(data[data['tipodata']=='Conjunto'])
    
        datamarket = pd.DataFrame()
        if sum(data['tipodata']=='Market')>0:
            datamarket = duplicated_description(data[data['tipodata']=='Market'])
        data = dataconjunto.append(datamarket)    
    return data

def georreferenciacion(inputvar):
    direccion = formato_direccion(inputvar['direccion'])
    direccion = f'{direccion},bogota,colombia'
    punto     = requests.get(f'https://maps.googleapis.com/maps/api/geocode/json?address={direccion}&key=AIzaSyAgT26vVoJnpjwmkoNaDl1Aj3NezOlSpKs')
    response  = json.loads(punto.text)['results']
    inputvar.update({'latitud':response[0]["geometry"]["location"]['lat'],'longitud':response[0]["geometry"]["location"]['lng'],'direccion_formato':response[0]["formatted_address"]})
    return inputvar

#-----------------------------------------------------------------------------#
# formato_direccion
#-----------------------------------------------------------------------------#
def formato_direccion(x):
    resultado = x
    try:
        address = ''
        x       = x.upper()
        x       = re.sub('[^A-Za-z0-9]',' ', x).strip() 
        x       = re.sub(re.compile(r'\s+'),' ', x).strip()
        numbers = re.sub(re.compile(r'\s+'),' ', re.sub('[^0-9]',' ', x)).strip().split(' ')
        vector  = ['ESTE','OESTE','SUR']
        for i in range(0,min(3,len(numbers))):
            try:
                initial = x.find(numbers[i],0)
                z       = x.find(numbers[i+1],initial+len(numbers[i]))
                result  = x[0:z].strip()
            except:
                result = x
            if i==2:
                if any([w in result.upper() for w in vector]):
                    result = numbers[i]+' '+[w for w in vector if w in result.upper()][0]
                else:
                    result = numbers[i]            
            address = address+' '+result
            z = x.find(result)
            x = x[(z+len(result)):].strip()
        resultado = address.strip()
        try: 
            #resultado = re.sub("[A-Za-z]+", lambda ele: " " + ele[0] + " ", resultado)
            resultado = re.sub(re.compile(r'\s+'),' ', resultado).strip()
            resultado = indicador_via(resultado)
        except: pass
    except: pass
    try: resultado = re.sub(re.compile(r'\s+'),'+', resultado).strip()
    except: pass
    return resultado

def indicador_via(x):
    m       = re.search("\d", x).start()
    tipovia = x[:m].strip()
    prefijos = {'D':{'d','diagonal','dg', 'diag', 'dg.', 'diag.', 'dig'},
                'T':{'t','transv', 'tranv', 'tv', 'tr', 'tv.', 'tr.', 'tranv.', 'transv.', 'transversal', 'tranversal'},
                'C':{'c','avenida calle','avenida cll','avenida cl','calle', 'cl', 'cll', 'cl.', 'cll.', 'ac', 'a calle', 'av calle', 'av cll', 'a cll'},
                'AK':{'avenida carrera','avenida cr','avenida kr','ak', 'av cr', 'av carrera', 'av cra'},
                'K':{'k','carrera', 'cr', 'cra', 'cr.', 'cra.', 'kr', 'kr.', 'kra.', 'kra'},
                'A':{'av','avenida'}}
    for key, values in prefijos.items():
        if tipovia.lower() in values:
            x = x.replace(tipovia,key)
            break
    return x

#-----------------------------------------------------------------------------#
# url update
#-----------------------------------------------------------------------------#
def urlupdate(data):
    pool           = Pool(10)
    futures        = []
    datafinal = pd.DataFrame()
    for i in range(len(data)):  
        inputvar = data.iloc[i].to_dict()
        futures.append(pool.apply_async(fuenteupdate,args = (inputvar, )))
    #for future in tqdm(futures):
    for future in futures:
        try: datafinal = datafinal.append([future.get()])
        except: pass
    datafinal.index = range(len(datafinal))
    return datafinal
    
def fuenteupdate(inputvar): 
    result = {'activo':0,'id':inputvar['id'],'valorventa_new':None,'valorarriendo_new':None,'areaconstruida_new':None,'imagenes_new':''}
    if 'fuente' in inputvar:
        if   inputvar['fuente']=='M2': result = M2(inputvar)  
        elif inputvar['fuente']=='FR': result = FR(inputvar)
        elif inputvar['fuente']=='CC': result = CC(inputvar)
        elif inputvar['fuente']=='PP': result = PP(inputvar)
    return result 

# Metrocuadrado
def M2(inputvar):
    
    headers_getinmueble = {'authority':'www.metrocuadrado.com',
                         'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                         'accept-encoding':'gzip, deflate, br',
                         'cookie':'visid_incap_434661=hjhD3pTOTImpvPDHBbVMIU5M6V4AAAAAQUIPAAAAAACkDZw6A2qwPXxaO7VyvF5F; incap_ses_988_434661=cMapWeC51msf8YPo0RS2DU5M6V4AAAAAIhV77ejUakC/84UMYyzM0g==',
                         'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                         'x-api-key':'P1MfFHfQMOtL16Zpg36NcntJYCLFm8FqFfudnavl',
                         }
    isactive     = False
    today        = datetime.now(tz=pytz.timezone('America/Bogota')).strftime("%Y-%m-%d %H:%M:%S")
    result       = {'activo':0,'id':inputvar['id'],'valorventa_new':None,'valorarriendo_new':None,'areaconstruida_new':None,'imagenes_new':''}
    url          = inputvar['url']
    r            = requests.get(url,headers=headers_getinmueble,timeout=30,verify=False)
    soup         = BeautifulSoup(r.text,'html.parser')
    try:
        try:    z = json.loads(soup.find_all('script',type='application/json')[0].getText)['props']['initialProps']['pageProps']['realEstate']
        except: z = json.loads(soup.find_all('script',type='application/json')[0].next_element)['props']['initialProps']['pageProps']['realEstate']
        try:    
            result.update({'valorventa_new':float(z['salePrice'])})
            isactive = True
        except: pass
        try:
            result.update({'areaconstruida_new':float(z['areac'])})
            isactive = True
        except: pass
        try:    
            result.update({'valorarriendo_new':float(z['rentPrice'])})
            isactive = True
        except: pass
    except: pass
    try:
        imagenes = []
        for i in z['images']: 
            imagenes.append(i['image'])
        result['imagenes_new'] = json.dumps(imagenes)
    except: pass
    if isactive: result['activo'] = 1    
    return result
        
# Finca Raíz
def FR(inputvar):
    isactive     = False
    today        = datetime.now(tz=pytz.timezone('America/Bogota')).strftime("%Y-%m-%d %H:%M:%S")
    result       = {'activo':0,'id':inputvar['id'],'valorventa_new':None,'valorarriendo_new':None,'areaconstruida_new':None,'imagenes_new':''}
    url          = inputvar['url']
    r            = requests.get(url,timeout=30)
    soup         = BeautifulSoup(r.text,'html.parser')
    try: 
        datajson = json.loads(soup.find(['script'], {'type': 'application/json'}).next_element)
        for i in ['venta','arriendo']:
            if i in datajson['props']['pageProps']['offer']['name'].lower(): 
                try:
                    result[f'valor{i}_new'] = Price.fromstring(str(datajson['props']['pageProps']['price'])).amount_float
                    isactive = True
                except: pass
        result['areaconstruida_new'] = float(datajson['props']['pageProps']['area'])
        isactive = True
        result['imagenes_new'] = json.dumps(datajson['props']['pageProps']['media']['photos'])
    except: pass
    if isactive: result['activo'] = 1
    return result
                
# Cien cuadras
def CC(inputvar):
    isactive     = False
    today        = datetime.now(tz=pytz.timezone('America/Bogota')).strftime("%Y-%m-%d %H:%M:%S")
    result       = {'activo':0,'id':inputvar['id'],'valorventa_new':None,'valorarriendo_new':None,'areaconstruida_new':None,'imagenes_new':''}
    url          = inputvar['url']
    r            = requests.get(url,timeout=30)
    soup         = BeautifulSoup(r.text,'html.parser')
    try:
        z        = soup.find('script',type='application/json').next_element
        z        = z.replace('&q;','"')
        z        = json.loads(z)
        try: 
            result['valorventa_new'] = z['dataKey']['sellingprice']
            isactive = True
        except: pass
        try: 
            result['valorarriendo_new'] = z['dataKey']['leasefee']
            isactive = True
        except: pass
        try: 
            result['areaconstruida_new'] = z['dataKey']['propertyFeatures']['builtArea']
            isactive = True
        except: pass   
        try: 
            imagenes = []
            for iiter in z['dataKey']['propertyFeatures']['photosPropertyData']:
                imagenes.append(iiter['url'])
            if imagenes!=[]:
                result['imagenes_new'] = imagenes
        except: pass
    except: pass
    if isactive: result['activo'] = 1
    return result

# Properati
def PP(inputvar):
    isactive     = False
    today        = datetime.now(tz=pytz.timezone('America/Bogota')).strftime("%Y-%m-%d %H:%M:%S")
    result       = {'activo':0,'id':inputvar['id'],'valorventa_new':None,'valorarriendo_new':None,'areaconstruida_new':None,'imagenes_new':''}
    url          = inputvar['url']
    r            = requests.get(url,timeout=30)
    soup         = BeautifulSoup(r.text,'html.parser')
    propertyinfo = {}            
    try:
        propertyinfo = json.loads(soup.find_all('script',type="application/json")[0].getText())['props']['pageProps']['property']
        propertyinfo.update({'valorstr':soup.find_all("span", {"class" : re.compile('.*StyledPrice.*')})[0].getText().strip()})
    except: 
        try:
            propertyinfo = json.loads(soup.find_all('script',type="application/json")[0].next_element)['props']['pageProps']['property']
            propertyinfo.update({'valorstr':soup.find_all("span", {"class" : re.compile('.*StyledPrice.*')})[0].getText().strip()})
        except: pass
    
    if 'price' in  propertyinfo:
        if 'amount' in propertyinfo['price']:
            for i in ['arriendo','venta']:
                if i in url.lower(): 
                    try:     
                        result[f'valor{i}_new'] = propertyinfo['price']['amount']
                        isactive = True
                    except: 
                        try: 
                            result[f'valor{i}_new'] = Price.fromstring(str(propertyinfo['valorstr'])).amount_float
                            isactive = True
                        except: pass
    try: 
        imgs = []
        if 'images' in propertyinfo:
            try:
                for j in propertyinfo['images']:
                    try: imgs.append('http'+j['sizes'][list(j['sizes'])[0]]['webp'].split('format(webp)')[1].strip().split('http')[1])
                    except: pass
            except: pass
        if imgs!=[]:    result.update({'imagenes_new': json.dumps(imgs)})
    except: pass
    if isactive: result['activo'] = 1
    return result

#-----------------------------------------------------------------------------#
# Eliminar registros con descripcion similar o igual
#-----------------------------------------------------------------------------#  
def duplicated_description(b):
    b                 = b.drop_duplicates(subset='descripcion',keep='first')
    b['descnew']      = b['descripcion'].apply(lambda x: re.sub(r'\s+',' ',x.lower()))
    b['index']        = b.index
    b.index           = range(len(b))
    b['isduplicated'] = 0
    b['coddup']       = None
    for i in range(len(b)):
        coddup  = b['index'].iloc[i]
        compare = b['descnew'].iloc[i]
        idd     = b['descnew'].apply(lambda x: fuzz.partial_ratio(compare,x))>=85
        idd.loc[i] = False
        idj        = b['index'].isin(b['coddup'].unique())
        idd  = (idd) & (b['isduplicated']==0) & (~idj)
        if sum(idd)>0:
            b.loc[idd,'isduplicated'] = 1
            b.loc[idd,'coddup']       = coddup
    b.index = b['index'] 
    b       = b[b['isduplicated']==0]
    b.drop(columns=['index','isduplicated','coddup','descnew'],inplace=True)
    return b


#-----------------------------------------------------------------------------#
# SKU, ID GENERADOR
#-----------------------------------------------------------------------------#  
def getsku(inputvar):
    if 'sku' not in inputvar or ('sku' in inputvar and inputvar['sku'] is None or inputvar['sku']==""):
        inputvar['sku'] = id_generator()
    return inputvar

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def dtype_inmueble(dataresult):
    varchartype   = [ 'sku','direccion','nombre_edificio','url','direccion_formato','coddir','dpto_ccdgo','mpio_ccdgo','setu_ccnct','secu_ccnct','scacodigo','scanombre','tiempodeconstruido','tipoinmueble','tiponegocio']
    inttype       = [ 'conjunto_ofertas','market_ofertas','conjunto_ofertas_activas','habitaciones','banos','garajes','estrato','num_piso','anos_antiguedad','num_ascensores','numerodeniveles','conjunto_unidades','antiguedad_min','antiguedad_max']
    doubletype    = ['precio_compra','conjunto_valormt2','conjunto_valorminimo','conjunto_valormaximo','market_precio_promedio','market_valorminimo','market_valormaximo','market_valormt2','valorestimado','valorestimado_mt2','valormt2_precio_ofrecido','valormt2_precio_compra','diferencia','areaconstruida','adminsitracion','precioventa','preciorenta','latitud','longitud']
    datetimetype  = ['fecha_registro']  
    dtype         = {}
    for i in varchartype:
        if i in dataresult:
            nlength = dataresult[i].apply(lambda x: len(str(x))).max()
            if nlength>40: nlength += 10
            dtype.update({i: VARCHAR(nlength)})
    for i in inttype:
        if i in dataresult:
            dataresult[i] = pd.to_numeric(dataresult[i],errors='coerce')
            dtype.update({i: INT})
    for i in doubletype:
        if i in dataresult:
            dataresult[i] = pd.to_numeric(dataresult[i],errors='coerce')
            dtype.update({i: DOUBLE})
    for i in datetimetype:
        if i in dataresult:
            dtype.update({i: DATETIME})    
    return dtype

def dtype_comparables(data):
    varchartype   = ['sku','direccion','fuente','descripcion','tiempodeconstruido','tipoinmueble','tiponegocio','url','tipodata']
    inttype       = ['estrato','garajes','habitaciones','activo']
    doubletype    = ['areaconstruida','latitud','longitud','valorarriendo','valorventa','valorventa_new','valorarriendo_new','valormt2_venta','valormt2_renta']
    datetimetype  = ['fecha_inicial']  
    dtype         = {}
    for i in varchartype:
        if i in data:
            nlength = data[i].apply(lambda x: len(str(x))).max()
            if nlength>40: nlength += 10
            dtype.update({i: VARCHAR(nlength)})
    for i in inttype:
        if i in data:
            data[i] = pd.to_numeric(data[i],errors='coerce')
            dtype.update({i: INT})
    for i in doubletype:
        if i in data:
            data[i] = pd.to_numeric(data[i],errors='coerce')
            dtype.update({i: DOUBLE})
    for i in datetimetype:
        if i in data:
            dtype.update({i: DATETIME})    
    
    return dtype

#-----------------------------------------------------------------------------#
# PRICING
#-----------------------------------------------------------------------------#  
def getpricing(inputvar):
    """
    inputvar = {
                "id_inmueble": 6,  # Entero o Null (Opcional)
                "direccion":"KR 6 86 60, Bogota",
                'areaconstruida':240,
                'habitaciones':3,
                'banos':4,
                'garajes':3,
                'estrato':6,
                'num_piso':4,
                'anos_antiguedad':17,
                'num_ascensores':1,
                'numerodeniveles':1,
                'adminsitracion':245000,
                'precioventa':240000000,
                'preciorenta':None,
                'nombre_edificio':'SUBA IMPERIAL',
                'url':'www.metrocuadrado.com',
                'metros':300
                }
    """
    if 'id_inmueble' not in inputvar:
        inputvar['id_inmueble'] = None
    for i in list(inputvar):
        if inputvar[i]=="":
            inputvar[i]=None
    for i in ["habitaciones","banos","garajes","estrato","num_piso","num_ascensores","numerodeniveles","anos_antiguedad"]:
        if i in inputvar:
            try: inputvar[i] = int(inputvar[i])
            except: pass
    for i in ["areaconstruida","adminsitracion","precioventa","preciorenta"]:
        if i in inputvar:
            try: inputvar[i] = float(inputvar[i])
            except: pass
    if 'nombre_edificio' in inputvar:
        try:
            inputvar['nombre_edificio'] = re.sub('\s+',' ',inputvar['nombre_edificio'].upper()).strip()
        except: pass
    if 'tipoinmueble' not in inputvar:
        inputvar['tipoinmueble'] = "Apartamento"
   
    
    # Parametros
    area         = inputvar['areaconstruida']
    ganancia     = 0.07
    tipoinmueble = inputvar['tipoinmueble']
    todaynum     = datetime.now(tz=pytz.timezone('America/Bogota'))
    metros       = 300 # Comparacion de mercado
    if 'metros' in inputvar: metros = inputvar['metros']

    # Estructura de la salida
    inputvar.update({'direccion_formato': None,'conjunto_unidades': None,'antiguedad_min': None,'antiguedad_max': None})
    inputvarsell = {'precio_compra': None,'conjunto_ofertas': None,'conjunto_valormt2': None,'conjunto_valorminimo': None,'conjunto_valormaximo': None,'market_precio_promedio': None,'market_ofertas': None,'market_valorminimo': None,'market_valormaximo': None,'market_valormt2': None,'conjunto_ofertas_activas':None}
    inputvarrent = {'precio_compra': None,'conjunto_ofertas': None,'conjunto_valormt2': None,'conjunto_valorminimo': None,'conjunto_valormaximo': None,'market_precio_promedio': None,'market_ofertas': None,'market_valorminimo': None,'market_valormaximo': None,'market_valormt2': None,'conjunto_ofertas_activas':None}

    #-------------------------------------------------------------------------#
    # Pricing por edificio (funciona unicamente en Bogota)
    data     = data_reference(inputvar)
    datasell = data[(data['tipodata']=='Conjunto') & (data['valorventa']>0)]
    datasell = datasell.sort_values(by='fecha_inicial',ascending=False).drop_duplicates(subset='descripcion',keep='first')       
    datarent = data[(data['tipodata']=='Conjunto') & (data['valorarriendo']>0)]
    datarent = datarent.sort_values(by='fecha_inicial',ascending=False).drop_duplicates(subset='descripcion',keep='first')       
    preciocompra = None

    if datasell.empty is False: 
        inputvarsell.update({'conjunto_ofertas':len(datasell),'conjunto_ofertas_activas':sum(datasell['activo']),'conjunto_valormt2':datasell['valormt2_venta'].median(),'conjunto_valorminimo':datasell['valorventa_new'].min(),'conjunto_valormaximo':datasell['valorventa_new'].max()})

    if datarent.empty is False: 
        inputvarrent.update({'precio_compra':datarent['valorarriendo_new'].median(),'conjunto_ofertas':len(datarent),'conjunto_ofertas_activas':sum(datarent['activo']),'conjunto_valormt2':datarent['valormt2_renta'].median(),'conjunto_valorminimo':datarent['valorarriendo_new'].min(),'conjunto_valormaximo':datarent['valorarriendo_new'].max()})
       
    if len(datasell)>=3:
        vardep = 'valorventa'
        if sum(datasell['valorventa_new'].notnull())>3:
            vardep = 'valorventa_new'
        p1    = datasell[vardep].median()*0.95
        admon = None
        if 'adminsitracion' in inputvar: admon = inputvar['adminsitracion']
        #presult      = precio_compra({'precio_venta':p1*0.95,'areaconstruida':area,'admon':admon,'ganancia':ganancia})
        #preciocompra = presult['preciocompra']
        presult      = precio_compra({'precio_venta':p1,'areaconstruida':area,'admon':admon,'ganancia':ganancia})
        preciocompra = presult['preciocompra']
        if 'precioventa' in inputvar and inputvar['precioventa']>0:
            preciocompra = min(presult['preciocompra'],inputvar['precioventa']*0.94)
            presult['preciocompra'] = preciocompra
        inputvarsell.update(presult)
        inputvarsell.update({'precio_compra':preciocompra})
        
    #-----------------------------------------------------------------------------#
    # Market
    datasell = data[(data['tipodata']=='Market') & (data['valorventa']>0)]
    datasell = datasell.sort_values(by='fecha_inicial',ascending=False).drop_duplicates(subset='descripcion',keep='first')       
    datarent = data[(data['tipodata']=='Market') & (data['valorarriendo']>0)]
    datarent = datarent.sort_values(by='fecha_inicial',ascending=False).drop_duplicates(subset='descripcion',keep='first')       

    if len(datasell)>=3:
        inputvarsell.update({'market_precio_promedio':datasell['valorventa'].median(),'market_ofertas':len(datasell),'market_valorminimo':datasell['valorventa'].min(),'market_valormaximo':datasell['valorventa'].max(),'market_valormt2':datasell['valormt2_venta'].median()})

    if len(datarent)>=3:
        inputvarrent.update({'market_precio_promedio':datarent['valorarriendo'].median(),'market_ofertas':len(datarent),'market_valorminimo':datarent['valorarriendo'].min(),'market_valormaximo':datarent['valorarriendo'].max(),'market_valormt2':datarent['valormt2_renta'].median()})
    
    #-----------------------------------------------------------------------------#
    # Nuevo modelo forecast   
    if inputvar['anos_antiguedad']<=1:
        inputvar['tiempodeconstruido'] = 'menor a 1 año'
    elif inputvar['anos_antiguedad']>1 and inputvar['anos_antiguedad']<=8:
        inputvar['tiempodeconstruido'] = '1 a 8 años'
    elif inputvar['anos_antiguedad']>8 and inputvar['anos_antiguedad']<=15:
        inputvar['tiempodeconstruido'] = '9 a 15 años'
    elif inputvar['anos_antiguedad']>15 and inputvar['anos_antiguedad']<=30:
        inputvar['tiempodeconstruido'] = '16 a 30 años'
    elif inputvar['anos_antiguedad']>30:
        inputvar['tiempodeconstruido'] = 'más de 30 años'

    inputvar['tipoinmueble'] = 'Apartamento'   
    inputvar['tiponegocio']  = 'sell'   
    inputvarsell.update(pricingforecast(inputvar))
    inputvar['tiponegocio']  = 'rent'
    inputvarrent.update(pricingforecast(inputvar))
    
    # Si no hay precio de compra: 
    if (preciocompra is None or pd.isna(preciocompra)) and 'valorestimado' in inputvarsell and inputvarsell['valorestimado']>0: 
        p1    = min(inputvarsell['valorestimado']*0.95,inputvar['precioventa']*0.95)
        admon = None
        if 'adminsitracion' in inputvar: admon = inputvar['adminsitracion']
        presult      = precio_compra({'precio_venta':p1,'areaconstruida':area,'admon':admon,'ganancia':ganancia+0.01})
        preciocompra = presult['preciocompra']
        inputvarsell.update({'precio_compra':preciocompra})
        
    # Exportar Data Inmueble
    if 'sku' not in inputvar or ('sku' in inputvar and inputvar['sku'] is None or inputvar['sku']==""):
        inputvar['sku'] = id_generator()
    
        # venta
    if 'precioventa' in inputvar and  inputvar['precioventa'] is not None and inputvar['precioventa']>0:   inputvarsell['valormt2_precio_ofrecido']  = inputvar['precioventa']/inputvar['areaconstruida']
    if 'precio_compra' in inputvarsell and  inputvarsell['precio_compra'] is not None and inputvarsell['precio_compra']>0: inputvarsell['valormt2_precio_compra']    = inputvarsell['precio_compra']/inputvar['areaconstruida']
    if 'precioventa' in inputvar and 'precio_compra' in inputvarsell:
        try: inputvarsell['diferencia'] = inputvarsell['precio_compra']/inputvar['precioventa']-1
        except: pass
        
        # renta
    if 'preciorenta' in inputvar and inputvar['preciorenta'] is not None and inputvar['preciorenta']>0:   inputvarrent['valormt2_precio_ofrecido']  = inputvar['preciorenta']/inputvar['areaconstruida']
    if 'precio_compra' in inputvarrent and inputvarrent['precio_compra'] is not None and inputvarrent['precio_compra']>0: inputvarrent['valormt2_precio_compra']    = inputvarrent['precio_compra']/inputvar['areaconstruida']
    if 'preciorenta' in inputvar and 'precio_compra' in inputvarrent:
        try: inputvarrent['diferencia'] = inputvarrent['precio_compra']/inputvar['preciorenta']-1
        except: pass

    inputvar['fecha_registro'] = todaynum.strftime("%Y-%m-%d %H:%M")
    
    inputvarsell.update(inputvar)
    dataresultsell = pd.DataFrame([inputvarsell])
    dataresultsell['tiponegocio'] = 'Venta'
    dataresultsell['precio_oferta'] = inputvar['precioventa']
    
    inputvarrent.update(inputvar)
    dataresultrent = pd.DataFrame([inputvarrent])
    dataresultrent['tiponegocio']   = 'Arriendo'
    dataresultrent['precio_oferta'] = inputvar['preciorenta']
    dataresult = dataresultsell.append(dataresultrent)

    # Id inmueble ya existente
    user     =st.secrets["buydepauser"]
    password =st.secrets["buydepapass"]
    host     =st.secrets["buydepahost"]
    database =st.secrets["buydepadatabase"]
    dataregistrostock = pd.DataFrame()
    datacomparestock  = pd.DataFrame()
    id_inmueble       = inputvar['id_inmueble']
    if id_inmueble is not None:
        db_connection     = sql.connect(user=user, password=password, host=host, database=database)
        dataregistrostock = pd.read_sql(f"SELECT id FROM {database}.data_api_pricing_registros WHERE id_inmueble={id_inmueble}" , con=db_connection)
        datacomparestock  = pd.read_sql(f"SELECT id FROM {database}.data_api_pricing_comparables WHERE id_inmueble={id_inmueble}" , con=db_connection)
        db_connection.close()
    
    if 'metros' in dataresult: del dataresult['metros']
    variables  = ['id_inmueble','sku','direccion','areaconstruida','habitaciones','banos','garajes','estrato','num_piso','anos_antiguedad','num_ascensores','numerodeniveles','adminsitracion','precioventa','preciorenta','nombre_edificio','url','direccion_formato','conjunto_unidades','antiguedad_min','antiguedad_max','coddir','latitud','longitud','dpto_ccdgo','mpio_ccdgo','setu_ccnct','secu_ccnct','scacodigo','scanombre','tiempodeconstruido','tipoinmueble','tiponegocio','fecha_registro','precio_oferta','precio_compra','conjunto_ofertas','conjunto_valormt2','conjunto_valorminimo','conjunto_valormaximo','market_precio_promedio','market_ofertas','market_valorminimo','market_valormaximo','market_valormt2','conjunto_ofertas_activas','valorestimado','valorestimado_mt2','valormt2_precio_ofrecido','valormt2_precio_compra','diferencia','precio_venta','retorno_bruto_esperado','retorno_neto_esperado','gn_compra','gn_venta','comisiones','otros_gastos']
    variables  = [x for x in variables if x in list(dataresult)]
    dtype      = dtype_inmueble(dataresult[variables])
    engine     = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
    dataresult[variables].to_sql('data_api_pricing_registros',engine,if_exists='append', index=False,chunksize=10,dtype=dtype)
    dataresult[variables].to_sql('data_api_pricing_registros_historico',engine,if_exists='append', index=False,chunksize=10,dtype=dtype)

    # Exportar Data Ofertas
    data['sku']            = inputvar['sku']
    data['id_inmueble']    = inputvar['id_inmueble']
    data['fecha_registro'] = inputvar['fecha_registro'] 
    variables   = ['id_inmueble','sku','direccion','fecha_registro','fecha_inicial','fuente','areaconstruida','descripcion','estrato','garajes','habitaciones','latitud','longitud','valorarriendo','valorventa','valorventa_new','valorarriendo_new','valormt2_venta','valormt2_renta','tiempodeconstruido','tipoinmueble','tiponegocio','url','tipodata','activo']
    variables   = [x for x in variables if x in list(data)]
    dtype       = dtype_comparables(data[variables])
    engine      = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
    data[variables].to_sql('data_api_pricing_comparables',engine,if_exists='append', index=False,chunksize=100,dtype=dtype)

    if dataregistrostock.empty is False:
        valores = list(dataregistrostock.apply(lambda x: tuple(x), axis=1).unique())
        db_connection = sql.connect(user=user, password=password, host=host, database=database)
        cursor        = db_connection.cursor()
        cursor.executemany("""DELETE FROM `{database}`.`data_api_pricing_registros` WHERE (`id` = %s ); """,valores)
        db_connection.commit()
        db_connection.close()

    if datacomparestock.empty is False:
        valores = list(datacomparestock.apply(lambda x: tuple(x), axis=1).unique())
        db_connection = sql.connect(user=user, password=password, host=host, database=database)
        cursor        = db_connection.cursor()
        cursor.executemany("""DELETE FROM `{database}`.`data_api_pricing_comparables` WHERE (`id` = %s ); """,valores)
        db_connection.commit()
        db_connection.close()
        
        
    # Actualziar data set PowerBI
        # https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset
    #url = 'https://api.powerbi.com/v1.0/myorg/datasets/48c2aa2c-edde-4b45-be16-1d4d53f82eab/refreshes'
    #headers = {"Authorization":"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzY3ZDFhN2EtOWQxMy00YWM4LThjZTgtYzNlMTdmYjYxOTMxLyIsImlhdCI6MTY2Mjk0MDg1NywibmJmIjoxNjYyOTQwODU3LCJleHAiOjE2NjI5NDYxODksImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVFFBeS84VEFBQUFpbGdGRzFlemVsV0tSR2dnZ1BqUTVKUk5WaU0zUWZuMDBHbTlHVWtqVzdSSEt0LzBNaUpYUG1tTHBiK0pZYkZvIiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6IjE4ZmJjYTE2LTIyMjQtNDVmNi04NWIwLWY3YmYyYjM5YjNmMyIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiR2F2aXJpYSIsImdpdmVuX25hbWUiOiJBbGVqYW5kcm8iLCJpcGFkZHIiOiIxODEuNjIuMjAuMjIwIiwibmFtZSI6IkFsZWphbmRybyBHYXZpcmlhIiwib2lkIjoiMDE1MjFkYjMtZDllOC00NzMyLWE3OWItOGM5OGU1MTJhMjdkIiwicHVpZCI6IjEwMDMyMDAxRDJFOEQ0NDciLCJyaCI6IjAuQVRRQWVocDlkaE9keUVxTTZNUGhmN1laTVFrQUFBQUFBQUFBd0FBQUFBQUFBQUEwQUdnLiIsInNjcCI6IkFwcC5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkV3JpdGUuQWxsIENvbnRlbnQuQ3JlYXRlIERhc2hib2FyZC5SZWFkLkFsbCBEYXNoYm9hcmQuUmVhZFdyaXRlLkFsbCBEYXRhZmxvdy5SZWFkLkFsbCBEYXRhZmxvdy5SZWFkV3JpdGUuQWxsIERhdGFzZXQuUmVhZC5BbGwgRGF0YXNldC5SZWFkV3JpdGUuQWxsIEdhdGV3YXkuUmVhZC5BbGwgR2F0ZXdheS5SZWFkV3JpdGUuQWxsIFBpcGVsaW5lLkRlcGxveSBQaXBlbGluZS5SZWFkLkFsbCBQaXBlbGluZS5SZWFkV3JpdGUuQWxsIFJlcG9ydC5SZWFkLkFsbCBSZXBvcnQuUmVhZFdyaXRlLkFsbCBTdG9yYWdlQWNjb3VudC5SZWFkLkFsbCBTdG9yYWdlQWNjb3VudC5SZWFkV3JpdGUuQWxsIFRlbmFudC5SZWFkLkFsbCBUZW5hbnQuUmVhZFdyaXRlLkFsbCBVc2VyU3RhdGUuUmVhZFdyaXRlLkFsbCBXb3Jrc3BhY2UuUmVhZC5BbGwgV29ya3NwYWNlLlJlYWRXcml0ZS5BbGwiLCJzaWduaW5fc3RhdGUiOlsia21zaSJdLCJzdWIiOiJQSE5YY0NKSUcyTmF5eFBQLV80NjBfMjJkWVFjWXR0aXJBLWYwdTVteTIwIiwidGlkIjoiNzY3ZDFhN2EtOWQxMy00YWM4LThjZTgtYzNlMTdmYjYxOTMxIiwidW5pcXVlX25hbWUiOiJhZ2F2aXJpYUBidXlkZXBhLmNvbSIsInVwbiI6ImFnYXZpcmlhQGJ1eWRlcGEuY29tIiwidXRpIjoic1kwQnZMUFhoMEdVcUZrZm1jUTJBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il19.exl2bdxYyZ9ihQifD-ezLYq6N91d9Yro6AXZ3NL4DXQBQ8MInWISMs3QjsEmHoibfZN0lJ9OpQg23mPA37sgS4sZvO4pbk0KpFYmVZrzTHZoWKIWlo0T6qkP04DE6lqOTiLWEjgud0jeYLjBrChIShsylwQJ4GcVOdiUQ1H0MlgWfd0EXNWk6ROL0S-6VPO1f9yERcfP5AIts9r0AXMMPzb-c94YZ8wpRtZHUOn5KSep-jnGJ_T6Mi-ll33u62CT2gqUjuqQsOWftC4pXAqYcHK6xtklODzB3KRpZK4HL_cgZrcEoczlMoD4XbkPhnW54vDtCo1moeEMr6dEBi2RCg",
    #           "Content-Type":"application/json"}
    #requests.post(url,headers = headers,timeout=30)
    
    result = {}
    result['id_inmueble'] = inputvar['id_inmueble']
    result['sku']         = inputvar['sku']
    return result



header        = st.container()
address       = st.container()
resultaddress = st.container()
features      = st.container()
results       = st.container()
results1      = st.container()

id_inmueble,direccion_formato,nombre_edificio,areaconstruida,habitaciones,banos,garajes,estrato,num_piso,anos_antiguedad,num_ascensores,numerodeniveles,adminsitracion,precioventa,preciorenta,url = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

with header:
    st.title('Pricing Buydepa Colombia')
    id_inmueble = st.text_input('ID Inmueble')
    url         = st.text_input('url de la oferta','')
    ciudad      = st.selectbox('Ciudad',options=['Bogota'])
    
with address:
    formulario   = st.columns(4)
    tipovia      = formulario[0].selectbox('Tipo via',options=['CL','KR','TR','DG'])
    complemento1 = formulario[1].text_input('Complemento 1')
    complemento2 = formulario[2].text_input('Complemento 2')
    complemento3 = formulario[3].text_input('Complemento 3')
    
with resultaddress:
    complemento1 = re.sub(r'\s+',' ',re.sub('[^0-9a-zA-Z]',' ',complemento1))
    complemento2 = re.sub(r'\s+',' ',complemento2)
    complemento3 = re.sub(r'\s+',' ',complemento3)
    direccion_formato = f'{tipovia} {complemento1} {complemento2} {complemento3}, {ciudad}'
    col1,col2 = st.columns(2)
    col1.text('Direccion: ')
    col2.write(direccion_formato)    

with features:
    nombre_edificio  = st.text_input('Nombre del conjunto: ','')
    areaconstruida   = st.slider('Area construida',min_value=30,max_value=150,value=50)
    habitaciones     = st.selectbox('# Habitaciones',options=[1,2,3,4])
    banos            = st.selectbox('# Banos',options=[1,2,3,4,5])
    garajes          = st.selectbox('# Garajes',options=[0,1,2,3])
    estrato          = st.selectbox('Estrato',options=[1,2,3,4,5,6])
    num_piso         = st.slider('Numero de piso',min_value=1,max_value=30)
    anos_antiguedad  = st.slider('Anos de antiguedad',min_value=0,max_value=50)
    num_ascensores   = st.selectbox('Tiene ascensor',options=['Si','No'])
    numerodeniveles  = st.selectbox('Numero de niveles',options=[1,2,3])
    preciorenta      = st.text_input('Precio de oferta en renta','')
    adminsitracion   = st.text_input('Valor de la adminsitracion','')
    precioventa      = st.text_input('Precio de oferta en venta','')
    
with results:
    try:    id_inmueble = int(id_inmueble)
    except: id_inmueble = None
    if   'Si' in num_ascensores: num_ascensores = 1
    elif 'No' in num_ascensores: num_ascensores = 0
    try:    adminsitracion = float(adminsitracion)
    except: adminsitracion = None
    try:    precioventa = float(precioventa)
    except: precioventa = None
    try:    preciorenta = float(preciorenta)
    except: preciorenta = None
    try:    nombre_edificio = nombre_edificio.upper()
    except: nombre_edificio = None    
    inputvar = {
                "id_inmueble": id_inmueble,  
                "direccion":direccion_formato,
                'areaconstruida':areaconstruida,
                'habitaciones':habitaciones,
                'banos':banos,
                'garajes':garajes,
                'estrato':estrato,
                'num_piso':num_piso,
                'anos_antiguedad':anos_antiguedad,
                'num_ascensores':num_ascensores,
                'numerodeniveles':numerodeniveles,
                'adminsitracion':adminsitracion,
                'precioventa':precioventa,
                'preciorenta':preciorenta,
                'nombre_edificio':nombre_edificio,
                'url':url,
                'metros':300        
        }
    st.text('Verificar la informacion antes de calcular el precio!')
    st.write(inputvar)   
    
with results1:
    st.text('Cuando la informacion este lista se activara el boton para calcular el precio!')
    idcontinue = True
    for i in [direccion_formato,areaconstruida,habitaciones,banos,garajes,estrato,num_piso,anos_antiguedad,num_ascensores,numerodeniveles,adminsitracion,precioventa]:
        if idcontinue:
            if i is None or i=='':
                idcontinue = False
    if idcontinue:
        if st.button('Calcular pricing'):
            resultadofun = getpricing(inputvar)
            col1,col2    = st.columns(2)
            col1.text('SKU: ')
            col2.write(resultadofun['sku'])
            st.text('Funcion realizada con exito') 
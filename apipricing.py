import streamlit as st
import re

from sidefunctions import getpricing

# anaconda promt: streamlit run D:\Dropbox\Empresa\Buydepa\COLOMBIA\DESARROLLO\streamlit\pricing\apipricing.py
# https://streamlit.io/
# En anaconda: pipreqs --encoding utf-8 "D:\Dropbox\Empresa\Buydepa\COLOMBIA\DESARROLLO\streamlit\pricing"
# https://docs.streamlit.io/library/api-reference/control-flow/


header        = st.container()
address       = st.container()
resultaddress = st.container()
features      = st.container()
results       = st.container()
results1      = st.container()

id_inmueble,direccion_formato,nombre_edificio,areaconstruida,habitaciones,banos,garajes,estrato,num_piso,anos_antiguedad,num_ascensores,numerodeniveles,adminsitracion,precioventa,preciorenta,url = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

with header:
    st.image("https://col-images-properties.s3.amazonaws.com/nuevologo.png",width=200)
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
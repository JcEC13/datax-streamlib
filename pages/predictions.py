import streamlit as st
import time
import random
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower

spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

df_metadata = spark.read.parquet("./datasets/metada-streamlit.parquet")
dfuser = spark.read.parquet("./datasets/users.parquet")

def read_dataset(option):        
    if option=='Bar':
        df = spark.read.parquet("./datasets/bar_part.snappy.parquet")
    if option=='Restaurant':
        df= spark.read.parquet("./datasets/restaurant_part.snappy.parquet")
    if option=='Cafe':
        df=spark.read.parquet("./datasets/cafe_part.snappy.parquet")

    return df      

def prediction(option,idclient):
    df = read_dataset(option)
    df = df.filter((col("id_name") ==idclient))
    joined_df = df.join(df_metadata,df.id_name_empresa==df_metadata.id_name_empresa)
    joined_df=joined_df.select('name','avg_rating','address')
    df2= joined_df.head(5)
    return df2

def random_client(dfread):
    idrnd=dfread.select('id_name').drop_duplicates()
    idrnd=idrnd.head(10)
    return idrnd[random.randint(0, len(idrnd))][0]

def idclient_generate(name,option):
    name=name.lower()
    df= dfuser.filter(lower(dfuser['name']).like(f"%{name}%"))
    id=df.head(1)
    dataset=read_dataset(option)
    if id==[]:
        idclient=random_client(dataset)
    else:
        idclient=str(id[0][0])
    
    df_fil=dataset.filter(dataset['id_name']==idclient)

    if df_fil.isEmpty():
        idclient=random_client(dataset)

    return idclient



st.title("Recommendation System - Beta")
name = st.text_input('Username:', 'Enter your name here')


st.write("Choose an option to search within the available options")

st.write("Category")
#a = st.sidebar.radio('Select one:', ['Delivery', 'Local'])

option = st.selectbox(
    'which category do you want to search?',
    ('Restaurant','Cafe', 'Bar'))

st.write('You selected:', option)
idclient=idclient_generate(name,option)

#genre = st.radio(
#    "Where do you want to receive your order?",
#    ('Delivery', 'In store'))

#st.write('You selected:', genre)

if st.button('Search'):
    # Add a placeholder
    latest_iteration = st.empty()
    bar = st.progress(0)
    data=prediction(option,idclient)
    for i in range(100):
    # Update the progress bar with each iteration.
        latest_iteration.text(f'{i+1} %')
        bar.progress(i + 1)
        time.sleep(0.05)
    mss=st.text('Results found')
    df=pd.DataFrame(data,columns=['Name store','Rating','Address'])
    st.dataframe(df)



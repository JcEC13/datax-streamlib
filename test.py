import streamlit as st
from PIL import Image


st.title("DATAX PREDICTIONS - BETA")


image = Image.open('./images/datax-logo.jpeg')

st.image(image, width=300)

st.write('Recommendation system for restaurants, cafes and bars in New York city.')

#logo = "<div style='text-align:center'><img src='./images/datax-logo.jpeg'></div>"
#st.markdown(logo, unsafe_allow_html=True)
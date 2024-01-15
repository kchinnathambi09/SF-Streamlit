import snowflake.connector 
import streamlit as st

sf_dev = snowflake.connector.connect(**st.secrets["snowflake"])

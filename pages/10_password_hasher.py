import streamlit as st
import streamlit_authenticator as stauth

st.title('Password Hasher')

with st.form('pw-hasher'):
    password = st.text_input('Please enter your password').strip()
    submiited = st.form_submit_button('Generate hashed password')
    if submiited:
        hashed_password = stauth.Hasher([password]).generate()[0]
        st.write('Your Hashed Password is :')
        st.text(hashed_password)

import asyncio
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import aioboto3
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
from botocore.exceptions import ClientError
from pydantic import BaseModel

st.set_page_config('Upload Page',  layout='centered')


class S3Credentials(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    endpoint_url: str


if 'aioboto3_session' not in st.session_state:
    st.session_state['aioboto3_session'] = aioboto3.Session()


def get_authenticator():
    _credentials = st.secrets.credentials.items()
    credentials = {'usernames': {}}
    keys = ('name', 'password')
    for c in _credentials:
        k = c[0]
        credentials['usernames'][k] = dict(zip(keys, c))

    authenticator = stauth.Authenticate(
        credentials,
        st.secrets['cookie']['name'],
        st.secrets['cookie']['key'],
        st.secrets['cookie']['expiry_days'])
    return authenticator


def get_username() -> str:
    return st.session_state['username']


@st.cache_data
def get_bucket_name() -> str:
    return st.secrets['linode_s3']['bucket_name']


@st.cache_data
def get_s3_credentials() -> dict:
    return S3Credentials(**st.secrets['linode_s3']).dict()


@st.cache_resource
def get_session() -> aioboto3.Session:
    return st.session_state['aioboto3_session']


def get_s3() -> aioboto3.Session.client:
    s3_credentials = get_s3_credentials()
    session = get_session()
    return session.client("s3", **s3_credentials)


async def create_presigned_url(s3: aioboto3.Session.client,
                               bucket_name: str,
                               object_name: str,
                               expiration: int = 86400) -> str:
    try:
        response = await s3.generate_presigned_url('get_object',
                                                   Params={'Bucket': bucket_name,
                                                           'Key': object_name},
                                                   ExpiresIn=expiration)
        st.session_state['gen_urls'].append(response)
    except ClientError as e:
        st.error(
            f"Unablr generate presigned url for {object_name}: {e} ({type(e)})")
        return

    # The response contains the presigned URL
    return response


async def upload(s3: aioboto3.Session.client,
                 bucket_name: str,
                 uploaded_file: list,
                 required_presigned_url: bool,
                 expiration: int = 86400) -> str:
    uploaded_filename = uploaded_file.name
    dummy_p = Path(uploaded_filename)
    username = get_username()
    object_name = username + '/' + dummy_p.stem + \
        '_' + uuid4().hex[:6] + dummy_p.suffix

    try:
        await s3.upload_fileobj(uploaded_file, bucket_name, object_name)
    except Exception as e:
        st.error(
            f"Unable upload {uploaded_filename} to {object_name}: {e} ({type(e)})")
        return

    if required_presigned_url:
        return await create_presigned_url(s3, bucket_name, object_name, expiration=expiration)


async def async_upload_files(s3: aioboto3.Session.client,
                             bucket_name: str,
                             uploaded_files: list,
                             required_presigned_url: bool,
                             return_exceptions: bool = True):
    tasks = [asyncio.create_task(upload(s3, bucket_name, uploaded_file, required_presigned_url))
             for uploaded_file in uploaded_files]
    return await asyncio.gather(*tasks, return_exceptions=return_exceptions)


@st.cache_data
def convert_df(df: pd.DataFrame, index: bool = False, header: bool = False):
    return df.to_csv(index=index, header=header).encode('utf-8')


async def main():
    # always clear gen_urls first
    st.session_state['gen_urls'] = []

    n_rate_limit = st.secrets['n_rate_limit']
    uploaded_files, uploaded, csv = None, None, None

    authenticator = get_authenticator()
    name, authentication_status, username = authenticator.login(
        'Login', 'main')

    if authentication_status:
        col1, col2, _ = st.columns([1, 1, 6])
        with col1:
            if st.button('Refresh'):
                st.experimental_rerun()
        with col2:
            authenticator.logout('Logout', 'main')
        st.title(f'Hello, {username}')

        with st.form('upload-form', clear_on_submit=True):
            uploaded_files = st.file_uploader(
                "Choose file(s)", accept_multiple_files=True)
            required_presigned_url = st.checkbox('Generate presigned url')
            uploaded = st.form_submit_button('Upload')

        if uploaded_files and uploaded:
            start = perf_counter()
            n_files = len(uploaded_files)
            div, _ = divmod(n_files, n_rate_limit)
            n = div+1
            bar = st.progress(0, text='Preparing...')
            bucket_name = get_bucket_name()
            async with get_s3() as s3:
                for i in range(n):
                    chunk_files = uploaded_files[(
                        i)*n_rate_limit: (i+1)*n_rate_limit]
                    await async_upload_files(s3, bucket_name, chunk_files, required_presigned_url)
                    current_progress = (i+1)/n
                    bar.progress(current_progress,
                                 text=f'Uploading...{current_progress*100:.2f}% done')
                if required_presigned_url:
                    urls = st.session_state['gen_urls']
                    df = pd.DataFrame(urls,
                                      index=range(1, len(urls)+1),
                                      columns=['gen_url'])
                    st.dataframe(df)
                    csv = convert_df(df)

                elapsed = perf_counter() - start
                st.success(f'Done, {elapsed:=.2f} secs!')

        if uploaded_files and uploaded and csv:
            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name='urls.csv',
                mime='text/csv')

    elif authentication_status is False:
        st.error('Username/password is incorrect')
    elif authentication_status is None:
        st.warning('Please enter your username and password')


if __name__ == '__main__':
    asyncio.run(main())

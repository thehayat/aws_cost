import boto3
import io
import pandas as pd
import streamlit as st
from datetime import datetime
import plotly.express as px
from pathlib import Path
import plotly.graph_objects as go
import pickle
import streamlit_authenticator as stauth

st.set_page_config(
    page_title="Cost Metering",
    page_icon="🎈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

import yaml
from yaml.loader import SafeLoader

with open('config.yml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)



with st.sidebar:
    st.title("Welcome to AWS Cost Analysis")
    # st.markdown(f"### Data Available from {min_date_row['Month']} to {}")
    name, authentication_status, username = authenticator.login("Login", "main")
    if authentication_status == False:
        st.error("Username/password is incorrect")

    if authentication_status == None:
        st.warning("Please enter your username and password")
    if authentication_status:
        df = pd.read_csv('sample.csv')
        df["timestamp"] = df['period'].apply(lambda x: datetime.strptime(str(x), '%Y%m'))
        df['Year'] = df['timestamp'].apply(lambda x: x.year)
        df['Month'] = df['timestamp'].apply(lambda x: x.month_name())
        # df['cost'] = df['cost'].apply(lambda x: int(x))
        df['cost'] = df['cost'].round(2)
        max_date_row = df.iloc[df['timestamp'].idxmax()]
        min_date_row = df.iloc[df['timestamp'].idxmin()]
        if 'start_date' not in st.session_state:
            st.session_state['start_date'] = min_date_row['timestamp']
        if 'end_date' not in st.session_state:
            st.session_state['end_date'] = max_date_row['timestamp']
        st.subheader("Select Start Date")
        start_date_cols = st.columns([1, 1])
        month_list = list(df.Month.unique())
        year_list = list(df.Year.unique())
        s_month = start_date_cols[0].selectbox("Select start month", options=month_list,
                                               index=month_list.index(min_date_row['Month']))
        s_year = start_date_cols[1].selectbox("Select start year", options=year_list,
                                              index=year_list.index(min_date_row['Year']))
        date_obj = datetime.strptime(f"{'01'}-{s_month}-{s_year}", "%d-%B-%Y")
        start_date = date_obj.strftime("%Y-%m-%d")
        st.session_state.start_date = start_date

        st.subheader("Select End Date")
        end_date_cols = st.columns([1, 1])
        e_month = end_date_cols[0].selectbox("Select end month", options=month_list,
                                             index=month_list.index(max_date_row['Month']))
        e_year = end_date_cols[1].selectbox("Select end year", options=year_list,
                                            index=year_list.index(max_date_row['Year']))
        date_obj = datetime.strptime(f"{'01'}-{e_month}-{e_year}", "%d-%B-%Y")
        end_date = date_obj.strftime("%Y-%m-%d")
        st.session_state.end_date = end_date
        authenticator.logout('Logout', 'main', key='unique_key')
if authentication_status:
    st.header("Cost data for resources and partners")
    st.dataframe(df)
    st.write("-----")
    df = df[(df['timestamp'] >= st.session_state.start_date) & (df['timestamp'] <= st.session_state.end_date)]

    ingestor = ['prod-ingestor-sencrop', "prod-ingestor-clearag", 'prod-ingestor-trapview', 'prod-ingestor-davis',
                'prod-ingestor-veris', 'prod-ingestor-pessl', 'prod-ingestor-arable', 'prod-ingestor-groguru',
                'prod-ingestor-valenco']

    observation_v2 = ['prod-cropwise-observations-lambda', 'prod-cropwise-observations-create-kafka-topics',
                      'prod-cropwise-observations-msk', 'prod-cropwise-observations-v2', 'prod-cropwise-observations',
                      'prod-cropwise-observations-cfstack', 'prod-cropwise-observations-eks']

    ingestor_filter = df["tag:user:name"].isin(ingestor)
    observation_v2_filter = df["tag:user:name"].isin(observation_v2)

    ingestor_cols = st.columns([1, 0.3, 2])
    ingestor_group = df[ingestor_filter].groupby(['tag:user:name'])['cost'].sum().reset_index()
    ingestor_fig = px.pie(ingestor_group, values='cost', names='tag:user:name', height=500, width=500, hole=0.5,
                          title='Cost percentage for ingestor')
    ingestor_fig.update_traces(hovertemplate=None, textposition='outside', textinfo='percent+label', rotation=50)
    ingestor_fig.update_layout(margin=dict(t=100, b=30, l=0, r=0), showlegend=False,
                               plot_bgcolor='#fafafa',
                               title_font=dict(size=30, color='#555', family="Lato, sans-serif"),
                               font=dict(size=12, color='#8a8d93'),
                               hoverlabel=dict(bgcolor="#444", font_size=13, font_family="Lato, sans-serif"))

    ingestor_cols[0].subheader("Total ingestor Cost")
    ingestor_cols[0].dataframe(ingestor_group, use_container_width=True)
    ingestor_cols[2].plotly_chart(ingestor_fig, use_container_width=True)
    st.write("-----")

    observation_cols = st.columns([1, 1.6])
    observation_v2_group = df[observation_v2_filter].groupby(['tag:user:name'])['cost'].sum().reset_index()
    observation_fig = px.pie(observation_v2_group, values='cost', names='tag:user:name', height=500, width=500, hole=0.5,
                             title='Cost percentage for observation V2')
    observation_fig.update_traces(hovertemplate=None, textposition='outside', textinfo='percent+label', rotation=50)
    observation_fig.update_layout(margin=dict(t=100, b=30, l=0, r=0), showlegend=False,
                                  plot_bgcolor='#fafafa',
                                  title_font=dict(size=30, color='#555', family="Lato, sans-serif"),
                                  font=dict(size=12, color='#8a8d93'),
                                  hoverlabel=dict(bgcolor="#444", font_size=13, font_family="Lato, sans-serif"))

    observation_cols[0].subheader("Total Observation V2 Cost")
    observation_cols[0].dataframe(observation_v2_group, use_container_width=True)
    observation_cols[1].plotly_chart(observation_fig, use_container_width=True)

    st.write("-----")
    monthly_cost = df.groupby('timestamp')['cost'].sum().reset_index()
    fig = px.bar(monthly_cost, x='timestamp', y='cost')
    st.plotly_chart(fig, use_container_width=True)

    st.write("-----")

    df.loc[df['cost'] < 10.0, 'productdetail'] = 'Other Resources'
    product_grouped = df.groupby(['productdetail'])['cost'].sum().reset_index()
    product_group_cols = st.columns([1, 1.7])
    product_group_cols[0].subheader("Product Group cost")
    product_group_cols[0].write("")
    product_group_cols[0].dataframe(product_grouped, use_container_width=True)
    product_grouped_fig = px.pie(product_grouped, values='cost', names='productdetail', title='Cost Based on the product',
                                 height=600, width=700, hole=0.5)
    product_grouped_fig.update_traces(hovertemplate=None, textposition='inside', textinfo='percent+label', rotation=170)
    product_grouped_fig.update_layout(margin=dict(t=100, b=30, l=0, r=0), showlegend=False,
                                      plot_bgcolor='#fafafa',
                                      title_font=dict(size=30, color='#555', family="Lato, sans-serif"),
                                      font=dict(size=12, color='#8a8d93'),
                                      hoverlabel=dict(bgcolor="#444", font_size=13, font_family="Lato, sans-serif"))
    product_group_cols[1].plotly_chart(product_grouped_fig, use_container_width=True)

    # -------------------------------------------
    # session = boto3.Session(profile_name='CONNECT-DEV')
    # print(session)
    # s3_client = session.client(service_name="s3")
    # # Iterates through all the objects, doing the pagination for you. Each obj
    # # is an ObjectSummary, so it doesn't contain the body. You'll need to call
    # # get to get the whole body.
    # obj = s3_client.get_object(
    #     Bucket='cropwise-connect-cloud8-test-integration',
    #     Key='pivottable-569741333449-202306.txt',
    # )
    # df = pd.read_csv(io.BytesIO(obj['Body'].read()))

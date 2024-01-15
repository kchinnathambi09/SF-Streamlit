import streamlit as st

import pandas as pd
import numpy as np
from importlib.resources import path

import plotly.express as px
import plotly.graph_objects as go
import altair as alt
from matplotlib import pyplot as plt

import toml
import time
import string

import snowflake_compute_sql as sql
import snowflake_connection as conn

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

def compute_metrics():
    
    # Configuration
    st.markdown(
        """
                <style>
                [data-testid="stSidebar"] {background-color: white }
                </style>

                <style>
                [data-testid="stMetricValue"] { font-size: 30px; color: #36454F ; font-weight: bold;}
                </style>

                <style>
                [data-testid="stMetricLabel"] 
                    {  font-size: 20px; color:#2c698d; font-weight: bold;  }
                </style>   

                <style>
                [data-testid="metric-container"] 
                    {   box-shadow: rgb(0 0 0 / 20%) 0px 2px 1px -1px, rgb(0 0 0 / 14%) 0px 1px 1px 0px, rgb(0 0 0 / 12%) 0px 1px 3px 0px; border-radius: 5px; padding: 5% 5% 5% 10%;
                        background-color: #E0E0E0
                                        }     
                </style>

                <style>
                data-testid="stSidebar"] {      border: solid Red;
                                                border-radius: 5px;
                                                padding: 5% 5% 5% 10%;}                       
                </style>

        """, 
        unsafe_allow_html=True, 
    )
    
    #Title
    st.markdown(
        """
            <h3
                style='text-align: center; 
                color: black; 
                background-color: white ;
                border: 10px black;
                border-radius: 5px;'>Snowflake Cost Overview</h3>
        """
        , unsafe_allow_html=True)

    def targetspend_change():
        targetspend_value = st.session_state.TargetSpendKey     

    with st.sidebar:
        with st.expander('**Set Cost**', expanded=False):
            targetspend_value = 2000
            CostPerCredit   =   st.slider("Cost per credit", 2.0, 4.0, 3.0, 0.05, key='CreditCostKey')
            costpertb       =   st.slider("Cost Per TB", 20, 45, 40, 1, key='StorageCostKey')
            targetspend     =   st.slider("Yearly Target",  min_value   =   0,
                                                            max_value   =   10000, 
                                                            value       =   targetspend_value, 
                                                            step        =   50, 
                                                            key         =   "TargetSpendKey", 
                                                            on_change   =   targetspend_change)
            
#------------------------------------------------------------------------------------------------------------------------------------------------------------
                                                    #Read all necessary data 
#------------------------------------------------------------------------------------------------------------------------------------------------------------

#----------------------------------------------         FETCH DATE/MONTH/YEAR DETAILS           ------------------------------------------------#
    date_details        =   pd.read_sql(sql.date_details, conn.sf_dev)
    
    # get the individual details 
    current_mth         =   date_details.iloc[0,0]                  #Current month
    last_mth            =   date_details.iloc[0,1]                  #Last month

    current_qtr         =   date_details.iloc[0,2]                  #Current quarter
    if current_qtr == 1:
        last_qtr            =   4                                   #Last quarter
    else:
        last_qtr            =   current_qtr - 1                     #Last quarter

    current_year        =   date_details.iloc[0,3]                    #Current Year 
    last_year           =   current_year - 1                        #Last year

#----------------------------------------------         COST SPENT CURRENT YEAR           ------------------------------------------------#

    CostSpent_curryear  = pd.read_sql(sql.CostSpent_curryear.format(CostPerCredit), conn.sf_dev)


    if CostSpent_curryear.iloc[0,0] < targetspend:            # Cost utilization percentage for the current year compared to the target cost
        targetspend_delta   = ((CostSpent_curryear.iloc[0,0] / targetspend) * 100).astype(float).round(2)
    else:
        targetspend_delta   = ((CostSpent_curryear.iloc[0,0] / targetspend) * -100).astype(float).round(2)
        
#----------------------------------------------         COST SPENT CURRENT YEAR           ------------------------------------------------#

#----------------------------------------------             YTD metrics           ------------------------------------------------#

    CostSpent_YTD       =   pd.read_sql(sql.CostSpent_YTD.format(CostPerCredit), conn.sf_dev)
    CostSpent_YTD       =   pd.DataFrame(CostSpent_YTD).astype(float).round(2) 

#----------------------------------------------             YTD metrics           ------------------------------------------------#

#----------------------------------------------             MTD metrics           ------------------------------------------------#

    mtd_metrics         =   pd.read_sql(sql.mtd_metrics.format(CostPerCredit, CostPerCredit, CostPerCredit), conn.sf_dev)

    if current_mth == 1:
        computecost_currmth = mtd_metrics["OVERALL_COST_MTD"][(mtd_metrics["MTH"] == current_mth) & (mtd_metrics["YEAR_NUM"] == current_year)]
        computecost_lastmth = mtd_metrics["OVERALL_COST_MTD"][(mtd_metrics["MTH"] == last_mth) & (mtd_metrics["YEAR_NUM"] == last_year)]   
    else:
        computecost_currmth = mtd_metrics["OVERALL_COST_MTD"][(mtd_metrics["MTH"] == current_mth) & (mtd_metrics["YEAR_NUM"] == current_year)]
        computecost_lastmth = mtd_metrics["OVERALL_COST_MTD"][(mtd_metrics["MTH"] == last_mth) & (mtd_metrics["YEAR_NUM"] == current_year)]   

    # Compute cost difference between current and the last month 
    delta_compute_cost_mtd = (computecost_currmth.iloc[0] - computecost_lastmth.iloc[0]).astype(float).round(2)

#----------------------------------------------             MTD metrics           ------------------------------------------------#


    #   QTD metrics
    qtd_metrics         =   pd.read_sql(sql.qtd_metrics.format(CostPerCredit, CostPerCredit), conn.sf_dev)

    if current_mth == 1:
        computecost_currqtr = qtd_metrics["COMPUTE_COST_QTD"][(qtd_metrics["QTR"] == current_qtr) & (qtd_metrics["YEAR_NUM"] == current_year)]
        computecost_lastqtr = qtd_metrics["COMPUTE_COST_QTD"][(qtd_metrics["QTR"] == last_qtr) & (qtd_metrics["YEAR_NUM"] == last_year)]
        computecost_lastyr_sameqtr = qtd_metrics["COMPUTE_COST_QTD"][(qtd_metrics["QTR"] == last_qtr) & (qtd_metrics["YEAR_NUM"] == last_year)]        
    else:
        computecost_currqtr = qtd_metrics["COMPUTE_COST_QTD"][(qtd_metrics["QTR"] == current_qtr) & (qtd_metrics["YEAR_NUM"] == current_year)]
        computecost_lastqtr = qtd_metrics["COMPUTE_COST_QTD"][(qtd_metrics["QTR"] == last_qtr) & (qtd_metrics["YEAR_NUM"] == current_year)]

    # Compute cost difference between current and the last quarter 
    delta_compute_cost_qtr = (computecost_currqtr.iloc[0] - computecost_lastqtr.iloc[0]).astype(float).round(2)

    #Overall cost ( compute and storage cost)
    Overall_cost    =   pd.read_sql(sql.Overall_cost.format(CostPerCredit, costpertb), conn.sf_dev)

    #Heat Map 
    hours_by_day    =   pd.read_sql(sql.hours_by_day.format(CostPerCredit), conn.sf_dev)

    #Top 10 warehouses
    warehouse_spend_top10   =   pd.read_sql(sql.warehouse_spend_top10.format(CostPerCredit), conn.sf_dev)

    #Cost split - pie
    cost_split      =   pd.read_sql(sql.cost_split.format(CostPerCredit, CostPerCredit, costpertb), conn.sf_dev)

    # Column layout
    costdisplay, CostSpentYTD, CostSpentCurrYear, ComputeCostQTD, ComputeCostMTD = st.columns([0.1, 0.225, 0.225, 0.225, 0.225], gap="small")

    CostSpentYTD.metric(
        label = "**Cost Spent - YTD**",
        value = f'$ {CostSpent_YTD.iloc[0,0]}',
        delta = '-2000',
        delta_color = "inverse",
        help  = 'Overall cost spent **(Year-to-date)**'
    ) 

    CostSpentCurrYear.metric(
        label = f"Cost Spent - {current_year}",
        value = f'$ {CostSpent_curryear.iloc[0,0]}',
        delta = f'{targetspend_delta} %',
        delta_color = "normal",
        help  = f'Overall cost spent for the year {current_year} & \n\n Target cost utilization percentage'
    ) 

    ComputeCostQTD.metric(
        label = "**Compute cost - QTD**",
        value = f'$ {computecost_currqtr.iloc[0]}',
        delta = str(delta_compute_cost_qtr),
        delta_color = "inverse",
        help  = 'Compute cost spent QTD & the cost difference between selected quarter and the previous'                
        )  

    ComputeCostMTD.metric(
        label =  "**Compute cost - MTD**",
        value = f'$ {computecost_currmth.iloc[0]}',
        delta = str(delta_compute_cost_mtd),
        delta_color = "inverse",
        help  = 'Compute cost spent MTD & the cost difference between selected month and the previous'
        )                
    
    with costdisplay:
        st.write('**Cost per credit:**  ', CostPerCredit)
        st.write('**Cost per TB:**      ',     costpertb)
        st.write('**Yearly target:**    ',   targetspend)

#########--------------------------- Stacked charts ------------------------#################

    st.write("")
    Rolling12months, Rolling7heatmap    =    st.columns(2)
    warehouse_top10, costsplit          =    st.columns(2)    

    Rolling_12_stacked   =   px.bar(Overall_cost,x = 'TIME_IN_MONTHS',y = 'COST',color='CATEGORY', text_auto = True, barmode='stack' )
    
    Rolling_12_stacked.update_xaxes(showgrid=False)  
    Rolling_12_stacked.update_yaxes(showgrid=False)     

    Rolling_12_stacked.update_layout(xaxis_title = 'Time in months', yaxis_title = 'Overall Cost Spent', 
                                    width = 820, height = 420,
                                    legend=dict(yanchor="top", y=7 , xanchor="center", x=0.90,  orientation="h"))
    
    with Rolling12months:
        Rolling12months_hd = '<h6 style="text-align: center; font-size: 17px">Overall Cost spent rolling 12 months</h6>'
        st.markdown(Rolling12months_hd, unsafe_allow_html= True)         
        st.plotly_chart(Rolling_12_stacked)

    Rolling_7_heatmap  =   alt.Chart(hours_by_day).mark_bar(size=40).encode(
                                    alt.Y('DATE:N', title='Date'),
                                    alt.X('HOURS:N', title='Hour of the day') ,
                                    alt.Color('COST_SPENT:Q', title='Cost Spent').legend(None)).properties(width=700, height=400)

    with Rolling7heatmap:
        HeatMap_hd = '<h6 style="text-align: center; font-size: 17px">Heat Map hours/Rolling 7 days</h6>'
        st.markdown(HeatMap_hd, unsafe_allow_html= True)          
        st.altair_chart(Rolling_7_heatmap, theme="streamlit", use_container_width=True) 


    #Build bar chart for top10 warehouses 
    wh_top10_Chart = px.bar(warehouse_spend_top10, x="OVERALLCOST", y="WAREHOUSE_NAME", color='OVERALLCOST', text_auto= True, orientation='h')

    #Update the bar chart layout    
    wh_top10_Chart.update_layout(xaxis_title='Overall Cost', yaxis_title='Warehouse Name',  autosize=False, width=500, height=400, yaxis=dict(autorange="reversed"), showlegend=False)    

    with warehouse_top10:
        wh_top10_hd = '<h6 style="text-align: center; font-size: 20px">Cost Spent by top 10 Warehouses</h6>'
        st.markdown(wh_top10_hd, unsafe_allow_html= True)
        st.plotly_chart(wh_top10_Chart,use_container_width=True)  


    ###CostPercent###
    cost_split_chart = px.pie(cost_split, 
            values= "COST_SPENT", 
            names = 'CATEGORY', 
            hole=0.3)

    cost_split_chart.update_layout(autosize=True,
                            width=500,
                            height=400)

    with costsplit:
        costsplit_hd = '<h6 style="text-align: center; font-size: 20px"> Compute cost vs Cloud Services cost vs Storage cost</h6>'
        st.markdown(costsplit_hd, unsafe_allow_html= True)
        st.plotly_chart(cost_split_chart, use_container_width=True)

compute_metrics()
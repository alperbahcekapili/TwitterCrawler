


from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
import streamlit as st
import pandas as pd


if "temp_df" not in st.session_state:

    temp_df = pd.read_csv("/home/alper/TwitterCrawler/tweets_dump_csv/2022-09-22 15:05:16.946097/bim/en/2022-09-22 15:05:23.447212.csv")
    gb = GridOptionsBuilder.from_dataframe(temp_df)
    gb.configure_pagination(paginationAutoPageSize=True) #Add pagination
    gb.configure_side_bar() #Add a sidebar
    #gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren="Group checkbox select children") #Enable multi-row selection
    gridOptions = gb.build()

    grid_response = AgGrid(
        temp_df,
        gridOptions=gridOptions,
        data_return_mode='AS_INPUT', 
        update_mode='MODEL_CHANGED', 
        fit_columns_on_grid_load=False,
        theme='material', #Add theme color to the table
        enable_enterprise_modules=True,
        height=500, 
        width='100%',
        reload_data=True
    )


    f = open("/home/alper/TwitterCrawler/temp", "a+")
    f.write("Refreshed")
    f.close()
    st.write("Refreshed")

    st.session_state["temp_df"] = temp_df

else:
    temp_df = st.session_state["temp_df"]

    gb = GridOptionsBuilder.from_dataframe(temp_df)
    gb.configure_pagination(paginationAutoPageSize=True) #Add pagination
    gb.configure_side_bar() #Add a sidebar
    #gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren="Group checkbox select children") #Enable multi-row selection
    gridOptions = gb.build()

    grid_response = AgGrid(
        temp_df,
        gridOptions=gridOptions,
        data_return_mode='AS_INPUT', 
        update_mode='MODEL_CHANGED', 
        fit_columns_on_grid_load=False,
        theme='material', #Add theme color to the table
        enable_enterprise_modules=True,
        height=500, 
        width='100%',
        reload_data=True
    )

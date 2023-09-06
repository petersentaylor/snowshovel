import streamlit as st
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
import utils as u



# connect to Snowflake
session = Session.builder.configs(st.secrets["db_credentials"]).create()


def loadInferAndPersist(file) -> snowpark.DataFrame:
    file_df = pd.read_csv(file)
    snowparkDf=session.write_pandas(file_df,file.name,auto_create_table = True, overwrite=True)
    return snowparkDf

st.header("thriftbooks data uploader")
file = st.file_uploader("Drop your CSV here", type={"csv"})
if file is not None:
    df= loadInferAndPersist(file)
    st.subheader("Great, your data has been uploaded to Snowflake!")
    
    with st.expander("Technical information"):
        
        u.describeSnowparkDF(df)
        st.write("Data loaded to Snowflake:")
        st.dataframe(df)

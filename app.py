import streamlit as st
import os
from snowflake.snowpark import Session

MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]

# ... (mantén todos los prompts y funciones igual)

def create_snowflake_session():
    private_key = os.environ["SNOWFLAKE_PRIVATE_KEY"].replace("\\n", "\n").encode()
    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": private_key,
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA")
    }
    return Session.builder.configs(connection_parameters).create()

if "snowflake_session" not in st.session_state:
    st.session_state.snowflake_session = create_snowflake_session()

session = st.session_state.snowflake_session

# Ya no uses Root(), navega solo por SQL con session
# ...
# main() y resto igual
main()

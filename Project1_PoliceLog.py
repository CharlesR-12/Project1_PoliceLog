#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# 📌 SECURECHECK - POLICE TRAFFIC LOGS | DASHBOARD PROJECT
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# ============================================================
# 📦 Import necessary libraries
# ============================================================
import pandas as pd
import streamlit as st
import psycopg2
from psycopg2 import errors
import plotly.express as px
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 01  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-01 -- 📥 Load and clean CSV data
# ============================================================
file_path = r"E:\Guvi_Class\Traffic_Data_Log_Project.csv"

try:
    Traffic = pd.read_csv(file_path)
    Traffic['stop_date'] = pd.to_datetime(Traffic['stop_date'], errors='coerce')
    Traffic['stop_time'] = pd.to_datetime(Traffic['stop_time'], format='%H:%M:%S', errors='coerce').dt.time
    print("✅ DATA CLEANING DONE")
except FileNotFoundError:
    st.error(f"❌ File not found at path: {file_path}")
    st.stop()
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 02  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-02 --  🗄️ Define PostgreSQL connection parameters
# ============================================================
host = "localhost"
port = 5432
username = "postgres"
password = "123456"
database = "Charles_Project_01"

# ============================================================
# Step-03 -- 🛠️ Create database connection
# ============================================================
def create_connection():
    try:
        conn = psycopg2.connect(
            host=host, port=port, user=username, password=password, database=database
        )
        print("✅ Connected to database.")
        return conn
    except Exception as e:
        print(f"❌ Connection error: {e}")
        st.error(f"Database Connection error: {e}")
        return None

# ============================================================
# Step-04 -- 🗄️ Create database if not exists
# ============================================================
def create_database():
    try:
        with psycopg2.connect(
            dbname="postgres", user=username, password=password, host=host, port=port
        ) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                try:
                    cursor.execute(f"CREATE DATABASE {database}")
                    print("✅ Database Created Successfully.")
                except errors.DuplicateDatabase:
                    print(f"⚠️ Database '{database}' already exists.")
    except Exception as e:
        print(f"❌ Error creating database: {e}")
# ===========================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 03  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-05 -- 📄 Create 'Project01_Traffic' table
# ============================================================
def create_traffic_table():
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS Project01_Traffic (
                        id SERIAL PRIMARY KEY,
                        country_name VARCHAR(100),
                        stop_date DATE,
                        driver_age INTEGER,
                        driver_gender VARCHAR(10),
                        driver_race VARCHAR(50),
                        vehicle_number VARCHAR(50),
                        is_arrested BOOLEAN,
                        search_conducted BOOLEAN,
                        drugs_related_stop BOOLEAN,
                        violation VARCHAR(255),
                        violation_raw VARCHAR(255),
                        stop_duration VARCHAR(50),
                        stop_outcome VARCHAR(255)
                    );
                """)
                conn.commit()
                print("✅ Traffic table created.")
        except Exception as e:
            print(f"❌ Error creating table: {e}")
        finally:
            conn.close()

# ============================================================
# Step-06 -- 🧹 Clean table before inserting new data
# ============================================================

def truncate_traffic_table():
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE Project01_Traffic;")
                conn.commit()
                print("✅ Existing data truncated.")
        except Exception as e:
            print(f"❌ Error truncating table: {e}")
        finally:
            conn.close()
# ===========================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 05  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-07 -- 📤 Insert data into the table
# ============================================================
def insert_traffic_data(df):
    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO Project01_Traffic (
                        country_name, stop_date, driver_age, driver_gender, driver_race,
                        vehicle_number, is_arrested, search_conducted, drugs_related_stop,
                        violation, violation_raw, stop_duration, stop_outcome
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                data_tuples = list(df[[ 
                    'country_name', 'stop_date', 'driver_age', 'driver_gender', 'driver_race',
                    'vehicle_number', 'is_arrested', 'search_conducted', 'drugs_related_stop',
                    'violation', 'violation_raw', 'stop_duration', 'stop_outcome'
                ]].itertuples(index=False, name=None))
                cursor.executemany(insert_query, data_tuples)
                conn.commit()
                print(f"✅ Inserted {cursor.rowcount} rows.")
        except Exception as e:
            print(f"❌ Error inserting data: {e}")
        finally:
            conn.close()
# ===========================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 06  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-08 -- 📥 Fetch data from database
# ============================================================

def fetch_data(query):
    conn = create_connection()
    if conn is None:
        st.error("❌ No database connection.")
        return pd.DataFrame()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=colnames)
    except Exception as e:
        st.error(f"❌ Query error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 07  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-09 -- 📦 STREAMLIT APP PAGE PRIVIEW
# ============================================================

st.set_page_config(page_title="POLICE TRAFFIC DASHBOARD", layout="wide", page_icon="🌎");st.markdown('<h2 style="color:gray; font-weight:bold; font-style:italic; font-size:25px;', unsafe_allow_html=True)

st.markdown('<h1 style="text-align:center; color:red; font-weight:bold; font-size:40px;">👮🏻‍♂️🚨 POLICE SECURE CHECK DASHBOARD 🚨👮🏻‍♂️</h1>', unsafe_allow_html=True)

st.markdown('<h2 style="color:blue; font-weight:bold; font-style:italic; font-size:30px;">🚙🏍️ Police Vehicle Checking Logs 🏍️🚙</h2>', unsafe_allow_html=True)

query = "SELECT * FROM Project01_Traffic"
data = fetch_data(query)

st.dataframe(data, use_container_width=True)

if not data.empty and 'violation' in data.columns:
    st.success("✅ Data loaded successfully!")
else:
    st.warning("⚠️ No data found or 'violation' column missing.")

# ============================================================
# Step-10 -- 📊 Dashboard Columns Guidelines Creation (Column Tab-1 to Column Tab-5)
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 08  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-11 -- 📊 Dashboard Tabs
# ============================================================

st.markdown('<p style="font-size:30px; text-align:left; font-style:italic; font-weight:bold; color:#0000FF;">🚦Traffic Logs Dashboard Tabs🗃️</p>', unsafe_allow_html=True)

st.markdown(
    """
    <style>
    /* Target tab labels */
    div[data-baseweb="tab"] button {
        font-size: 20px !important;       /* Increase font size */
        font-weight: 700 !important;      /* Make text bold */
        color: #003366 !important;        /* Change text color */
        font-family: 'Poppins', sans-serif !important; /* Optional font family */
    }

    /* Optional: Change color of active tab label */
    div[data-baseweb="tab"] button[aria-selected="true"] {
        color: #005999 !important;
        font-weight: 800 !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 09  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-12 -- 📊 Dashboard Tabs Creation (Tab-1 to Tab-5)
# ============================================================

# Create Tabs

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🌍 Country", "🚓 Violation", "📊 Outcome", "🚨 Arrest/Warning", "🧑 Gender"
])

debug = False  # Set True to debug columns
if debug:
    st.write("Available columns in data:", data.columns)

# ============================================================
# Step-13 -- Tab Query Creation
# ============================================================

# 📊 Tab 1: Top 5 Country
with tab1:
    if not data.empty and 'country_name' in data.columns:
        c_data = data['country_name'].value_counts().nlargest(5).reset_index()
        c_data.columns = ['Country Name', 'Count']
        fig_country_tab1 = px.bar(
            c_data, x='Country Name', y='Count',
            title="Top Countries", color='Country Name',
            width=700, height=400
        )
        st.plotly_chart(fig_country_tab1, use_container_width=False)
    else:
        st.warning("❌ No data for 'country_name'.")

# 📊 Tab 2: Violation Data
with tab2:
    if not data.empty and 'violation' in data.columns:
        v_data = data['violation'].value_counts().reset_index()
        v_data.columns = ['Violation', 'Count']
        fig_violation_tab2 = px.bar(
            v_data, x='Violation', y='Count',
            title="Violation Data", color='Violation',
            width=700, height=400
        )
        st.plotly_chart(fig_violation_tab2, use_container_width=False)
    else:
        st.warning("❌ No data for 'violation'.")

# 📊 Tab 3: Stop Outcome Data
with tab3:
    if not data.empty and 'stop_outcome' in data.columns:
        so_data = data['stop_outcome'].value_counts().reset_index()
        so_data.columns = ['Stop Outcome', 'Count']
        fig_stop_outcome_tab3 = px.bar(
            so_data, x='Stop Outcome', y='Count',
            title="Stop Outcome Data", color='Stop Outcome',
            width=700, height=400
        )
        st.plotly_chart(fig_stop_outcome_tab3, use_container_width=False)
    else:
        st.warning("❌ No data for 'stop_outcome'.")

# 📊 Tab 4: Arrest vs Warning Data
with tab4:
    if not data.empty and 'is_arrested' in data.columns:
        arrest_data = data['is_arrested'].value_counts().reset_index()
        arrest_data.columns = ['Is Arrested', 'Count']
        fig_arrest_tab4 = px.pie(
            arrest_data, names='Is Arrested', values='Count',
            title="Arrest vs Warning Data", hole=0.4,
            width=700, height=400
        )
        st.plotly_chart(fig_arrest_tab4, use_container_width=False)
    else:
        st.warning("❌ No data for 'is_arrested'.")

# 📊 Tab 5: Gender Distribution
with tab5:
    if not data.empty and 'driver_gender' in data.columns:
        g_data = data['driver_gender'].value_counts().reset_index()
        g_data.columns = ['Gender', 'Count']
        fig_gender_tab5 = px.pie(
            g_data, names='Gender', values='Count',
            title="Gender Distribution", hole=0.4,
            width=700, height=400
        )
        st.plotly_chart(fig_gender_tab5, use_container_width=False)
    else:
        st.warning("❌ No data for 'driver_gender'.")

# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 10  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-14 -- Columns Creation
# ============================================================

st.markdown('<p style="font-size:30px; text-align:left; font-style:italic; font-weight:bold; color:#0000FF;">🎯 Guidelines 📈</p>', unsafe_allow_html=True)

st.markdown("""
<style>
/* Box Container for General Sections */
.box-container {
    background: linear-gradient(135deg, #f9f9f9, #e0e0e0);
    padding: 20px;
    border-radius: 15px;
    box-shadow: 0 4px 10px rgba(0,0,0,0.1);
    margin-bottom: 20px;
}

/* Outline Box for Metric Display */
.outline-box {
    border: 4px solid #0078FF;
    border-radius: 12px;
    padding: 20px;
    background-color: #fdfdfd;
    box-shadow: 0 2px 6px rgba(0,0,0,0.05);
    margin-bottom: 20px;
}
</style>
""", unsafe_allow_html=True)


col1, col2, col3, col4, col5 = st.columns(5)

# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 11  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-15 -- Columns -- TABS -- Creation
# ============================================================

# Total Police Stops
with col1:
    total_stops = data.shape[0]
    st.markdown(f"""
        <div class="outline-box" style="text-align:center">
            <h4 style="color:#0078FF; font-weight:bold; font-size:22px;">Total Police Stops</h4>
            <h2 style="font-weight:bold; color:#333333;">{total_stops}</h2>
        </div>
        """, unsafe_allow_html=True)

# ============================================================

# Total Arrests
with col2:
    arrests = data[data['stop_outcome'].str.contains("arrest", case=False, na=False)].shape[0]
    st.markdown(f"""
        <div class="outline-box" style="text-align:center">
            <h4 style="color:#FF5733; font-weight:bold; font-size:22px;">Total Arrests</h4>
            <h2 style="font-weight:bold; color:#333333;">{arrests}</h2>
        </div>
        """, unsafe_allow_html=True)

# ============================================================

# Total Warnings
with col3:
    warnings = data[data['stop_outcome'].str.contains("warning", case=False, na=False)].shape[0]
    st.markdown(f"""
        <div class="outline-box" style="text-align:center">
            <h4 style="color:#FFC300; font-weight:bold; font-size:22px;">Total Warnings</h4>
            <h2 style="font-weight:bold; color:#333333;">{warnings}</h2>
        </div>
        """, unsafe_allow_html=True)

# ============================================================

# Drug Related Stops
with col4:
    drug_related_stops = data[data['violation'].str.contains("drug", case=False, na=False)].shape[0]
    st.markdown(f"""
        <div class="outline-box" style="text-align:center">
            <h4 style="color:#28B463; font-weight:bold; font-size:22px;">Drug Related Stops</h4>
            <h2 style="font-weight:bold; color:#333333;">{drug_related_stops}</h2>
        </div>
        """, unsafe_allow_html=True)

# ============================================================

# Unique Violation Types
with col5:
    unique_violations = data['violation'].nunique()
    st.markdown(f"""
        <div class="outline-box" style="text-align:center">
            <h4 style="color:#9B59B6; font-weight:bold; font-size:22px;">Violation Types</h4>
            <h2 style="font-weight:bold; color:#333333;">{unique_violations}</h2>
        </div>
        """, unsafe_allow_html=True)

# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 12  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-16 -- 📊 Advance Insights -- Questions Header
# ============================================================

st.markdown('<p style="font-size:30px; text-align:left; font-style:italic; font-weight:bold; color:#0000FF;">🎯 Advance Insights 📈</p>', unsafe_allow_html=True)

# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 13  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-17 -- 📊 Advance Insights -- Selectbox with Labeled Questions (Q-1 to Q-7)
# ============================================================

col1, col2 = st.columns(2)

with col1:
    selected_query = st.selectbox(
        "",
        [
            "Q-1: Total Number of Police Stops",
            "Q-2: Count of Stops by Violation Types",
            "Q-3: Number of Arrests vs. Warnings",
            "Q-4: Average Age of Drivers Stopped",
            "Q-5: Top 5 Most Frequent Search Types",
            "Q-6: Count of Stops by Gender",
            "Q-7: Most Common Violation for Arrests"
        ],
        key="query_selectbox",
        label_visibility="collapsed"
    )
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 13  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-18 -- 📊 Advance Insights -- Selectbox with Labeled Questions (Q-1 to Q-7)
# ============================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("👇 Show Result 👇"):
        if selected_query == "Q-1: Total Number of Police Stops":
            total_stops = Traffic.shape[0]
            st.success(f"✅ Total Number of Police Stops: **{total_stops}**")

        elif selected_query == "Q-2: Count of Stops by Violation Types":
            result = Traffic['violation'].value_counts().reset_index()
            result.columns = ['Violation Type', 'Total Stops']
            st.dataframe(result)

        elif selected_query == "Q-3: Number of Arrests vs. Warnings":
            Traffic['outcome'] = Traffic['is_arrested'].apply(lambda x: 'Arrested' if x else 'Warning/Other')
            result = Traffic['outcome'].value_counts().reset_index()
            result.columns = ['Outcome', 'Total Count']
            st.dataframe(result)

        elif selected_query == "Q-4: Average Age of Drivers Stopped":
            avg_age = round(Traffic['driver_age'].mean(), 1)
            st.success(f"🚗 Average Age of Drivers Stopped: **{avg_age} years**")

        elif selected_query == "Q-5: Top 5 Most Frequent Search Types":
            if 'search_type' in Traffic.columns:
                result = (Traffic[Traffic['search_type'].notnull() & (Traffic['search_type'] != '')]
                        ['search_type'].value_counts().head(5)
                        .reset_index())
                result.columns = ['Search Type', 'Total Searches']
                st.dataframe(result)
            else:
                st.warning("No 'search_type' column in data.")

        elif selected_query == "Q-6: Count of Stops by Gender":
            result = Traffic['driver_gender'].value_counts().reset_index()
            result.columns = ['Gender', 'Total Stops']
            st.dataframe(result)

        elif selected_query == "Q-7: Most Common Violation for Arrests":
            result = (Traffic[Traffic['is_arrested'] == 1]
                    ['violation'].value_counts().head(1)
                    .reset_index())
            result.columns = ['Violation', 'Total Arrests']
            st.dataframe(result)

    else:
        st.warning("Please select a valid question from the list.")
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 14  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-19 --  📊 Project Question -- Selectbox with Questions (Q-1 to Q-14)
# ============================================================

st.markdown('<p style="font-size:30px; text-align:left; font-style:italic; font-weight:bold; color:#0000FF;">📌 Project Question Q-1 to Q-14 ✍</p>', unsafe_allow_html=True)

# Database connection function
def fetch_data(query):
    conn = psycopg2.connect(
        host="localhost",
        database="Charles_Project_01",
        user="postgres",
        password="123456"
    )
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    return df

# ============================================================
# Step-20 -- Project Questions (Q-1 to Q-14)
# ============================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    QUESTION_LIST = [
        "What are the top 10 vehicle_Number involved in drug-related stops?",
        "Which vehicles were most frequently searched?",
        "Driver age group with highest arrest rate?",
        "Gender distribution of drivers stopped in each country?",
        "Race and gender combination with highest search rate?",
        "Time of day with most traffic stops?",
        "Average stop duration for different violations?",
        "Are stops during night more likely to lead to arrests?",
        "Violations most associated with searches or arrests?",
        "Violations most common among younger drivers (<25)?",
        "Violation rarely resulting in search or arrest?",
        "Countries with highest rate of drug-related stops?",
        "Arrest rate by country and violation?",
        "Country with most stops with search conducted?"
    ]
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 15  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-21 -- Mapping Query (Q-1 to Q-14)
# ============================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    queries = {
    QUESTION_LIST[0]: """SELECT vehicle_number, COUNT(*) AS drug_stops FROM "Project01_Traffic" WHERE drugs_related_stop = TRUE GROUP BY vehicle_number ORDER BY drug_stops DESC LIMIT 10;""",
    QUESTION_LIST[1]: """SELECT vehicle_number, COUNT(*) AS total_searches FROM "Project01_Traffic" WHERE search_conducted = TRUE GROUP BY vehicle_number ORDER BY total_searches DESC;""",
    QUESTION_LIST[2]: """SELECT CASE WHEN driver_age < 20 THEN 'Under 20' WHEN driver_age BETWEEN 21 AND 35 THEN '21-35' WHEN driver_age BETWEEN 36 AND 50 THEN '36-50' WHEN driver_age BETWEEN 51 AND 65 THEN '51-65' ELSE '65+' END AS age_group, COUNT(*) AS total_stops, SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests, ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent FROM "Project01_Traffic" GROUP BY age_group ORDER BY arrest_rate_percent DESC;""",
    QUESTION_LIST[3]: """SELECT driver_gender, country_name, COUNT(*) AS drivers_stopped FROM "Project01_Traffic" GROUP BY driver_gender, country_name ORDER BY driver_gender, country_name;""",
    QUESTION_LIST[4]: """SELECT driver_gender, driver_race, COUNT(*) AS drivers_stopped FROM "Project01_Traffic" GROUP BY driver_gender, driver_race ORDER BY drivers_stopped DESC LIMIT 1;""",
    QUESTION_LIST[5]: """SELECT stop_time AS hour_of_day, COUNT(*) AS Most_Traffice_Stop FROM "Project01_Traffic" GROUP BY hour_of_day ORDER BY Most_Traffice_Stop DESC;""",
    QUESTION_LIST[6]: """SELECT violation_raw, AVG(CASE stop_duration WHEN '0-15 Min' THEN 5 WHEN '16-30 Min' THEN 10 WHEN '30+ Min' THEN 20 END) AS avg_stop_duration_estimate FROM "Project01_Traffic" GROUP BY violation_raw ORDER BY avg_stop_duration_estimate DESC;""",
    QUESTION_LIST[7]: """SELECT CASE WHEN stop_time BETWEEN '04:00:00' AND '11:59:59' THEN 'Morning' WHEN stop_time BETWEEN '12:00:00' AND '16:59:59' THEN 'Afternoon' WHEN stop_time BETWEEN '17:00:00' AND '21:59:59' THEN 'Evening' ELSE 'Night' END AS time_of_day, COUNT(*) AS total_stops, SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests, ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent FROM "Project01_Traffic" GROUP BY time_of_day ORDER BY total_stops DESC;""",
    QUESTION_LIST[8]: """SELECT violation, COUNT(*) AS total_stops, SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS searches, SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrests, ROUND(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate_percent, ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent FROM "Project01_Traffic" GROUP BY violation ORDER BY search_rate_percent DESC, arrest_rate_percent DESC;""",
    QUESTION_LIST[9]: """SELECT violation, COUNT(*) AS total_stops_under_25 FROM "Project01_Traffic" WHERE driver_age < 25 GROUP BY violation ORDER BY total_stops_under_25 DESC;""",
    QUESTION_LIST[10]: """SELECT violation, COUNT(*) AS total_stops, SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches, SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests, ROUND(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate_percent, ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent FROM "Project01_Traffic" GROUP BY violation ORDER BY search_rate_percent ASC, arrest_rate_percent ASC LIMIT 5;""",
    QUESTION_LIST[11]: """SELECT country_name, COUNT(*) AS total_stops, SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) AS drug_stops, ROUND(SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS drug_stop_rate_percent FROM "Project01_Traffic" GROUP BY country_name ORDER BY drug_stop_rate_percent DESC;""",
    QUESTION_LIST[12]: """SELECT country_name, violation, COUNT(*) AS total_stops, SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests, ROUND(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate_percent FROM "Project01_Traffic" GROUP BY country_name, violation ORDER BY arrest_rate_percent DESC, total_stops DESC;""",
    QUESTION_LIST[13]: """SELECT country_name, COUNT(*) AS total_search_stops FROM "Project01_Traffic" WHERE search_conducted = TRUE GROUP BY country_name ORDER BY total_search_stops DESC;"""
}

# ============================================================
# Step-22 -- Show Result Button - 
# ============================================================
selected_question = st.selectbox("", QUESTION_LIST)

if st.button("👇 Show Result 👇👇"):
    sql_query = queries.get(selected_question)
    if sql_query:
        result_df = fetch_data(sql_query)
        if not result_df.empty:
            st.success("✅ Query executed successfully!")
            st.dataframe(result_df)
        else:
            st.warning("⚠️ No data returned for this query.")
    else:
        st.error("❌ Query not found for the selected question.")
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 16  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-23 -- 📊 Project Complex Question Labels for Dropdown
# ============================================================

st.markdown('<p style="font-size:30px; text-align:left; font-style:italic; font-weight:bold; color:#0000FF;">📌 Project Complex Question (Q-1 to Q-6) ✍</p>', unsafe_allow_html=True)

QUESTION_LIST = [
        "Complex Question-1. Yearly Breakdown of Stops and Arrests by Country (Using Subquery and Window Functions)",
        "Complex Question-2. Driver Violation Trends Based on Age and Race (Join with Subquery)",
        "Complex Question-3. Time Period Analysis of Stops (Joining with Date Functions), Number of Stops by Year, Month, Hour of the Day",
        "Complex Question-4. Violations with High Search and Arrest Rates (Window Function)",
        "Complex Question-5. Driver Demographics by Country (Age, Gender, and Race)",
        "Complex Question-6. Top 5 Violations with Highest Arrest Rates"
    ]

# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 17  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-24 -- Mapping Queries to Each Question Label (Q1 to Q6)
# ============================================================

query = {

    # Complex Question-1
    QUESTION_LIST[0]: """
    WITH yearly_data AS (
        SELECT country_name, EXTRACT(YEAR FROM stop_date::date) AS year,
               COUNT(*) AS total_stops,
               SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
        FROM "Project01_Traffic"
        GROUP BY country_name, EXTRACT(YEAR FROM stop_date::date)
    )
    SELECT country_name, year, total_stops, total_arrests,
           ROUND((total_arrests::decimal / NULLIF(total_stops, 0)) * 100, 2) AS arrest_rate_percent,
           SUM(total_stops) OVER (PARTITION BY country_name ORDER BY year) AS cumulative_stops,
           SUM(total_arrests) OVER (PARTITION BY country_name ORDER BY year) AS cumulative_arrests
    FROM yearly_data
    ORDER BY country_name, year;
    """,

    # Complex Question-2
    QUESTION_LIST[1]: """
    WITH violation_summary AS (
        SELECT driver_age, driver_race, violation, COUNT(*) AS total_stops
        FROM "Project01_Traffic"
        GROUP BY driver_age, driver_race, violation
    )
    SELECT v.driver_age, v.driver_race, v.violation, v.total_stops,
           ROUND(v.total_stops::decimal / NULLIF(t.total_stops_by_age_race, 0) * 100, 2) AS stop_percentage_within_group
    FROM violation_summary v
    JOIN (
        SELECT driver_age, driver_race, COUNT(*) AS total_stops_by_age_race
        FROM "Project01_Traffic"
        GROUP BY driver_age, driver_race
    ) t
    ON v.driver_age = t.driver_age AND v.driver_race = t.driver_race
    ORDER BY v.driver_race, v.driver_age, v.total_stops DESC;
    """,

    # Complex Question-3
    QUESTION_LIST[2]: """
    SELECT EXTRACT(YEAR FROM stop_date::date) AS year,
           EXTRACT(MONTH FROM stop_date::date) AS month,
           EXTRACT(HOUR FROM stop_time::time) AS hour,
           COUNT(*) AS total_stops
    FROM "Project01_Traffic"
    GROUP BY EXTRACT(YEAR FROM stop_date::date),
             EXTRACT(MONTH FROM stop_date::date),
             EXTRACT(HOUR FROM stop_time::time)
    ORDER BY year, month, hour;
    """,

    # Complex Question-4
    QUESTION_LIST[3]: """
    WITH violation_stats AS (
        SELECT violation, COUNT(*) AS total_stops,
               SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
               SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
        FROM "Project01_Traffic"
        GROUP BY violation
    )
    SELECT violation, total_stops, total_searches, total_arrests,
           ROUND((total_searches::decimal / NULLIF(total_stops, 0)) * 100, 2) AS search_rate_percent,
           ROUND((total_arrests::decimal / NULLIF(total_stops, 0)) * 100, 2) AS arrest_rate_percent,
           RANK() OVER (ORDER BY (total_searches::decimal / NULLIF(total_stops, 0)) DESC) AS search_rate_rank,
           RANK() OVER (ORDER BY (total_arrests::decimal / NULLIF(total_stops, 0)) DESC) AS arrest_rate_rank
    FROM violation_stats
    ORDER BY search_rate_rank, arrest_rate_rank;
    """,

    # Complex Question-5
    QUESTION_LIST[4]: """
    SELECT country_name, driver_gender, driver_race, driver_age, COUNT(*) AS total_stops
    FROM "Project01_Traffic"
    GROUP BY country_name, driver_gender, driver_race, driver_age
    ORDER BY country_name, driver_gender, driver_race, driver_age;
    """,

    # Complex Question-6
    QUESTION_LIST[5]: """
    SELECT violation, COUNT(*) AS total_stops,
           SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
           ROUND((SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END)::decimal / NULLIF(COUNT(*), 0)) * 100, 2) AS arrest_rate_percent
    FROM "Project01_Traffic"
    GROUP BY violation
    ORDER BY arrest_rate_percent DESC
    LIMIT 5;
    """
}
# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 18  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-24 -- Print a Query Example (Optional Testing)
# ============================================================

print(query[QUESTION_LIST[0]])

# ============================================================
# Step-25 -- Streamlit Interface for Selecting & Executing Query
# ============================================================

selected_question = st.selectbox("", QUESTION_LIST)

if st.button("👇 Show Result 👇👇👇"):
    sql_query = query.get(selected_question)
    
    if sql_query:
        result_df = fetch_data(sql_query)

        if not result_df.empty:
            st.success("✅ Query executed successfully!")
            st.dataframe(result_df)
        else:
            st.warning("⚠️ No data returned for this query.")
    else:
        st.error("❌ Query not found for the selected question.")

# ============================================================

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
# 📌📌 STAGE -- 17  📌📌
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# ============================================================
# Step-24 -- 📊 Predicted Outcome
# ============================================================

st.markdown('<p style="font-size:30px; text-align:left; font-style:italic; font-weight:bold; color:#0000FF;">📌 Predict Stop Outcome & Violation ✍</p>', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    data = pd.DataFrame({
        'stop_duration': ['0-15 Min', '16-30 Min', '30+ Min'],
        'driver_gender': ['Male', 'Female', 'Male'],
        'driver_age': [27, 35, 42],
        'search_conducted': [1, 0, 1],
        'drugs_related_stop': [0, 1, 0],
        'stop_outcome': ['Warning', 'Arrest', 'Citation'],
        'violation': ['Speeding', 'DUI', 'Seatbelt']
    })

# ============================================================
# Step-25 -- 📊 Predicted Stop Outcome Filter form inputs
# ============================================================

    with st.form("add_record_form"):
        stop_date = st.date_input("Stop Date")
        stop_time = st.time_input("Stop Time")
        country_name = st.text_input("Country Name")
        driver_gender = st.selectbox("Driver Gender", ["Male", "Female", "Other"])
        driver_age = st.number_input("Driver Age", min_value=15, max_value=120, value=27)
        driver_race = st.text_input("Driver Race")
        search_conducted = st.selectbox("Was a Search Conducted?", ["0", "1"])
        search_type = st.text_input("Search Type")
        drugs_related_stop = st.selectbox("Was it Drugs Related Stop?", ["0", "1"])
        stop_duration = st.selectbox("Stop Duration", data['stop_duration'].dropna().unique())
        vehicle_number = st.text_input("Vehicle Number")
        timestamp = pd.Timestamp.now()

        submitted = st.form_submit_button("✅ Estimate Stop Outcome & Violation")

    if submitted:
        search_conducted_val = int(search_conducted)
        drugs_related_stop_val = int(drugs_related_stop)

        filtered_data = data[
            (data['driver_gender'] == driver_gender) &
            (data['driver_age'] == driver_age) &
            (data['search_conducted'] == search_conducted_val) &
            (data['drugs_related_stop'] == drugs_related_stop_val) &
            (data['stop_duration'] == stop_duration)
        ]

        if not filtered_data.empty:
            predicted_outcome = filtered_data['stop_outcome'].mode()[0]
            predicted_violation = filtered_data['violation'].mode()[0]
        else:
            predicted_outcome = "Warning"
            predicted_violation = "Unknown Violation"

        search_text = "A search was conducted" if search_conducted_val else "No search was conducted"
        drug_text = "was drug-related" if drugs_related_stop_val else "was not drug-related"
        
        st.markdown(
            '<p style="font-size:30px; text-align:left; font-weight:bold; color:#0000FF;">👮🚓  Prediction Summary 🔎✍</p>',
            unsafe_allow_html=True
        )
# ============================================================
# Step-26 -- 📊 Predicted Stop Outcome Markdown
# ============================================================     
        st.markdown(f"""
        - **Predicted Violation:** `{predicted_violation}`
        - **Predicted Stop Outcome:** `{predicted_outcome}`

        A **{driver_age}**-year-old **{driver_gender}** driver in **{country_name}** was stopped at **{stop_time.strftime('%I:%M %p')}** on **{stop_date}**.  
        {search_text}, and the stop {drug_text}.  
        **Stop Duration:** `{stop_duration}`  
        **Vehicle Number:** `{vehicle_number}`
        """)

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####

# 📌📌 END -- SECURECHECK - POLICE TRAFFIC LOGS | DASHBOARD PROJECT  📌📌

#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####
#####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####  #####aa
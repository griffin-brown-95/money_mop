import streamlit as st
import pandas as pd
import os

# --- Config ---
DATA_PATH = 'data/processed/'

# --- Load Most Recent Enriched Data ---
def load_latest_data(path):
    files = sorted([f for f in os.listdir(path) if f.endswith('.csv')], reverse=True)
    if not files:
        st.error("No data files found.")
        return pd.DataFrame()
    return pd.read_csv(os.path.join(path, files[0]), parse_dates=['date'])

df = load_latest_data(DATA_PATH)

if df.empty:
    st.stop()

# --- Sidebar Filters ---
st.sidebar.title("Filters")
company_filter = st.sidebar.multiselect("Company", df['company'].unique(), default=list(df['company'].unique()))
department_filter = st.sidebar.multiselect("Department", df['department'].unique(), default=list(df['department'].unique()))
date_range = st.sidebar.date_input("Date Range", [df['date'].min(), df['date'].max()])

# --- Apply Filters ---
df = df[
    (df['company'].isin(company_filter)) &
    (df['department'].isin(department_filter)) &
    (df['date'] >= pd.to_datetime(date_range[0])) &
    (df['date'] <= pd.to_datetime(date_range[1]))
]

# --- KPIs ---
total_spend = df['amount'].sum()
num_transactions = len(df)
num_violations = df['is_policy_violation'].sum()
total_savings = df['potential_savings'].sum()


col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Spend", f"${total_spend:,.2f}")
col2.metric("Transactions", f"{num_transactions}")
col3.metric("Violations", f"{num_violations}")
col4.metric("Potential Savings", f"${total_savings:,.2f}")

# --- Charts ---
st.subheader("Spend Over Time")
df_by_date = df.groupby('date')['amount'].sum().reset_index()
st.line_chart(df_by_date.rename(columns={"date": "index"}).set_index("index"))

st.subheader("Violations by Department")
violation_counts = df[df['is_policy_violation']].groupby('department').size()
st.bar_chart(violation_counts)

st.subheader("Top Merchants by Spend")
top_merchants = df.groupby('merchant')['amount'].sum().nlargest(10)
st.bar_chart(top_merchants)

# --- Raw Data Preview ---
with st.expander("View Raw Data"):
    st.dataframe(df)

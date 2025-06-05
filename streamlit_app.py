import streamlit as st
import pandas as pd
from io import BytesIO

st.title("ZIP Code Checker: Claimed vs Available")

# File upload
buyer_file = st.file_uploader("Upload Buyer ZIP File (with 'Buyer Name' and 'Zip Code')", type=["xlsx"])
new_zip_file = st.file_uploader("Upload ZIP Codes to Check (with 'Zip Code')", type=["xlsx"])

# Helper: Convert DataFrame to Excel bytes
def to_excel_bytes(df):
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    return output.getvalue()

if buyer_file and new_zip_file:
    # Load data
    df_buyers = pd.read_excel(buyer_file)
    df_new = pd.read_excel(new_zip_file)

    # Extract root buyer name (first word)
    df_buyers['Root Buyer'] = df_buyers['Buyer Name'].astype(str).str.split().str[0]
    df_new = df_new.rename(columns=lambda x: x.strip())

    # Create ZIP → buyer mapping
    zip_to_buyers = (
        df_buyers.groupby('Zip Code')['Root Buyer']
        .unique()
        .reset_index()
        .rename(columns={'Root Buyer': 'Buyers'})
    )
    zip_to_buyers['Buyers'] = zip_to_buyers['Buyers'].apply(lambda x: ", ".join(sorted(set(x))))

    # Merge new ZIP codes with buyer info
    df_result = df_new.merge(zip_to_buyers, on='Zip Code', how='left')

    # Separate free and claimed ZIPs
    free_zips = df_result[df_result['Buyers'].isna()]
    claimed_zips = df_result[df_result['Buyers'].notna()]

    # Filter by buyer name
    all_buyers = sorted(set(
        sum(claimed_zips['Buyers'].dropna().str.split(',').tolist(), [])
    ))
    selected_buyer = st.selectbox("Filter claimed ZIPs by buyer", ["(Show All)"] + all_buyers)

    if selected_buyer != "(Show All)":
        claimed_filtered = claimed_zips[claimed_zips['Buyers'].str.contains(selected_buyer)]
    else:
        claimed_filtered = claimed_zips

    # Show data
    st.subheader("Full ZIP Code Match Table")
    st.dataframe(df_result)

    st.download_button("Download Full ZIP Table", to_excel_bytes(df_result), file_name="zip_match_full.xlsx")

    st.subheader("Available ZIP Codes")
    st.dataframe(free_zips)
    st.download_button("Download Free ZIPs", to_excel_bytes(free_zips), file_name="free_zips.xlsx")

    st.subheader("Claimed ZIP Codes" + (f" (Filtered by: {selected_buyer})" if selected_buyer != "(Show All)" else ""))
    st.dataframe(claimed_filtered)
    st.download_button("Download Claimed ZIPs", to_excel_bytes(claimed_filtered), file_name="claimed_zips_filtered.xlsx")

import streamlit as st
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import os
from pathlib import Path
import time
import tempfile
import io

class ContractZipAnalyzer:
    def __init__(self):
        # Initialize data storage in session state
        if 'contract_zip_map' not in st.session_state:
            st.session_state.contract_zip_map = defaultdict(set)
            st.session_state.zip_contract_map = defaultdict(set)
            st.session_state.contract_info = {}
            st.session_state.main_file_loaded = False
            st.session_state.logs = []
            st.session_state.output_files = {}
            st.session_state.temp_dir = tempfile.mkdtemp(prefix="streamlit_detail_")
    
    def log_message(self, message):
        """Add message to logs"""
        timestamp = time.strftime('%H:%M:%S')
        st.session_state.logs.append(f"{timestamp} - {message}")
        st.write(f"{timestamp} - {message}")
    
    def process_chunk(self, chunk):
        """Process a single chunk of data"""
        for _, row in chunk.iterrows():
            contract = row['Contract Name']
            zip_code = str(row['Zip Code']).strip()
            state_id = str(row.get('State ID', '')).strip()
            
            # Skip invalid zip codes
            if not zip_code or zip_code == 'nan':
                continue
                
            # Store contract-zip mapping
            st.session_state.contract_zip_map[contract].add(zip_code)
            st.session_state.zip_contract_map[zip_code].add(contract)
            
            # Store contract info (only first occurrence)
            if contract not in st.session_state.contract_info:
                st.session_state.contract_info[contract] = {
                    'buyer_name': row.get('Buyer Name', ''),
                    'buyer_id': row.get('Buyer ID', ''),
                    'vertical_name': row.get('Vertical Name', ''),
                    'contract_status': row.get('Contract Status', ''),
                    'zip_states': {}
                }
            
            # Store state info for each zip
            st.session_state.contract_info[contract]['zip_states'][zip_code] = state_id
    
    def load_main_file(self, main_file, chunk_size, filter_active):
        """Load and process the main contracts file"""
        try:
            self.log_message("Starting to load main contracts file...")
            
            # Reset data structures
            st.session_state.contract_zip_map.clear()
            st.session_state.zip_contract_map.clear()
            st.session_state.contract_info.clear()
            st.session_state.output_files.clear()
            
            total_rows = 0
            active_rows = 0
            chunk_count = 0
            
            # Read CSV from uploaded file
            main_df = pd.read_csv(main_file, chunksize=chunk_size, low_memory=False)
            
            for chunk in main_df:
                chunk_count += 1
                total_rows += len(chunk)
                
                # Clean column names
                chunk.columns = chunk.columns.str.strip()
                
                # Filter active contracts and buyers if specified
                if filter_active:
                    if 'Contract Status' in chunk.columns:
                        chunk = chunk[chunk['Contract Status'].str.lower() == 'active']
                    if 'Buyer Status' in chunk.columns:
                        chunk = chunk[chunk['Buyer Status'].str.lower() == 'active']
                
                # Remove rows with missing contract name or zip code
                chunk = chunk.dropna(subset=['Contract Name', 'Zip Code'])
                active_rows += len(chunk)
                
                if chunk.empty:
                    continue
                
                # Process chunk
                self.process_chunk(chunk)
                
                if chunk_count % 10 == 0:
                    self.log_message(f"Processed {chunk_count} chunks...")
            
            total_contracts = len(st.session_state.contract_zip_map)
            total_zips = len(st.session_state.zip_contract_map)
            
            self.log_message(f"Main file loaded successfully!")
            self.log_message(f"Total rows: {total_rows:,}, Active rows: {active_rows:,}")
            self.log_message(f"Contracts: {total_contracts:,}, Unique ZIP codes: {total_zips:,}")
            
            st.session_state.main_file_loaded = True
            
        except Exception as e:
            self.log_message(f"Error loading main file: {str(e)}")
            st.error(f"Failed to load main file: {str(e)}")
    
    def analyze_main_data(self):
        """Generate the main analysis reports"""
        try:
            self.log_message("Starting main analysis...")
            
            output_dir = st.session_state.temp_dir
            Path(output_dir).mkdir(exist_ok=True)
            
            # Generate contract summary
            self.log_message("Generating contract summary...")
            summary_data = []
            
            for contract, zips in st.session_state.contract_zip_map.items():
                info = st.session_state.contract_info[contract]
                
                # Count overlaps
                overlap_count = 0
                overlapping_contracts = set()
                for zip_code in zips:
                    other_contracts = st.session_state.zip_contract_map[zip_code] - {contract}
                    overlap_count += len(other_contracts)
                    overlapping_contracts.update(other_contracts)
                
                summary_data.append({
                    'Contract Name': contract,
                    'Buyer Name': info['buyer_name'],
                    'Buyer ID': info['buyer_id'],
                    'Vertical Name': info['vertical_name'],
                    'Contract Status': info['contract_status'],
                    'Total ZIP Codes': len(zips),
                    'Total ZIP Matches': overlap_count,
                    'Unique Contracts Overlapping': len(overlapping_contracts)
                })
            
            summary_df = pd.DataFrame(summary_data).sort_values('Total ZIP Codes', ascending=False)
            
            # Generate active counts
            active_counts = {
                'Metric': ['Active Contracts', 'Active Buyers'],
                'Count': [
                    len(summary_df[summary_df['Contract Status'].str.lower() == 'active']),
                    len(summary_df[summary_df['Contract Status'].str.lower() == 'active']['Buyer Name'].unique())
                ]
            }
            active_counts_df = pd.DataFrame(active_counts)
            
            # Export contract summary and active counts
            summary_path = os.path.join(output_dir, "contract_summary.xlsx")
            with pd.ExcelWriter(summary_path) as writer:
                summary_df.to_excel(writer, sheet_name="Contract Summary", index=False)
                active_counts_df.to_excel(writer, sheet_name="Active Counts", index=False)
            self.log_message(f"Contract summary exported: {len(summary_df)} contracts")
            self.log_message(f"Active counts: {active_counts['Count'][0]} contracts, {active_counts['Count'][1]} buyers")
            st.session_state.output_files['contract_summary.xlsx'] = summary_path
            
            # Generate detailed matches view
            self.log_message("Generating detailed matches view...")
            detailed_data = []
            
            for contract, zips in st.session_state.contract_zip_map.items():
                contract_info = st.session_state.contract_info[contract]
                
                for zip_code in sorted(zips):
                    other_contracts = st.session_state.zip_contract_map[zip_code] - {contract}
                    match_string = ", ".join(sorted(other_contracts)) if other_contracts else ""
                    state_id = contract_info['zip_states'].get(zip_code, '')
                    
                    detailed_data.append({
                        'Contract Name': contract,
                        'Buyer Name': contract_info['buyer_name'],
                        'Buyer ID': contract_info['buyer_id'],
                        'Vertical Name': contract_info['vertical_name'],
                        'Contract Status': contract_info['contract_status'],
                        'State ID': state_id,
                        'Zip Code': zip_code,
                        'MATCH': match_string
                    })
            
            detailed_df = pd.DataFrame(detailed_data)
            
            # Generate contract match counts for detailed view
            contract_match_counts = Counter()
            for _, row in detailed_df.iterrows():
                if row['MATCH']:
                    matches = row['MATCH'].split(", ")
                    contract_match_counts[row['Contract Name']] += len(matches)
            
            match_counts_df = pd.DataFrame.from_dict(
                contract_match_counts, orient='index', columns=['Total ZIP Matches']
            ).reset_index().rename(columns={'index': 'Contract Name'})
            match_counts_df = match_counts_df.sort_values('Total ZIP Matches', ascending=False)
            
            # Export detailed matches, match counts, and active counts
            detailed_path = os.path.join(output_dir, "detailed_contract_zip_matches.xlsx")
            with pd.ExcelWriter(detailed_path) as writer:
                detailed_df.to_excel(writer, sheet_name="Detailed Matches", index=False)
                match_counts_df.to_excel(writer, sheet_name="Contract Match Counts", index=False)
                active_counts_df.to_excel(writer, sheet_name="Active Counts", index=False)
            self.log_message(f"Detailed matches exported: {len(detailed_df):,} rows")
            self.log_message(f"Contract match counts exported: {len(match_counts_df)} contracts")
            st.session_state.output_files['detailed_contract_zip_matches.xlsx'] = detailed_path
            
            # Log top 10 contracts by match count
            self.log_message("Top 10 contracts by ZIP matches:")
            for i, (contract, count) in enumerate(match_counts_df.head(10).itertuples(index=False, name=None)):
                self.log_message(f"{i+1}. {contract}: {count} matches")
            
            # Export individual contract sheets for contracts with matches
            self.log_message("Exporting individual contract sheets...")
            contracts_with_matches = summary_df[summary_df['Unique Contracts Overlapping'] > 0]['Contract Name'].tolist()
            
            # Split into multiple Excel files if too many contracts
            batch_size = 20
            for i in range(0, len(contracts_with_matches), batch_size):
                batch_contracts = contracts_with_matches[i:i+batch_size]
                batch_num = i // batch_size + 1
                
                batch_path = os.path.join(output_dir, f"contract_matches_batch_{batch_num}.xlsx")
                with pd.ExcelWriter(batch_path) as writer:
                    for contract in batch_contracts:
                        # Get all rows for this contract
                        contract_data = detailed_df[detailed_df['Contract Name'] == contract]
                        
                        if not contract_data.empty:
                            # Clean sheet name
                            sheet_name = contract.replace('/', '_').replace('\\', '_').replace('[', '').replace(']', '')[:31]
                            contract_data.to_excel(writer, sheet_name=sheet_name, index=False)
                
                self.log_message(f"Exported batch {batch_num} with {len(batch_contracts)} contracts")
                st.session_state.output_files[f"contract_matches_batch_{batch_num}.xlsx"] = batch_path
            
            self.log_message(f"Exported {len(contracts_with_matches)} individual contract sheets in {batch_num} batch file(s)")
            self.log_message("Main analysis completed successfully!")
            
        except Exception as e:
            self.log_message(f"Error in main analysis: {str(e)}")
            st.error(f"Analysis failed: {str(e)}")
    
    def analyze_new_zips(self, new_zip_file):
        """Analyze new ZIP codes against existing contracts"""
        try:
            self.log_message("Loading new ZIP codes file...")
            
            # Load new ZIP codes file
            new_zip_df = pd.read_csv(new_zip_file)
            new_zip_df.columns = new_zip_df.columns.str.strip()
            
            # Expected columns: 'Zip Code' or 'ZIP Code', optionally 'State'
            zip_col = None
            for col in new_zip_df.columns:
                if 'zip' in col.lower():
                    zip_col = col
                    break
            
            if zip_col is None:
                raise ValueError("No ZIP code column found. Expected column containing 'zip'")
            
            self.log_message(f"Found {len(new_zip_df)} new ZIP codes to analyze")
            
            # Analyze matches
            self.log_message("Matching new ZIP codes against active contracts...")
            
            match_results = []
            new_zips = new_zip_df[zip_col].astype(str).str.strip().unique()
            
            for zip_code in new_zips:
                if zip_code in st.session_state.zip_contract_map:
                    matching_contracts = st.session_state.zip_contract_map[zip_code]
                    
                    for contract in matching_contracts:
                        contract_info = st.session_state.contract_info[contract]
                        state_id = contract_info['zip_states'].get(zip_code, '')
                        
                        match_results.append({
                            'New ZIP Code': zip_code,
                            'State ID': state_id,
                            'Matching Contract': contract,
                            'Buyer Name': contract_info['buyer_name'],
                            'Buyer ID': contract_info['buyer_id'],
                            'Vertical Name': contract_info['vertical_name'],
                            'Contract Status': contract_info['contract_status'],
                            'Contract Total ZIP Codes': len(st.session_state.contract_zip_map[contract])
                        })
            
            if match_results:
                match_df = pd.DataFrame(match_results)
                
                # Generate contract match counts
                contract_match_counts = Counter(match_df['Matching Contract'])
                match_counts_df = pd.DataFrame.from_dict(
                    contract_match_counts, orient='index', columns=['Total ZIP Matches']
                ).reset_index().rename(columns={'index': 'Contract Name'})
                match_counts_df = match_counts_df.sort_values('Total ZIP Matches', ascending=False)
                
                # Generate active counts
                active_match_df = match_df[match_df['Contract Status'].str.lower() == 'active']
                active_counts = {
                    'Metric': ['Active Contracts', 'Active Buyers'],
                    'Count': [
                        len(active_match_df['Matching Contract'].unique()),
                        len(active_match_df['Buyer Name'].unique())
                    ]
                }
                active_counts_df = pd.DataFrame(active_counts)
                
                # Export matches, match counts, and active counts
                output_dir = st.session_state.temp_dir
                Path(output_dir).mkdir(exist_ok=True)
                new_zip_path = os.path.join(output_dir, "new_zip_matches.xlsx")
                with pd.ExcelWriter(new_zip_path) as writer:
                    match_df.to_excel(writer, sheet_name="ZIP Matches", index=False)
                    match_counts_df.to_excel(writer, sheet_name="Contract Match Counts", index=False)
                    active_counts_df.to_excel(writer, sheet_name="Active Counts", index=False)
                
                # Summary statistics
                total_new_zips = len(new_zips)
                matched_zips = len(match_df['New ZIP Code'].unique())
                total_matches = len(match_df)
                unique_contracts = len(match_df['Matching Contract'].unique())
                
                self.log_message(f"New ZIP analysis completed!")
                self.log_message(f"Total new ZIP codes: {total_new_zips}")
                self.log_message(f"ZIP codes with matches: {matched_zips}")
                self.log_message(f"Total matches found: {total_matches}")
                self.log_message(f"Unique contracts matched: {unique_contracts}")
                self.log_message(f"Active counts: {active_counts['Count'][0]} contracts, {active_counts['Count'][1]} buyers")
                self.log_message(f"Results exported to: new_zip_matches.xlsx")
                
                # Log top 10 contracts by match count
                self.log_message("Top 10 contracts by ZIP matches:")
                for i, (contract, count) in enumerate(match_counts_df.head(10).itertuples(index=False, name=None)):
                    self.log_message(f"{i+1}. {contract}: {count} matches")
                
                st.session_state.output_files['new_zip_matches.xlsx'] = new_zip_path
            
            else:
                self.log_message("No matches found for the new ZIP codes.")
            
        except Exception as e:
            self.log_message(f"Error analyzing new ZIP codes: {str(e)}")
            st.error(f"New ZIP analysis failed: {str(e)}")

def main():
    st.set_page_config(page_title="Contract ZIP Code", layout="wide")
    st.title("Contract ZIP Code")
    st.markdown("""
    ****Load the Main Contracts File
Click “Browse” under Main Contracts File.

Select a CSV file containing contract data.

Optionally adjust:
Chunk Size (default 50000) to manage memory usage.

Filter: Uncheck if you want to include inactive contracts or buyers.

Click “Load Main File” to process and load the data.

(Optional) Load New ZIP Codes File

Click “Browse” under New ZIP Codes File (Optional).

Select a CSV file with a list of ZIP codes (and optionally State).
Used to check which contracts these ZIPs match.

Set Output Directory
Choose or confirm a directory where the result Excel files will be saved.

Run Analyses
Click “Generate Main Analysis” to:

Get ZIP code overlaps across contracts.

Generates:
contract_summary.xlsx

detailed_contract_zip_matches.xlsx

Individual files per contract with overlaps.

Click “Match New ZIP Codes” to:

See which existing contracts cover those ZIPs.

Generate new_zip_matches.xlsx.

Review Output
The text box at the bottom shows real-time logs and top overlapping contracts.

Excel files are saved in the output directory you selected.****

Expected Input Format
Main Contracts File (CSV):
Must contain these columns (case-insensitive):

.Contract Name
.Buyer Name
.Buyer ID
.Zip Code
.State ID (optional)
.Contract Status (to filter active contracts)
.Buyer Status (to filter active buyers)

*New ZIP Codes File (CSV):
Must contain at least:
****Zip Code (column name containing “zip” is sufficient)
State (optional) 
""")
    
    analyzer = ContractZipAnalyzer()
    
    # Input section
    st.header("1. Main Contracts File")
    main_file = st.file_uploader("Upload Main Contracts CSV File", type=["csv"], key="main_file")
   
    
    
    col1, col2 = st.columns(2)
    with col1:
        filter_active = st.checkbox("Filter only Active contracts and buyers", value=True)
    with col2:
        chunk_size = st.number_input("Chunk size", min_value=1000, value=50000, step=1000)
    
    if st.button("Load Main File", disabled=not main_file):
        if main_file:
            with st.spinner("Loading main file..."):
                analyzer.load_main_file(main_file, chunk_size, filter_active)
    
    # New ZIP codes file
    st.header("2. New ZIP Codes File (Optional)")
    new_zip_file = st.file_uploader("Upload New ZIP Codes CSV File", type=["csv"], key="new_zip_file")
    st.write("Expected columns: ZIP Code, State (optional)")
    
    # Analysis buttons
    st.header("Analysis")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Generate Main Analysis", disabled=not st.session_state.main_file_loaded):
            with st.spinner("Generating main analysis..."):
                analyzer.analyze_main_data()
    with col2:
        if st.button("Match New ZIP Codes", disabled=not (st.session_state.main_file_loaded and new_zip_file)):
            with st.spinner("Matching new ZIP codes..."):
                analyzer.analyze_new_zips(new_zip_file)
    
    # Output section
    st.header("Output")
    if st.session_state.logs:
        st.subheader("Logs")
        st.text_area("Analysis Logs", value="\n".join(st.session_state.logs), height=300, disabled=True)
    
    if st.session_state.output_files:
        st.subheader("Download Results")
        for file_name, file_path in st.session_state.output_files.items():
            with open(file_path, "rb") as f:
                st.download_button(
                    label=f"Download {file_name}",
                    data=f,
                    file_name=file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

if __name__ == "__main__":
    main()

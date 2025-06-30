import os
import glob
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# -------------------------- CONFIGURATION --------------------------
input_folder = "F:/un-sorted/Clients/RACM/RAW/Wind CAPEX/NEW"
exchange_rates = 'F:/un-sorted/Clients/RACM/RAW/Exchange Rates/Euro to USD Exchange Rates_20231207.csv'
expected_columns = {
    'series_info_id', 'date', 'region', 'sub_region', 'facility_size',
    'turbine_nameplate_capacity', 'rotor_diameter', 'tower_height',
    'drive_train', 'forecast_type', 'cost_element_category',
    'cost_element_subcategory', 'dollars_per_mw'
}

# Replace with your SQL connection string
# Example: "postgresql://user:password@localhost:5432/mydatabase"
windcapex_db = "postgresql://username:password@host:port/dbname"
table_name = "wind_capex_data"

# ------------------------------------------------------------------

# Set up DB engine
engine = create_engine(windcapex_db)

# Read exchange rates
exrates = pd.read_csv(exchange_rates, dtype={
    'vintage': str, 'country': str,
    'from_currency': str, 'to_currency': str,
    'year': str, 'rate_multiplier': float
})[['year', 'rate_multiplier']]

# Define the transformation pipeline
def process_csv(input_file):
    try:
        df = pd.read_csv(input_file)

        # Header validation
        missing = expected_columns - set(df.columns)
        if missing:
            print(f"⚠️ Skipping {os.path.basename(input_file)} — missing columns: {missing}")
            return None

        # Cast necessary columns
        df = df.astype({
            'series_info_id': int,
            'date': str,
            'region': str,
            'sub_region': str,
            'facility_size': str,
            'turbine_nameplate_capacity': str,
            'rotor_diameter': str,
            'tower_height': str,
            'drive_train': str,
            'forecast_type': str,
            'cost_element_category': str,
            'cost_element_subcategory': str,
            'dollars_per_mw': float
        })

        # Merge exchange rates
        df['year'] = df['date'].str[:4]
        df = df.merge(exrates, on='year', how='left')

        #Transform
        df = df.rename(columns={
            'region': 'fact_Region',
            'date': 'fact_Quarter',
            'facility_size': 'fact_Facility_Size',
            'dollars_per_mw': 'fact_Dollar_MW',
            'sub_region': 'fact_Sub_Region',
            'cost_element_subcategory': 'fact_Item',
            'turbine_nameplate_capacity': 'fact_Turbine_Nameplate_Capacity',
            'rotor_diameter': 'fact_Rotor_Diameter',
            'tower_height': 'fact_Tower_Height',
            'drive_train': 'fact_Drive_Train',
            'forecast_type': 'Forecast_Type'
        })

        df['_fact_Asset_Type'] = 'Wind CAPEX'
        df['fact_Dollar_KW'] = df['fact_Dollar_MW'] * 0.001
        df['fact_Dollar_W'] = df['fact_Dollar_MW'] * 1e-6
        df['fact_Payment_Date'] = '9/1/2022'
        df['Dimension_PA_Taxonomy_FullQual'] = 'Rotating Equipment>Wind Turbines>Wind Turbines'
        df['fact_Raw_Supplier'] = 'NONE'
        df['Dimension_Supplier_Hierarchy_FullQual'] = 'NONE'
        df['Dimension_Time_FullQual'] = '2022>2022 Q3>Sep 2022'
        df['Dimension_Cost_Element_FullQual'] = df['cost_element_category'] + '>' + df['fact_Item']
        df['fact_Euro_MW'] = df['fact_Dollar_MW'] * df['rate_multiplier']
        df['fact_Euro_KW'] = df['fact_Euro_MW'] * 0.001
        df['fact_Euro_W'] = df['fact_Euro_MW'] * 1e-6

        return df

    except Exception as e:
        print(f"❌ Error processing {input_file}: {e}")
        return None

# Process all CSV files
start_time = datetime.now()
csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
all_dfs = []

for csv_file in csv_files:
    df = process_csv(csv_file)
    if df is not None:
        all_dfs.append(df)

# Concatenate all valid DataFrames and load to SQL
if all_dfs:
    final_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
    try:
        final_df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ Loaded {len(final_df)} rows into table '{table_name}'")
    except SQLAlchemyError as db_err:
        print(f"❌ Database error: {db_err}")
else:
    print("⚠️ No valid files processed.")

print('⏱️ Duration:', datetime.now() - start_time)

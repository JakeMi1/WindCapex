import os
from datetime import datetime
import pandas as pd

# ------------------FILE PATH PROMPT--------------------

output_path = input("Enter the file path you'd like to export to: ")

#-------------------------------------------------------

# Configuration
input_file = "https://raw.githubusercontent.com/JakeMi1/WindCapex/refs/heads/main/Sample%20Data/Raw/RACM_Wind_CAPEX_sampledata.csv"
exchange_rates = "https://raw.githubusercontent.com/JakeMi1/WindCapex/refs/heads/main/Sample%20Data/Exchange%20Rates/Euro%20to%20USD%20Exchange%20Rates_20231207.csv"
output_file = os.path.join(output_path, f"Wind Capex OUT {datetime.today().strftime('%Y-%m-%d')}.csv")

# Required columns for validation
expected_columns = {
    'series_info_id', 'date', 'region', 'sub_region', 'facility_size',
    'turbine_nameplate_capacity', 'rotor_diameter', 'tower_height',
    'drive_train', 'forecast_type', 'cost_element_category',
    'cost_element_subcategory', 'dollars_per_mw'
}

# Read exchange rates
exrates = pd.read_csv(exchange_rates, dtype={
    'vintage': str, 
    'country': str,
    'from_currency': str, 
    'to_currency': str,
    'year': str, 
    'rate_multiplier': float
})[['year', 'rate_multiplier']]

# Define transform function
def process_csv(url):
    try:
        df = pd.read_csv(url)

        # Header validation
        missing = expected_columns - set(df.columns)
        if missing:
            print(f"⚠️ Skipping file — missing columns: {missing}")
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

        # Transform
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
        df['fact_Dollar_W'] = df['fact_Dollar_MW'] * 0.000001
        df['fact_Payment_Date'] = '9/1/2022'
        df['Dimension_PA_Taxonomy_FullQual'] = 'Rotating Equipment>Wind Turbines>Wind Turbines'
        df['fact_Raw_Supplier'] = 'NONE'
        df['Dimension_Supplier_Hierarchy_FullQual'] = 'NONE'
        df['Dimension_Time_FullQual'] = '2022>2022 Q3>Sep 2022'
        df['Dimension_Cost_Element_FullQual'] = df['cost_element_category'] + '>' + df['fact_Item']
        df['fact_Euro_MW'] = df['fact_Dollar_MW'] * df['rate_multiplier']
        df['fact_Euro_KW'] = df['fact_Euro_MW'] * 0.001
        df['fact_Euro_W'] = df['fact_Euro_MW'] * 0.000001

        return df

    except Exception as e:
        print(f"❌ Error reading or processing GitHub CSV: {e}")
        return None

# Run ETL
start_time = datetime.now()
df = process_csv(input_file)

if df is not None:
    df.drop_duplicates(inplace=True)
    df.to_csv(output_file, index=False)
    print(f"✅ Output written to: {output_file}")
else:
    print("⚠️ No valid data to write.")

print("⏱️ Duration:", datetime.now() - start_time)

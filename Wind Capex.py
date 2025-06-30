import os
from datetime import datetime
import glob
import pandas as pd
start_time = datetime.now()

# Folder path containing CSV files
input_folder = "F:/un-sorted/Clients/RACM/RAW/Wind CAPEX/NEW"
output_folder = "F:/un-sorted/Clients/RACM/OUT/Python_test"
exchange_rates = 'F:/un-sorted/Clients/RACM/RAW/Exchange Rates/Euro to USD Exchange Rates_20231207.csv'

# Desired column order
out_columns = ['_fact_Asset_Type', 
               'fact_Region', 
               'fact_Module_Type', 
               'fact_Facility_Size', 
               'fact_Cost_Subcategory', 
               'fact_Cost_Item', 
               'fact_Dollar_MW', 
               'fact_Dollar_KW', 
               'fact_Dollar_W', 
               'fact_Payment_Date', 
               'Dimension_PA_Taxonomy_FullQual', 
               'fact_Raw_Supplier', 
               'Dimension_Supplier_Hierarchy_FullQual', 
               'Dimension_Time_FullQual', 
               'Dimension_Cost_Element_FullQual', 
               'fact_Sub_Region', 
               'fact_Module_Origin', 
               'fact_Racking_Type', 
               'fact_Inverter_Type', 
               'fact_Power_Type', 
               'fact_Asset_Age__Years_', 
               'fact_Item', 
               'fact_Quarter', 
               'fact_Currency', 
               'fact_Real_Year', 
               'fact_Turbine_Nameplate_Capacity', 
               'fact_Rotor_Diameter', 
               'fact_Tower_Height', 
               'fact_Drive_Train', 
               'fact_Battery_Type', 
               'fact_Battery_Duration', 
               'fact_Dollar_Wh', 
               'fact_Euro_MW', 
               'fact_Euro_KW', 
               'fact_Euro_W', 
               'fact_Labor_Type', 
               'fact_Tariff', 
               'fact_Dollar_KWH', 
               'fact_Dollar_MWH', 
               'Euro_WH', 
               'Euro_KWH', 
               'Euro_MWH', 
               'fact_Capacity_Guarantee', 
               'Wafer_Type', 
               'Dispatch_Power', 
               'COD', 'Incoterm', 
               'Forecast_Type', 
               'Voltage_kV', 
               'Power_Rating', 
               'Control_Building', 
               'Length_of_Line', 
               'Circuit_Type', 
               'Total_Cost', 
               'Total_Cost_Euro', 
               'Veg_Mgmt',
               'iso_rto']

# Function to reorder columns in a CSV file using pandas
def reorder_columns(input_file, output_file):
    # Read input CSV file into a pandas DataFrame
    df = pd.read_csv(input_file, dtype={'series_info_id': int,
                                        'date':str,
                                        'region':str,
                                        'sub_region':str,
                                        'facility_size':str,
                                        'turbine_nameplate_capacity':str,
                                        'rotor_diameter':str,
                                        'tower_height':str,
                                        'drive_train':str,
                                        'forecast_type':str,
                                        'cost_element_category':str,
                                        'cost_element_subcategory':str,
                                        'dollars_per_mw':float})
    exrates = pd.read_csv(exchange_rates, dtype={'vintage':str,
                                                 'country':str,
                                                 'from_currency':str,
                                                 'to_currency':str,
                                                 'year':str,
                                                 'rate_multiplier':float})

    # Join Exchange Rates
    df['year']=df['date'].str[:4]
    df = pd.merge(df,exrates,on='year')

    # Rename columns
    df = df.rename(columns={'region':'fact_Region',
                            'date':'fact_Quarter',
                            'facility_size':'fact_Facility_Size',
                            'dollars_per_mw':'fact_Dollar_MW',
                            'sub_region':'fact_Sub_Region',
                            'cost_element_subcategory':'fact_Item',
                            'time_period':'fact_Quarter',
                            'turbine_nameplate_capacity':'fact_Turbine_Nameplate_Capacity',
                            'rotor_diameter':'fact_Rotor_Diameter',
                            'tower_height':'fact_Tower_Height',
                            'drive_train':'fact_Drive_Train',
                            'forecast_type':'Forecast_Type'})
    
    # Create formulas
    df['_fact_Asset_Type']='Wind CAPEX'
    df['fact_Dollar_KW']=df.fact_Dollar_MW*0.001
    df['fact_Dollar_W']=df.fact_Dollar_MW*0.000001
    df['fact_Payment_Date']='9/1/2022'
    df['Dimension_PA_Taxonomy_FullQual']='Rotating Equipment>Wind Turbines>Wind Turbines'
    df['fact_Raw_Supplier']='NONE'
    df['Dimension_Supplier_Hierarchy_FullQual']='NONE'
    df['Dimension_Time_FullQual']='2022>2022 Q3>Sep 2022'
    df['Dimension_Cost_Element_FullQual']=df['cost_element_category']+'>'+df['fact_Item']
    df['fact_Euro_MW']=df.fact_Dollar_MW*df.rate_multiplier
    df['fact_Euro_KW']=df.fact_Dollar_MW*df.rate_multiplier*0.001
    df['fact_Euro_W']=df.fact_Dollar_MW*df.rate_multiplier*0.000001

    # Reorder columns
    df = df.reindex(columns=out_columns)

    # Remove Duplicates
    # df.drop_duplicates()

    # Write modified data to output CSV file
    df.to_csv(output_file, index=False, mode='a', header=os.path.isfile(output_file)==False)
    
    print(f"CSV file '{output_file}' has been created successfully.")

# Find all CSV files in the specified folder
csv_files = glob.glob(os.path.join(input_folder, '*.csv'))

# Process each CSV file
for csv_file in csv_files:
    # Define output file name
    #output_file = os.path.join(output_folder, f"modified_{os.path.basename(csv_file)}")
    output_file = os.path.join(output_folder, f"Wind Capex OUT {datetime.today().strftime('%Y-%m-%d')}.csv")
    
    # Reorder columns and write to output file
    reorder_columns(csv_file, output_file)

end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))
import csv
import pandas as pd
from utils import clean_column_name, map_column_name, UploadToBlob
from config import config


class PrepareTransformData:
    def __init__(self, data, company, file):
        self.data = None
        self.company = None
        self.file = None
        self.transform_unique_file(data, company, file)
    
    def transform_unique_file(self, data, company, file):
        df = data
        company = company
        
        
        #Transform every thing here
        #1. Transpose the data
        df_transposed = df.T
      
        # Step 2: Set the first row as the new header and remove it from data
        new_header = df_transposed.iloc[0]
        df_transposed = df_transposed[1:]
        df_transposed.columns = new_header
        
        # Step 3: Reset the index and rename the 'index' column to 'Year'
        df_transposed = df_transposed.reset_index(drop=False)
        df_transposed = df_transposed.rename(columns={'index': 'Year'})
        
        # Step 4: Convert the 'Year' column to string format
        df_transposed['Year'] = df_transposed['Year'].astype(str)
      
        # Step 5: Create a new column to extract the first 4 characters of 'Year' (year prefix)
        df_transposed['year'] = df_transposed['Year'].str[:4]
        
        # Step 6: Group by 'Year_prefix' and join the rows by taking the largest value for each column
        def join_rows(group):
            # Joining by taking the largest value for overlapping columns
            return group.max(axis=0, skipna=True)
       
        # Step 7: Group the rows by 'Year_prefix' and apply the join operation
        df_transposed_grouped = df_transposed.groupby('year', group_keys=False).apply(join_rows)
        
        # Step 8: Reset the index to keep the 'Year' column as part of the DataFrame
        df_transposed_grouped = df_transposed_grouped.reset_index(drop=True)
      
        # Step 9: Replace NaN values with a placeholder if necessary (e.g., 'NaN')
        df_transposed_grouped = df_transposed_grouped.fillna('NaN')

        
        df_transposed_grouped.insert(1, 'symbol', 'AAPL')
       
        df_transposed_grouped = df_transposed_grouped[['year'] + [col for col in df_transposed_grouped.columns if col != 'year']]

        df_transposed_grouped = df_transposed_grouped.drop('Year', axis=1, errors='ignore')
        
        column_names = df_transposed_grouped.columns.to_list()
    
        columns_to_remove = ['symbol', 'year']
        
        column_names = [col for col in column_names if col not in columns_to_remove]
        

        changed_columns_names = map_column_name(column_names)

        final_column = columns_to_remove + changed_columns_names
        df_transposed_grouped.columns = final_column

        # Step 1: Identify duplicate columns by their names
        duplicates = df_transposed_grouped.columns[df_transposed_grouped.columns.duplicated()].unique()

        # Dictionary to store average columns for later concatenation
        avg_columns = {}

        for col in duplicates:
            # Find all columns with the same name
            col_indices = [i for i, c in enumerate(df_transposed_grouped.columns) if c == col]
            
            # Convert columns to numeric (ignoring errors for non-numeric data)
            numeric_columns = df_transposed_grouped.iloc[:, col_indices].apply(pd.to_numeric, errors='coerce')
            
            # Calculate the mean across these columns
            avg_columns[f'{col}_avg'] = numeric_columns.mean(axis=1)

        # Step 2: Keep only non-duplicate columns
        df_new = df_transposed_grouped.drop(columns=duplicates)

        # Step 3: Use pd.concat to add all average columns at once
        if avg_columns:
            avg_df = pd.DataFrame(avg_columns)
            df_new = pd.concat([df_new, avg_df], axis=1)

        #3. add a new column called company
        #add a new column fin_ration_index (unique for each row)

        #convert the transformed df to dict
        data_to_upload = df_new.to_dict()
        file_name = f"{company}_{file.lower()}"
        balance_sheet = UploadToBlob(config.container_name, filename=file_name, data=data_to_upload)
import pandas as pd

class Cleaner:
    @staticmethod
    def clean_chunk(df):
        """
        Cleans a raw dataframe chunk.
        """
        # Strip whitespaces from string columns
        for col in df.select_dtypes(['object']).columns:
            df[col] = df[col].str.strip()
            
        # Handle special values if any
        # e.g., #NULO# -> None
        df.replace('#NULO#', None, inplace=True)
        
        return df

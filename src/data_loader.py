import os
import pandas as pd


class DataLoader:
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None

    def load(self):
        """
        Load dataset from CSV or Excel.
        """
        self._validate_file()
        self._read_file()
        self._clean_columns()
        self._finalize()
        return self.df

    def _validate_file(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        if os.path.getsize(self.file_path) == 0:
            raise ValueError("File is empty.")

    def _read_file(self):
        if self.file_path.endswith(".csv"):
            self.df = pd.read_csv(self.file_path)
        elif self.file_path.endswith(".xlsx"):
            self.df = pd.read_excel(self.file_path)
        else:
            raise ValueError("Unsupported file format. Use CSV or XLSX.")

    def _clean_columns(self):
        # Remove leading/trailing spaces
        self.df.columns = self.df.columns.str.strip()

        # Check duplicate column names
        if self.df.columns.duplicated().any():
            raise ValueError("Duplicate column names detected.")

    def _finalize(self):
        # Reset index
        self.df.reset_index(drop=True, inplace=True)

        # Check if dataframe is empty
        if self.df.shape[0] == 0:
            raise ValueError("Dataset contains no rows.")

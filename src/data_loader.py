import pandas as pd
import os
import csv
import re

# 1. Custom Exception để hứng lỗi Duplicate cụ thể
class DuplicateColumnError(Exception):
    pass

class DataLoader:
    def __init__(self, file_path: str, max_size_mb: int = 50):
        self.file_path = file_path
        self.max_size_mb = max_size_mb
        self.df = None
        self.metadata = {}
        self.warnings = []

    def validate_and_load(self, action: str = 'check'):
        """
        Main method để load dữ liệu.
        Args:
            action (str): 
                - 'check': (Mặc định) Kiểm tra lỗi, nếu trùng cột -> Raise DuplicateColumnError.
                - 'rename': Tự động đổi tên (Col, Col.1).
                - 'keep_first': Giữ cột đầu, bỏ cột trùng phía sau.
        Returns:
            dict: Metadata của file (rows, columns, warnings...).
        """
        if self.df is not None:
            self._check_mixed_datatypes()
            self._finalize()
        # 1. Kiểm tra File tồn tại và Kích thước
        self._basic_validation()

        # 2. Đọc file (Xử lý Encoding & Check Duplicate trong này)
        self._read_file(action)
        
        # 3. Kiểm tra Mixed Datatype (Cảnh báo)
        self._check_mixed_datatypes()

        # 4. Hoàn tất (Reset index, check empty rows)
        self._finalize()

        return {
            "file_name": os.path.basename(self.file_path),
            "size_readable": self._get_readable_size(),
            "rows": self.df.shape[0],
            "columns": self.df.shape[1],
            "column_names": list(self.df.columns),
            "warnings": self.warnings
        }

    def _basic_validation(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        file_size = os.path.getsize(self.file_path)
        if file_size == 0:
            raise ValueError("File is empty.")
        
        if file_size > self.max_size_mb * 1024 * 1024:
            raise ValueError(f"File too large. Limit is {self.max_size_mb}MB.")

    def _read_file(self, action):
        ext = self.file_path.split('.')[-1].lower()

        if ext == 'csv':
            self._read_csv(action)
        elif ext in ['xlsx', 'xls']:
            self._read_excel(action)
        else:
            raise ValueError("Unsupported file format. Use CSV or Excel.")

    def _read_csv(self, action):
        # Danh sách encoding phổ biến để thử
        encodings = ['utf-8', 'latin1', 'cp1252', 'utf-16']
        
        for enc in encodings:
            try:
                # --- BƯỚC 1: CHECK RAW HEADER (Để pass test case) ---
                # Chỉ check khi action là 'check'
                if action == 'check':
                    with open(self.file_path, 'r', encoding=enc, newline='') as f:
                        # Dùng csv.reader để xử lý đúng chuẩn CSV (dấu phẩy trong ngoặc kép)
                        reader = csv.reader(f)
                        try:
                            header = next(reader) # Lấy dòng đầu tiên
                        except StopIteration:
                            raise ValueError("File is empty (no header).")
                        
                        # So sánh độ dài list và set để tìm trùng lặp
                        if len(header) != len(set(header)):
                            raise DuplicateColumnError("Duplicate column names detected.")

                # --- BƯỚC 2: LOAD BẰNG PANDAS ---
                # Pandas mặc định sẽ rename cột trùng thành .1, .2 -> Phù hợp với action='rename'
                self.df = pd.read_csv(self.file_path, encoding=enc)

                # --- BƯỚC 3: XỬ LÝ KEEP FIRST ---
                if action == 'keep_first':
                    # Loại bỏ các cột được Pandas tự động thêm hậu tố .1, .2
                    # Regex này tìm các cột kết thúc bằng dấu chấm và số (vd: "Name.1")
                    self.df = self.df.loc[:, ~self.df.columns.str.match(r'.*\.\d+$')]
                
                return # Đọc thành công thì thoát hàm
                
            except UnicodeDecodeError:
                continue # Thử encoding tiếp theo
            except DuplicateColumnError as e:
                raise e # Ném lỗi duplicate ra ngay
            except Exception as e:
                # Nếu lỗi không phải do encoding thì báo lỗi luôn
                if enc == encodings[-1]: 
                    raise ValueError(f"Error reading CSV: {str(e)}")

        raise ValueError("Encoding error. Could not decode file with common encodings.")
    
    def _read_excel(self, action):
        try:
            # 1. Luôn load dữ liệu lên trước
            self.df = pd.read_excel(self.file_path)
            
            # 2. Kiểm tra trùng cột nếu action là 'check'
            # Pandas tự đổi tên trùng thành 'a', 'a.1'. 
            # Regex này tìm các cột có đuôi .số
            is_duplicate = self.df.columns.str.match(r'.*\.\d+$').any()
            
            if action == 'check' and is_duplicate:
                raise DuplicateColumnError("Duplicate column names detected.")
            
            # 3. Nếu là keep_first, lọc bỏ các cột .1, .2
            if action == 'keep_first':
                self.df = self.df.loc[:, ~self.df.columns.str.match(r'.*\.\d+$')]
                
        except DuplicateColumnError:
            raise
        except Exception as e:
            raise ValueError(f"Error reading Excel: {str(e)}")

    def _check_mixed_datatypes(self):
        if self.df is None: return
        
        for col in self.df.columns:
            # Chỉ check cột object (text)
            if self.df[col].dtype == 'object':
                # Bỏ qua null, lấy danh sách các kiểu dữ liệu trong cột
                types = self.df[col].dropna().apply(type).unique()
                if len(types) > 1:
                    self.warnings.append(f"Column '{col}' has mixed datatypes (e.g., Number and String).")

    def _finalize(self):
        if self.df is not None:
            self.df.reset_index(drop=True, inplace=True)
            if self.df.shape[0] == 0:
                 raise ValueError("Dataset contains no rows.")

    def _get_readable_size(self):
        size = os.path.getsize(self.file_path)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} TB"
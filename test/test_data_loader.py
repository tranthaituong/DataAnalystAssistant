import os
import pytest
import tempfile
import pandas as pd

from src.data_loader import DataLoader, DuplicateColumnError


# =========================
# Helper tạo file tạm
# =========================

def create_temp_file(content: str, suffix=".csv", encoding="utf-8"):
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode="w", encoding=encoding)
    temp.write(content)
    temp.close()
    return temp.name


# =========================
# BASIC VALIDATION
# =========================

def test_file_not_found():
    loader = DataLoader("not_exist.csv")
    with pytest.raises(FileNotFoundError):
        loader.validate_and_load()


def test_empty_file():
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    temp.close()

    loader = DataLoader(temp.name)
    with pytest.raises(ValueError, match="File is empty"):
        loader.validate_and_load()

    os.unlink(temp.name)


def test_file_too_large():
    path = create_temp_file("a,b\n1,2")

    loader = DataLoader(path, max_size_mb=0)
    with pytest.raises(ValueError, match="File too large"):
        loader.validate_and_load()

    os.unlink(path)


# =========================
# CSV DUPLICATE LOGIC
# =========================

def test_duplicate_check_mode():
    path = create_temp_file("a,a\n1,2")

    loader = DataLoader(path)
    with pytest.raises(DuplicateColumnError):
        loader.validate_and_load(action="check")

    os.unlink(path)


def test_duplicate_keep_first():
    path = create_temp_file("a,a\n1,2")

    loader = DataLoader(path)
    result = loader.validate_and_load(action="keep_first")

    assert result["columns"] == 1
    assert result["column_names"] == ["a"]

    os.unlink(path)


def test_duplicate_rename_mode():
    path = create_temp_file("a,a\n1,2")

    loader = DataLoader(path)
    result = loader.validate_and_load(action="rename")

    # pandas sẽ rename thành a và a.1
    assert "a" in result["column_names"]
    assert any(col.endswith(".1") for col in result["column_names"])

    os.unlink(path)


# =========================
# NORMAL CSV LOAD
# =========================

def test_normal_csv_load():
    path = create_temp_file("a,b\n1,2\n3,4")

    loader = DataLoader(path)
    result = loader.validate_and_load()

    assert result["rows"] == 2
    assert result["columns"] == 2
    assert result["warnings"] == []

    os.unlink(path)


# =========================
# MIXED DATATYPE WARNING
# =========================

def test_mixed_datatype_warning():
    import pandas as pd
    
    # 1. Khởi tạo loader (giả lập hoặc tạo instance thật)
    loader = DataLoader("dummy_path.xlsx") 
    
    # 2. Thay vì đọc file, ta "bơm" trực tiếp DataFrame lỗi vào
    loader.df = pd.DataFrame({
        "a": [1, "hello", 3.5]  # Có int, str, và float
    })
    
    # 3. Chạy hàm kiểm tra
    loader._check_mixed_datatypes()

    # 4. Kiểm tra kết quả
    # Lúc này types sẽ là [int, str, float] -> len > 1
    assert len(loader.warnings) == 1
    assert "Column 'a' has mixed datatypes" in loader.warnings[0]
    


# =========================
# DATASET WITH NO ROWS
# =========================

def test_no_rows_dataset():
    path = create_temp_file("a,b\n")  # header only

    loader = DataLoader(path)
    with pytest.raises(ValueError, match="Dataset contains no rows"):
        loader.validate_and_load()

    os.unlink(path)


# =========================
# UNSUPPORTED FORMAT
# =========================

def test_unsupported_format():
    path = create_temp_file("dummy", suffix=".txt")

    loader = DataLoader(path)
    with pytest.raises(ValueError, match="Unsupported file format"):
        loader.validate_and_load()

    os.unlink(path)


# =========================
# EXCEL LOAD TEST
# =========================

def test_excel_load():
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    temp.close()

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df.to_excel(temp.name, index=False)

    loader = DataLoader(temp.name)
    result = loader.validate_and_load()

    assert result["rows"] == 2
    assert result["columns"] == 2

    os.unlink(temp.name)

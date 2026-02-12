from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
import os
import shutil
from src.data_loader import DataLoader, DuplicateColumnError

app = FastAPI(title="Data Validator API")

# Tạo thư mục upload nếu chưa có
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def remove_file(path: str):
    if os.path.exists(path):
        os.remove(path)

@app.post("/upload/")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    action: str = "check"
):
    # 1. Lưu file tạm thời
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Đảm bảo file sẽ được xóa sau khi trả về response
    background_tasks.add_task(remove_file, file_path)

    try:
        # 2. Sử dụng DataLoader đã viết
        loader = DataLoader(file_path)
        result = loader.validate_and_load(action=action)
        
        return {
            "status": "success",
            "data": result
        }

    except DuplicateColumnError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
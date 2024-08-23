from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse
import shutil
import os

APP = FastAPI()

def process_file(file_path: str):
    from utils.parse_zip import ZipParser
    parser = ZipParser()
    parser.extract_json_from_zip(file_path)
    parser.json_to_redis()
    shutil.rmtree("tmp")

@APP.get("/", response_class=HTMLResponse)
async def serve_upload_form():
    return open("upload_form.html", "r").read()

@APP.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    os.makedirs("tmp", exist_ok=True)
    temp_file_path = f"tmp/{file.filename}"
    with open(temp_file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    
    process_file(temp_file_path)
    
    return {"message": "File uploaded successfully"}
    
    
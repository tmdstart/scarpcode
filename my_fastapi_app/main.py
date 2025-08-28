# main.py

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

# models.py에서 DataManager 클래스를 임포트
from models import DataManager

app = FastAPI()
templates = Jinja2Templates(directory="templates")
data_manager = DataManager()

@app.get("/")
def get_items_page(request: Request):
    """
    사용자 요청을 받아 데이터를 가져와 화면에 표시하는 컨트롤러
    """
    # 1. 모델에서 데이터를 가져옵니다.
    items = data_manager.get_all_items()
    
    # 2. 뷰에 데이터를 전달해 화면을 렌더링합니다.
    return templates.TemplateResponse("detail.html", {"request": request, "items": items})
    #templates.TemplateResponse("items.html", {"request": request, "items": items})
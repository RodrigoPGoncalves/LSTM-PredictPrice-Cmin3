from fastapi import FastAPI,HTTPException, APIRouter
from pydantic import BaseModel
from typing import List
from fastapi.responses import JSONResponse
import uvicorn
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models.model import Model
import preprocess.preprocess as pp

class NumberRequest(BaseModel):
    number: int

class DataEntry(BaseModel):
    high_dolar: float
    low_dolar: float
    open_dolar: float
    close_dolar: float
    close_TIOc1: float
    open_TIOc1: float
    high_TIOc1: float
    low_TIOc1: float
    high_cmin3: float
    low_cmin3: float
    open_cmin3: float
    volume_cmin3: float

class Payload(BaseModel):
    number: int
    data: List[DataEntry]

pp_object = pp.PreProcess()
pp_object.download_and_save_files()
pp_object.process_and_save_files()

md_object = Model(pp_object)

app = FastAPI()

@app.get("/")
async def  root():
    return {"message":"Bem vindo a API de previsão da ação CMIN3."}

@app.get("/loadModels")
async def  get_load_models():
    bool_info, message = md_object.load_models_mlflow()
    if(bool_info):
        return JSONResponse(
                status_code=200,  
                content={"message": message}
            )
    raise HTTPException(status_code=400, detail=message)

@app.post("/predict_period")
async def  post_period_and_predict(request: NumberRequest):
    bool_info, message = md_object.predict_period(request.number)
    if(bool_info):
        return JSONResponse(
                status_code=200,  
                content={"message": "Predição feita com sucesso",
                            "data": message}
            )
    raise HTTPException(status_code=400, detail=message)

@app.post("/predict_new_values_period")
async def  post_period_and_predict(payload: Payload):
    bool_info, message = md_object.predict_new_values_period(payload.data, payload.number)
    if(bool_info):
        return JSONResponse(
                status_code=200,  
                content={"message": "Predição feita com sucesso",
                            "data": message}
            )
    raise HTTPException(status_code=400, detail=message)
    
if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1", port=8000)

from fastapi import FastAPI, Request, Form, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from src.pipelines.prediction_pipeline import PredictPipeline

import pandas as pd
import os
import uvicorn

app = FastAPI()
templates = Jinja2Templates(directory="templates")

def get_player_names():
    
    data = pd.read_csv(os.path.join("artifacts", "prediction_data.csv"))
    data['full_name'] = data['first_name'] + ' ' + data['last_name']

    names = list(data['full_name'])
    

    return names


@app.get("/", response_class=HTMLResponse)
async def home_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/predict", response_class=HTMLResponse)
async def get_prediction_form(request: Request):
    
    player_names = get_player_names()
    
    return templates.TemplateResponse("form.html", {"request": request, "players": player_names})

@app.post("/predict", response_class=HTMLResponse)
async def make_prediction(
    request: Request,
    player: str = Form(...)):
    
    predicted = PredictPipeline().predict_points_for_single_player(player_name=player)

    return templates.TemplateResponse("results.html", {"request": request, 'name': player, 'predicted': predicted})

if __name__ == '__main__':
  uvicorn.run(app, host='localhost', port=8010 )


#uvicorn main:app --reload --port 3000
import pytz
from datetime import datetime, timedelta
import re
from annotation_process import process_result,get_metadata
huso_horario_utc_menos_5 = pytz.timezone('America/Bogota')  # Puedes ajustar el huso horario según tus necesidades

# fastapi_app/main.py
from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/webhook")
async def receive_webhook(request: Request):
    payload = await request.json()
    # Procesa el payload del webhook aquí
    print(payload)
    print("-----------------------------------------------------")
    print(payload['task'])
    metadata = get_metadata(payload)
    df_result= process_result(payload.get("annotation").get("result"),metadata)
    print(df_result)
    

    
    return {"message": "Webhook received successfully"}




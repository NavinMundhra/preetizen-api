from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import Dict, Any

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "API is working successfully!"}

@app.post("/create-order")
async def create_order(request: Request):
    try:
        payload = await request.json()
        if not payload:
            raise HTTPException(status_code=400, detail="Empty payload received.")
        return {"status": "success", "data": payload}
    except Exception:
        raise HTTPException(status_code=400, detail="No valid JSON payload received.")

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict, Any
from datetime import datetime
import os
import json

app = FastAPI()

ORDERS_FILE = "orders.json"

@app.get("/")
def read_root():
    return {"message": "API is working successfully!"}

# POST: Store incoming order data
@app.post("/create-order")
async def create_order(request: Request):
    try:
        payload = await request.json()
        if not payload:
            raise HTTPException(status_code=400, detail="Empty payload received.")
        
        # Add timestamp
        payload_with_time = {
            "received_at": datetime.utcnow().isoformat() + "Z",
            "data": payload
        }

        # Load existing data
        if os.path.exists(ORDERS_FILE):
            with open(ORDERS_FILE, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        else:
            existing_data = []

        # Append new payload
        existing_data.append(payload_with_time)

        # Save updated list
        with open(ORDERS_FILE, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=2)

        return {"status": "success", "message": "Order stored successfully."}

    except Exception:
        raise HTTPException(status_code=400, detail="No valid JSON payload received.")

# GET: Return stored orders
@app.get("/create-order")
def get_orders():
    if not os.path.exists(ORDERS_FILE):
        return JSONResponse(content={"orders": []})
    
    with open(ORDERS_FILE, "r", encoding="utf-8") as f:
        orders = json.load(f)
    
    return {"orders": orders}

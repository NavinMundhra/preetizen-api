from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import Dict, Any

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "API is working successfully!"}

# # Define expected structure (optional, for validation)
# class OrderPayload(BaseModel):
#     order_id: str
#     customer_name: str
#     email: str
#     phone: str
#     address: Dict[str, str]
#     line_items: list
#     total_amount: float
#     created_at: str

@app.post("/create-order")
async def create_order():
    # For now, just echo back the order payload
    return {"status": "success", "data": payload}

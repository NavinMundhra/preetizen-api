from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Dict, Any, List, Optional
from datetime import datetime
import os
import json
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Wix Orders Processing API")

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Please set SUPABASE_URL and SUPABASE_KEY in .env file")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Test order IDs to exclude
TEST_ORDER_IDS = [10001, 10002, 10003, 10004, 10049, 10061, 10114, 10115, 10450, 10451, 10452]

# Temporary storage for backup
ORDERS_FILE = "orders_backup.json"

class OrderProcessor:
    @staticmethod
    def extract_order_data(payload: Dict) -> List[Dict]:
        """Extract and process order data from Wix payload"""
        expanded_rows = []
        
        try:
            # Extract main order info
            order_data = payload.get("data", {})
            order_number = int(order_data.get("orderNumber", 0))
            
            # Skip test orders
            if order_number in TEST_ORDER_IDS:
                logger.info(f"Skipping test order: {order_number}")
                return []
            
            # Extract order date
            created_date = order_data.get("createdDate", "")
            order_date = datetime.fromisoformat(created_date.replace('Z', '+00:00')) if created_date else None
            
            # Extract contact and shipping info
            contact = order_data.get("contact", {})
            shipping_info = order_data.get("shippingInfo", {})
            logistics = shipping_info.get("logistics", {})
            shipping_dest = logistics.get("shippingDestination", {})
            address = shipping_dest.get("address", {})
            contact_details = shipping_dest.get("contactDetails", {})
            
            # Extract payment and fulfillment status
            payment_status = order_data.get("paymentStatus", "NOT_PAID").upper()
            fulfillment_status = order_data.get("status", "").upper()
            
            # Extract totals
            price_summary = order_data.get("priceSummary", {})
            
            # Shared info for all line items
            shared_info = {
                "original_order_id": order_number,
                "order_date": order_date,
                "payment_status": payment_status,
                "fulfillment_status": fulfillment_status,
                "tracking_number": "",  # Will be updated when fulfillment is processed
                "shipping_provider": "",
                "first_name": contact_details.get("firstName", "").strip().title(),
                "last_name": contact_details.get("lastName", "").strip(),
                "email": contact.get("email", ""),
                "phone": contact_details.get("phone", ""),
                "delivery_option": logistics.get("deliveryTime", ""),
                "estimated_delivery": logistics.get("deliveryTime", ""),
                "city": address.get("city", ""),
                "street_address": address.get("addressLine", ""),
                "country": address.get("country", ""),
                "postal_code": address.get("postalCode", ""),
                "weight": 0,  # Will be calculated per item
                "subtotal": float(price_summary.get("subtotal", {}).get("value", 0)),
                "tax": float(price_summary.get("tax", {}).get("value", 0)),
                "shipping_charge": float(price_summary.get("shipping", {}).get("value", 0)),
                "discount": float(price_summary.get("discount", {}).get("value", 0)),
                "total_amount": float(price_summary.get("total", {}).get("value", 0))
            }
            
            # Process line items
            line_items = order_data.get("lineItems", [])
            for idx, item in enumerate(line_items, 1):
                # Extract item options
                description_lines = item.get("descriptionLines", [])
                size = ""
                color = ""
                
                for desc_line in description_lines:
                    if desc_line.get("name") == "Sizes":
                        size = desc_line.get("description", "")
                    elif desc_line.get("name") == "Colour":
                        color = desc_line.get("description", "")
                
                # Create unique order ID with item index
                today_str = datetime.today().strftime("%Y%m%d")
                weekday_str = datetime.today().strftime("%a").upper()
                unique_order_id = f"{order_number}Q{idx}{weekday_str}"
                
                item_row = {
                    "order_id": unique_order_id,
                    "item_index": idx,
                    "translated_name": item.get("itemName", ""),
                    "sku": item.get("sku", ""),
                    "quantity": item.get("quantity", 1),
                    "total_price": float(item.get("totalPrice", {}).get("value", 0)),
                    "size": size,
                    "color": color,
                    "custom_size_note": ""  # Add logic if custom fields exist
                }
                
                expanded_rows.append({**shared_info, **item_row})
            
            return expanded_rows
            
        except Exception as e:
            logger.error(f"Error processing order: {str(e)}")
            return []
    
    @staticmethod
    def create_delhivery_manifest(order_row: Dict) -> Dict:
        """Create Delhivery manifest entry from order row"""
        
        # Calculate unit price with COD charges if applicable
        base_price = order_row["total_price"] - order_row["discount"]
        shipping = 80 if order_row["payment_status"] != "PAID" and base_price < 2000 else 0
        unit_price = base_price + shipping
        
        return {
            "sale_order_number": f"PZ{order_row['order_id']}",
            "order_id": order_row["order_id"],
            "pickup_location_name": "Preetizen Lifestyle",
            "transport_mode": "Surface",
            "payment_mode": "Prepaid" if order_row["payment_status"] == "PAID" else "COD",
            "customer_name": f"{order_row['first_name']} {order_row['last_name']}",
            "customer_phone": order_row["phone"],
            "shipping_address_line1": order_row["street_address"],
            "shipping_city": order_row["city"],
            "shipping_pincode": order_row["postal_code"],
            "shipping_state": "West Bengal",  # You might want to make this dynamic
            "item_sku_code": order_row["sku"],
            "item_sku_name": f"{order_row['translated_name']} - Size: {order_row['size'].upper()} - Colour: {order_row['color']}",
            "quantity_ordered": order_row["quantity"],
            "unit_item_price": unit_price,
            "length_cm": 35,
            "breadth_cm": 25,
            "height_cm": 5,
            "weight_gm": 250
        }

@app.get("/")
def read_root():
    return {"message": "Wix Orders Processing API is running!", "status": "healthy"}

@app.post("/webhook/order")
async def process_order_webhook(request: Request, background_tasks: BackgroundTasks):
    """Process incoming Wix order webhook"""
    try:
        payload = await request.json()
        
        if not payload:
            raise HTTPException(status_code=400, detail="Empty payload received")
        
        # Store backup locally
        background_tasks.add_task(save_backup, payload)
        
        # Process order data
        processor = OrderProcessor()
        order_rows = processor.extract_order_data(payload)
        
        if not order_rows:
            return {"status": "skipped", "message": "Test order or no valid data"}
        
        # Insert into database
        for order_row in order_rows:
            try:
                # Insert into orders table
                order_result = supabase.table("orders").upsert(
                    order_row,
                    on_conflict="order_id"
                ).execute()
                
                # Create and insert Delhivery manifest
                manifest_data = processor.create_delhivery_manifest(order_row)
                manifest_result = supabase.table("delhivery_manifest").upsert(
                    manifest_data,
                    on_conflict="sale_order_number"
                ).execute()
                
                logger.info(f"Successfully processed order: {order_row['order_id']}")
                
            except Exception as e:
                logger.error(f"Error inserting order {order_row.get('order_id')}: {str(e)}")
                continue
        
        return {
            "status": "success",
            "message": f"Processed {len(order_rows)} line items",
            "order_ids": [row["order_id"] for row in order_rows]
        }
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Webhook processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backfill/csv")
async def backfill_from_csv(csv_file_path: str):
    """Backfill historical data from CSV file"""
    try:
        if not os.path.exists(csv_file_path):
            raise HTTPException(status_code=404, detail="CSV file not found")
        
        # Read CSV
        df = pd.read_csv(csv_file_path)
        
        # Process each row
        success_count = 0
        error_count = 0
        
        for _, row in df.iterrows():
            try:
                # Convert DataFrame row to dict and handle NaN values
                order_data = row.where(pd.notnull(row), None).to_dict()
                
                # Ensure required fields
                if not order_data.get("order_id"):
                    continue
                
                # Insert into orders table
                result = supabase.table("orders").upsert(
                    order_data,
                    on_conflict="order_id"
                ).execute()
                
                success_count += 1
                
            except Exception as e:
                logger.error(f"Error processing CSV row: {str(e)}")
                error_count += 1
                continue
        
        return {
            "status": "success",
            "message": f"Backfill completed: {success_count} success, {error_count} errors"
        }
        
    except Exception as e:
        logger.error(f"Backfill error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders")
async def get_orders(
    limit: int = 100,
    offset: int = 0,
    payment_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None
):
    """Retrieve orders from database with optional filters"""
    try:
        query = supabase.table("orders").select("*")
        
        if payment_status:
            query = query.eq("payment_status", payment_status.upper())
        if fulfillment_status:
            query = query.eq("fulfillment_status", fulfillment_status.upper())
        
        result = query.range(offset, offset + limit - 1).execute()
        
        return {
            "status": "success",
            "count": len(result.data),
            "orders": result.data
        }
        
    except Exception as e:
        logger.error(f"Error fetching orders: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/{order_id}")
async def get_order_by_id(order_id: str):
    """Get specific order by ID"""
    try:
        result = supabase.table("orders").select("*").eq("order_id", order_id).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Order not found")
        
        return {
            "status": "success",
            "order": result.data[0]
        }
        
    except Exception as e:
        logger.error(f"Error fetching order {order_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/manifest/export")
async def export_delhivery_manifest(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Export Delhivery manifest for a date range"""
    try:
        query = supabase.table("delhivery_manifest").select("*")
        
        if start_date and end_date:
            query = query.gte("created_at", start_date).lte("created_at", end_date)
        
        result = query.execute()
        
        if not result.data:
            return {"status": "success", "message": "No manifest entries found", "data": []}
        
        # Format for CSV export
        df = pd.DataFrame(result.data)
        
        # Add asterisks to column names as per Delhivery requirements
        exclude_cols = ["length_cm", "breadth_cm", "height_cm", "weight_gm"]
        df.columns = [
            col if col in exclude_cols else f"*{col}"
            for col in df.columns
        ]
        
        # Save to temporary file
        temp_file = f"delhivery_manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(temp_file, index=False)
        
        return {
            "status": "success",
            "message": f"Manifest exported to {temp_file}",
            "count": len(result.data),
            "file": temp_file
        }
        
    except Exception as e:
        logger.error(f"Error exporting manifest: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_order_statistics():
    """Get order statistics"""
    try:
        # Get total orders
        total_orders = supabase.table("orders").select("id", count="exact").execute()
        
        # Get orders by status
        paid_orders = supabase.table("orders").select("id", count="exact").eq("payment_status", "PAID").execute()
        unpaid_orders = supabase.table("orders").select("id", count="exact").eq("payment_status", "NOT_PAID").execute()
        
        # Get today's orders
        today_start = datetime.now().replace(hour=0, minute=0, second=0).isoformat()
        today_orders = supabase.table("orders").select("id", count="exact").gte("created_at", today_start).execute()
        
        return {
            "status": "success",
            "statistics": {
                "total_orders": total_orders.count,
                "paid_orders": paid_orders.count,
                "unpaid_orders": unpaid_orders.count,
                "today_orders": today_orders.count
            }
        }
        
    except Exception as e:
        logger.error(f"Error fetching statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def save_backup(payload: Dict):
    """Save order payload to local backup file"""
    try:
        payload_with_time = {
            "received_at": datetime.utcnow().isoformat() + "Z",
            "data": payload
        }
        
        if os.path.exists(ORDERS_FILE):
            with open(ORDERS_FILE, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        else:
            existing_data = []
        
        existing_data.append(payload_with_time)
        
        with open(ORDERS_FILE, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=2)
            
        logger.info(f"Backup saved for order at {payload_with_time['received_at']}")
        
    except Exception as e:
        logger.error(f"Error saving backup: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
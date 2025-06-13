# app/api/report.py

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from fastapi.responses import Response # NEW: Import Response for file downloads
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime
import pytz
import uuid # For generating unique report IDs
import asyncio # For simulating async work (not used in this version but good practice)
import io # NEW: For in-memory string buffer
import pandas as pd # NEW: For DataFrame and CSV generation

# Local imports
from ..database import get_db, SessionLocal
from ..models import StoreStatus, Report, ReportStatusEnum # Ensure ReportStatusEnum is imported
from ..services.report_calculator import generate_report_for_store # Our calculation logic

router = APIRouter()

# --- Global variable to store ongoing reports' status and results ---
# In a real-world, high-traffic application, this would be stored in a more persistent
# and distributed manner (e.g., Redis, a dedicated reports table in DB)
# For this project, an in-memory dictionary is sufficient.
# Format: {report_id: {"status": ReportStatusEnum, "data": List[Dict] | Dict[str, str] (for errors)}}
reports_cache = {}


async def _generate_report_task(db: Session, report_id: str, current_timestamp_utc: datetime):
    """
    Background task to perform the heavy report generation.
    """
    try:
        reports_cache[report_id]["status"] = ReportStatusEnum.running
        print(f"[{report_id}] Report generation started.")

        all_store_ids = db.query(StoreStatus.store_id).distinct().all()
        all_store_ids_list = [s[0] for s in all_store_ids]

        report_results = []
        # Process each store
        for s_id in all_store_ids_list:
            store_report = generate_report_for_store(db, s_id, current_timestamp_utc)
            report_results.append(store_report)
            # await asyncio.sleep(0.01) # Simulate some work to avoid blocking event loop completely if needed

        reports_cache[report_id]["data"] = report_results # Store the list of dictionaries
        reports_cache[report_id]["status"] = ReportStatusEnum.complete
        print(f"[{report_id}] Report generation complete. Total stores: {len(report_results)}")

    except Exception as e:
        reports_cache[report_id]["status"] = ReportStatusEnum.failed
        reports_cache[report_id]["data"] = {"error": str(e), "message": "Report generation failed."}
        print(f"[{report_id}] Report generation failed: {e}")
        import traceback
        traceback.print_exc() # Print traceback to console for debugging
    finally:
        # Ensure the session is closed even if an error occurs
        db.close()

# --- API Endpoints ---

@router.post("/trigger_report", status_code=status.HTTP_200_OK)
async def trigger_report(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Triggers the generation of an uptime/downtime report.
    Returns a report_id to check the status later.
    """
    report_id = str(uuid.uuid4()) # Generate a unique ID for this report

    # Determine the current timestamp for report generation.
    # As per problem statement, this is the max timestamp among all observations.
    # We fetch it dynamically here for robustness, assuming it's available in the DB.
    current_timestamp_utc_db = db.query(func.max(StoreStatus.timestamp_utc)).scalar()
    if not current_timestamp_utc_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No store status data found to generate a report. Please load data first."
        )
    # Ensure it's timezone-aware UTC, as our calculation logic expects it
    if current_timestamp_utc_db.tzinfo is None:
        current_timestamp_utc = pytz.utc.localize(current_timestamp_utc_db)
    else:
        current_timestamp_utc = current_timestamp_utc_db.astimezone(pytz.utc)

    # Store initial report status in cache
    reports_cache[report_id] = {"status": ReportStatusEnum.pending, "data": None}

    # Add the report generation to background tasks
    # A separate SessionLocal() is crucial here for the background task's independent DB session.
    background_tasks.add_task(_generate_report_task, SessionLocal(), report_id, current_timestamp_utc)

    print(f"Report generation triggered with ID: {report_id}")
    return {"report_id": report_id, "status": ReportStatusEnum.pending.value}


@router.get("/get_report/{report_id}") # Removed status_code here as it will be dynamic (200 for CSV/JSON, 404/500 for errors)
async def get_report(report_id: str):
    """
    Retrieves the status and result of a generated report.
    If the report is complete, returns the data as a CSV file.
    Otherwise, returns JSON with status.
    """
    report_entry = reports_cache.get(report_id)

    if not report_entry:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report with ID '{report_id}' not found."
        )

    status_enum = report_entry["status"] # This is the Enum directly

    if status_enum == ReportStatusEnum.complete:
        report_data = report_entry["data"]
        if not report_data: # Defensive check
            print(f"[{report_id}] ERROR: Report status is complete but data is missing.")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Report data is missing after completion."
            )

        # Create a pandas DataFrame from the list of dictionaries
        df = pd.DataFrame(report_data)

        # Define the desired column order as specified in the problem statement
        expected_columns_order = [
            "store_id",
            "uptime_last_hour",
            "uptime_last_day",
            "uptime_last_week",
            "downtime_last_hour",
            "downtime_last_day",
            "downtime_last_week",
        ]
        
        # Filter and reorder columns to match the requirement
        # Only include columns that actually exist in the DataFrame
        final_columns = [col for col in expected_columns_order if col in df.columns]
        df = df[final_columns]

        # Write DataFrame to a CSV string in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False) # index=False prevents writing DataFrame index as a column
        csv_string = csv_buffer.getvalue()

        # Return as a CSV file response
        # The 'Content-Disposition' header tells the browser to download the file.
        return Response(
            content=csv_string,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=report_{report_id}.csv"}
        )
    elif status_enum == ReportStatusEnum.failed:
        # If the report failed, return appropriate error status and details
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=report_entry.get("data", {"error": "Unknown error", "message": "Report generation failed."})
        )
    else: # ReportStatusEnum.pending or ReportStatusEnum.running
        # If report is not complete or failed, return JSON status
        print(f"Report status requested for ID {report_id}: {status_enum.value}")
        return {"report_id": report_id, "status": status_enum.value}
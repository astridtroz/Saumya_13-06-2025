

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from fastapi.responses import Response 
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime
import pytz
import uuid 
import asyncio
import io
import pandas as pd 


from ..database import get_db, SessionLocal
from ..models import StoreStatus, Report, ReportStatusEnum 
from ..services.report_calculator import generate_report_for_store 

router = APIRouter()

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
        for s_id in all_store_ids_list:
            store_report = generate_report_for_store(db, s_id, current_timestamp_utc)
            report_results.append(store_report)
    

        reports_cache[report_id]["data"] = report_results 
        reports_cache[report_id]["status"] = ReportStatusEnum.complete
        print(f"[{report_id}] Report generation complete. Total stores: {len(report_results)}")

    except Exception as e:
        reports_cache[report_id]["status"] = ReportStatusEnum.failed
        reports_cache[report_id]["data"] = {"error": str(e), "message": "Report generation failed."}
        print(f"[{report_id}] Report generation failed: {e}")
        import traceback
        traceback.print_exc() 
    finally:
        db.close()



@router.post("/trigger_report", status_code=status.HTTP_200_OK)
async def trigger_report(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
  
    report_id = str(uuid.uuid4()) 

    current_timestamp_utc_db = db.query(func.max(StoreStatus.timestamp_utc)).scalar()
    if not current_timestamp_utc_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No store status data found to generate a report. Please load data first."
        )
    
    if current_timestamp_utc_db.tzinfo is None:
        current_timestamp_utc = pytz.utc.localize(current_timestamp_utc_db)
    else:
        current_timestamp_utc = current_timestamp_utc_db.astimezone(pytz.utc)


    reports_cache[report_id] = {"status": ReportStatusEnum.pending, "data": None}


    background_tasks.add_task(_generate_report_task, SessionLocal(), report_id, current_timestamp_utc)

    print(f"Report generation triggered with ID: {report_id}")
    return {"report_id": report_id, "status": ReportStatusEnum.pending.value}


@router.get("/get_report/{report_id}") 
async def get_report(report_id: str):

    report_entry = reports_cache.get(report_id)

    if not report_entry:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Report with ID '{report_id}' not found."
        )

    status_enum = report_entry["status"] 

    if status_enum == ReportStatusEnum.complete:
        report_data = report_entry["data"]
        if not report_data: 
            print(f"[{report_id}] ERROR: Report status is complete but data is missing.")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Report data is missing after completion."
            )

        df = pd.DataFrame(report_data)

        expected_columns_order = [
            "store_id",
            "uptime_last_hour",
            "uptime_last_day",
            "uptime_last_week",
            "downtime_last_hour",
            "downtime_last_day",
            "downtime_last_week",
        ]
        
        final_columns = [col for col in expected_columns_order if col in df.columns]
        df = df[final_columns]

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False) 
        csv_string = csv_buffer.getvalue()

        return Response(
            content=csv_string,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=report_{report_id}.csv"}
        )
    elif status_enum == ReportStatusEnum.failed:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=report_entry.get("data", {"error": "Unknown error", "message": "Report generation failed."})
        )
    else: 
        print(f"Report status requested for ID {report_id}: {status_enum.value}")
        return {"report_id": report_id, "status": status_enum.value}
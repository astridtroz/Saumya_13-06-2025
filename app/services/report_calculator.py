import pandas as pd
from datetime import datetime , timedelta, time
import pytz
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
import math

from .. models import StoreStatus, BusinessHours, StoreTimezone, StoreStatusEnum
from ..database import SessionLocal
 
def get_store_config(db: Session, store_id: str):
    default_timezone_str='America/Chicago'
    timezone_obj= pytz.timezone(default_timezone_str)
    store_tz_record=db.query(StoreTimezone).filter(StoreTimezone.store_id==store_id).first()

    if store_tz_record:
        try:
            timezone_obj=pytz.timezone(store_tz_record.timezone_str)
        except pytz.UnknownTimeZoneError:
            print(f"Warning: Unknown timezone '{store_tz_record.timezone_str}' for store {store_id}. Using default '{default_timezone_str}'.")
    business_hours={}
    business_hours_records= db.query(BusinessHours).filter(BusinessHours.store_id==store_id).all()

    if business_hours_records:
        for bh in business_hours_records:
            business_hours.setdefault(bh.day_of_week, []).append({
                'start': bh.start_time_local,
                'end': bh.end_time_local
            })
    else:
       for day in range(7): 
            business_hours.setdefault(day, []).append({
                'start': time(0, 0, 0), 
                'end': time(23, 59, 59) 
            })
    return timezone_obj,business_hours

def is_within_business_hours(dt_local : datetime, business_hours: dict)->bool:
    day_of_week= dt_local.weekday()
    current_time=dt_local.time()

    if day_of_week not in business_hours:
        return False
    
    for bh_slot in business_hours[day_of_week]:
        start_time=bh_slot['start']
        end_time=bh_slot['end']
        
        if start_time<end_time:
            if start_time<=current_time<end_time:
                return True
        else:
            if current_time>=start_time or current_time<end_time:
                return True
    return False

def calculate_overlap_minutes(start_dt_utc: datetime, end_dt_utc: datetime,
                              business_hours: dict, timezone_obj: pytz.timezone) -> float:
    total_bh_minutes = 0.0
    current_dt_local = start_dt_utc.astimezone(timezone_obj)
    end_dt_local = end_dt_utc.astimezone(timezone_obj)

    if current_dt_local >= end_dt_local:
        return 0.0
    while current_dt_local < end_dt_local:
        next_minute_local = current_dt_local + timedelta(minutes=1)
        segment_end_local = min(next_minute_local, end_dt_local)

        if is_within_business_hours(current_dt_local, business_hours):
            duration_seconds = (segment_end_local - current_dt_local).total_seconds()
            total_bh_minutes += duration_seconds / 60.0
        current_dt_local = segment_end_local
    return total_bh_minutes


def get_status_for_period(db: Session, store_id: str, start_time_utc: datetime, end_time_utc: datetime):
    observations = db.query(StoreStatus).filter(
        StoreStatus.store_id == store_id,
        StoreStatus.timestamp_utc >= start_time_utc,
        StoreStatus.timestamp_utc <= end_time_utc
    ).order_by(StoreStatus.timestamp_utc).all()
    last_known_status_before_period = db.query(StoreStatus).filter(
        StoreStatus.store_id == store_id,
        StoreStatus.timestamp_utc < start_time_utc
    ).order_by(StoreStatus.timestamp_utc.desc()).first()

    if last_known_status_before_period:
        if not observations or observations[0].timestamp_utc != start_time_utc:
            initial_status_obj = StoreStatus(
                store_id=store_id,
                timestamp_utc=start_time_utc,
                status=last_known_status_before_period.status
            )
            observations.insert(0, initial_status_obj)
    return observations


def calculate_uptime_downtime_for_period(
    db: Session,
    store_id: str,
    period_start_utc: datetime,
    period_end_utc: datetime,
    timezone_obj: pytz.timezone,
    business_hours: dict
) -> tuple[float, float]:
    total_uptime_minutes = 0.0
    total_downtime_minutes = 0.0

    observations = get_status_for_period(db, store_id, period_start_utc, period_end_utc)
    if not observations:
        print(f"DEBUG: No polls found for store {store_id} in period {period_start_utc} to {period_end_utc}. Assuming online during business hours.")
        uptime_minutes = calculate_overlap_minutes(period_start_utc, period_end_utc, business_hours, timezone_obj)
        return uptime_minutes, 0.0
    
   
    current_status = None
    
    last_processed_timestamp_utc = period_start_utc 
    

    if observations[0].timestamp_utc == period_start_utc:
        current_status = observations[0].status
        observations.pop(0) 

   
    for obs in observations:
        segment_end_utc = obs.timestamp_utc
        if last_processed_timestamp_utc < segment_end_utc and current_status is not None:
       
            overlap_start_utc = max(last_processed_timestamp_utc, period_start_utc)
            overlap_end_utc = min(segment_end_utc, period_end_utc)

            if overlap_start_utc < overlap_end_utc:
                bh_overlap_minutes = calculate_overlap_minutes(
                    overlap_start_utc, overlap_end_utc, business_hours, timezone_obj
                )

                if current_status == StoreStatusEnum.active:
                    total_uptime_minutes += bh_overlap_minutes
                elif current_status == StoreStatusEnum.inactive:
                    total_downtime_minutes += bh_overlap_minutes

        current_status = obs.status
        last_processed_timestamp_utc = obs.timestamp_utc

    if last_processed_timestamp_utc < period_end_utc and current_status is not None:
        bh_overlap_minutes = calculate_overlap_minutes(
            last_processed_timestamp_utc, period_end_utc, business_hours, timezone_obj
        )
        if current_status == StoreStatusEnum.active:
            total_uptime_minutes += bh_overlap_minutes
        elif current_status == StoreStatusEnum.inactive:
            total_downtime_minutes += bh_overlap_minutes

    return total_uptime_minutes, total_downtime_minutes


def generate_report_for_store(db: Session, store_id: str, current_timestamp_utc: datetime) -> dict:
   
    timezone_obj, business_hours = get_store_config(db, store_id)


    last_hour_start_utc = current_timestamp_utc - timedelta(hours=1)
    uptime_1h_minutes, downtime_1h_minutes = calculate_uptime_downtime_for_period(
        db, store_id, last_hour_start_utc, current_timestamp_utc, timezone_obj, business_hours
    )
    last_day_start_utc = current_timestamp_utc - timedelta(days=1)
    uptime_24h_minutes, downtime_24h_minutes = calculate_uptime_downtime_for_period(
        db, store_id, last_day_start_utc, current_timestamp_utc, timezone_obj, business_hours
    )

    last_week_start_utc = current_timestamp_utc - timedelta(weeks=1)
    uptime_7d_minutes, downtime_7d_minutes = calculate_uptime_downtime_for_period(
        db, store_id, last_week_start_utc, current_timestamp_utc, timezone_obj, business_hours
    )
    return {
        "store_id": store_id,
        "uptime_last_hour": math.ceil(uptime_1h_minutes), # minutes
        "downtime_last_hour": math.ceil(downtime_1h_minutes), # minutes
        "uptime_last_day": math.ceil(uptime_24h_minutes / 60), # hours
        "downtime_last_day": math.ceil(downtime_24h_minutes / 60), # hours
        "uptime_last_week": math.ceil(uptime_7d_minutes / 60), # hours
        "downtime_last_week": math.ceil(downtime_7d_minutes / 60) # hours
    }


if __name__ == "__main__":
    import os
    import sys
    script_dir = os.path.dirname(os.path.abspath(__file__))
    app_dir_path = os.path.join(script_dir, '..')
    if app_dir_path not in sys.path:
        sys.path.insert(0, app_dir_path)
        print(f"DEBUG: Added '{app_dir_path}' to sys.path for standalone testing.")
    from database import SessionLocal

    db = SessionLocal()
    try:
        GLOBAL_MAX_TIMESTAMP_FOR_CALCULATION = datetime(2025, 1, 25, 18, 13, 22, tzinfo=pytz.utc)
        all_store_ids = db.query(StoreStatus.store_id).distinct().all()
        all_store_ids_list = [s[0] for s in all_store_ids]

        if all_store_ids_list:
            print(f"\nCalculating report for {len(all_store_ids_list)} stores up to {GLOBAL_MAX_TIMESTAMP_FOR_CALCULATION.isoformat()} UTC...\n")
            for s_id in all_store_ids_list:
                report_data = generate_report_for_store(db, s_id, GLOBAL_MAX_TIMESTAMP_FOR_CALCULATION)
                print(f"Report for Store {s_id}:")
                for key, value in report_data.items():
                    print(f"  {key}: {value}")
                print("-" * 30)
        else:
            print("No store status data found in the database. Ensure load_data.py was run successfully.")

    except Exception as e:
        print(f"An error occurred during standalone calculation: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


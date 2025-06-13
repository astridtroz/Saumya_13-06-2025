import pandas as pd
import datetime
from datetime import datetime, time
import pytz 
import os
import sys


sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))


from app.database import SessionLocal, engine, Base
from app.models import StoreStatus, BusinessHours, StoreTimezone, StoreStatusEnum, ReportStatusEnum


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data') 
STATUS_CSV = os.path.join(DATA_DIR, 'store_status.csv')
BUSINESS_HOURS_CSV = os.path.join(DATA_DIR, 'business_hours.csv')
TIMEZONES_CSV = os.path.join(DATA_DIR, 'timezones.csv')

DEFAULT_TIMEZONE = 'America/Chicago' 
GLOBAL_MAX_TIMESTAMP_UTC = None 


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



def load_timezones():
    print("Loading timezones...")
    db = next(get_db()) 
    try:
        df_tz = pd.read_csv(TIMEZONES_CSV)
        for index, row in df_tz.iterrows(): #for each store check and update newest timezone available
            store_id = str(row['store_id'])
            timezone_str = str(row['timezone_str'])
            existing_tz = db.query(StoreTimezone).filter_by(store_id=store_id).first()
            if existing_tz:
                if existing_tz.timezone_str != timezone_str:
                    existing_tz.timezone_str = timezone_str    
            else:
                new_tz = StoreTimezone(store_id=store_id, timezone_str=timezone_str)
                db.add(new_tz)
        db.commit() 
    except Exception as e:
        db.rollback() 
        print(f"Error loading timezones: {e}")
        raise
    finally:
        db.close()


def load_business_hours():
    db = next(get_db())
    try:
        df_bh = pd.read_csv(BUSINESS_HOURS_CSV)
        df_bh['dayOfWeek'] = df_bh['dayOfWeek'].astype(int)
        all_store_ids = set(df_bh['store_id'].unique())
        unique_store_ids_in_db = db.query(StoreStatus.store_id).distinct().all()
        unique_store_ids_in_db.extend(db.query(StoreTimezone.store_id).distinct().all())
        all_store_ids.update([s[0] for s in unique_store_ids_in_db])

        for store_id_int in all_store_ids:
            store_id = str(store_id_int)
            store_has_business_hours = False
            db.query(BusinessHours).filter_by(store_id=store_id).delete()

            store_df_bh = df_bh[df_bh['store_id'] == store_id_int]
            if not store_df_bh.empty:
                store_has_business_hours = True
                for index, row in store_df_bh.iterrows():
                    new_bh = BusinessHours(
                        store_id=store_id,
                        day_of_week=row['dayOfWeek'],
                        start_time_local=datetime.strptime(row['start_time_local'], '%H:%M:%S').time(),
                        end_time_local=datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
                    )
                    db.add(new_bh)
            else:
                for day in range(7): 
                    new_bh = BusinessHours(
                        store_id=store_id,
                        day_of_week=day,
                        start_time_local=time(0, 0, 0), 
                        end_time_local=time(23, 59, 59) 
                    )
                    db.add(new_bh)
        db.commit()
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()

def load_store_status():
    db = next(get_db())
    global GLOBAL_MAX_TIMESTAMP_UTC
    try:
        df_ss = pd.read_csv(STATUS_CSV)
        df_ss['timestamp_utc'] = pd.to_datetime(df_ss['timestamp_utc'], utc=True)

        if not df_ss.empty:
            GLOBAL_MAX_TIMESTAMP_UTC = df_ss['timestamp_utc'].max()

        else:
            GLOBAL_MAX_TIMESTAMP_UTC =datetime.datetime.now(pytz.utc) 

        db.query(StoreStatus).delete()
        for index, row in df_ss.iterrows():
            new_ss = StoreStatus(
                store_id=str(row['store_id']),
                timestamp_utc=row['timestamp_utc'],
                status=StoreStatusEnum(row['status']) 
            )
            db.add(new_ss)
        db.commit()
        print("Store status loaded.")

    except Exception as e:
        db.rollback()
        print(f"Error loading store status: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    print("Starting data ingestion process...")
    Base.metadata.create_all(bind=engine)
    load_timezones()
    load_business_hours()
    load_store_status()
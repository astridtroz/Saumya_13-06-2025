from sqlalchemy import Column, Integer, String, DateTime,Time, Enum, Float , Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import uuid
import datetime
import enum
from datetime import timezone

from app.database import Base

class StoreStatusEnum(enum.Enum):
    active="active"
    inactive="inactive"

class ReportStatusEnum(enum.Enum):
    running="Running"
    complete="Complete"
    failed="Failed"

class StoreStatus(Base):
    __tablename__="store_status"

    id=Column(Integer, primary_key=True, index=True)
    store_id=Column(String, index=True, nullable=False)
    timestamp_utc=Column(DateTime(timezone=True), nullable=False)
    status=Column(Enum(StoreStatusEnum), nullable=False)

class BusinessHours(Base):
    __tablename__="business_hours"

    id=Column(Integer, primary_key=True, index=True)
    store_id=Column(String, index=True, nullable=False)
    day_of_week=Column(Integer, nullable=False)
    start_time_local=Column(Time(timezone=False), nullable=False)
    end_time_local=Column(Time(timezone=False), nullable=False)

class StoreTimezone(Base):
    __tablename__="store_timezone"

    id=Column(Integer, primary_key=True, index=True)
    store_id=Column(String, unique= True, index= True, nullable=False)
    timezone_str=Column(String, nullable=False)

class Report(Base):
    __tablename__= "reports"

    id=Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique= True, index=True)
    status=Column(Enum(ReportStatusEnum), nullable=False, default= ReportStatusEnum.running)
    generated_at=Column(DateTime(timezone=True), default= datetime.timezone.utc)
    report_file_path= Column(String, nullable=True)
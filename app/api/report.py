from fastapi import APIRouter

router= APIRouter(
    prefix="/reports",
    tags=["Reports"]
)

@router.get("/")
async def read_reports_root():
    return {"message": "Report API root"}
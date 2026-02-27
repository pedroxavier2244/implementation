from fastapi import APIRouter, HTTPException, Query

from api.schemas.alerts import AlertOut
from shared.db import get_db_session
from shared.models import AlertEvent

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertOut])
def list_alerts(severity: str | None = None, limit: int = Query(20, le=100), offset: int = Query(0)):
    with get_db_session() as session:
        query = session.query(AlertEvent)
        if severity:
            query = query.filter(AlertEvent.severity == severity)
        items = query.order_by(AlertEvent.created_at.desc()).offset(offset).limit(limit).all()
        return [AlertOut.model_validate(item) for item in items]


@router.get("/{alert_id}", response_model=AlertOut)
def get_alert(alert_id: str):
    with get_db_session() as session:
        alert = session.query(AlertEvent).filter_by(id=alert_id).first()
        if alert is None:
            raise HTTPException(status_code=404, detail="Alert not found")
        return AlertOut.model_validate(alert)

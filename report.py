from database import SessionLocal
from models import KPIReport
from loguru import logger
import json
from datetime import datetime

class ReportGenerator:
    @staticmethod
    def save_and_generate(property_name: str, kpis: dict):
        db = SessionLocal()
        try:
            db_report = KPIReport(
                property_name=property_name,
                rooms_booked=kpis["rooms_booked"],
                ota_bookings=kpis["ota_bookings"],
                direct_bookings=kpis["direct_bookings"],
                occupancy_percentage=kpis["occupancy_percentage"],
                available_rooms=kpis["available_rooms"],
                adr=kpis["adr"],
                revpar=kpis["revpar"],
                cancellation_count=kpis["cancellation_count"],
                no_show_count=kpis["no_show_count"],
                alerts=json.dumps(kpis["alerts"])
            )
            db.add(db_report)
            db.commit()
            
            logger.info(f"--- Operational Report: {property_name} ---")
            logger.info(f"Occupancy: {kpis['occupancy_percentage']}% | ADR: ${kpis['adr']} | RevPAR: ${kpis['revpar']}")
            if kpis['alerts']:
                logger.warning(f"ALERTS: {', '.join(kpis['alerts'])}")
            logger.info("-" * 40)
            
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to save report for {property_name}: {e}")
        finally:
            db.close()

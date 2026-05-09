from loguru import logger

class KPIProcessor:
    @staticmethod
    def process(raw_data: dict, property_name: str):
        prop_data = raw_data.get(property_name, {})
        bookings = prop_data.get("bookings", [])
        inventory = prop_data.get("inventory", {"total": 0, "available": 0})

        total_rooms = inventory.get("total", 0)
        available_rooms = inventory.get("available", 0)
        rooms_booked = total_rooms - available_rooms

        ota_bookings = sum(1 for b in bookings if b.get("source") == "OTA" and b.get("status") == "Confirmed")
        direct_bookings = sum(1 for b in bookings if b.get("source") == "Direct" and b.get("status") == "Confirmed")
        cancellations = sum(1 for b in bookings if b.get("status") == "Cancelled")
        no_shows = sum(1 for b in bookings if b.get("status") == "No Show")
        total_revenue = sum(b.get("revenue", 0.0) for b in bookings if b.get("status") == "Confirmed")

        occupancy_percentage = (rooms_booked / total_rooms * 100) if total_rooms > 0 else 0
        adr = (total_revenue / rooms_booked) if rooms_booked > 0 else 0
        revpar = (total_revenue / total_rooms) if total_rooms > 0 else 0
        
        ota_dependency = (ota_bookings / (ota_bookings + direct_bookings) * 100) if (ota_bookings + direct_bookings) > 0 else 0

        alerts = []
        if occupancy_percentage < 50:
            alerts.append("Occupancy below 50%")
        elif occupancy_percentage > 90:
            alerts.append("Occupancy above 90%")
        
        if ota_dependency > 70:
            alerts.append("OTA dependency above 70%")

        kpi_data = {
            "rooms_booked": rooms_booked,
            "ota_bookings": ota_bookings,
            "direct_bookings": direct_bookings,
            "occupancy_percentage": round(occupancy_percentage, 2),
            "available_rooms": available_rooms,
            "adr": round(adr, 2),
            "revpar": round(revpar, 2),
            "cancellation_count": cancellations,
            "no_show_count": no_shows,
            "alerts": alerts
        }
        return kpi_data

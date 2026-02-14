from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


# --- Client → Server events ---


class LocationUpdate(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    timestamp: datetime
    accuracy: float = Field(..., ge=0)


class RideRequest(BaseModel):
    pickup_lat: float = Field(..., ge=-90, le=90)
    pickup_lon: float = Field(..., ge=-180, le=180)
    dropoff_lat: float = Field(..., ge=-90, le=90)
    dropoff_lon: float = Field(..., ge=-180, le=180)


class RideCancel(BaseModel):
    ride_id: UUID
    reason: str = Field(..., max_length=500)


class RideStatusRequest(BaseModel):
    ride_id: UUID


# --- Server → Client events (from Kafka) ---


class RideUpdateEvent(BaseModel):
    ride_id: str
    status: str
    passenger_id: str | None = None
    driver_id: str | None = None
    timestamp: datetime


class DriverAssignedEvent(BaseModel):
    ride_id: str
    passenger_id: str
    driver_id: str
    driver_name: str
    vehicle_make: str
    vehicle_model: str
    license_plate: str


class DriverLocationToPassengerEvent(BaseModel):
    ride_id: str
    passenger_id: str
    driver_id: str
    latitude: float
    longitude: float
    timestamp: datetime

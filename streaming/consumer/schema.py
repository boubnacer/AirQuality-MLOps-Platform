from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime


class Location(BaseModel):
    lat: float
    lon: float
    city: str
    zone: str


class Measurements(BaseModel):
    pm2_5: Optional[float] = None
    pm10: Optional[float] = None
    no2: Optional[float] = None
    o3: Optional[float] = None
    co: Optional[float] = None
    so2: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None

    @validator("pm2_5", "pm10", "no2", "o3", "so2", pre=True, always=True)
    def non_negative_ug(cls, v):
        if v is not None and v < 0:
            return None
        return v

    @validator("co", pre=True, always=True)
    def non_negative_co(cls, v):
        if v is not None and v < 0:
            return None
        return v

    @validator("temperature", pre=True, always=True)
    def valid_temp(cls, v):
        if v is not None and not (-50 <= v <= 60):
            return None
        return v

    @validator("humidity", pre=True, always=True)
    def valid_humidity(cls, v):
        if v is not None and not (0 <= v <= 100):
            return None
        return v


class SensorPayload(BaseModel):
    sensor_id: str
    timestamp: datetime
    location: Location
    measurements: Measurements
    battery_level: Optional[int] = Field(None, ge=0, le=100)
    status: Optional[str] = "active"

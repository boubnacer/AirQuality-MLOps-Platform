from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class LocationInput(BaseModel):
    lat: float
    lon: float
    city: str = "unknown"
    zone: str = "urban_background"


class MeasurementsInput(BaseModel):
    pm2_5: Optional[float] = None
    pm10: Optional[float] = None
    no2: Optional[float] = None
    o3: Optional[float] = None
    co: Optional[float] = None
    so2: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None


class PredictRequest(BaseModel):
    sensor_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    location: LocationInput
    measurements: MeasurementsInput
    battery_level: Optional[int] = Field(None, ge=0, le=100)
    status: Optional[str] = "active"


class PredictResponse(BaseModel):
    sensor_id: str
    timestamp: datetime
    anomaly_probability: Optional[float] = None
    is_anomaly: Optional[bool] = None
    pm2_5_forecast: Optional[float] = None
    no2_forecast: Optional[float] = None
    model_name: str
    model_version: str


class HealthResponse(BaseModel):
    status: str
    classifier_name: str
    classifier_version: str
    classifier_stage: str
    forecaster_name: str
    forecaster_version: str
    forecaster_stage: str
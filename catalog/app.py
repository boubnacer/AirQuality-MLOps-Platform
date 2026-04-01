import os
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from typing import List, Optional
from pydantic import BaseModel
from datetime import date

app = FastAPI(title="Air Quality Data Catalog", version="1.0.0")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "airquality"),
    "user": os.getenv("POSTGRES_USER", "airquality"),
    "password": os.getenv("POSTGRES_PASSWORD", "airquality123"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


class DataSource(BaseModel):
    id: int
    name: str
    source_type: str
    description: Optional[str]


class TableMeta(BaseModel):
    id: int
    table_name: str
    schema_name: str
    description: Optional[str]


class LineageEdge(BaseModel):
    source_name: str
    target_name: str
    transformation: Optional[str]


class QualityMetric(BaseModel):
    table_name: str
    pipeline_date: date
    row_count: Optional[int]
    null_rate: Optional[float]
    freshness_minutes: Optional[int]
    schema_drift: bool


@app.get("/sources", response_model=List[DataSource])
def list_sources():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, source_type, description FROM catalog.data_sources ORDER BY name")
            return [dict(r) for r in cur.fetchall()]


@app.get("/tables", response_model=List[TableMeta])
def list_tables(source_id: Optional[int] = None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            if source_id:
                cur.execute(
                    "SELECT id, table_name, schema_name, description FROM catalog.tables WHERE source_id = %s",
                    (source_id,),
                )
            else:
                cur.execute("SELECT id, table_name, schema_name, description FROM catalog.tables ORDER BY table_name")
            return [dict(r) for r in cur.fetchall()]


@app.get("/lineage", response_model=List[LineageEdge])
def get_lineage():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT source_name, target_name, transformation FROM catalog.lineage_edges ORDER BY id")
            return [dict(r) for r in cur.fetchall()]


@app.get("/quality", response_model=List[QualityMetric])
def get_quality(table_name: Optional[str] = None, days: int = 7):
    with get_conn() as conn:
        with conn.cursor() as cur:
            if table_name:
                cur.execute(
                    """
                    SELECT table_name, pipeline_date, row_count, null_rate, freshness_minutes, schema_drift
                    FROM catalog.quality_metrics
                    WHERE table_name = %s AND pipeline_date >= CURRENT_DATE - %s
                    ORDER BY pipeline_date DESC
                    """,
                    (table_name, days),
                )
            else:
                cur.execute(
                    """
                    SELECT table_name, pipeline_date, row_count, null_rate, freshness_minutes, schema_drift
                    FROM catalog.quality_metrics
                    WHERE pipeline_date >= CURRENT_DATE - %s
                    ORDER BY pipeline_date DESC, table_name
                    """,
                    (days,),
                )
            return [dict(r) for r in cur.fetchall()]


@app.get("/", response_class=HTMLResponse)
def index():
    return """
    <html><head><title>Air Quality Data Catalog</title>
    <style>body{font-family:monospace;max-width:800px;margin:40px auto;padding:0 20px}
    a{color:#2563eb}h1{color:#1e40af}</style></head>
    <body>
    <h1>Air Quality Data Catalog</h1>
    <ul>
      <li><a href="/sources">Data Sources</a></li>
      <li><a href="/tables">Tables</a></li>
      <li><a href="/lineage">Data Lineage</a></li>
      <li><a href="/quality">Quality Metrics</a></li>
      <li><a href="/docs">API Docs</a></li>
    </ul>
    </body></html>
    """


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

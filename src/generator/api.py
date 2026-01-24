"""HTTP API for generator control."""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import structlog

from src.generator.service import get_generator

logger = structlog.get_logger()

app = FastAPI(
    title="Logcelot Log Generator API",
    description="Control API for log generation service",
    version="1.0.0",
)


class ErrorSpikeRequest(BaseModel):
    """Request model for error spike."""
    error_count: int = Field(default=500, ge=1, le=10000)
    duration_seconds: int = Field(default=30, ge=1, le=600)


class DeadServiceRequest(BaseModel):
    """Request model for dead service simulation."""
    service_name: str = Field(..., min_length=1, max_length=100)
    duration: int = Field(default=300, ge=1, le=3600)


class ParseErrorRequest(BaseModel):
    """Request model for parse error injection."""
    count: int = Field(default=100, ge=1, le=1000)
    ratio: float = Field(default=0.1, ge=0.0, le=1.0)


class RPSRequest(BaseModel):
    """Request model for RPS update."""
    rps: int = Field(..., ge=1, le=10000)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "logcelot-log-generator",
        "status": "running",
        "endpoints": {
            "stats": "GET /stats",
            "pause": "POST /pause",
            "resume": "POST /resume",
            "set_rps": "POST /rps",
            "error_spike": "POST /scenario/spike",
            "dead_service": "POST /scenario/dead",
            "parse_errors": "POST /scenario/parse_errors",
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    generator = get_generator()
    stats = generator.get_stats()
    
    return {
        "status": "healthy" if generator.running else "stopped",
        "running": generator.running,
        "paused": generator.paused,
    }


@app.get("/stats")
async def get_stats():
    """Get generator statistics."""
    generator = get_generator()
    return generator.get_stats()


@app.post("/start")
async def start_generator():
    """Start log generation."""
    generator = get_generator()
    
    if generator.running:
        raise HTTPException(status_code=400, detail="Generator already running")
    
    await generator.start()
    
    return {"status": "started"}


@app.post("/stop")
async def stop_generator():
    """Stop log generation."""
    generator = get_generator()
    
    if not generator.running:
        raise HTTPException(status_code=400, detail="Generator not running")
    
    await generator.stop()
    
    return {"status": "stopped"}


@app.post("/pause")
async def pause_generator():
    """Pause log generation."""
    generator = get_generator()
    
    if not generator.running:
        raise HTTPException(status_code=400, detail="Generator not running")
    
    return generator.pause()


@app.post("/resume")
async def resume_generator():
    """Resume log generation."""
    generator = get_generator()
    
    if not generator.running:
        raise HTTPException(status_code=400, detail="Generator not running")
    
    return generator.resume()


@app.post("/rps")
async def set_rps(request: RPSRequest):
    """Set target RPS for generation."""
    generator = get_generator()
    return generator.set_rps(request.rps)


@app.post("/scenario/spike")
async def trigger_error_spike(request: ErrorSpikeRequest):
    """Trigger error spike scenario."""
    generator = get_generator()
    
    if not generator.running:
        raise HTTPException(status_code=400, detail="Generator not running")
    
    result = await generator.trigger_error_spike(
        error_count=request.error_count,
        duration_seconds=request.duration_seconds
    )
    
    return result


@app.post("/scenario/dead")
async def simulate_dead_service(request: DeadServiceRequest):
    """Simulate dead service scenario."""
    generator = get_generator()
    
    if not generator.running:
        raise HTTPException(status_code=400, detail="Generator not running")
    
    result = await generator.simulate_dead_service(
        service_name=request.service_name,
        duration=request.duration
    )
    
    return result


@app.post("/scenario/parse_errors")
async def inject_parse_errors(request: ParseErrorRequest):
    """Inject parse errors for DLQ testing."""
    generator = get_generator()
    
    if not generator.running:
        raise HTTPException(status_code=400, detail="Generator not running")
    
    result = await generator.inject_parse_errors(
        count=request.count,
        ratio=request.ratio
    )
    
    return result

"""Scenario controller for generating different log patterns."""

import asyncio
from enum import Enum
from typing import Optional
import structlog

from src.generator.templates import LogTemplateFactory

logger = structlog.get_logger()


class ScenarioType(str, Enum):
    """Available generation scenarios."""
    NORMAL = "normal"
    ERROR_SPIKE = "error_spike"
    DEAD_SERVICE = "dead_service"
    PARSE_ERROR = "parse_error"
    DEGRADATION = "degradation"


class ScenarioController:
    """Controller for managing log generation scenarios."""
    
    def __init__(self):
        self.template_factory = LogTemplateFactory()
        self.excluded_services: set[str] = set()
        self.current_scenario: Optional[ScenarioType] = None
        self._running = False
    
    def is_running(self) -> bool:
        """Check if scenario is currently running."""
        return self._running
    
    async def run_normal_operation(
        self,
        rps: int = 100,
        level_distribution: dict[str, float] | None = None
    ) -> list[dict]:
        """Generate normal operation logs.
        
        Args:
            rps: Requests per second (logs to generate)
            level_distribution: Distribution of log levels
            
        Returns:
            List of log entries
        """
        self.current_scenario = ScenarioType.NORMAL
        self._running = True
        
        if level_distribution is None:
            level_distribution = {
                "INFO": 0.85,
                "WARN": 0.10,
                "ERROR": 0.04,
                "FATAL": 0.01,
            }
        
        logs = self.template_factory.generate_batch(
            count=rps,
            level_distribution=level_distribution
        )
        
        # Filter out excluded services (for dead service scenario)
        if self.excluded_services:
            logs = [
                log for log in logs
                if log.get("source_service") not in self.excluded_services
            ]
        
        return logs
    
    async def generate_error_spike(
        self,
        error_count: int = 500,
        duration_seconds: int = 30,
        error_type: str | None = None
    ) -> list[dict]:
        """Generate error spike for demo.
        
        Args:
            error_count: Total number of errors to generate
            duration_seconds: Duration of spike
            error_type: Specific error type to generate (optional)
            
        Returns:
            List of error log entries
        """
        self.current_scenario = ScenarioType.ERROR_SPIKE
        self._running = True
        
        errors_per_batch = error_count // duration_seconds
        
        logger.warning(
            "error_spike_started",
            error_count=error_count,
            duration=duration_seconds,
            errors_per_batch=errors_per_batch,
        )
        
        errors = []
        for _ in range(errors_per_batch):
            error_log = self.template_factory.error_log()
            if error_type:
                error_log["error_type"] = error_type
            errors.append(error_log)
        
        return errors
    
    async def simulate_dead_service(
        self,
        service_name: str,
        duration: int = 300
    ) -> None:
        """Simulate dead service by excluding it from generation.
        
        Args:
            service_name: Name of service to mark as dead
            duration: Duration in seconds
        """
        self.current_scenario = ScenarioType.DEAD_SERVICE
        self._running = True
        
        logger.warning(
            "dead_service_simulation_started",
            service=service_name,
            duration=duration,
        )
        
        self.excluded_services.add(service_name)
        
        # Schedule service resurrection
        await asyncio.sleep(duration)
        
        self.excluded_services.discard(service_name)
        
        logger.info(
            "dead_service_simulation_ended",
            service=service_name,
        )
        
        self._running = False
    
    async def generate_parse_errors(
        self,
        count: int = 10,
        ratio: float = 0.1
    ) -> list[str]:
        """Generate malformed logs for DLQ testing.
        
        Args:
            count: Number of logs to generate
            ratio: Ratio of malformed logs (0.0 to 1.0)
            
        Returns:
            List of log strings (some malformed)
        """
        self.current_scenario = ScenarioType.PARSE_ERROR
        self._running = True
        
        malformed_count = int(count * ratio)
        valid_count = count - malformed_count
        
        logs = []
        
        # Generate valid logs
        valid_logs = self.template_factory.generate_batch(valid_count)
        logs.extend([str(log) for log in valid_logs])
        
        # Generate malformed logs
        for _ in range(malformed_count):
            malformed = self.template_factory.malformed_log()
            logs.append(malformed)
        
        logger.info(
            "parse_errors_generated",
            total=count,
            malformed=malformed_count,
            valid=valid_count,
        )
        
        return logs
    
    async def simulate_degradation(
        self,
        rps: int = 100,
        degradation_factor: float = 0.3
    ) -> list[dict]:
        """Simulate service degradation with increased response times and warnings.
        
        Args:
            rps: Requests per second
            degradation_factor: Factor for increasing response times (0.0 to 1.0)
            
        Returns:
            List of log entries with degraded performance
        """
        self.current_scenario = ScenarioType.DEGRADATION
        self._running = True
        
        # Generate logs with more warnings
        level_distribution = {
            "INFO": 0.60,
            "WARN": 0.30,  # Increased from 0.10
            "ERROR": 0.08,  # Increased from 0.04
            "FATAL": 0.02,  # Increased from 0.01
        }
        
        logs = self.template_factory.generate_batch(
            count=rps,
            level_distribution=level_distribution
        )
        
        # Increase response times for HTTP logs
        for log in logs:
            if "http_response_time_ms" in log:
                original_time = log["http_response_time_ms"]
                log["http_response_time_ms"] = int(original_time * (1 + degradation_factor))
                
                # Update message
                if log["http_response_time_ms"] > 1000:
                    log["level"] = "WARN"
                    log["message"] += " [SLOW]"
        
        return logs
    
    def stop(self) -> None:
        """Stop current scenario."""
        self._running = False
        self.current_scenario = None
        logger.info("scenario_stopped")
    
    def get_status(self) -> dict:
        """Get current scenario status."""
        return {
            "running": self._running,
            "current_scenario": self.current_scenario.value if self.current_scenario else None,
            "excluded_services": list(self.excluded_services),
        }

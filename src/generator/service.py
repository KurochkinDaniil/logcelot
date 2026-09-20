"""Main log generator service."""

import asyncio
from typing import Optional
import structlog

from src.generator.config import GeneratorSettings, get_generator_settings
from src.generator.kafka_producer import ResilientKafkaProducer, CircuitBreakerOpenError
from src.generator.scenarios import ScenarioController, ScenarioType
from src.generator.templates import LogTemplateFactory

logger = structlog.get_logger()


class LogGeneratorService:
    """Main log generator service.
    
    Orchestrates log generation with different scenarios and manages
    Kafka producer with automatic failover.
    """
    
    def __init__(self, settings: Optional[GeneratorSettings] = None):
        """Initialize generator service.
        
        Args:
            settings: Generator settings (defaults to environment config)
        """
        self.settings = settings or get_generator_settings()
        
        self.producer = ResilientKafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            topic=self.settings.kafka_topic,
        )
        
        self.scenario_controller = ScenarioController()
        self.template_factory = LogTemplateFactory()
        
        self.running = False
        self.paused = False
        self.current_rps = self.settings.generator_rps
        
        # Statistics
        self.total_generated = 0
        self.total_sent = 0
        self.total_failed = 0
    
    async def start(self) -> None:
        """Start the generator service."""
        if self.running:
            logger.warning("generator_already_running")
            return
        
        await self.producer.start()
        self.running = True
        
        logger.info(
            "generator_service_started",
            kafka_brokers=self.settings.kafka_bootstrap_servers,
            topic=self.settings.kafka_topic,
            target_rps=self.current_rps,
            scenario=self.settings.generator_scenario,
        )
        
        # Start generation loop
        asyncio.create_task(self._generation_loop())
    
    async def stop(self) -> None:
        """Stop the generator service."""
        if not self.running:
            return
        
        self.running = False
        await self.producer.stop()
        
        logger.info(
            "generator_service_stopped",
            total_generated=self.total_generated,
            total_sent=self.total_sent,
            total_failed=self.total_failed,
        )
    
    async def _generation_loop(self) -> None:
        """Main generation loop."""
        logger.info("generation_loop_started")
        
        while self.running:
            if self.paused:
                await asyncio.sleep(1)
                continue
            
            try:
                # Generate logs based on current scenario
                logs = await self.scenario_controller.run_normal_operation(
                    rps=self.current_rps
                )
                
                self.total_generated += len(logs)
                
                # Send logs to Kafka
                await self._send_logs(logs)
                
                # Wait for next batch (1 second interval for RPS control)
                await asyncio.sleep(1)
                
            except CircuitBreakerOpenError:
                logger.warning(
                    "circuit_breaker_open_pausing_generation",
                    pause_duration=30,
                )
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(
                    "generation_loop_error",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                await asyncio.sleep(5)
    
    async def _send_logs(self, logs: list[dict]) -> None:
        """Send logs to Kafka with error handling.
        
        Args:
            logs: List of log entries to send
        """
        for log in logs:
            try:
                await self.producer.send_with_failover(log)
                self.total_sent += 1
                
            except CircuitBreakerOpenError:
                # Circuit breaker open - stop sending
                self.total_failed += 1
                raise
                
            except Exception as e:
                logger.error(
                    "log_send_failed",
                    error=str(e),
                    log_preview=str(log)[:100],
                )
                self.total_failed += 1
    
    async def trigger_error_spike(
        self,
        error_count: int = 500,
        duration_seconds: int = 30
    ) -> dict:
        """Trigger error spike scenario.
        
        Args:
            error_count: Total number of errors to generate
            duration_seconds: Duration of spike in seconds
            
        Returns:
            Status dict with spike details
        """
        logger.warning(
            "triggering_error_spike",
            error_count=error_count,
            duration=duration_seconds,
        )
        
        errors_per_second = error_count // duration_seconds
        spike_start = self.total_sent
        
        for _ in range(duration_seconds):
            if not self.running:
                break
            
            error_logs = await self.scenario_controller.generate_error_spike(
                error_count=errors_per_second,
                duration_seconds=1
            )
            
            await self._send_logs(error_logs)
            await asyncio.sleep(1)
        
        spike_sent = self.total_sent - spike_start
        
        logger.info(
            "error_spike_completed",
            errors_sent=spike_sent,
            target=error_count,
        )
        
        return {
            "status": "completed",
            "errors_sent": spike_sent,
            "target": error_count,
            "duration": duration_seconds,
        }
    
    async def simulate_dead_service(
        self,
        service_name: str,
        duration: int = 300
    ) -> dict:
        """Simulate a dead service.
        
        Args:
            service_name: Name of service to mark as dead
            duration: Duration in seconds
            
        Returns:
            Status dict
        """
        logger.warning(
            "simulating_dead_service",
            service=service_name,
            duration=duration,
        )
        
        # Run simulation in background
        asyncio.create_task(
            self.scenario_controller.simulate_dead_service(service_name, duration)
        )
        
        return {
            "status": "started",
            "service": service_name,
            "duration": duration,
        }
    
    async def inject_parse_errors(
        self,
        count: int = 100,
        ratio: float = 0.1
    ) -> dict:
        """Inject parse errors for DLQ testing.
        
        Args:
            count: Number of logs to generate
            ratio: Ratio of malformed logs
            
        Returns:
            Status dict
        """
        logger.info(
            "injecting_parse_errors",
            count=count,
            malformed_ratio=ratio,
        )
        
        malformed_logs = await self.scenario_controller.generate_parse_errors(
            count=count,
            ratio=ratio
        )
        
        sent = 0
        failed = 0
        
        for log_str in malformed_logs:
            try:
                # Send as raw string (some will be malformed)
                await self.producer.producer.send_and_wait(
                    self.settings.kafka_topic,
                    value=log_str.encode("utf-8")
                )
                sent += 1
            except Exception:
                failed += 1
        
        return {
            "status": "completed",
            "total": count,
            "sent": sent,
            "failed": failed,
            "malformed_ratio": ratio,
        }
    
    def pause(self) -> dict:
        """Pause log generation."""
        self.paused = True
        logger.info("generator_paused")
        return {"status": "paused"}
    
    def resume(self) -> dict:
        """Resume log generation."""
        self.paused = False
        logger.info("generator_resumed")
        return {"status": "resumed"}
    
    def set_rps(self, rps: int) -> dict:
        """Set target RPS for generation.
        
        Args:
            rps: Target requests per second
            
        Returns:
            Status dict with new RPS
        """
        old_rps = self.current_rps
        self.current_rps = rps
        
        logger.info(
            "rps_updated",
            old_rps=old_rps,
            new_rps=rps,
        )
        
        return {
            "status": "updated",
            "old_rps": old_rps,
            "new_rps": rps,
        }
    
    def get_stats(self) -> dict:
        """Get generator statistics.
        
        Returns:
            Dict with generator stats
        """
        producer_stats = self.producer.get_stats()
        scenario_status = self.scenario_controller.get_status()
        
        return {
            "running": self.running,
            "paused": self.paused,
            "current_rps": self.current_rps,
            "total_generated": self.total_generated,
            "total_sent": self.total_sent,
            "total_failed": self.total_failed,
            "producer": producer_stats,
            "scenario": scenario_status,
        }


# Global generator instance
_generator_instance: Optional[LogGeneratorService] = None


def get_generator() -> LogGeneratorService:
    """Get or create global generator instance."""
    global _generator_instance
    
    if _generator_instance is None:
        _generator_instance = LogGeneratorService()
    
    return _generator_instance

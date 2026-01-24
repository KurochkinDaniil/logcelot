"""Resilient Kafka producer with failover support."""

import asyncio
import json
import time
from typing import Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaConnectionError
import structlog

logger = structlog.get_logger()


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class ResilientKafkaProducer:
    """Kafka producer with automatic failover and circuit breaker.
    
    Features:
    - Round-robin broker selection on failure
    - Automatic reconnection with exponential backoff
    - Circuit breaker pattern to prevent overwhelming failed cluster
    - Metrics tracking for sent/failed messages
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        max_failures: int = 10,
        circuit_open_duration: int = 30,
    ):
        """Initialize resilient Kafka producer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            topic: Topic name for log messages
            max_failures: Number of consecutive failures before opening circuit
            circuit_open_duration: Seconds to wait before attempting reconnection
        """
        self.brokers = [s.strip() for s in bootstrap_servers.split(",")]
        self.topic = topic
        self.max_failures = max_failures
        self.circuit_open_duration = circuit_open_duration
        
        self.producer: Optional[AIOKafkaProducer] = None
        self.current_broker_idx = 0
        
        # Circuit breaker state
        self.circuit_open = False
        self.circuit_open_time: Optional[float] = None
        self.failed_attempts = 0
        
        # Metrics
        self.messages_sent = 0
        self.messages_failed = 0
        self.reconnections = 0
        
        self._started = False
    
    async def start(self) -> None:
        """Start the Kafka producer."""
        await self._connect_to_broker(self.current_broker_idx)
        self._started = True
        logger.info(
            "resilient_producer_started",
            brokers=self.brokers,
            topic=self.topic,
        )
    
    async def stop(self) -> None:
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            self.producer = None
        self._started = False
        logger.info(
            "resilient_producer_stopped",
            messages_sent=self.messages_sent,
            messages_failed=self.messages_failed,
            reconnections=self.reconnections,
        )
    
    async def send_with_failover(self, log_data: dict[str, Any]) -> None:
        """Send log with automatic failover on broker failure.
        
        Args:
            log_data: Log entry data to send
            
        Raises:
            CircuitBreakerOpenError: If circuit breaker is open
        """
        if not self._started:
            raise RuntimeError("Producer not started. Call start() first.")
        
        # Check circuit breaker
        if self.circuit_open:
            await self._check_circuit_breaker()
            if self.circuit_open:
                raise CircuitBreakerOpenError(
                    f"Circuit breaker open. Will retry after {self.circuit_open_duration}s"
                )
        
        max_retries = len(self.brokers) * 2
        
        for attempt in range(max_retries):
            try:
                # Send message
                value = json.dumps(log_data).encode("utf-8")
                await self.producer.send_and_wait(self.topic, value=value)
                
                # Success
                self.messages_sent += 1
                self.failed_attempts = 0
                return
                
            except KafkaConnectionError as e:
                logger.warning(
                    "broker_connection_failed",
                    broker_idx=self.current_broker_idx,
                    broker=self.brokers[self.current_broker_idx],
                    attempt=attempt + 1,
                    error=str(e),
                )
                await self._reconnect_to_next_broker()
                await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                
            except KafkaError as e:
                logger.error(
                    "kafka_send_error",
                    error=str(e),
                    error_type=type(e).__name__,
                    attempt=attempt + 1,
                )
                self.messages_failed += 1
                self.failed_attempts += 1
                
                if self.failed_attempts >= self.max_failures:
                    self._open_circuit_breaker()
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker opened after {self.max_failures} failures"
                    )
                
                await asyncio.sleep(0.1 * (attempt + 1))
                
            except Exception as e:
                logger.error(
                    "unexpected_send_error",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                self.messages_failed += 1
                raise
        
        # All retries exhausted
        self.messages_failed += 1
        self._open_circuit_breaker()
        raise CircuitBreakerOpenError("All brokers unavailable after max retries")
    
    async def _connect_to_broker(self, broker_idx: int) -> None:
        """Connect to specific broker by index."""
        broker = self.brokers[broker_idx]
        
        try:
            if self.producer:
                await self.producer.stop()
            
            self.producer = AIOKafkaProducer(
                bootstrap_servers=broker,
                compression_type="gzip",
                acks=1,
                request_timeout_ms=5000,
                retry_backoff_ms=100,
            )
            
            await self.producer.start()
            
            logger.info(
                "connected_to_broker",
                broker=broker,
                broker_idx=broker_idx,
            )
            
        except Exception as e:
            logger.error(
                "broker_connection_failed",
                broker=broker,
                broker_idx=broker_idx,
                error=str(e),
            )
            raise
    
    async def _reconnect_to_next_broker(self) -> None:
        """Reconnect to next broker in round-robin fashion."""
        self.current_broker_idx = (self.current_broker_idx + 1) % len(self.brokers)
        self.reconnections += 1
        
        logger.info(
            "reconnecting_to_next_broker",
            broker_idx=self.current_broker_idx,
            broker=self.brokers[self.current_broker_idx],
            reconnections=self.reconnections,
        )
        
        try:
            await self._connect_to_broker(self.current_broker_idx)
        except Exception as e:
            logger.error("reconnection_failed", error=str(e))
    
    async def _is_producer_healthy(self) -> bool:
        """Check if producer connection is healthy."""
        if not self.producer:
            return False
        
        # Simple check - producer is started
        return hasattr(self.producer, "_client") and self.producer._client is not None
    
    def _open_circuit_breaker(self) -> None:
        """Open circuit breaker after too many failures."""
        self.circuit_open = True
        self.circuit_open_time = time.time()
        
        logger.warning(
            "circuit_breaker_opened",
            failed_attempts=self.failed_attempts,
            duration=self.circuit_open_duration,
        )
    
    async def _check_circuit_breaker(self) -> None:
        """Check if circuit breaker should be closed."""
        if not self.circuit_open or not self.circuit_open_time:
            return
        
        elapsed = time.time() - self.circuit_open_time
        
        if elapsed >= self.circuit_open_duration:
            self.circuit_open = False
            self.circuit_open_time = None
            self.failed_attempts = 0
            
            logger.info(
                "circuit_breaker_closed",
                elapsed_seconds=int(elapsed),
            )
            
            # Try reconnecting
            await self._reconnect_to_next_broker()
    
    def get_stats(self) -> dict[str, Any]:
        """Get producer statistics."""
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "reconnections": self.reconnections,
            "circuit_open": self.circuit_open,
            "current_broker": self.brokers[self.current_broker_idx],
            "failed_attempts": self.failed_attempts,
        }

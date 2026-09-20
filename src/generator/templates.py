"""Log templates for realistic data generation."""

import random
from datetime import datetime, timedelta
from typing import Any, Callable
from faker import Faker

fake = Faker()


class LogTemplateFactory:
    """Factory for generating realistic log entries."""
    
    # Service names
    SERVICES = [
        "api-gateway",
        "auth-service",
        "user-service",
        "order-service",
        "payment-service",
        "notification-service",
        "inventory-service",
        "analytics-service",
    ]
    
    # HTTP paths
    HTTP_PATHS = [
        "/api/users",
        "/api/users/{id}",
        "/api/orders",
        "/api/orders/{id}",
        "/api/products",
        "/api/products/{id}",
        "/api/auth/login",
        "/api/auth/logout",
        "/api/payments",
        "/health",
        "/metrics",
    ]
    
    # Error types
    ERROR_TYPES = [
        "DatabaseConnectionError",
        "ValidationError",
        "TimeoutError",
        "NullPointerException",
        "OutOfMemoryError",
        "ServiceUnavailableError",
        "AuthenticationError",
        "RateLimitExceeded",
    ]
    
    @staticmethod
    def web_access_log() -> dict[str, Any]:
        """Generate web access log (nginx/apache style)."""
        methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]
        
        # Status code distribution (realistic)
        status_distribution = {
            200: 0.65,
            201: 0.10,
            204: 0.05,
            400: 0.05,
            401: 0.03,
            404: 0.04,
            500: 0.04,
            503: 0.04,
        }
        
        method = random.choice(methods)
        path = random.choice(LogTemplateFactory.HTTP_PATHS).replace("{id}", str(random.randint(1, 10000)))
        status = random.choices(
            list(status_distribution.keys()),
            weights=list(status_distribution.values())
        )[0]
        
        # Response time based on status
        if status >= 500:
            duration = random.randint(1000, 10000)  # Slower on errors
        elif status == 404:
            duration = random.randint(5, 50)  # Fast on 404
        else:
            duration = random.randint(10, 500)  # Normal
        
        user_id = fake.uuid4() if random.random() > 0.3 else None
        
        level = "ERROR" if status >= 500 else "WARN" if status >= 400 else "INFO"
        
        return {
            "source": "http",
            "source_service": "api-gateway",
            "source_host": fake.ipv4(),
            "level": level,
            "message": f"{method} {path} - {status} ({duration}ms)",
            "format": "json",
            "http_method": method,
            "http_path": path,
            "http_status": status,
            "http_response_time_ms": duration,
            "user_id": user_id,
            "request_id": fake.uuid4(),
            "trace_id": fake.uuid4(),
            "metadata": {
                "user_agent": fake.user_agent(),
                "client_ip": fake.ipv4(),
            }
        }
    
    @staticmethod
    def application_log(level: str = "INFO", service: str | None = None) -> dict[str, Any]:
        """Generate application log entry."""
        if service is None:
            service = random.choice(LogTemplateFactory.SERVICES)
        
        messages = {
            "INFO": [
                "Request processed successfully",
                "User authenticated",
                "Cache hit for key",
                "Database connection established",
                "Background job completed",
                "Configuration reloaded",
                "Health check passed",
            ],
            "WARN": [
                "Slow query detected (>1000ms)",
                "Cache miss for frequently accessed key",
                "Retry attempt {attempt} for operation",
                "Deprecated API endpoint called",
                "High memory usage detected",
                "Connection pool nearly exhausted",
            ],
            "ERROR": [
                "Failed to process payment",
                "Database query timeout",
                "External API returned error",
                "Invalid request payload",
                "Authentication failed",
                "Rate limit exceeded",
            ],
            "FATAL": [
                "Database connection pool exhausted",
                "Out of memory error",
                "Critical service dependency unavailable",
                "Unrecoverable error in transaction",
            ]
        }
        
        message = random.choice(messages.get(level, messages["INFO"]))
        
        log = {
            "source": "kafka",
            "source_service": service,
            "source_host": fake.hostname(),
            "level": level,
            "message": message,
            "format": "json",
            "trace_id": fake.uuid4(),
            "request_id": fake.uuid4(),
            "metadata": {
                "environment": "production",
                "version": f"v{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 99)}",
            }
        }
        
        # Add user_id for some logs
        if random.random() > 0.5:
            log["user_id"] = fake.uuid4()
        
        # Add error details for ERROR/FATAL
        if level in ["ERROR", "FATAL"]:
            log["error_type"] = random.choice(LogTemplateFactory.ERROR_TYPES)
            log["error_stack"] = fake.text(max_nb_chars=500)
        
        return log
    
    @staticmethod
    def database_log() -> dict[str, Any]:
        """Generate database-related log."""
        operations = ["SELECT", "INSERT", "UPDATE", "DELETE"]
        tables = ["users", "orders", "products", "payments", "inventory"]
        
        operation = random.choice(operations)
        table = random.choice(tables)
        duration = random.randint(5, 2000)
        
        level = "WARN" if duration > 1000 else "INFO"
        
        return {
            "source": "kafka",
            "source_service": "database-proxy",
            "source_host": fake.hostname(),
            "level": level,
            "message": f"{operation} query on {table} completed in {duration}ms",
            "format": "json",
            "metadata": {
                "query_type": operation,
                "table": table,
                "duration_ms": duration,
                "rows_affected": random.randint(1, 100),
            }
        }
    
    @staticmethod
    def syslog_log() -> dict[str, Any]:
        """Generate syslog-style system log."""
        facilities = ["kernel", "user", "mail", "daemon", "auth", "syslog", "local0"]
        
        messages = [
            "Service started successfully",
            "Configuration file reloaded",
            "System resource usage within normal parameters",
            "Scheduled backup completed",
            "Certificate will expire in 30 days",
            "Disk usage at 75%",
        ]
        
        return {
            "source": "file",
            "source_service": "syslog",
            "source_host": fake.hostname(),
            "level": "INFO",
            "message": random.choice(messages),
            "format": "syslog",
            "metadata": {
                "facility": random.choice(facilities),
                "pid": random.randint(1000, 99999),
            }
        }
    
    @staticmethod
    def error_log(service: str | None = None) -> dict[str, Any]:
        """Generate error log with stack trace."""
        if service is None:
            service = random.choice(LogTemplateFactory.SERVICES)
        
        error_type = random.choice(LogTemplateFactory.ERROR_TYPES)
        
        error_messages = {
            "DatabaseConnectionError": "Failed to establish database connection",
            "ValidationError": "Request validation failed: invalid email format",
            "TimeoutError": "Operation timed out after 30 seconds",
            "NullPointerException": "Attempted to access null reference",
            "OutOfMemoryError": "Java heap space exhausted",
            "ServiceUnavailableError": "Downstream service not responding",
            "AuthenticationError": "Invalid credentials provided",
            "RateLimitExceeded": "API rate limit exceeded for user",
        }
        
        return {
            "source": "kafka",
            "source_service": service,
            "source_host": fake.hostname(),
            "level": "ERROR",
            "message": error_messages.get(error_type, "An error occurred"),
            "format": "json",
            "error_type": error_type,
            "error_stack": fake.text(max_nb_chars=800),
            "trace_id": fake.uuid4(),
            "request_id": fake.uuid4(),
            "user_id": fake.uuid4() if random.random() > 0.5 else None,
            "metadata": {
                "error_code": f"ERR_{random.randint(1000, 9999)}",
            }
        }
    
    @staticmethod
    def malformed_log() -> str:
        """Generate intentionally malformed log for DLQ testing."""
        malformed_types = [
            '{"invalid": "json", missing_fields',  # Incomplete JSON
            '{"source": 123, "level": "INFO"}',  # Wrong types
            'not json at all',  # Plain text
            '{"level": "INVALID_LEVEL", "source": "test"}',  # Invalid enum
            '{}',  # Empty object
        ]
        return random.choice(malformed_types)
    
    @staticmethod
    def generate_batch(count: int, level_distribution: dict[str, float] | None = None) -> list[dict[str, Any]]:
        """Generate a batch of logs with specified level distribution.
        
        Args:
            count: Number of logs to generate
            level_distribution: Dict with levels and their probabilities
                               Default: {"INFO": 0.85, "WARN": 0.10, "ERROR": 0.04, "FATAL": 0.01}
        """
        if level_distribution is None:
            level_distribution = {
                "INFO": 0.85,
                "WARN": 0.10,
                "ERROR": 0.04,
                "FATAL": 0.01,
            }
        
        logs = []
        for _ in range(count):
            # Choose level based on distribution
            level = random.choices(
                list(level_distribution.keys()),
                weights=list(level_distribution.values())
            )[0]
            
            # Choose log type
            log_type = random.choices(
                ["web", "app", "db", "syslog"],
                weights=[0.40, 0.35, 0.15, 0.10]
            )[0]
            
            if log_type == "web":
                log = LogTemplateFactory.web_access_log()
            elif log_type == "db":
                log = LogTemplateFactory.database_log()
            elif log_type == "syslog":
                log = LogTemplateFactory.syslog_log()
            else:
                log = LogTemplateFactory.application_log(level=level)
            
            logs.append(log)
        
        return logs

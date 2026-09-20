"""Locust load testing for Logcelot API."""

import random
from datetime import datetime
from locust import HttpUser, task, between
from faker import Faker

fake = Faker()

LOG_LEVELS = ['DEBUG', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO', 'INFO', 
              'WARN', 'WARN', 'ERROR', 'FATAL']

SERVICES = ['auth-api', 'user-api', 'payment-api', 'notification-api',
            'order-api', 'inventory-api', 'analytics-api', 'gateway']


class LogcelotUser(HttpUser):
    wait_time = between(0.1, 0.5)
    
    def on_start(self):
        self.service_name = random.choice(SERVICES)
    
    @task(70)
    def send_single_json_log(self):
        log = {
            'source': 'http',
            'source_service': self.service_name,
            'level': random.choice(LOG_LEVELS),
            'message': f"Request to {fake.uri_path()} completed",
            'payload': {
                'user_id': fake.uuid4(),
                'trace_id': fake.uuid4(),
                'http_method': random.choice(['GET', 'POST', 'PUT', 'DELETE']),
                'http_path': fake.uri_path(),
                'http_status': random.choice([200, 201, 400, 404, 500]),
                'response_time_ms': random.randint(10, 500),
            }
        }
        
        self.client.post("/logs", json=log, name="/logs [single]")
    
    @task(25)
    def send_batch_json_logs(self):
        batch_size = random.randint(10, 50)
        logs = [
            {
                'source': 'http',
                'source_service': self.service_name,
                'level': random.choice(LOG_LEVELS),
                'message': fake.sentence(),
                'payload': {'trace_id': fake.uuid4()}
            }
            for _ in range(batch_size)
        ]
        
        self.client.post("/logs/batch", json={'logs': logs}, name=f"/logs/batch [x{batch_size}]")
    
    @task(3)
    def send_syslog(self):
        priority = random.randint(128, 135)
        timestamp = datetime.utcnow().isoformat() + 'Z'
        hostname = fake.hostname()
        syslog = f"<{priority}>1 {timestamp} {hostname} {self.service_name} {random.randint(1000, 9999)} ID{random.randint(1, 999)} - {fake.sentence()}"
        
        self.client.post("/logs/raw?format=syslog", data=syslog, 
                        headers={'Content-Type': 'text/plain'}, name="/logs/raw [syslog]")
    
    @task(2)
    def send_clf(self):
        timestamp = datetime.utcnow().strftime('%d/%b/%Y:%H:%M:%S +0000')
        clf = f'{fake.ipv4()} - {fake.user_name()} [{timestamp}] "{random.choice(["GET", "POST"])} {fake.uri_path()} HTTP/1.1" {random.choice([200, 404, 500])} {random.randint(100, 10000)}'
        
        self.client.post("/logs/raw?format=clf", data=clf,
                        headers={'Content-Type': 'text/plain'}, name="/logs/raw [clf]")

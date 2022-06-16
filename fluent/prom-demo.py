from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from bson import ObjectId
import time

class TimerExecutionMonitor:
    def __init__(self,
        execution_name: str,
        env: str,
        prom_collector_registry: CollectorRegistry,
    ):
        self.execution_env = env
        self.start_gauge = Gauge(
            f"rx_{execution_name}_execution_start_time",
            "The start time of the specified execution",
            labelnames = ["env"],
            registry = prom_collector_registry,
        )
        self.stop_gauge = Gauge(
            f"rx_{execution_name}_execution_end_time",
            "The end time of the specified execution",
            labelnames = ["env", "status"],
            registry = prom_collector_registry,
        )
    
    def __enter__(self):    
        self.start_gauge.labels(self.execution_env).set_to_current_time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # stop the execution
        status = "success"
        if exc_type:
            status = "failed"
        self.stop_gauge.labels(self.execution_env, status).set_to_current_time()

execution_env = "test"
push_gateway_url = "rx-prom-server:9091"

# define the push metrics
prom_collector_registry = CollectorRegistry()
execution_id = ObjectId()

try:
    with TimerExecutionMonitor(
        execution_name = "fetch_ledger_data",
        env = execution_env,
        prom_collector_registry = prom_collector_registry,
    ):
        time.sleep(3)
        raise ValueError("Err")
finally:
    print("pushed")
    push_to_gateway(
        push_gateway_url,
        job = f"execution",
        registry = prom_collector_registry,
    )

from contextlib import ExitStack
from pathlib import Path

from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import (
    ExecWaitStrategy,
    HttpWaitStrategy,
    LogMessageWaitStrategy,
)

from tests.settings import TESTCONTAINERS


class ContainerManager:
    def __init__(self):
        self.active_containers: dict[str, DockerContainer] = {}
        self._stack = ExitStack()

    def start_containers(self):
        try:
            self.active_containers["postgres"] = self._stack.enter_context(
                DockerContainer(TESTCONTAINERS["postgres"]["image"])
                .with_envs(POSTGRES_USER="test", POSTGRES_PASSWORD="test", POSTGRES_DB="payments")
                .with_exposed_ports(5432)
                .waiting_for(
                    ExecWaitStrategy(
                        ["pg_isready", "-h", "127.0.0.1", "-U", "test", "-d", "payments"]
                    ).with_startup_timeout(60)
                )
            )
            self.active_containers["rabbitmq"] = self._stack.enter_context(
                DockerContainer(TESTCONTAINERS["rabbitmq"]["image"])
                .with_envs(RABBITMQ_DEFAULT_USER="test", RABBITMQ_DEFAULT_PASS="test")
                .with_exposed_ports(5672)
                .waiting_for(
                    LogMessageWaitStrategy("Server startup complete").with_startup_timeout(90)
                )
            )
            self.active_containers["webhook"] = self._stack.enter_context(
                DockerContainer(TESTCONTAINERS["webhook"]["image"])
                .with_copy_into_container(
                    Path(TESTCONTAINERS["webhook"]["script"]), "/tmp/receiver.py"
                )
                .with_command(["python", "/tmp/receiver.py"])
                .with_exposed_ports(9000)
                .waiting_for(HttpWaitStrategy(9000).for_status_code(200).with_startup_timeout(30))
            )
        except BaseException:
            self.stop_containers()
            raise

    def stop_containers(self):
        try:
            self._stack.close()
        finally:
            self.active_containers.clear()

    def address(self, service: str, port: int) -> str:
        container = self.active_containers[service]
        return f"{container.get_container_host_ip()}:{container.get_exposed_port(port)}"

    @property
    def database_url(self) -> str:
        return f"postgresql+asyncpg://test:test@{self.address('postgres', 5432)}/payments"

    @property
    def rabbitmq_url(self) -> str:
        return f"amqp://test:test@{self.address('rabbitmq', 5672)}/"

    @property
    def webhook_url(self) -> str:
        return f"http://{self.address('webhook', 9000)}"

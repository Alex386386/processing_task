from collections.abc import Iterator

import pytest

from tests.fixtures.containers import ContainerManager


@pytest.fixture(scope="session")
def testcontainers_manager() -> Iterator[ContainerManager]:
    manager = ContainerManager()
    try:
        manager.start_containers()
        yield manager
    finally:
        manager.stop_containers()


@pytest.fixture(scope="session")
def postgres_url(testcontainers_manager: ContainerManager) -> str:
    return testcontainers_manager.database_url

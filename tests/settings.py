TESTCONTAINERS = {
    "postgres": {"image": "postgres:17-alpine"},
    "rabbitmq": {"image": "rabbitmq:4.2-management-alpine"},
    "webhook": {
        "image": "python:3.13-slim",
        "script": "tests/fixtures/webhook_receiver.py",
    },
}

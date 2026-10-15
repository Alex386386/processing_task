class IdempotencyConflict(Exception):
    pass


class AttemptFailed(Exception):
    def __init__(self, attempt: int, reason: str):
        self.attempt = attempt
        self.reason = reason
        super().__init__(reason)


class InvalidEvent(Exception):
    pass

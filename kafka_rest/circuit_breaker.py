import time

class CircuitBreaker(object):
    def __init__(self, threshold, duration_seconds):
        self.threshold = threshold
        self.duration_seconds = duration_seconds
        self.failure_count = 0
        self._last_tripped = 0 # timestamp

    @property
    def tripped(self):
        return time.time() < self._last_tripped + self.duration_seconds

    def record_failure(self, value=1):
        self.failure_count += value
        if not self.tripped and self.failure_count >= self.threshold:
            self._last_tripped = time.time()

    def reset(self):
        self.failure_count = 0
        self._last_tripped = 0

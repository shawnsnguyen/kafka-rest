from collections import namedtuple
import time

class Message(namedtuple('Message', ['topic', 'value', 'key', 'partition', 'retry_after_time', 'attempt_number'])):
    # Comparisons are defined for the sake of the retry PriorityQueues
    # and will not make sense for other comparisons
    def __eq__(self, other):
        return self.retry_after_time == other.retry_after_time

    def __ne__(self, other):
        return self.retry_after_time == other.retry_after_time

    def __lt__(self, other):
        return self.retry_after_time < other.retry_after_time

    def __le__(self, other):
        return self.retry_after_time <= other.retry_after_time

    def __gt__(self, other):
        return self.retry_after_time > other.retry_after_time

    def __ge__(self, other):
        return self.retry_after_time >= other.retry_after_time

    # Defined for the sake of tests
    def true_equals(self, other):
        if not isinstance(other, Message):
            return False
        for idx in range(len(self)):
            if self[idx] != other[idx]:
                return False
        return True

    def can_retry(self, client):
        return self.attempt_number < client.retry_max_attempts

    def for_retry(self, client):
        """Return a new Message object with the same data as this one
        but with incremented retry metadata."""
        backoff = client.retry_base_seconds ** self.attempt_number
        return Message(self.topic, self.value, self.key, self.partition,
                       time.time() + backoff, self.attempt_number + 1)

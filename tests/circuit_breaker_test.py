import time

from unittest import TestCase

from kafka_rest.circuit_breaker import CircuitBreaker

class TestCircuitBreaker(TestCase):
    def setUp(self):
        self.breaker = CircuitBreaker(2, 1)

    def test_not_tripped_at_init(self):
        self.assertFalse(self.breaker.tripped)

    def test_not_tripped_before_threshold(self):
        self.breaker.record_failure()
        self.assertEqual(1, self.breaker.failure_count)
        self.assertFalse(self.breaker.tripped)

    def test_tripped_after_two_incs(self):
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.assertEqual(2, self.breaker.failure_count)
        self.assertTrue(self.breaker.tripped)

    def test_tripped_after_one_multi_inc(self):
        self.breaker.record_failure(2)
        self.assertEqual(2, self.breaker.failure_count)
        self.assertTrue(self.breaker.tripped)

    def test_tripped_after_one_overkill_multi_inc(self):
        self.breaker.record_failure(100)
        self.assertEqual(100, self.breaker.failure_count)
        self.assertTrue(self.breaker.tripped)

    def test_resets_after_duration_elapses(self):
        self.breaker.record_failure(2)
        self.assertTrue(self.breaker.tripped)
        time.sleep(1)
        self.assertFalse(self.breaker.tripped)

    def test_reset_after_init(self):
        self.breaker.reset()
        self.assertFalse(self.breaker.tripped)

    def test_reset_untrips(self):
        self.breaker.record_failure(2)
        self.assertTrue(self.breaker.tripped)
        self.breaker.reset()
        self.assertEqual(0, self.breaker.failure_count)
        self.assertFalse(self.breaker.tripped)

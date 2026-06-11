import pytest

from bollhav.model.batch import (
    Batch,
    TimeChunking,
    MAX_BATCH_SIZE,
    validate_batch_size,
)


class TestBatchDefaults:
    def test_size_default(self):
        assert Batch().size == 20000

    def test_interval_defaults(self):
        b = Batch()
        assert isinstance(b.time, TimeChunking)
        assert b.time.chunk == "@daily"


class TestBatchSizeCap:
    def test_max_constant(self):
        assert MAX_BATCH_SIZE == 100000

    def test_exactly_at_cap_ok(self):
        b = Batch(size=100000)
        assert b.size == 100000

    def test_below_cap_ok(self):
        b = Batch(size=100)
        assert b.size == 100

    def test_over_cap_raises(self):
        with pytest.raises(ValueError, match="exceeds max 100000"):
            Batch(size=100001)

    def test_error_names_batch_size_source(self):
        with pytest.raises(ValueError, match="Batch.size"):
            Batch(size=100001)

    def test_batch_constructs_with_custom_size(self):
        b = Batch(size=500)
        assert b.size == 500


class TestValidateBatchSize:
    def test_at_cap_passes(self):
        validate_batch_size(100000, "anywhere")

    def test_over_cap_raises(self):
        with pytest.raises(ValueError, match="exceeds max 100000"):
            validate_batch_size(100001, "anywhere")

    def test_source_appears_in_message(self):
        with pytest.raises(ValueError, match="Batch.size"):
            validate_batch_size(100001, "Batch.size")

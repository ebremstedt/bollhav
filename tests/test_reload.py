import pytest

from bollhav.model.batch import (
    Batch,
    ChunkMode,
    IntervalChunks,
    MAX_BATCH_SIZE,
    RowChunks,
    validate_batch_size,
)


class TestBatchDefaults:
    def test_mode_defaults_to_interval(self):
        assert Batch().mode is ChunkMode.INTERVAL

    def test_row_batch_size_default(self):
        assert Batch().row.batch_size == 10000

    def test_interval_defaults(self):
        b = Batch()
        assert isinstance(b.interval, IntervalChunks)
        assert b.interval.expression == "@daily"


class TestRowChunksCap:
    def test_max_constant(self):
        assert MAX_BATCH_SIZE == 10000

    def test_exactly_at_cap_ok(self):
        r = RowChunks(batch_size=10000)
        assert r.batch_size == 10000

    def test_below_cap_ok(self):
        r = RowChunks(batch_size=100)
        assert r.batch_size == 100

    def test_over_cap_raises(self):
        with pytest.raises(ValueError, match="exceeds max 10000"):
            RowChunks(batch_size=10001)

    def test_error_names_rowchunks_source(self):
        with pytest.raises(ValueError, match="RowChunks"):
            RowChunks(batch_size=99999)

    def test_batch_constructs_with_custom_row_chunks(self):
        b = Batch(mode=ChunkMode.ROW, row=RowChunks(batch_size=500))
        assert b.mode is ChunkMode.ROW
        assert b.row.batch_size == 500


class TestValidateBatchSize:
    def test_at_cap_passes(self):
        validate_batch_size(10000, "anywhere")

    def test_over_cap_raises(self):
        with pytest.raises(ValueError, match="exceeds max 10000"):
            validate_batch_size(10001, "anywhere")

    def test_source_appears_in_message(self):
        with pytest.raises(ValueError, match="r_row_ tag"):
            validate_batch_size(20000, "r_row_ tag")

# from unittest.mock import MagicMock, patch
# import polars as pl
# import pytest
# from bollhav.postgres.write_modes import write


# def test_invalid_write_mode():
#     conn = MagicMock()
#     model = MagicMock()
#     model.write_mode = MagicMock()

#     with pytest.raises(ValueError):
#         write(conn=conn, df_gen=iter([pl.DataFrame({"a": [1]})]), model=model)

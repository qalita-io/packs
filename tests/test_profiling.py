from unittest.mock import mock_open, patch, MagicMock
import profiling_pack.main

@patch("builtins.open", new_callable=mock_open)
@patch("glob.glob", MagicMock(return_value=["tests/data/iris.csv"]))
@patch("pandas.read_csv", MagicMock(return_value=MagicMock()))
def test_main_flow(mocked_open, mocked_glob, mocked_read_csv):
    profiling_pack.main()
    mocked_open.assert_called_once_with("source_conf.json", "r", encoding="utf-8")
    mocked_glob.assert_called_once_with("tests/data/iris.csv")
    mocked_read_csv.assert_called_once()

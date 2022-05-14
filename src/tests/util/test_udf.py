from src.util.udf import filename_cleanup

def test_get_season_from_filename():
    dummy_filename = "path/to/file/from/season-9999_json.json"
    test_output = filename_cleanup(dummy_filename)
    assert test_output == "9999"

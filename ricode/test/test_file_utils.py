import unittest
import os
import sys
from unittest.mock import patch, mock_open, MagicMock
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from ricode.utils.file_utils import (
    find_year_in_list,
    format_and_return_date,
    write_raw_file,
    no_stats_collected,
    stat_collected,
    camel_case,
)
from datetime import datetime


class TestFileUtils(unittest.TestCase):

    @patch("ricode.utils.file_utils.re.findall")
    def test_find_year_in_list(self, mock_findall):
        mock_findall.side_effect = lambda pattern, text: ["2023"] if "2023" in text else []
        dates = [MagicMock(text="Match Date: 2023-04-21")]
        result = find_year_in_list(dates)
        self.assertEqual(result, "2023")

    def test_format_and_return_date(self):
        date_str = "Fri 21 Apr, 2023"
        result = format_and_return_date(date_str)
        self.assertEqual(result, ["20230421", "2023"])

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_write_raw_file(self, mock_open_func, mock_makedirs):
        root_folder = "test_root"
        team = "South Africa"
        year = "2023"
        file_prefix = "match"
        raw = "Match data"
        write_raw_file(root_folder, team, year, file_prefix, raw)
        mock_makedirs.assert_called_once_with(os.path.join(root_folder, "games"), exist_ok=True)
        mock_open_func.assert_called_once_with(
            os.path.join(root_folder, "games", f"{file_prefix}_{team}_{year}.txt"),
            "w",
            encoding="utf-8",
        )
        mock_open_func().write.assert_called_once_with("Match data")

    @patch("os.walk")
    @patch("ricode.utils.global_parameters.BRONZE_GAME_STAT_PATH", "test_path")
    def test_no_stats_collected(self, mock_os_walk):
        mock_os_walk.return_value = [
            ("test_path\\game_stats\\2023", [], ["file1.txt", "file2.txt"]),
            ("test_path\\game_stats\\2024", [], ["file1.txt"]),
        ]
        result = no_stats_collected(stat_no=2)
        self.assertEqual(result, ["game_stats"])

    @patch("glob.glob")
    @patch("ricode.utils.global_parameters.BRONZE_GAME_STAT_PATH", "test_path")
    def test_stat_collected(self, mock_glob):
        mock_glob.return_value = [
            "test_path\\game_stats\\2023\\file1_stat.txt",
            "test_path\\game_stats\\2024\\file2_stat.txt",
        ]
        result = stat_collected("stat")
        self.assertEqual(result, ["file1", "file2"])

    def test_camel_case(self):
        str_val = "south-africa"
        result = camel_case(str_val)
        self.assertEqual(result, "South Africa")


if __name__ == "__main__":
    unittest.main()
"""
Unit tests for mavlog2csv script.
"""
import os
import sys
from unittest.mock import MagicMock

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


sys.modules["pymavlink"] = MagicMock()
sys.modules["pymavlink.mavutil"] = MagicMock()
sys.modules["pymavlink.CSVReader"] = MagicMock()
sys.modules["pymavlink.DFReader"] = MagicMock()
sys.modules["pymavlink.dialects.v10.ardupilotmega"] = MagicMock()

from src.mavlog2csv import get_mode_string, is_message_bad, message_to_row  # noqa: E402


def test_mode_mapping():
    """Test that mode numbers are correctly mapped to strings."""
    assert get_mode_string(0) == "MANUAL"
    assert get_mode_string(11) == "RTL"
    assert get_mode_string(10) == "AUTO"
    assert get_mode_string(5) == "FBWA"

    assert get_mode_string(999) == "999"


def test_is_message_bad():
    """Test is_message_bad function."""
    assert is_message_bad(None) is True

    class BadMsg:
        """Mock bad message."""

        def get_type(self):
            return "BAD_DATA"

    assert is_message_bad(BadMsg()) is True

    class GoodMsg:
        """Mock good message."""

        def get_type(self):
            return "GPS"

    assert is_message_bad(GoodMsg()) is False


def test_message_to_row_zero_values():
    """Test that 0 values are preserved and not converted to empty strings."""
    msg = MagicMock()
    msg.get_type.return_value = "TEST"
    msg.TimeUS = 123456
    msg._timestamp = 1000000
    msg.Value = 0
    msg.ZeroValue = 0.0

    class TestMsg:
        def get_type(self):
            return "TEST"

        TimeUS = 123456
        _timestamp = 1000000
        Value = 0
        ZeroValue = 0.0
        Empty = None

    msg = TestMsg()

    columns = ["Value", "ZeroValue", "Empty"]
    row = message_to_row(msg, columns)

    assert row["TEST.Value"] == 0
    assert row["TEST.Value"] != ""
    assert row["TEST.ZeroValue"] == 0.0
    assert row["TEST.Empty"] == ""

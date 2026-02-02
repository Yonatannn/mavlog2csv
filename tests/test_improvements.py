"""
Unit tests for improved_mavlog2csv script.
"""
import sys
import unittest
from unittest.mock import MagicMock

# Mock pymavlink modules BEFORE importing the script
# pylint: disable=wrong-import-position
sys.modules["pymavlink"] = MagicMock()
sys.modules["pymavlink.mavutil"] = MagicMock()
sys.modules["pymavlink.CSVReader"] = MagicMock()
sys.modules["pymavlink.DFReader"] = MagicMock()
sys.modules["pymavlink.dialects.v10.ardupilotmega"] = MagicMock()

# Now we can import the script
try:
    from src.improved_mavlog2csv import get_mode_string, is_message_bad
except ImportError:
    # Handle cases where src is not in path for pylini analysis
    pass


class TestMavlogImprovements(unittest.TestCase):
    """Test suite for mavlog2csv improvements."""

    def test_mode_mapping(self):
        """Test that mode numbers are correctly mapped to strings."""
        # Test known mappings
        self.assertEqual(get_mode_string(0), "MANUAL")
        self.assertEqual(get_mode_string(11), "RTL")
        self.assertEqual(get_mode_string(10), "AUTO")
        self.assertEqual(get_mode_string(5), "FBWA")

        # Test unknown mapping (fallback to stringified number)
        self.assertEqual(get_mode_string(999), "999")

    def test_is_message_bad(self):
        """Test is_message_bad function."""
        self.assertTrue(is_message_bad(None))

        # Mock object for bad data
        class BadMsg:
            # pylint: disable=too-few-public-methods
            """Mock bad message."""

            def get_type(self):
                """Return bad type."""
                return "BAD_DATA"

        self.assertTrue(is_message_bad(BadMsg()))

        class GoodMsg:
            # pylint: disable=too-few-public-methods
            """Mock good message."""

            def get_type(self):
                """Return good type."""
                return "GPS"

        self.assertFalse(is_message_bad(GoodMsg()))


if __name__ == "__main__":
    unittest.main()

import csv
import os
import sys
from unittest.mock import MagicMock

import pytest

from sync_logs import get_arm_disarm_times, parse_csv_timestamps, sync_and_write

# Add src to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)


@pytest.fixture
def mock_files(tmp_path):
    bin_file = tmp_path / "test.bin"
    csv_file = tmp_path / "test.csv"
    output_file = tmp_path / "test_synced.csv"
    return str(bin_file), str(csv_file), str(output_file)


def test_get_arm_disarm_times_success(mock_files, monkeypatch):
    bin_file, _, _ = mock_files

    # Create dummy bin file
    with open(bin_file, "w") as f:
        f.write("dummy")

    # Mock mavutil.mavlink_connection
    mock_conn = MagicMock()
    mock_mavutil = MagicMock()
    mock_mavutil.mavlink_connection.return_value = mock_conn
    monkeypatch.setattr("sync_logs.mavutil", mock_mavutil)

    arm_msg = MagicMock()
    arm_msg.get_type.return_value = "EV"
    arm_msg.Id = 10
    arm_msg.TimeUS = 10_000_000

    disarm_msg = MagicMock()
    disarm_msg.get_type.return_value = "EV"
    disarm_msg.Id = 11
    disarm_msg.TimeUS = 20_000_000

    # Configure return values for recv_match
    mock_conn.recv_match.side_effect = [arm_msg, disarm_msg, None]

    arm_sec, disarm_sec = get_arm_disarm_times(bin_file)

    assert arm_sec == 10.0
    assert disarm_sec == 20.0


def test_parse_csv_timestamps(mock_files):
    _, csv_file, _ = mock_files

    # Create dummy CSV
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "data"])
        writer.writerow(["2026-02-02T10:00:00:000", "d1"])
        writer.writerow(["2026-02-02T10:00:05:000", "d2"])
        writer.writerow(["2026-02-02T10:00:10:000", "d3"])

    start_dt, end_dt, rows, _, _ = parse_csv_timestamps(csv_file)

    assert start_dt.minute == 0
    assert start_dt.second == 0
    assert end_dt.second == 10
    assert len(rows) == 3


def test_sync_and_write(mock_files, monkeypatch):
    bin_file, csv_file, output_file = mock_files

    # Mock get_arm_disarm_times
    monkeypatch.setattr("sync_logs.get_arm_disarm_times", lambda x: (100.0, 110.0))

    # Mock parse_csv_timestamps
    import datetime

    start = datetime.datetime(2026, 2, 2, 10, 0, 0)
    end = datetime.datetime(2026, 2, 2, 10, 0, 20)
    rows = [
        {"time": "2026-02-02T10:00:00:000", "val": 1},
        {"time": "2026-02-02T10:00:10:000", "val": 2},  # Halfway
        {"time": "2026-02-02T10:00:20:000", "val": 3},
    ]
    monkeypatch.setattr(
        "sync_logs.parse_csv_timestamps",
        lambda x: (start, end, rows, ["time", "val"], "time"),
    )

    sync_and_write(bin_file, csv_file, output_file)

    # Verify output
    with open(output_file, "r") as f:
        reader = csv.DictReader(f)
        out_rows = list(reader)

    assert len(out_rows) == 3
    # Scale = 10s / 20s = 0.5
    # Row 1: Start -> 100.0 + 0 * 0.5 = 100.0
    # Row 2: +10s -> 100.0 + 10 * 0.5 = 105.0
    # Row 3: +20s -> 100.0 + 20 * 0.5 = 110.0

    assert float(out_rows[0]["VirtualTime"]) == pytest.approx(100.0)
    assert float(out_rows[1]["VirtualTime"]) == pytest.approx(105.0)
    assert float(out_rows[2]["VirtualTime"]) == pytest.approx(110.0)

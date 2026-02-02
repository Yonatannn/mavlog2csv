import csv
import os
from unittest.mock import MagicMock

import pytest

from mavlog2csv.sync import get_arm_disarm_times, parse_csv_timestamps, sync_and_write


@pytest.fixture
def mock_files(tmp_path):
    bin_file = tmp_path / "test.bin"
    csv_file = tmp_path / "test.csv"
    output_file = tmp_path / "test_synced.csv"
    return str(bin_file), str(csv_file), str(output_file)


def test_get_arm_disarm_times_success(mock_files, monkeypatch):
    bin_file, _, _ = mock_files

    with open(bin_file, "w") as f:
        f.write("dummy")

    mock_conn = MagicMock()
    mock_mavutil = MagicMock()
    mock_mavutil.mavlink_connection.return_value = mock_conn
    monkeypatch.setattr("mavlog2csv.sync.mavutil", mock_mavutil)

    msg1 = MagicMock()
    msg1.get_type.return_value = "ARM"
    msg1.TimeUS = 10_000_000
    msg1.ArmState = 0

    msg2 = MagicMock()
    msg2.get_type.return_value = "ARM"
    msg2.TimeUS = 20_000_000
    msg2.ArmState = 1

    msg3 = MagicMock()
    msg3.get_type.return_value = "ARM"
    msg3.TimeUS = 30_000_000
    msg3.ArmState = 0

    msg4 = MagicMock()
    msg4.get_type.return_value = "ARM"
    msg4.TimeUS = 40_000_000
    msg4.ArmState = 0

    mock_conn.recv_match.side_effect = [msg1, msg2, msg3, msg4, None]

    arm_sec, disarm_sec = get_arm_disarm_times(bin_file)

    assert arm_sec == 20.0
    assert disarm_sec == 40.0

    base, _ = os.path.splitext(bin_file)
    arm_csv = f"{base}_arm.csv"
    assert os.path.exists(arm_csv)

    with open(arm_csv, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 4
        assert rows[0]["ArmState"] == "0"
        assert rows[1]["ArmState"] == "1"


def test_parse_csv_timestamps(mock_files):
    _, csv_file, _ = mock_files

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

    monkeypatch.setattr("mavlog2csv.sync.get_arm_disarm_times", lambda x: (100.0, 110.0))

    import datetime

    start = datetime.datetime(2026, 2, 2, 10, 0, 0)
    end = datetime.datetime(2026, 2, 2, 10, 0, 20)
    rows = [
        {"time": "2026-02-02T10:00:00:000", "val": 1},
        {"time": "2026-02-02T10:00:10:000", "val": 2},
        {"time": "2026-02-02T10:00:20:000", "val": 3},
    ]
    monkeypatch.setattr(
        "mavlog2csv.sync.parse_csv_timestamps",
        lambda x: (start, end, rows, ["time", "val"], "time"),
    )

    sync_and_write(bin_file, csv_file, output_file)

    with open(output_file, "r") as f:
        reader = csv.DictReader(f)
        out_rows = list(reader)

    assert len(out_rows) == 3

    assert float(out_rows[0]["VirtualTime"]) == pytest.approx(100.0)
    assert float(out_rows[1]["VirtualTime"]) == pytest.approx(105.0)
    assert float(out_rows[2]["VirtualTime"]) == pytest.approx(110.0)

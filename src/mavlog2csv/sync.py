#!/usr/bin/env python3
"""
Script to synchronize an external CSV log with a Pixhawk .bin log.
External CSV is synchronized based on the start (ARM) and end (DISARM) times
found in the Pixhawk log.
"""

import argparse
import csv
import datetime
import logging
import os
import sys
from typing import List, Optional, Tuple

try:
    from pymavlink import mavutil
except ImportError:
    print("Error: pymavlink is required. Install it with 'pip install pymavlink'.")
    sys.exit(1)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def get_arm_disarm_times(bin_file: str) -> Tuple[float, float]:
    """
    Parses a Pixhawk .bin file to find identifying ARM and DISARM events using ARM.ArmState.
    - Exports all ARM messages to <bin_file>_arm.csv.
    - Returns (first_arm_time_sec, last_disarm_time_sec).
    """
    if not os.path.exists(bin_file):
        raise FileNotFoundError(f"Binary file not found: {bin_file}")

    logger.info("Parsing .bin file: %s", bin_file)
    mav_conn = mavutil.mavlink_connection(bin_file)

    arm_messages = []

    while True:
        msg = mav_conn.recv_match(type=["ARM"], blocking=False)
        if msg is None:
            break

        if msg.get_type() == "BAD_DATA":
            continue

        if msg.get_type() == "ARM":
            t_sec = msg.TimeUS / 1_000_000.0
            arm_state = getattr(msg, "ArmState", None)
            arm_messages.append({"TimeUS": msg.TimeUS, "TimeS": t_sec, "ArmState": arm_state})

    logger.info("Found %d ARM messages.", len(arm_messages))

    base, _ = os.path.splitext(bin_file)
    arm_csv_file = f"{base}_arm.csv"
    logger.info("Exporting ARM messages to: %s", arm_csv_file)

    if arm_messages:
        keys = arm_messages[0].keys()
        with open(arm_csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(arm_messages)
    else:
        logger.warning("No ARM messages found to export.")

    first_arm_time = None
    last_disarm_time = None

    for msg in arm_messages:
        state = msg["ArmState"]
        t_sec = msg["TimeS"]

        if first_arm_time is None and state == 1:
            first_arm_time = t_sec

        if state == 0:
            last_disarm_time = t_sec

    if first_arm_time is None:
        raise ValueError("Could not find any ARM event (ArmState=1).")

    if last_disarm_time is None:
        raise ValueError("Could not find any DISARM event (ArmState=0).")

    if last_disarm_time < first_arm_time:
        pass

    logger.info("First ARM Time: %.4f", first_arm_time)
    logger.info("Last DISARM Time: %.4f", last_disarm_time)

    return first_arm_time, last_disarm_time


def parse_csv_timestamps(
    csv_file: str,
) -> Tuple[datetime.datetime, datetime.datetime, List[dict], List[str]]:
    """
    Parses the CSV file to get start/end timestamps and loaded data.
    Expected timestamp format: 2026-02-02T13:23:42:231 (SS:mmm)
    Returns (start_dt, end_dt, rows, fieldnames)
    """
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    logger.info("Parsing CSV file: %s", csv_file)

    rows = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    if not rows:
        raise ValueError("CSV file is empty.")

    def parse_dt(dt_str: str) -> datetime.datetime:
        parts = dt_str.split("T")
        if len(parts) != 2:
            raise ValueError(f"Invalid time format: {dt_str}")

        date_part = parts[0]
        time_part = parts[1]

        if ":" in time_part:
            t_parts = time_part.rsplit(":", 1)
            if len(t_parts) == 2 and len(t_parts[1]) == 3:
                normalized = f"{date_part}T{t_parts[0]}.{t_parts[1]}"
                return datetime.datetime.fromisoformat(normalized)

        raise ValueError(f"Could not parse timestamp: {dt_str}")

    time_col = "time" if "time" in fieldnames else "Time"

    start_time_str = rows[0][time_col]
    end_time_str = rows[-1][time_col]

    start_dt = parse_dt(start_time_str)
    end_dt = parse_dt(end_time_str)

    logger.info("CSV Start Time: %s", start_dt)
    logger.info("CSV End Time: %s", end_dt)

    return start_dt, end_dt, rows, fieldnames, time_col


def sync_and_write(bin_file: str, csv_file: str, output_file: Optional[str] = None) -> None:
    """
    Main function to sync logs and write output.
    """
    bin_start_sec, bin_end_sec = get_arm_disarm_times(bin_file)
    bin_duration = bin_end_sec - bin_start_sec
    logger.info("Pixhawk Log Duration: %.2f seconds", bin_duration)

    csv_start_dt, csv_end_dt, rows, fieldnames, time_col = parse_csv_timestamps(csv_file)
    csv_duration = (csv_end_dt - csv_start_dt).total_seconds()
    logger.info("CSV Log Duration: %.2f seconds", csv_duration)

    if csv_duration <= 0:
        raise ValueError("CSV duration is zero or negative.")

    scale_factor = bin_duration / csv_duration
    logger.info("Time Scale Factor: %.4f", scale_factor)

    if not output_file:
        base, ext = os.path.splitext(csv_file)
        output_file = f"{base}_synced{ext}"

    logger.info("Writing synchronized log to: %s", output_file)

    new_fieldnames = ["VirtualTime"] + fieldnames

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
        writer.writeheader()

        for row in rows:
            row_time_str = row[time_col]

            try:
                dt = datetime.datetime.fromisoformat(row_time_str)
            except ValueError:
                if row_time_str.count(":") == 3:
                    clean_str = row_time_str[::-1].replace(":", ".", 1)[::-1]
                    dt = datetime.datetime.fromisoformat(clean_str)
                else:
                    clean_str = row_time_str[:-4] + "." + row_time_str[-3:]
                    dt = datetime.datetime.fromisoformat(clean_str)

            offset = (dt - csv_start_dt).total_seconds()

            virtual_time_sec = bin_start_sec + (offset * scale_factor)

            row["VirtualTime"] = f"{virtual_time_sec:.6f}"

            writer.writerow(row)

    logger.info("Done.")


def main():
    parser = argparse.ArgumentParser(description="Sync external CSV with Pixhawk .bin log.")
    parser.add_argument("bin_file", help="Path to Pixhawk .bin file")
    parser.add_argument("csv_file", help="Path to external CSV log file")
    parser.add_argument("-o", "--output", help="Output synced CSV file")

    args = parser.parse_args()

    try:
        sync_and_write(args.bin_file, args.csv_file, args.output)
    except Exception as e:
        logger.error("Error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()

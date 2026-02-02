# Mavlog2CSV

A Python toolset for processing ArduPilot DataFlash logs (`.bin`) and synchronizing them with external CSV logs.

## Features

-   **Mavlog2CSV Conversion**: Convert DataFlash logs to CSV format with specific columns.
    -   Supports ArduPlane mode mapping (Number -> String).
    -   Filters directly by column names (e.g., `MODE.Mode`, `GPS.Lat`).
-   **Log Synchronization**: Synchronize external CSV logs with Pixhawk binary logs based on ARM/DISARM events.
    -   Uses `ARM.ArmState` to detect session boundaries.
    -   Scales time to match the Pixhawk log duration.

## Installation

Requires Python 3 and `pymavlink`.

```bash
pip install pymavlink
```

## Usage

### Convert .bin to .csv

```bash
python src/mavlog2csv.py -c MODE.Mode -c GPS.Lat input.bin -o output.csv
```

### Sync External Logs

```bash
python src/sync_logs.py input.bin external_log.csv -o synced_output.csv
```

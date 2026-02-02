# -*- coding: utf-8 -*-
"""
Improved version of mavlog2csv.
- Adds automatic ArduPlane mode mapping (Number -> String).
- Restricts input to .bin files (DataFlash logs).
"""
import argparse
import collections
import contextlib
import csv
import datetime
import logging
import operator
import re
import sys
import textwrap
from typing import (
    IO,
    Any,
    ContextManager,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

try:
    from pymavlink import mavutil
    from pymavlink.CSVReader import CSVReader
    from pymavlink.DFReader import DFReader
    from pymavlink.dialects.v10.ardupilotmega import MAVLink_message
    from pymavlink.mavutil import mavserial
except ImportError:
    pass
    pass

logger = logging.getLogger(__name__)

PLANE_MODE_MAPPING = {
    0: "MANUAL",
    1: "CIRCLE",
    2: "STABILIZE",
    3: "TRAINING",
    4: "ACRO",
    5: "FBWA",
    6: "FBWB",
    7: "CRUISE",
    8: "AUTOTUNE",
    10: "AUTO",
    11: "RTL",
    12: "LOITER",
    13: "TAKEOFF",
    14: "AVOID_ADSB",
    15: "GUIDED",
    16: "INITIALISING",
    17: "QSTABILIZE",
    18: "QHOVER",
    19: "QLOITER",
    20: "QLAND",
    21: "QRTL",
    22: "QAUTOTUNE",
    23: "QACRO",
    24: "THERMAL",
}


def get_mode_string(mode_num: int) -> str:
    """Convert mode number to string using the mapping."""
    return PLANE_MODE_MAPPING.get(int(mode_num), str(mode_num))


def is_message_bad(message: Optional[MAVLink_message]) -> bool:
    """Check if message is bad and work working with"""
    return bool(message is None or (message and message.get_type() == "BAD_DATA"))


@contextlib.contextmanager
def mavlink_connect(device: str) -> Union["mavserial", "DFReader", "CSVReader"]:
    """
    Create mavlink connection to a device.
    Strictly checks for .bin extension for DFReader.
    """
    if not device.lower().endswith(".bin"):
        raise ValueError("Input file must be a .bin file (DataFlash log).")

    conn = mavutil.mavlink_connection(device)
    logger.debug("Connecting to %s", device)
    yield conn
    logger.debug("Closing connection to %s", device)
    conn.close()


def parse_cli_column(cli_col: str) -> Tuple[str, str]:
    """
    Parse CLI provided column into message type and column name parts.
    """
    match = re.match(r"(?P<message_type>\w+)\.(?P<column>\w+)", cli_col)
    if not match:
        raise ValueError(
            f"""\
            Specified column is not correct format:
            Column "{cli_col}" must be <Message type>.<Column>.
            For example: GPS.Lat
        """
        )
    return match.group(1), match.group(2)


def open_output(output: Optional[str] = None) -> ContextManager[IO]:
    """
    Either opens a file `output` for writing or returns STDOUT stream"""
    if output:
        return open(output, "w", newline="", encoding="utf-8")
    return contextlib.nullcontext(sys.stdout)


def iter_mavlink_messages(
    device: str, types: Set[str], skip_n_arms: int = 0
) -> Iterator[MAVLink_message]:
    """
    Return iterator over mavlink messages of `types` from `device`.
    If skip_n_arms is not zero, return messages only after skip_n_arms ARM events.
    """
    types = types.copy()
    types = types.copy()
    types.add("EV")

    n_message = 0
    n_armed = 0
    with mavlink_connect(device) as mav_conn:
        while True:
            message: Optional[MAVLink_message] = mav_conn.recv_match(
                blocking=False, type=types
            )
            n_message += 1

            if message is None:
                logger.debug("Stopping processing at %s message", n_message)
                break

            if is_message_bad(message):
                continue

            if message.get_type() == "EV" and getattr(message, "Id", None) == 10:
                logger.debug("Found ARM event: %s", message)
                n_armed += 1

            if n_armed < skip_n_arms or message.get_type() == "EV":
                continue

            yield message


def message_to_row(message: MAVLink_message, columns: List[str]) -> Dict[str, Any]:
    """Convert mavlink message to output row, mapping modes if applicable."""
    row: Dict[str, Any] = {}

    if hasattr(message, "TimeUS"):
        row["TimeUS"] = message.TimeUS
        row["TimeS"] = round(message.TimeUS / 1_000_000, 2)
    else:
        row["TimeUS"] = 0
        row["TimeS"] = 0

    # Timestamp handling
    if hasattr(message, "_timestamp"):
        dt = datetime.datetime.fromtimestamp(message._timestamp)
        row["Date"] = dt.date().isoformat()
        row["Time"] = dt.time().isoformat()
    else:
        row["Date"] = ""
        row["Time"] = ""

    msg_type = message.get_type()

    for col in columns:
        col_value = getattr(message, col, None)

        if msg_type == "MODE" and col in ["Mode", "ModeNum"]:
            if col_value is not None:
                col_value = get_mode_string(col_value)

        if col_value is None:
            col_value = ""

        row[f"{msg_type}.{col}"] = col_value

    return row


def mavlog2csv(
    device: str, columns: List[str], output: Optional[str] = None, skip_n_arms: int = 0
):
    """
    Convert ardupilot telemetry log into csv with selected columns.
    """
    parsed_columns: List[Tuple[str, str]] = list(map(parse_cli_column, columns))

    message_type_filter: Set[str] = set(map(operator.itemgetter(0), parsed_columns))

    message_type_columns: Dict[str, List[str]] = collections.defaultdict(list)
    for message_type, column in parsed_columns:
        message_type_columns[message_type].append(column)

    header = [
        "TimeUS",
        "TimeS",
        "Date",
        "Time",
        *columns,
    ]

    with open_output(output) as output_file:
        csv_writer = csv.DictWriter(
            output_file,
            fieldnames=header,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )
        csv_writer.writeheader()
        for message in iter_mavlink_messages(
            device=device, types=message_type_filter, skip_n_arms=skip_n_arms
        ):
            message_type = message.get_type()
            if message_type in message_type_columns:
                row = message_to_row(message, message_type_columns[message_type])
                csv_writer.writerow(row)


class OutputFormatter:
    """Helper class for formatting output."""

    @staticmethod
    def get_example_usage():
        """Returns example usage string."""
        return """\
            Example usage:

            # Get Mode and GPS
            python improved_mavlog2csv.py -c MODE.Mode -c GPS.Lat 2023-09-17.bin

            # Output to file
            python improved_mavlog2csv.py -c MODE.Mode -o output.csv 2023-09-17.bin
        """


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description=textwrap.dedent(mavlog2csv.__doc__),  # type: ignore
        epilog=textwrap.dedent(OutputFormatter.get_example_usage()),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("input", help="Input file name (.bin).")
    parser.add_argument(
        "-o",
        "--output",
        help="Output file name. If not set, script will output into stdout.",
    )
    parser.add_argument(
        "-c",
        "--col",
        action="append",
        required=True,
        help="Specify telemetry columns to output. Use MODE.Mode for flight modes.",
    )
    parser.add_argument(
        "--skip-n-arms",
        type=int,
        default=0,
        help="Skip N arm events before logging.",
    )

    args = parser.parse_args()

    if not args.input.lower().endswith(".bin"):
        print("Error: Input file must be a .bin file.", file=sys.stderr)
        sys.exit(1)

    mavlog2csv(
        device=args.input,
        columns=args.col,
        skip_n_arms=args.skip_n_arms,
        output=args.output,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    main()

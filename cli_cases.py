"""Command-line interface for Akamai Case Management API operations."""

import argparse
import json
from pathlib import Path
from typing import Any

from akamai_client import get_akamai_session
from akamai_cases import (
    CASE_TYPE,
    TEMPLATE_FILE,
    download_case_attachment,
    get_case_details,
    get_cases,
    write_cases_xlsx,
)

CASE_TYPE_OPTIONS: dict[str, tuple[CASE_TYPE, ...]] = {
    "MY_ACTIVE_CASES": ("MY_ACTIVE_CASES",),
    "MY_CLOSED_CASES": ("MY_CLOSED_CASES",),
    "ALL_ACTIVE_CASES": ("ALL_ACTIVE_CASES",),
    "ALL_CLOSED_CASES": ("ALL_CLOSED_CASES",),
    "ALL_CASES": ("ALL_ACTIVE_CASES", "ALL_CLOSED_CASES"),
}
CASE_TYPES = tuple(CASE_TYPE_OPTIONS)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report, inspect, or download Akamai support cases."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    report_parser = commands.add_parser(
        "report",
        help="Create a cases report in JSON or Excel format",
    )
    report_parser.add_argument(
        "account_ids",
        nargs="+",
        help="One or more Akamai account IDs",
    )
    report_parser.add_argument(
        "--type",
        dest="case_type",
        choices=CASE_TYPES,
        help="Case type to fetch (default: ALL_CASES); includes active and closed",
    )
    report_parser.add_argument(
        "--duration",
        type=int,
        default=370,
        help="Number of days back from case creation date (default: 370)",
    )
    report_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Write output to a .json or .xlsx file instead of standard output",
    )
    report_parser.add_argument(
        "--template",
        type=Path,
        help="Excel template to use when writing an .xlsx file",
    )

    case_parser = commands.add_parser(
        "case",
        help="Retrieve one case's details, comments, and attachment metadata",
    )
    case_parser.add_argument("case_id", metavar="CASEID", help="Akamai case number")
    case_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Write case details to a .json file instead of standard output",
    )

    attachment_parser = commands.add_parser(
        "attachment",
        help="Download an attachment from a case",
    )
    attachment_parser.add_argument("case_id", metavar="CASEID", help="Akamai case number")
    attachment_parser.add_argument(
        "attachment_id",
        metavar="ATTACHMENTID",
        help="Akamai attachment ID",
    )
    attachment_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="Path to write the downloaded attachment",
    )

    args = parser.parse_args()
    output_path: Path | None = args.output
    if args.command == "report":
        if args.duration < 0:
            report_parser.error("--duration must be zero or greater")
        output_format = (
            output_path.suffix.casefold() if output_path is not None else None
        )
        if output_format not in (None, ".json", ".xlsx"):
            report_parser.error("--output must end with .json or .xlsx")
        if args.template is not None and output_format != ".xlsx":
            report_parser.error("--template requires --output with an .xlsx file")
    elif args.command == "case" and args.output is not None:
        if output_path is None or output_path.suffix.casefold() != ".json":
            case_parser.error("--output must end with .json")

    session, base_url = get_akamai_session()

    if args.command == "attachment":
        if output_path is None:
            attachment_parser.error("--output is required")
            return
        download_case_attachment(
            session,
            base_url,
            args.case_id,
            args.attachment_id,
            output_path,
        )
        print(f"Wrote attachment to {output_path}")
        return

    if args.command == "case":
        details = get_case_details(session, base_url, args.case_id)
        serialized = json.dumps(details, indent=2) + "\n"
        if output_path is not None:
            output_path.write_text(serialized, encoding="utf-8")
            print(f"Wrote case details to {output_path}")
        else:
            print(serialized, end="")
        return

    case_types = CASE_TYPE_OPTIONS[args.case_type or "ALL_CASES"]
    cases_by_sheet: dict[str, list[dict[str, Any]]] = {"Open": [], "Closed": []}
    for case_type in case_types:
        sheet_name = "Closed" if case_type.endswith("_CLOSED_CASES") else "Open"
        for account_id in args.account_ids:
            cases_by_sheet[sheet_name].extend(
                get_cases(
                    session,
                    base_url,
                    account_id,
                    case_type=case_type,
                    duration=args.duration,
                )
            )

    cases = cases_by_sheet["Open"] + cases_by_sheet["Closed"]
    if output_format == ".json":
        if output_path is None:
            report_parser.error("--output is required for file output")
            return
        output_path.write_text(json.dumps(cases, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {len(cases)} cases to {output_path}")
    elif output_format == ".xlsx":
        if output_path is None:
            report_parser.error("--output is required for file output")
            return
        write_cases_xlsx(output_path, cases_by_sheet, args.template or TEMPLATE_FILE)
        print(f"Wrote {len(cases)} cases to {output_path}")
    else:
        print(json.dumps(cases, indent=2))


if __name__ == "__main__":
    main()

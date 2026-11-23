"""Reusable functions for the Akamai Case Management API."""

from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qs, quote, urljoin, urlparse

from akamai_client import raise_for_status
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

CASE_TYPE = Literal[
    "MY_ACTIVE_CASES",
    "MY_CLOSED_CASES",
    "ALL_ACTIVE_CASES",
    "ALL_CLOSED_CASES",
]
CASES_URL_PATH = "/case-management/v3/cases"
PAGE_SIZE = 200
TEMPLATE_FILE = Path(__file__).with_name("akamai_cases_template.xlsx")
CATEGORY_LABELS = {
    "TECHNICAL": "TECH",
    "PROFESSIONAL_SERVICES": "PS",
    "SECURITY": "SEC",
    "BUSINESS_SUPPORT": "BUS",
}
SEVERITY_LABELS = {
    "1-Major Impact": "1-High",
    "2-Moderate Impact": "2-Med",
    "3-Low Impact": "3-Low",
}


def get_cases(
    session: Any,
    base_url: str,
    account_id: str,
    case_type: CASE_TYPE = "ALL_ACTIVE_CASES",
    duration: int = 370,
) -> list[dict[str, Any]]:
    """Fetch all matching cases for one account, following every results page."""
    if not account_id:
        raise ValueError("account_id must not be empty")
    if duration < 0:
        raise ValueError("duration must be zero or greater")

    url = urljoin(base_url, CASES_URL_PATH)
    params: dict[str, str | int | None] = {
        "accountIds": account_id,
        "type": case_type,
        "duration": duration,
        "limit": PAGE_SIZE,
        # This API uses accountIds and does not support accountSwitchKey.
        "accountSwitchKey": None,
    }
    cases = []

    while True:
        response = session.get(url, params=params)
        raise_for_status(response)
        data = response.json()
        cases.extend(data["cases"])

        next_link = next(
            (link["href"] for link in data.get("links", []) if link.get("rel") == "next"),
            None,
        )
        if next_link is None:
            return cases

        cursor_values = parse_qs(urlparse(next_link).query).get("cursor")
        if not cursor_values:
            raise ValueError("Next-page link did not contain a cursor")
        params["cursor"] = cursor_values[0]


def _get_case_collection(
    session: Any,
    url: str,
    collection_name: str,
) -> list[dict[str, Any]]:
    """Fetch every page from a case comments or attachments endpoint."""
    params: dict[str, str | int | None] = {
        "limit": PAGE_SIZE,
        # These endpoints do not support accountSwitchKey.
        "accountSwitchKey": None,
    }
    items = []

    while True:
        response = session.get(url, params=params)
        raise_for_status(response)
        data = response.json()
        items.extend(data[collection_name])

        next_link = next(
            (link["href"] for link in data.get("links", []) if link.get("rel") == "next"),
            None,
        )
        if next_link is None:
            return items

        cursor_values = parse_qs(urlparse(next_link).query).get("cursor")
        if not cursor_values:
            raise ValueError("Next-page link did not contain a cursor")
        params["cursor"] = cursor_values[0]


def get_case_details(
    session: Any,
    base_url: str,
    case_id: str,
) -> dict[str, Any]:
    """Fetch a case, all of its comments, and its attachment metadata."""
    if not case_id:
        raise ValueError("case_id must not be empty")

    case_url = urljoin(base_url, f"{CASES_URL_PATH}/{quote(case_id, safe='')}")
    response = session.get(case_url, params={"accountSwitchKey": None})
    raise_for_status(response)
    case = response.json()

    return {
        "case": case,
        "comments": _get_case_collection(session, f"{case_url}/comments", "comments"),
        "attachments": _get_case_collection(
            session, f"{case_url}/attachments", "attachments"
        ),
    }


def download_case_attachment(
    session: Any,
    base_url: str,
    case_id: str,
    attachment_id: str,
    output_path: Path,
) -> None:
    """Download an attachment from a case to a file."""
    if not case_id:
        raise ValueError("case_id must not be empty")
    if not attachment_id:
        raise ValueError("attachment_id must not be empty")

    url = urljoin(
        base_url,
        f"{CASES_URL_PATH}/{quote(case_id, safe='')}/attachments/"
        f"{quote(attachment_id, safe='')}",
    )
    with session.get(
        url,
        params={"accountSwitchKey": None},
        stream=True,
    ) as response:
        raise_for_status(response)
        with output_path.open("wb") as attachment_file:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if chunk:
                    attachment_file.write(chunk)


def _lookup_case_value(data: dict[str, Any], key: str) -> Any:
    key_folded = key.casefold()
    return next(
        (value for name, value in data.items() if name.casefold() == key_folded),
        None,
    )


def _case_value(case: dict[str, Any], column: str) -> Any:
    column_folded = column.casefold()
    if column_folded == "category":
        parent = _lookup_case_value(case, "category")
        category_id = (
            _lookup_case_value(parent, "categoryId")
            if isinstance(parent, dict)
            else None
        )
        if category_id is None:
            return None
        return CATEGORY_LABELS.get(category_id, category_id)
    if column_folded.startswith("account"):
        parent = _lookup_case_value(case, "account")
        value = (
            _lookup_case_value(parent, column)
            if isinstance(parent, dict)
            else None
        )
    elif column_folded.startswith("category"):
        parent = _lookup_case_value(case, "category")
        value = (
            _lookup_case_value(parent, column)
            if isinstance(parent, dict)
            else None
        )
    elif column_folded.startswith("contact"):
        parent = _lookup_case_value(case, "contact")
        field = column[len("contact") :]
        value = (
            _lookup_case_value(parent, field) if isinstance(parent, dict) else None
        )
    else:
        value = _lookup_case_value(case, column)

    if column_folded == "severity" and value is not None:
        return SEVERITY_LABELS.get(str(value), value)
    return value


def _excel_value(column: str, value: Any) -> Any:
    if column.casefold() in {"createddate", "updateddate"} and isinstance(
        value, str
    ):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    return value


def write_cases_xlsx(
    output_path: Path,
    cases_by_sheet: dict[str, list[dict[str, Any]]],
    template_path: Path = TEMPLATE_FILE,
) -> None:
    """Write cases into a copy of the Open/Closed worksheet template."""
    workbook = load_workbook(template_path)
    if not {"Open", "Closed"}.issubset(workbook.sheetnames):
        raise ValueError("Template must contain Open and Closed worksheets")
    template_sheets = {
        "Open": workbook["Open"],
        "Closed": workbook["Closed"],
    }

    for sheet_name in ("Open", "Closed"):
        worksheet = workbook[sheet_name]

        if worksheet.max_row > 1:
            for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
                for cell in row:
                    cell.value = None

        headers = [cell.value for cell in worksheet[1]]
        for row_number, case in enumerate(cases_by_sheet.get(sheet_name, []), start=2):
            for column_number, header in enumerate(headers, start=1):
                if header is not None:
                    column_letter = get_column_letter(column_number)
                    cell = worksheet.cell(row=row_number, column=column_number)
                    for style_sheet_name in (
                        sheet_name,
                        "Closed" if sheet_name == "Open" else "Open",
                    ):
                        style_source = template_sheets[style_sheet_name].cell(
                            row=2, column=column_number
                        )
                        if style_source.has_style:
                            cell._style = copy(style_source._style)
                            break
                    else:
                        column_style = worksheet.column_dimensions[column_letter]
                        if column_style.has_style:
                            cell._style = copy(column_style._style)

                    value = _case_value(case, str(header))
                    cell.value = _excel_value(str(header), value)

    workbook.save(output_path)

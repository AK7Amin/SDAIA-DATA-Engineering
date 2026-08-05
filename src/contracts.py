"""Data contract at the ingestion boundary (rubric deliverable 1).

Scope decision (defended in README): the contract rejects records that are
STRUCTURALLY unusable — wrong types, missing/blank required fields, unparseable
dates, invalid invoice format. Negative quantities are NOT rejected here: in
this dataset they are genuine cancellations (business-valid records), so they
pass the contract and are classified in the Silver layer instead.
"""
from datetime import datetime
import re

from pydantic import BaseModel, field_validator

DATE_FORMAT = "%m/%d/%Y %H:%M"
INVOICE_RE = re.compile(r"^[A-Za-z]?\d{5,6}$")


class RetailTransaction(BaseModel):
    InvoiceNo: str
    StockCode: str
    Description: str = ""
    Quantity: int
    InvoiceDate: str
    UnitPrice: float
    CustomerID: str = ""      # 25% missing in the real data; not needed for gold
    Country: str

    @field_validator("InvoiceNo")
    @classmethod
    def invoice_format(cls, v: str) -> str:
        v = v.strip()
        if not INVOICE_RE.match(v):
            raise ValueError(f"InvoiceNo format invalid: '{v}'")
        return v

    @field_validator("StockCode", "Country")
    @classmethod
    def required_not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("required field is blank")
        return v.strip()

    @field_validator("InvoiceDate")
    @classmethod
    def date_parseable(cls, v: str) -> str:
        try:
            datetime.strptime(v.strip(), DATE_FORMAT)
        except ValueError:
            raise ValueError(f"InvoiceDate not parseable as M/D/YYYY H:MM: '{v}'")
        return v.strip()

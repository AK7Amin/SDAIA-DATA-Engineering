"""Unit tests for the ingestion data contract (src/contracts.py).

Scope (per contracts.py docstring): the contract rejects records that are
STRUCTURALLY unusable (bad types, blank required fields, unparseable dates,
invalid invoice format). Negative Quantity and cancellation-prefixed
InvoiceNo values are legitimate business records and must PASS.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest
from pydantic import ValidationError

from contracts import RetailTransaction


def make_row(**overrides):
    """A known-good row; pass overrides to mutate individual fields for a test."""
    row = dict(
        InvoiceNo="536365",
        StockCode="85123A",
        Description="WHITE HANGING HEART T-LIGHT HOLDER",
        Quantity=6,
        InvoiceDate="12/1/2010 8:26",
        UnitPrice=2.55,
        CustomerID="17850",
        Country="United Kingdom",
    )
    row.update(overrides)
    return row


class TestValidRow:
    def test_valid_row_passes(self):
        txn = RetailTransaction(**make_row())
        assert txn.InvoiceNo == "536365"
        assert txn.Quantity == 6
        assert txn.Country == "United Kingdom"

    def test_valid_row_strips_whitespace(self):
        row = make_row(
            InvoiceNo=" 536365 ",
            StockCode=" 85123A ",
            Country=" United Kingdom ",
            InvoiceDate=" 12/1/2010 8:26 ",
        )
        txn = RetailTransaction(**row)
        assert txn.InvoiceNo == "536365"
        assert txn.StockCode == "85123A"
        assert txn.Country == "United Kingdom"
        assert txn.InvoiceDate == "12/1/2010 8:26"


class TestInvoiceNo:
    def test_garbage_invoice_no_rejected(self):
        with pytest.raises(ValidationError):
            RetailTransaction(**make_row(InvoiceNo="FREE-STUFF"))

    def test_cancellation_prefix_passes(self):
        """'C' prefix marks a cancellation invoice — structurally valid, must pass."""
        txn = RetailTransaction(**make_row(InvoiceNo="C536365"))
        assert txn.InvoiceNo == "C536365"


class TestInvoiceDate:
    def test_unparseable_date_rejected(self):
        with pytest.raises(ValidationError):
            RetailTransaction(**make_row(InvoiceDate="not-a-date"))

    def test_no_leading_zeros_passes(self):
        """UCI dataset dates have no leading zeros, e.g. '12/1/2010 8:26'."""
        txn = RetailTransaction(**make_row(InvoiceDate="12/1/2010 8:26"))
        assert txn.InvoiceDate == "12/1/2010 8:26"


class TestQuantity:
    def test_non_numeric_quantity_rejected(self):
        with pytest.raises(ValidationError):
            RetailTransaction(**make_row(Quantity="many"))

    def test_negative_quantity_passes(self):
        """Negative quantities are genuine cancellations — business-valid, must pass."""
        txn = RetailTransaction(**make_row(Quantity=-5))
        assert txn.Quantity == -5


class TestRequiredFields:
    def test_blank_stock_code_rejected(self):
        with pytest.raises(ValidationError):
            RetailTransaction(**make_row(StockCode=""))

    def test_blank_country_rejected(self):
        with pytest.raises(ValidationError):
            RetailTransaction(**make_row(Country=""))

    def test_blank_customer_id_passes(self):
        """CustomerID is optional — 25% missing in the real data."""
        txn = RetailTransaction(**make_row(CustomerID=""))
        assert txn.CustomerID == ""

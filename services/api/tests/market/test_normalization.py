from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from crypto_research.market.binance.archive_paths import DatasetKind
from crypto_research.market.binance.normalization import (
    DuplicatePrimaryKeyError,
    NormalizationError,
    normalize_csv,
)

FIXTURES = Path(__file__).parent / "fixtures"
START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 1, 1, 0, 2, tzinfo=UTC)


def test_normalizes_klines_with_string_derived_arrow_decimals_and_millisecond_times() -> None:
    normalized = normalize_csv(
        DatasetKind.KLINES, (FIXTURES / "klines.csv").read_bytes(), START, END
    )

    assert normalized.primary_key_fields == ("open_time",)
    assert normalized.row_count == 2
    assert normalized.duplicates_removed == 0
    assert normalized.table.schema.field("open").type == pa.decimal128(38, 18)
    assert normalized.table.column("open")[0].as_py() == Decimal("42000.123456789012345678")
    assert normalized.table.column("open_time").to_pylist() == [1704067200000, 1704067260000]
    assert normalized.table.column("close_time").to_pylist() == [1704067259999, 1704067319999]


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda csv: csv.replace(b"open_time,", b"wrong_header,"), "header"),
        (lambda csv: csv.replace(b",0\n", b"\n", 1), "column count"),
        (lambda csv: csv.replace(b"1704067200000", b"1704060000000"), "outside"),
        (
            lambda csv: b"\n".join(
                [*csv.splitlines()[:1], csv.splitlines()[2], csv.splitlines()[1]]
            )
            + b"\n",
            "sorted",
        ),
        (lambda csv: csv + csv.splitlines()[1] + b"\n", "duplicate"),
        (lambda csv: csv.replace(b"1704067259999", b"1704067260000"), "close_time"),
    ],
)
def test_rejects_invalid_kline_rows(mutator, message: str) -> None:
    csv = mutator((FIXTURES / "klines.csv").read_bytes())

    with pytest.raises(NormalizationError, match=message):
        normalize_csv(DatasetKind.KLINES, csv, START, END)


def test_duplicate_error_keeps_count_for_manifest_audit() -> None:
    csv = (FIXTURES / "klines.csv").read_bytes()

    with pytest.raises(DuplicatePrimaryKeyError, match="duplicate") as error:
        normalize_csv(DatasetKind.KLINES, csv + csv.splitlines()[1] + b"\n", START, END)

    assert error.value.duplicate_count == 1


def test_normalizes_funding_and_aggregate_trade_primary_keys_without_float_conversion() -> None:
    funding = normalize_csv(
        DatasetKind.FUNDING_RATE, (FIXTURES / "funding.csv").read_bytes(), START, END
    )
    trades = normalize_csv(
        DatasetKind.AGG_TRADES, (FIXTURES / "agg_trades.csv").read_bytes(), START, END
    )

    assert funding.primary_key_fields == ("funding_time",)
    assert funding.table.column("funding_rate")[0].as_py() == Decimal("0.000100000000000001")
    assert trades.primary_key_fields == ("aggregate_trade_id",)
    assert trades.table.column("price")[0].as_py() == Decimal("42000.123456789012345678")
    assert trades.table.column("is_buyer_maker")[0].as_py() is False


def test_rejects_duplicate_aggregate_trade_ids() -> None:
    csv = (FIXTURES / "agg_trades.csv").read_bytes()

    with pytest.raises(DuplicatePrimaryKeyError, match="duplicate") as error:
        normalize_csv(
            DatasetKind.AGG_TRADES,
            csv + csv.splitlines()[1] + b"\n",
            START,
            END,
        )

    assert error.value.duplicate_count == 1

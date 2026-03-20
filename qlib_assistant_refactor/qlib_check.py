from __future__ import annotations

from pathlib import Path

from .config import AppConfig
from .qlib_env import ensure_qlib, provider_uri_str


def run_qlib_smoke(config: AppConfig) -> dict[str, object]:
    try:
        import qlib
        from qlib.data import D
    except ImportError as exc:
        raise RuntimeError(
            "qlib is not installed in the current environment. "
            "Use .venv/bin/pip install pyqlib first."
        ) from exc

    provider_uri = provider_uri_str(config)
    ensure_qlib(config)

    latest_trade_date = (
        Path(provider_uri) / "calendars" / "day.txt"
    ).read_text(encoding="utf-8").splitlines()[-1].strip()

    instruments = D.list_instruments(D.instruments("csi300"), latest_trade_date, latest_trade_date)
    if not instruments:
        raise RuntimeError("No CSI300 instruments found for latest trade date")

    if isinstance(instruments, dict):
        sample_list = sorted(instruments.keys())
    else:
        sample_list = sorted(instruments)

    sample = sample_list[0]
    feature_df = D.features(
        [sample],
        ["$open", "$close", "$high", "$low", "$volume"],
        start_time=latest_trade_date,
        end_time=latest_trade_date,
        freq="day",
    )

    return {
        "provider_uri": provider_uri,
        "latest_trade_date": latest_trade_date,
        "csi300_count": len(sample_list),
        "sample_instrument": sample,
        "sample_features": feature_df.head().to_string(),
    }

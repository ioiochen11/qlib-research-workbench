from __future__ import annotations

import pandas as pd
from qlib.data.dataset.processor import DropCol, Processor, get_group_columns


class RawClosePriceFilter(Processor):
    """Drop samples whose raw close price is above the configured cap."""

    def __init__(self, max_price: float, fields_group: str = "feature", price_column: str = "RAW_CLOSE"):
        self.max_price = float(max_price)
        self.fields_group = fields_group
        self.price_column = price_column

    def __call__(self, df: pd.DataFrame):
        if df.empty:
            return df

        series = self._price_series(df)
        if series is None:
            return df

        mask = series.notna() & (pd.to_numeric(series, errors="coerce") <= self.max_price)
        return df.loc[mask.fillna(False)]

    def readonly(self) -> bool:
        return True

    def _price_series(self, df: pd.DataFrame) -> pd.Series | None:
        if isinstance(df.columns, pd.MultiIndex):
            cols = get_group_columns(df, self.fields_group)
            for col in cols:
                if col[-1] == self.price_column:
                    return df[col]
            return None
        if self.price_column in df.columns:
            return df[self.price_column]
        return None


class DropRawCloseColumn(DropCol):
    def __init__(self, price_column: str = "RAW_CLOSE"):
        super().__init__([price_column])

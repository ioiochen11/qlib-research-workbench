from __future__ import annotations

from qlib.contrib.data.handler import Alpha158
from qlib.contrib.data.loader import Alpha158DL


class Alpha158PriceFiltered(Alpha158):
    """Alpha158 with an extra raw close column used for <= max_price sample filtering."""

    def __init__(self, max_price: float = 30.0, **kwargs):
        infer_processors = list(kwargs.pop("infer_processors", []))
        if max_price is not None:
            infer_processors = [
                {
                    "class": "RawClosePriceFilter",
                    "module_path": "qlib_assistant_refactor.qlib_processors",
                    "kwargs": {"max_price": float(max_price)},
                },
                {
                    "class": "DropRawCloseColumn",
                    "module_path": "qlib_assistant_refactor.qlib_processors",
                },
                *infer_processors,
            ]

        super().__init__(infer_processors=infer_processors, **kwargs)

    def get_feature_config(self):
        fields, names = Alpha158DL.get_feature_config(
            {
                "kbar": {},
                "price": {
                    "windows": [0],
                    "feature": ["OPEN", "HIGH", "LOW", "VWAP"],
                },
                "rolling": {},
            }
        )
        return [*fields, "$close / $factor"], [*names, "RAW_CLOSE"]

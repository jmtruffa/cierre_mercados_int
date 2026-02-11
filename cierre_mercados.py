#!/usr/bin/env python3
"""
Descarga precios de Yahoo Finance (vía yfinance) y genera un CSV
con la variación diaria (%) del último cierre disponible.

Uso:
    python cierre_mercados.py

Requisitos:
    pip install yfinance pandas numpy
"""

import os
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

# Mapeo "tus tickers" -> "símbolos Yahoo"
TICKER_MAP = {
    "SPX":  "^GSPC",
    "NDX":  "^NDX",
    "VIX":  "^VIX",
    "ILF":  "ILF",
    "EWZ":  "EWZ",
    "EMB":  "EMB",
    "/CL":  "CL=F",      # WTI Crude Oil futures
    "GLD":  "GLD",
    "XLE":  "XLE",
    "XLC":  "XLC",
    "XLP":  "XLP",
    "XLK":  "XLK",
    "XLV":  "XLV",
    "QTUM": "QTUM",
    "SOXX": "SOXX",
    "TSLA": "TSLA",
    "AAPL": "AAPL",
    "GOOG": "GOOG",
    "NVDA": "NVDA",
    "META": "META",
    "MSFT": "MSFT",
    "AMZN": "AMZN",
    "RGTI": "RGTI",
    "QBTS": "QBTS",
    "IONQ": "IONQ",
}

OUTPUT_DIR = "/home/jmt/cierre-jornada"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "variacion_diaria.csv")
LOOKBACK = "30d"


def main():
    print("=================================")
    print(f"Generando cierre tickers internacionales a las {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    symbols = list(dict.fromkeys(TICKER_MAP.values()))  # únicos, preserva orden

    # Descarga de precios ajustados
    px = yf.download(
        tickers=symbols,
        period=LOOKBACK,
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="column",
    )

    if px.empty:
        raise RuntimeError("No se descargaron datos. Revisá conexión o símbolos.")

    # Normalizar a DataFrame de Close con columnas = símbolos
    if isinstance(px.columns, pd.MultiIndex):
        close = px["Close"].copy()
    else:
        close = px[["Close"]].rename(columns={"Close": symbols[0]}).copy()

    close = close.dropna(how="all")

    # Armar tabla con última variación disponible por ticker
    rows = []
    for label, sym in TICKER_MAP.items():
        if sym not in close.columns:
            rows.append([label, sym, np.nan, np.nan, np.nan, None])
            continue

        s = close[sym].dropna()
        if len(s) < 2:
            rows.append([label, sym, np.nan, np.nan, np.nan,
                         s.index[-1].date() if len(s) else None])
            continue

        last_px = float(s.iloc[-1])
        prev_px = float(s.iloc[-2])
        chg = (last_px / prev_px - 1.0) * 100.0

        rows.append([label, sym, last_px, prev_px, round(chg, 2),
                     s.index[-1].date()])

    out = pd.DataFrame(
        rows,
        columns=["Ticker", "YahooSymbol", "Close_last", "Close_prev",
                 "Var_diaria_%", "Fecha_last"],
    )

    # Crear directorio de salida si no existe
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    out.to_csv(OUTPUT_FILE, index=False)
    print(f"Archivo generado: {OUTPUT_FILE}")
    print("===============================================")


if __name__ == "__main__":
    main()

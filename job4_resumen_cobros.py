import pandas as pd
from pathlib import Path
import os

# === 1) Config ===
BASE_DIR = Path(os.path.dirname(os.path.dirname(__file__)))

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

cobros_path    = RAW_DIR / "cobros_raw.csv"
creditos_path  = RAW_DIR / "creditos_raw.csv"
jugadores_path = RAW_DIR / "jugadores_raw.csv"
out_path       = PROCESSED_DIR / "cobros_resumen_mes_categoria.csv"


def normalize_id(series: pd.Series) -> pd.Series:
    """Convert IDs like 123.0 -> '123' and strip spaces."""
    return (
        series.astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )


# === 2) Load data ===
df_cobros    = pd.read_csv(cobros_path)
df_creditos  = pd.read_csv(creditos_path)
df_jugadores = pd.read_csv(jugadores_path)

# --- 2.1 Normalize IDs for joins ---
# cobros: idCredito (to join with creditos)
df_cobros["idCredito_norm"] = normalize_id(df_cobros["idCredito"])

# creditos: 'id' is the SharePoint item id for the crédito
df_creditos["ID_norm"] = normalize_id(df_creditos["id"])

# jugadores: 'id' is the player id
df_jugadores["idJugador_norm"] = normalize_id(df_jugadores["id"])

# === 3) Join cobros -> creditos -> jugadores to get categoria ===

# 3.1 cobros + creditos
df = df_cobros.merge(
    df_creditos[["id", "ID_norm", "idJugador"]],
    left_on="idCredito_norm",
    right_on="ID_norm",
    how="left",
    suffixes=("", "_cred")
)

# 3.2 join jugadores by idJugador
df["idJugador_norm"] = normalize_id(df["idJugador"])
df = df.merge(
    df_jugadores[["id", "idJugador_norm", "categoria"]],
    on="idJugador_norm",
    how="left",
    suffixes=("", "_jug")
)

# === 4) Clean dates & amounts ===

# fechaCobro -> datetime
df["fechaCobro"] = pd.to_datetime(df["fechaCobro"], errors="coerce")
df = df.dropna(subset=["fechaCobro"])  # keep only rows with valid date

# montoCobrado -> numeric
df["montoCobrado"] = pd.to_numeric(df["montoCobrado"], errors="coerce").fillna(0)

# year / month
df["anio"] = df["fechaCobro"].dt.year
df["mes"]  = df["fechaCobro"].dt.month

# Optional pretty month label, e.g. 2025-11
# year / month
df["anio"] = df["fechaCobro"].dt.year
df["mes"]  = df["fechaCobro"].dt.month

# Pretty YYYY-MM monthly label
df["mes_label"] = pd.to_datetime(
    df.rename(columns={"anio": "year", "mes": "month"})[["year", "month"]]
      .assign(day=1),
    errors="coerce"
).dt.strftime("%Y-%m")


# === 5) Group by month + categoria ===
resumen = (
    df.groupby(["anio", "mes_label", "categoria"], dropna=False)
      .agg(
          num_cobros=("id", "count"),          # how many cobros
          total_cobrado=("montoCobrado", "sum")  # total amount
      )
      .reset_index()
)

# Order columns nicely
resumen = resumen[["anio", "mes_label", "categoria", "num_cobros", "total_cobrado"]]

# === 6) Save to CSV ===
resumen.to_csv(out_path, index=False, encoding="utf-8-sig")

print(f"✅ Resumen escrito en: {out_path}")
print(resumen.head())

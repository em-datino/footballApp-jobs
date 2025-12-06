import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# === 1) Config ===
BASE_DIR = Path(os.path.dirname(os.path.dirname(__file__)))

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)  # ensure it exists

cobros_path    = RAW_DIR / "cobros_raw.csv"
creditos_path  = RAW_DIR / "creditos_raw.csv"
jugadores_path = RAW_DIR / "jugadores_raw.csv"

# output CSV (same as your previous job)
out_csv_path   = PROCESSED_DIR / "cobros_resumen_mes_categoria.csv"
# output PDF
out_pdf_path   = PROCESSED_DIR / "reporte_ingresos_mensual.pdf"


def normalize_id(series: pd.Series) -> pd.Series:
    """Convert IDs like 123.0 -> '123' and strip spaces."""
    return (
        series.astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )


def build_resumen() -> pd.DataFrame:
    """Build monthly income by category, same logic you already use."""
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

    # Pretty YYYY-MM monthly label
    df["mes_label"] = (
        df["fechaCobro"]
        .dt.to_period("M")
        .dt.to_timestamp()
        .dt.strftime("%Y-%m")
    )

    # === 5) Group by month + categoria ===
    resumen = (
        df.groupby(["anio", "mes_label", "categoria"], dropna=False)
          .agg(
              num_cobros=("id", "count"),            # how many cobros
              total_cobrado=("montoCobrado", "sum")  # total amount
          )
          .reset_index()
    )

    # Order columns nicely
    resumen = resumen[["anio", "mes_label", "categoria", "num_cobros", "total_cobrado"]]

    # === 6) Save to CSV (optional, same as your current job) ===
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    resumen.to_csv(out_csv_path, index=False, encoding="utf-8-sig")

    return resumen


def generate_pdf_report(resumen: pd.DataFrame, pdf_path: Path):
    """Create a PDF report with plots + tables from the resumen."""
    # Ensure sorting by month
    resumen = resumen.sort_values(["anio", "mes_label"])

    # === Totals by month ===
    totales_mes = (
        resumen.groupby(["anio", "mes_label"], as_index=False)["total_cobrado"]
               .sum()
               .sort_values(["anio", "mes_label"])
    )
    # 3% per month
    totales_mes["ingreso_3pct"] = totales_mes["total_cobrado"] * 0.03

    # Pivot for category breakdown: rows = month, cols = categoria
    pivot_cat = resumen.pivot_table(
        index="mes_label",
        columns="categoria",
        values="total_cobrado",
        aggfunc="sum",
        fill_value=0,
    ).sort_index()

    # Some high-level metrics for the cover page
    total_ingresos = float(totales_mes["total_cobrado"].sum())
    num_meses = len(totales_mes)
    promedio_mensual = float(totales_mes["total_cobrado"].mean()) if num_meses > 0 else 0.0
    mes_min = totales_mes["mes_label"].min() if num_meses > 0 else "-"
    mes_max = totales_mes["mes_label"].max() if num_meses > 0 else "-"
    # 3% of last month income
    ingreso_ultimo_mes = (
        float(totales_mes.iloc[-1]["total_cobrado"]) if num_meses > 0 else 0.0
    )
    ingreso_ultimo_mes_3pct = ingreso_ultimo_mes * 0.03

    def fmt_currency(x: float) -> str:
        return f"${x:,.2f}"

    # Open PDF
    with PdfPages(pdf_path) as pdf:
        # Metadata
        d = pdf.infodict()
        d["Title"] = "Reporte de Ingresos Mensuales"
        d["Author"] = "redskins_dashboard"
        d["CreationDate"] = datetime.now()

        # === Page 1: Cover / summary ===
        fig0, ax0 = plt.subplots(figsize=(8.27, 11.0))  # A4-like
        ax0.axis("off")

        fig0.suptitle("Reporte de ingresos por mes y categoría", fontsize=18, y=0.93)

        text_lines = [
            f"Período: {mes_min} a {mes_max}",
            "",
            f"Ingreso total en el período: {fmt_currency(total_ingresos)}",
            f"Promedio mensual: {fmt_currency(promedio_mensual)}",
            "",
            f"Ingreso último mes ({mes_max}): {fmt_currency(ingreso_ultimo_mes)}",
            f"3% del ingreso del último mes: {fmt_currency(ingreso_ultimo_mes_3pct)}",
            "",
            f"Fecha de generación del reporte: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        ]

        y = 0.75
        for line in text_lines:
            ax0.text(
                0.05,
                y,
                line,
                fontsize=12,
                transform=fig0.transFigure,
                va="top",
            )
            y -= 0.05

        # Footer
        fig0.text(0.5, 0.02, "Página 1 - Resumen ejecutivo", ha="center", fontsize=8)

        pdf.savefig(fig0)
        plt.close(fig0)

        # === Page 2: Total income by month (bars) + 3% line ===

        fig1, ax1 = plt.subplots(figsize=(8.27, 5.0))  # roughly A4 width

        x = totales_mes["mes_label"]
        y_total = totales_mes["total_cobrado"]
        y_3pct = totales_mes["ingreso_3pct"]

        # Bars — default color
        bars = ax1.bar(x, y_total, label="Ingreso total", alpha=0.85)

        # 3% line — clearer + distinct default style
        line = ax1.plot(
            x,
            y_3pct,
            marker="o",
            markersize=8,
            linewidth=2.5,
            linestyle="--",      # <-- makes it visually distinct
            label="3% del ingreso"
        )

        # Title + subtitle
        ax1.set_title("Ingresos totales por mes", fontsize=15, pad=15)
        ax1.text(0.5, 1.03, 
                "Barras = Ingreso total, Línea = 3% del ingreso mensual",
                ha="center",
                va="bottom",
                fontsize=10,
                transform=ax1.transAxes)

        ax1.set_xlabel("Mes")
        ax1.set_ylabel("Monto ($)")

        plt.setp(ax1.get_xticklabels(), rotation=45, ha="right")

        # Improved grid
        ax1.grid(True, axis="y", linestyle="--", alpha=0.35, linewidth=0.7)

        # Improved legend
        ax1.legend(
            fontsize=10,
            frameon=True,
            edgecolor="gray",
            loc="upper left"
        )

        plt.tight_layout(rect=[0.03, 0.07, 0.97, 0.92])
        fig1.text(0.5, 0.02, "Página 2 - Ingresos totales y 3%", ha="center", fontsize=8)

        pdf.savefig(fig1)
        plt.close(fig1)


        # === Page 3: Ingreso por categoría y mes (stacked bar) ===
        fig2, ax2 = plt.subplots(figsize=(8.27, 5.0))

        bottom = None
        for categoria in pivot_cat.columns:
            values = pivot_cat[categoria]
            if bottom is None:
                ax2.bar(pivot_cat.index, values, label=str(categoria))
                bottom = values
            else:
                ax2.bar(pivot_cat.index, values, bottom=bottom, label=str(categoria))
                bottom = bottom + values

        ax2.set_title("Ingresos por categoría y mes", fontsize=14)
        ax2.set_xlabel("Mes")
        ax2.set_ylabel("Monto ($)")
        plt.setp(ax2.get_xticklabels(), rotation=45, ha="right")
        ax2.legend(title="Categoría", fontsize=8)

        ax2.grid(True, axis="y", linestyle="--", alpha=0.4)

        plt.tight_layout(rect=[0.03, 0.07, 0.97, 0.95])
        fig2.text(0.5, 0.02, "Página 3 - Distribución por categoría", ha="center", fontsize=8)

        pdf.savefig(fig2)
        plt.close(fig2)

        # === Page 4: Tables (totals + pivot) ===
        fig3, ax3 = plt.subplots(figsize=(8.27, 11.0))  # portrait-ish
        ax3.axis("off")

        # Prepare table 1: Totales por mes (with 3%)
        tot_tbl = totales_mes.copy()
        tot_tbl["total_cobrado"] = tot_tbl["total_cobrado"].round(2)
        tot_tbl["ingreso_3pct"] = tot_tbl["ingreso_3pct"].round(2)

        # Table 2: Pivot by category
        cat_tbl = pivot_cat.round(2).reset_index()

        fig3.suptitle("Resumen tabular de ingresos", fontsize=16, y=0.96)

        # Table 1: top half
        ax_top = fig3.add_axes([0.05, 0.55, 0.9, 0.35])
        ax_top.axis("off")
        t1 = ax_top.table(
            cellText=tot_tbl.values,
            colLabels=tot_tbl.columns,
            loc="center",
        )
        t1.auto_set_font_size(False)
        t1.set_fontsize(8)
        t1.scale(1, 1.2)

        ax_top.set_title("Totales por mes (incluye 3%)", fontsize=12, pad=10)

        # Table 2: bottom half
        ax_bottom = fig3.add_axes([0.05, 0.05, 0.9, 0.4])
        ax_bottom.axis("off")
        t2 = ax_bottom.table(
            cellText=cat_tbl.values,
            colLabels=cat_tbl.columns,
            loc="center",
        )
        t2.auto_set_font_size(False)
        t2.set_fontsize(7)
        t2.scale(1, 1.2)

        ax_bottom.set_title("Ingresos por mes y categoría", fontsize=12, pad=10)

        fig3.text(0.5, 0.02, "Página 4 - Tablas de detalle", ha="center", fontsize=8)

        pdf.savefig(fig3)
        plt.close(fig3)


def main():
    print(f"BASE_DIR:      {BASE_DIR.resolve()}")
    print(f"DATA_DIR:      {DATA_DIR.resolve()}")
    print(f"PROCESSED_DIR: {PROCESSED_DIR.resolve()}")

    resumen = build_resumen()
    print(f"✅ CSV written to: {out_csv_path.resolve()}")

    try:
        print(f"➡️  Generating PDF at: {out_pdf_path.resolve()}")
        generate_pdf_report(resumen, out_pdf_path)
        print(f"✅ PDF written to: {out_pdf_path.resolve()}")
    except Exception as e:
        print("❌ Error while generating PDF:", e)
        raise


if __name__ == "__main__":
    main()

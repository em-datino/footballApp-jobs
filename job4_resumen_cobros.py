import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from pypdf import PdfReader, PdfWriter

# === 1) Config ===
BASE_DIR = Path(os.path.dirname(os.path.dirname(__file__)))

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)  # ensure it exists

cobros_path    = RAW_DIR / "cobros_raw.csv"
creditos_path  = RAW_DIR / "creditos_raw.csv"
jugadores_path = RAW_DIR / "jugadores_raw.csv"

# letterhead template (must exist)
TEMPLATE_PDF_PATH = RAW_DIR / "Plantilla.pdf"

# global CSV with all months
out_csv_path   = PROCESSED_DIR / "cobros_resumen_mes_categoria.csv"


# =========================
# Helpers
# =========================

def normalize_id(series: pd.Series) -> pd.Series:
    """Convert IDs like 123.0 -> '123' and strip spaces."""
    return (
        series.astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )


def ensure_all_categories(resumen_mes: pd.DataFrame, all_categories: pd.Index, anio_val: int, month_label: str) -> pd.DataFrame:
    """
    Ensure the month dataframe includes all categories (even if not present that month),
    filling missing ones with 0s.
    """
    if all_categories is None or len(all_categories) == 0:
        return resumen_mes

    # build a full set of category rows for this month
    base = pd.DataFrame({"categoria": list(all_categories)})
    base["anio"] = anio_val
    base["mes_label"] = month_label

    # left join month data onto full category list
    out = base.merge(
        resumen_mes[["anio", "mes_label", "categoria", "num_cobros", "total_cobrado"]],
        on=["anio", "mes_label", "categoria"],
        how="left"
    )

    out["num_cobros"] = out["num_cobros"].fillna(0).astype(int)
    out["total_cobrado"] = out["total_cobrado"].fillna(0.0).astype(float)

    return out



def build_resumen() -> pd.DataFrame:
    """
    Build monthly income by category for ALL months.

    Output:
      One row per (anio, mes_label, categoria) for every month-category combination,
      even if that category had 0 payments in that month.
    """
    # === 2) Load data ===
    df_cobros    = pd.read_csv(cobros_path)
    df_creditos  = pd.read_csv(creditos_path)
    df_jugadores = pd.read_csv(jugadores_path)

    # --- 2.1 Normalize IDs for joins ---
    df_cobros["idCredito_norm"]    = normalize_id(df_cobros["idCredito"])
    df_creditos["ID_norm"]         = normalize_id(df_creditos["id"])
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
    df = df.dropna(subset=["fechaCobro"])  # only rows with valid date

    # montoCobrado -> numeric
    df["montoCobrado"] = pd.to_numeric(df["montoCobrado"], errors="coerce").fillna(0.0)

    # year / month + label
    df["anio"] = df["fechaCobro"].dt.year.astype(int)

    df["mes_label"] = (
        df["fechaCobro"]
        .dt.to_period("M")
        .dt.to_timestamp()
        .dt.strftime("%Y-%m")
    )

    # Normalize categoria label (so NaN becomes consistent)
    df["categoria"] = df["categoria"].fillna("Sin categoría").astype(str).str.strip()

    # === 5) Group by month + categoria (only combos that exist in raw rows) ===
    resumen_base = (
        df.groupby(["anio", "mes_label", "categoria"], dropna=False)
          .agg(
              num_cobros=("id", "count"),
              total_cobrado=("montoCobrado", "sum")
          )
          .reset_index()
    )

    # === 6) Force ALL categories to appear for EVERY month ===
    # Master list of categories across all history
    all_categories = (
        df["categoria"]
        .dropna()
        .unique()
    )

    # Master list of months across all history
    all_months = (
        df[["anio", "mes_label"]]
        .drop_duplicates()
        .sort_values(["anio", "mes_label"])
        .reset_index(drop=True)
    )

    # Cross join months x categories
    months_df = all_months.assign(_k=1)
    cats_df = pd.DataFrame({"categoria": all_categories}).assign(_k=1)

    full_grid = (
        months_df.merge(cats_df, on="_k", how="outer")
                 .drop(columns=["_k"])
    )

    # Left join real resumen onto the full grid, fill missing with 0
    resumen = full_grid.merge(
        resumen_base,
        on=["anio", "mes_label", "categoria"],
        how="left"
    )

    resumen["num_cobros"] = resumen["num_cobros"].fillna(0).astype(int)
    resumen["total_cobrado"] = resumen["total_cobrado"].fillna(0.0).astype(float)

    # Order nicely
    resumen = resumen[["anio", "mes_label", "categoria", "num_cobros", "total_cobrado"]] \
               .sort_values(["anio", "mes_label", "categoria"]) \
               .reset_index(drop=True)

    # === 7) Save to CSV (all months) ===
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    resumen.to_csv(out_csv_path, index=False, encoding="utf-8-sig")

    return resumen






def generate_monthly_pdf_report(resumen_mes: pd.DataFrame, month_label: str, pdf_path: Path):
    """
    Create a PDF report for a single month (mes_label).
    resumen_mes contains ONLY rows for that month.
    """
    # safety
    if resumen_mes.empty:
        raise ValueError(f"resumen_mes vacío para {month_label}")

    # Basic metrics
    total_ingresos    = float(resumen_mes["total_cobrado"].sum())
    total_cobros      = int(resumen_mes["num_cobros"].sum())
    num_categorias    = int(resumen_mes["categoria"].nunique())
    anio_val          = int(resumen_mes["anio"].iloc[0])

    # 3% calculation
    tres_por_ciento   = total_ingresos * 0.03

    # per-category breakdown
    cat_summary = (
        resumen_mes
        .groupby("categoria", dropna=False)
        .agg(
            num_cobros=("num_cobros", "sum"),
            total_cobrado=("total_cobrado", "sum"),
        )
        .reset_index()
        .sort_values("total_cobrado", ascending=False)
    )

    # handle NaN category label
    cat_summary["categoria"] = cat_summary["categoria"].fillna("Sin categoría")

    def fmt_currency(x: float) -> str:
        return f"${x:,.2f}"

    # === Open PDF ===
    with PdfPages(pdf_path) as pdf:
        d = pdf.infodict()
        d["Title"] = f"Reporte de Ingresos Mensuales - {month_label}"
        d["Author"] = "redskins_dashboard"
        d["CreationDate"] = datetime.now()

        # =========================
        # Page 1: Summary
        # =========================
        fig1, ax1 = plt.subplots(figsize=(8.27, 11.0))  # A4-like
        ax1.axis("off")

        fig1.suptitle(
            f"Reporte de ingresos - {month_label}",
            fontsize=18,
            y=0.93
        )

        text_lines = [
            f"Año: {anio_val}",
            f"Mes: {month_label}",
            "",
            f"Ingreso total del mes: {fmt_currency(total_ingresos)}",
            f"3% del ingreso mensual: {fmt_currency(tres_por_ciento)}",   # 👈 NEW
            "",
            f"Número total de cobros: {total_cobros}",
            f"Número de categorías: {num_categorias}",
            "",
            f"Fecha de generación del reporte: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        ]

        y = 0.75
        for line in text_lines:
            ax1.text(
                0.08,
                y,
                line,
                fontsize=12,
                transform=fig1.transFigure,
                va="top",
            )
            y -= 0.05

        fig1.text(0.5, 0.02, "Página 1 - Resumen mensual", ha="center", fontsize=8)
        pdf.savefig(fig1)
        plt.close(fig1)

        # =========================
        # Page 2: Chart + Table
        # =========================
        fig2 = plt.figure(figsize=(8.27, 11.0))

        # -- Title
        fig2.suptitle(
            f"Distribución de ingresos por categoría - {month_label}",
            fontsize=16,
            y=0.97
        )

        # ---------- CHART AREA ----------
        ax_chart = fig2.add_axes([0.08, 0.54, 0.84, 0.32])  # (x, y, width, height)

        x = cat_summary["categoria"]
        y = cat_summary["total_cobrado"]

        bars = ax_chart.bar(x, y, alpha=0.9)

        ax_chart.set_ylabel("Monto ($)")
        ax_chart.set_xlabel("Categoría")
        plt.setp(ax_chart.get_xticklabels(), rotation=45, ha="right")

        ax_chart.grid(True, axis="y", linestyle="--", alpha=0.4)

        for bar in bars:
            height = bar.get_height()
            ax_chart.text(
                bar.get_x() + bar.get_width() / 2,
                height,
                fmt_currency(height),
                ha="center",
                va="bottom",
                fontsize=7,
                rotation=90
            )

        # ---------- TABLE AREA ----------
        ax_table = fig2.add_axes([0.08, 0.14, 0.84, 0.32])  # placed below chart
        ax_table.axis("off")

        table_df = cat_summary.copy()
        table_df["total_cobrado"] = table_df["total_cobrado"].round(2)

        table_obj = ax_table.table(
            cellText=table_df.values,
            colLabels=table_df.columns,
            loc="center",
        )
        table_obj.auto_set_font_size(False)
        table_obj.set_fontsize(8)
        table_obj.scale(1, 1.2)

        fig2.text(0.5, 0.03, "Página 2 - Tabla General de Ingresos por Categoría", ha="center", fontsize=8)

        pdf.savefig(fig2)
        plt.close(fig2)



def apply_letterhead(report_pdf: Path, letterhead_pdf: Path, output_pdf: Path):
    """
    Overlay each page of report_pdf on top of the first page of letterhead_pdf.
    Assumes both PDFs share the same page size.
    """
    if not letterhead_pdf.exists():
        raise FileNotFoundError(f"No se encontró la plantilla: {letterhead_pdf}")

    report_reader = PdfReader(str(report_pdf))
    writer = PdfWriter()

    for page in report_reader.pages:
        # Re-open template for a clean copy each time
        template_reader = PdfReader(str(letterhead_pdf))
        template_page = template_reader.pages[0]

        # Put report content on top of template
        template_page.merge_page(page)
        writer.add_page(template_page)

    with open(output_pdf, "wb") as f:
        writer.write(f)


def print_run_summary(resumen: pd.DataFrame, month_label: str | None = None) -> None:
    """
    Prints:
      - If month_label provided: monthly total + 3%
      - Always: grand total across ALL months in resumen
    """
    def fmt_currency(x: float) -> str:
        return f"${x:,.2f}"

    # ----- Monthly (optional) -----
    if month_label is not None:
        dfm = resumen[resumen["mes_label"] == month_label]
        month_total = float(dfm["total_cobrado"].sum())
        month_3pct  = month_total * 0.03

        print("\n================= RESUMEN DEL MES =================")
        print(f"Mes:                {month_label}")
        print(f"Total cobrado mes:   {fmt_currency(month_total)}")
        print(f"3% del mes:          {fmt_currency(month_3pct)}")
        print("===================================================\n")

    # ----- Grand total (always) -----
    grand_total = float(resumen["total_cobrado"].sum())
    grand_3pct  = grand_total * 0.03

    print("=============== RESUMEN GLOBAL (TODO) =============")
    print(f"Total cobrado global: {fmt_currency(grand_total)}")
    print(f"3% global:            {fmt_currency(grand_3pct)}")
    print("===================================================\n")


# =========================
# main
# =========================

def main():
    print(f"BASE_DIR:      {BASE_DIR.resolve()}")
    print(f"DATA_DIR:      {DATA_DIR.resolve()}")
    print(f"PROCESSED_DIR: {PROCESSED_DIR.resolve()}")

    if not TEMPLATE_PDF_PATH.exists():
        raise FileNotFoundError(
            f"No se encontró plantilla.pdf en: {TEMPLATE_PDF_PATH.resolve()}"
        )

    # 1) Build resumen (all months)
    resumen = build_resumen()
    print(f"✅ CSV con todos los meses: {out_csv_path.resolve()}")

    # 2) Get unique months (anio + mes_label) sorted
    unique_months = (
        resumen[["anio", "mes_label"]]
        .drop_duplicates()
        .sort_values(["anio", "mes_label"])
    )

    if unique_months.empty:
        print("⚠️ No hay datos para generar reportes mensuales.")
        return

    # 3) Generate one PDF per month
    for _, row in unique_months.iterrows():
        anio_val = int(row["anio"])
        month_label = row["mes_label"]

        print(f"➡️  Generando reporte para {month_label} ...")

        resumen_mes = resumen[
            (resumen["anio"] == anio_val) &
            (resumen["mes_label"] == month_label)
        ]

        if resumen_mes.empty:
            print(f"   ⚠️  Sin datos para {month_label}, se omite.")
            continue

        # temp + final paths
        raw_pdf_path   = PROCESSED_DIR / f"reporte_ingresos_{month_label}_tmp.pdf"
        final_pdf_path = PROCESSED_DIR / f"reporte_ingresos_{month_label}.pdf"

        try:
            # Generate raw (no letterhead)
            generate_monthly_pdf_report(resumen_mes, month_label, raw_pdf_path)
            print(f"   ✅ PDF base (sin membrete): {raw_pdf_path.resolve()}")

            # Apply letterhead
            apply_letterhead(raw_pdf_path, TEMPLATE_PDF_PATH, final_pdf_path)
            print(f"   ✅ PDF final con membrete: {final_pdf_path.resolve()}")
            print(f"   ✅ PDF final con membrete: {final_pdf_path.resolve()}")

            # Print month summary after successful generation
            print_run_summary(resumen, month_label=month_label)
        except Exception as e:
            print(f"   ❌ Error para {month_label}: {e}")
            raise
        finally:
            # Clean temp pdf
            try:
                raw_pdf_path.unlink()
            except FileNotFoundError:
                pass
    


if __name__ == "__main__":
    main()

import os
import pandas as pd

# Base dir = redskins_dashboard/
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # stripe_test/redskins_dashboard

DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

os.makedirs(PROCESSED_DIR, exist_ok=True)


def transform_cobros() -> None:

    # --- Load raw CSVs ---
    cobros_path    = os.path.join(RAW_DIR, "cobros_raw.csv")
    creditos_path  = os.path.join(RAW_DIR, "creditos_raw.csv")
    jugadores_path = os.path.join(RAW_DIR, "jugadores_raw.csv")

    df_cobros    = pd.read_csv(cobros_path)
    df_creditos  = pd.read_csv(creditos_path)
    df_jugadores = pd.read_csv(jugadores_path)

    # ----------------------------------------------------
    # 1. RENAME SharePoint columns to Power BI expected names
    # ----------------------------------------------------

    # COBROS
    df_cobros = df_cobros.rename(columns={
        "id": "ID",                        # main key
        "emailAdministrador": "emailAdministrador",
        "firmaConformidad": "firmaConformidad",
    })

    # CREDITOS
    df_creditos = df_creditos.rename(columns={
        "id": "ID",
    })

    # JUGADORES
    df_jugadores = df_jugadores.rename(columns={
        "id": "ID",
        "Title": "nombreJugador",
        # categoria and edad already correct
    })

    # ----------------------------------------------------
    # 2. TYPE HANDLING (safe)
    # ----------------------------------------------------

    numeric_cols = ["ID", "idCredito", "montoCuota", "montoCobrado"]
    for col in numeric_cols:
        if col in df_cobros.columns:
            df_cobros[col] = pd.to_numeric(df_cobros[col], errors="coerce")

    for col in ["latitud", "longitud"]:
        if col in df_cobros.columns:
            df_cobros[col] = pd.to_numeric(df_cobros[col], errors="coerce")

    if "fechaCobro" in df_cobros.columns:
        df_cobros["fechaCobro"] = pd.to_datetime(
            df_cobros["fechaCobro"], errors="coerce"
        ).dt.date

    # ----------------------------------------------------
    # 3. JOIN 1 — Cobros → Creditos (Credito_detalle)
    # ----------------------------------------------------

    df_cobros = df_cobros.merge(
        df_creditos[["ID", "idJugador", "articulos"]],
        how="left",
        left_on="idCredito",
        right_on="ID"
    )

    # drop the creditos.ID (the join key)
    if "ID" in df_cobros.columns:
        df_cobros = df_cobros.drop(columns=["ID"])


    # rename expanded columns to match Power BI
    df_cobros = df_cobros.rename(columns={
        "idJugador": "Credito_detalle.idJugador",
        "articulos": "Credito_detalle.articulos"
    })

    # ----------------------------------------------------
    # 4. JOIN 2 — Cobros → Jugadores
    # ----------------------------------------------------

    df_cobros = df_cobros.merge(
        df_jugadores[["ID", "nombreJugador", "categoria", "edad"]],
        how="left",
        left_on="Credito_detalle.idJugador",
        right_on="ID"
    )

    df_cobros = df_cobros.drop(columns=["ID"])

    df_cobros = df_cobros.rename(columns={
        "nombreJugador": "Jugadores.nombreJugador",
        "categoria": "Jugadores.categoria",
        "edad": "Jugadores.edad",
    })

    # ----------------------------------------------------
    # 5. SAVE FINAL VIEW
    # ----------------------------------------------------

    out_path = os.path.join(PROCESSED_DIR, "cobros_view.csv")
    df_cobros.to_csv(out_path, index=False)

    print(f"✔ cobros_view.csv written to {out_path} (rows={len(df_cobros)})")

def transform_creditos() -> None:
    """
    Replicates the Power Query logic for Creditos using the raw CSV:

      creditos_raw.csv  -> creditos_view.csv

    Columns in the output match the Power BI query:
      ID, idJugador, Title, nombreJugador, articulos, montoFinanciado,
      cantCuotas, montoCuota, emailAdministrador, fechaInicioTemp,
      diaDeCobro, finalizado, Item Type, Path
    """

    creditos_path = os.path.join(RAW_DIR, "creditos_raw.csv")
    df = pd.read_csv(creditos_path)

    # --- Rename SharePoint columns to Power BI names ---
    df = df.rename(columns={
        "id": "ID",  # main key
        # idJugador, articulos, montoFinanciado, cantCuotas, montoCuota,
        # emailAdministrador, fechaInicioTemp, finalizado, nombreJugador
        # already match or have the same semantic name.
    })

    # --- Type conversions (similar spirit to Table.TransformColumnTypes) ---

    # Numeric columns
    for col in ["ID", "idJugador", "montoFinanciado", "cantCuotas", "montoCuota"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Date
    if "fechaInicioTemp" in df.columns:
        s = pd.to_datetime(df["fechaInicioTemp"], errors="coerce", utc=True)
        # strip timezone → make it tz-naive
        try:
            s = s.dt.tz_convert(None)
        except TypeError:
            try:
                s = s.dt.tz_localize(None)
            except TypeError:
                pass
        df["fechaInicioTemp"] = s


    # Boolean-ish finalizado (if string, map to True/False)
    if "finalizado" in df.columns:
        df["finalizado"] = (
            df["finalizado"]
            .astype(str)
            .str.strip()
            .str.lower()
            .map({"true": True, "false": False, "1": True, "0": False})
        )

    # Ensure text columns are strings where present
    text_cols = [
        "Title",
        "nombreJugador",
        "articulos",
        "emailAdministrador",
        "diaDeCobro",
        "Item Type",
        "Path",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # --- Ensure all expected Power BI columns exist ---

    expected_cols = [
        "ID",
        "idJugador",
        "Title",
        "nombreJugador",
        "articulos",
        "montoFinanciado",
        "cantCuotas",
        "montoCuota",
        "emailAdministrador",
        "fechaInicioTemp",
        "diaDeCobro",
        "finalizado",
        "Item Type",
        "Path",
    ]

    # Create missing columns as nulls
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Reorder so expected columns come first, rest (metadata) after
    other_cols = [c for c in df.columns if c not in expected_cols]
    df = df[expected_cols + other_cols]

    # --- Save processed view ---
    out_path = os.path.join(PROCESSED_DIR, "creditos_view.csv")
    df.to_csv(out_path, index=False)
    print(f"✔ creditos_view.csv written to {out_path} (rows={len(df)})")



def transform_estado_general():
    """
    Replicates the entire Power BI Python block:
    - Expands creditos into cuotas (21 days per cuota)
    - Normalizes pagos
    - Assigns payments to cuotas
    - Computes estadoPago + estadoAcumulado per cuota
    - Computes estadoGeneral por jugador
    """

    from datetime import date, timedelta

    # =============== LOAD RAW DATA ===================
    creditos_path = os.path.join(RAW_DIR, "creditos_raw.csv")
    cobros_path   = os.path.join(RAW_DIR, "cobros_raw.csv")

    df_credito = pd.read_csv(creditos_path)
    df_cobros  = pd.read_csv(cobros_path)

    # --- normalize column names (Power BI-specific) ---
    df_credito = df_credito.rename(columns={"id": "ID"})
    df_cobros  = df_cobros.rename(columns={"id": "ID"})

    # --- Convert date columns in creditos ---
    if "fechaInicioTemp" in df_credito.columns:
        s = pd.to_datetime(df_credito["fechaInicioTemp"], errors="coerce", utc=True)
        try:
            s = s.dt.tz_convert(None)
        except TypeError:
            try:
                s = s.dt.tz_localize(None)
            except TypeError:
                pass
        df_credito["fechaInicioTemp"] = s

    # --- Convert date columns in cobros ---
    if "fechaCobro" in df_cobros.columns:
        s = pd.to_datetime(df_cobros["fechaCobro"], errors="coerce", utc=True)
        try:
            s = s.dt.tz_convert(None)
        except TypeError:
            try:
                s = s.dt.tz_localize(None)
            except TypeError:
                pass
        df_cobros["fechaCobro"] = s

    # --- Normalize numeric dtypes in creditos ---
    num_cols = ["ID", "idJugador", "cantCuotas", "montoFinanciado", "montoCuota"]
    for c in num_cols:
        if c in df_credito.columns:
            df_credito[c] = pd.to_numeric(df_credito[c], errors="coerce")

    if "montoCobrado" in df_cobros.columns:
        df_cobros["montoCobrado"] = pd.to_numeric(df_cobros["montoCobrado"], errors="coerce")

    # =============== EXPAND CREDITOS → CUOTAS ===================
    filas = []
    for _, row in df_credito.iterrows():
        if pd.isna(row.get("cantCuotas")) or pd.isna(row.get("fechaInicioTemp")):
            continue

        n = int(row["cantCuotas"])
        for cuota in range(1, n + 1):
            new = row.copy()
            new["nroCuota"] = cuota

            ini = row["fechaInicioTemp"] + timedelta(days=21 * (cuota - 1))
            fin = row["fechaInicioTemp"] + timedelta(days=21 * cuota)

            new["fechaInicio"] = ini
            new["fechaFin"] = fin
            new["rangoPago"] = f"{ini.strftime('%Y-%m-%d')} al {fin.strftime('%Y-%m-%d')}"

            filas.append(new)

    df_cuotas = pd.DataFrame(filas)

    # =============== PREP PAGO DATA ===================
    df_pagos = df_cobros[["ID", "idCredito", "fechaCobro", "montoCobrado"]].copy()

    df_pagos = df_pagos.rename(columns={
        "ID": "id_pago",
        "idCredito": "id_credito",
        "fechaCobro": "fecha_pago",
        "montoCobrado": "monto"
    })

    # normalize fecha_pago as tz-naive datetime
    s = pd.to_datetime(df_pagos["fecha_pago"], errors="coerce", utc=True)
    try:
        s = s.dt.tz_convert(None)
    except TypeError:
        try:
            s = s.dt.tz_localize(None)
        except TypeError:
            pass
    df_pagos["fecha_pago"] = s

    # normalize id_credito like original logic (.0 stripped)
    df_pagos["id_credito"] = df_pagos["id_credito"].astype(str).str.replace(".0", "", regex=False)

    # normalize df_cuotas["ID"] as string too for comparison
    df_cuotas["ID_str"] = df_cuotas["ID"].astype(str).str.replace(".0", "", regex=False)

    # =============== ASSIGN PAYMENTS TO CUOTAS ===============
    def asignar_pagos(row, pagos):
        pagos_credito = pagos[ pagos["id_credito"] == row["ID_str"] ]

        if pagos_credito.empty:
            return pd.Series([pd.NA, 0])

        # pagos en rango
        dentro = pagos_credito[
            (pagos_credito["fecha_pago"] >= row["fechaInicio"]) &
            (pagos_credito["fecha_pago"] <= row["fechaFin"])
        ]

        # última cuota maneja pagos fuera de rango
        es_ultima = row["nroCuota"] == df_cuotas[df_cuotas["ID_str"] == row["ID_str"]]["nroCuota"].max()

        if es_ultima:
            fuera = pagos_credito[pagos_credito["fecha_pago"] > row["fechaFin"]]
            pagos_final = pd.concat([dentro, fuera])
        else:
            pagos_final = dentro

        if pagos_final.empty:
            return pd.Series([pd.NA, 0])

        fechas = ", ".join(sorted(pagos_final["fecha_pago"].dt.strftime("%Y-%m-%d").unique()))
        suma = pagos_final["monto"].sum()
        return pd.Series([fechas, suma])

    df_cuotas[["fechaPagoReal", "sumaPagos"]] = df_cuotas.apply(
        lambda r: asignar_pagos(r, df_pagos),
        axis=1
    )

    # ensure fechaInicio / fechaFin are tz-naive datetimes
    for col in ["fechaInicio", "fechaFin"]:
        col_s = pd.to_datetime(df_cuotas[col], errors="coerce", utc=True)
        try:
            col_s = col_s.dt.tz_convert(None)
        except TypeError:
            try:
                col_s = col_s.dt.tz_localize(None)
            except TypeError:
                pass
        df_cuotas[col] = col_s

    # =============== ESTADO DE PAGO ===================
    FECHA_HOY = date(2025, 11, 2) #date.today()

    def estado_pago(row):
        # Convert fechaInicio/fechaFin to date for comparison
        finicio = pd.to_datetime(row["fechaInicio"], errors="coerce")
        ffin    = pd.to_datetime(row["fechaFin"], errors="coerce")
        finicio_date = finicio.date() if not pd.isna(finicio) else None
        ffin_date    = ffin.date() if not pd.isna(ffin) else None

        if pd.notna(row["fechaPagoReal"]):
            fechas = [pd.to_datetime(f, errors="coerce") for f in str(row["fechaPagoReal"]).split(", ")]
            fechas = [f for f in fechas if not pd.isna(f)]
            if not fechas:
                # no valid payment dates parsed
                if ffin_date is not None and ffin_date < FECHA_HOY:
                    return "MOROSO"
                return "VIGENTE"

            ultima = max(fechas)
            ultima_date = ultima.date()

            if finicio_date is not None and ffin_date is not None:
                if finicio_date <= ultima_date <= ffin_date:
                    return "PAGADO"
                elif ultima_date > ffin_date:
                    return "PAGO CON MORA"
                else:
                    return "PagoAnticipado"
            else:
                # if boundaries are weird, just treat as paid
                return "PAGADO"

        # No payments
        if ffin_date is not None and ffin_date < FECHA_HOY:
            return "MOROSO"
        return "VIGENTE"

    df_cuotas["estadoPago"] = df_cuotas.apply(estado_pago, axis=1)

    # =============== ACUMULADOS ===================
    df_cuotas["montoCuota"] = pd.to_numeric(df_cuotas["montoCuota"], errors="coerce").fillna(0)
    df_cuotas["sumaPagos"]  = pd.to_numeric(df_cuotas["sumaPagos"],  errors="coerce").fillna(0)

    df_cuotas = df_cuotas.sort_values(["nombreJugador", "nroCuota"])
    df_cuotas["montoCuotaAcum"] = df_cuotas.groupby("nombreJugador")["montoCuota"].cumsum()
    df_cuotas["sumaPagosAcum"]  = df_cuotas.groupby("nombreJugador")["sumaPagos"].cumsum()

    # =============== ESTADO ACUMULADO ===================
    totales = df_cuotas.groupby("nombreJugador").agg(
        totalCuotas=("montoCuota", "sum"),
        totalPagado=("sumaPagos", "sum")
    ).reset_index()

    df_cuotas = df_cuotas.merge(totales, on="nombreJugador", how="left")

    def estado_acumulado(row):
        if row["totalPagado"] > row["totalCuotas"]:
            return "PAGO EXCEDIDO"
        elif row["sumaPagosAcum"] == row["totalCuotas"]:
            return "DEUDA SALDADA"
        elif row["sumaPagosAcum"] >= row["montoCuotaAcum"]:
            return "AL CORRIENTE"
        else:
            return "MOROSO"

    df_cuotas["estadoAcumulado"] = df_cuotas.apply(estado_acumulado, axis=1)

    # =============== ESTADO GENERAL POR JUGADOR ===================
    resumen = []

    for nombre, grp in df_cuotas.groupby("nombreJugador"):
        grp = grp.sort_values("nroCuota").copy()

        total_cuotas = grp["montoCuota"].sum()
        total_pagado = grp["sumaPagos"].sum()

        # compute fechaFin_date for comparisons
        grp["fechaFin_dt"]   = pd.to_datetime(grp["fechaFin"], errors="coerce")
        grp["fechaFin_date"] = grp["fechaFin_dt"].dt.date

        ultima = grp.iloc[-1]
        ultima_fin_date = ultima["fechaFin_date"]
        ultima_vigente = ultima_fin_date is not None and ultima_fin_date >= FECHA_HOY

        if total_pagado >= total_cuotas:
            estado_general = "DEUDA SALDADA" if total_pagado == total_cuotas else "PAGO EXCEDIDO"
        else:
            if ultima_vigente:
                # use previous cuota if exists
                if len(grp) > 1:
                    prev = grp.iloc[-2]
                    estado_general = prev["estadoAcumulado"]
                else:
                    estado_general = ultima["estadoAcumulado"]
            else:
                vencidas = grp[
                    (grp["fechaFin_date"] < FECHA_HOY) &
                    (grp["montoCuotaAcum"] > grp["sumaPagosAcum"])
                ]
                estado_general = "MOROSO" if not vencidas.empty else "AL CORRIENTE"

        resumen.append({
            "ID": ultima["ID"],
            "nombreJugador": nombre,
            "totalCuotas": total_cuotas,
            "totalPagado": total_pagado,
            "estadoGeneral": estado_general
        })

    df_final = pd.DataFrame(resumen)

    # ================= SAVE ======================
    out_path = os.path.join(PROCESSED_DIR, "estado_general_view.csv")
    df_final.to_csv(out_path, index=False)
    print(f"✔ estado_general_view.csv written to {out_path} (rows={len(df_final)})")




# def transform_creditos_resumen():
#     """
#     Local equivalent of the Power BI Python+M block:

#     - Aggregate Cobros by idCredito (montoCuota_total, montoCobrado_total,
#       totalFechasCobros, cantidadCobros)
#     - Join with Creditos info
#     - Join with Jugadores (main player info)
#     - Add edadEtiqueta
#     - Join with estado_general_view (estadoGeneral -> estaFinalGeneral)
#     - Join (full outer) with jugadores again as data_jugadores
#     - Output a final CSV for Power BI
#     """

#     # ---------- Load raw / processed inputs ----------
#     creditos_path = os.path.join(RAW_DIR, "creditos_raw.csv")
#     cobros_path   = os.path.join(RAW_DIR, "cobros_raw.csv")
#     jugadores_path = os.path.join(RAW_DIR, "jugadores_raw.csv")
#     estado_path   = os.path.join(PROCESSED_DIR, "estado_general_view.csv")

#     df_credito   = pd.read_csv(creditos_path)
#     df_cobros    = pd.read_csv(cobros_path)
#     df_jugadores = pd.read_csv(jugadores_path)
#     df_estado    = pd.read_csv(estado_path)

#     # ---------- Normalize ID columns ----------
#     # Use 'id' from SharePoint as the numeric credit ID
#     df_credito = df_credito.rename(columns={"id": "ID"})
#     df_cobros  = df_cobros.rename(columns={"id": "ID"})  # ID of each pago (cobro)

#     # Normalize idCredito to string (strip ".0", spaces)
#     if "idCredito" in df_cobros.columns:
#         df_cobros["idCredito"] = (
#             df_cobros["idCredito"]
#             .astype(str)
#             .str.replace(".0", "", regex=False)
#             .str.strip()
#         )

#     # For join: create a string version of creditos.ID
#     df_credito["ID_str"] = (
#         df_credito["ID"]
#         .astype(str)
#         .str.replace(".0", "", regex=False)
#         .str.strip()
#     )

#     # ---------- Optional type handling for Cobros ----------
#     # Dates
#     if "fechaCobro" in df_cobros.columns:
#         df_cobros["fechaCobro"] = pd.to_datetime(df_cobros["fechaCobro"], errors="coerce")

#     # Numerics
#     for col in ["montoCuota", "montoCobrado"]:
#         if col in df_cobros.columns:
#             df_cobros[col] = pd.to_numeric(df_cobros[col], errors="coerce")

#     # ---------- Group Cobros by idCredito ----------
#     # Equivalent of df_grouped in your Power BI Python
#     agg_dict = {
#         "montoCuota":   "sum",
#         "montoCobrado": "sum",
#         "ID":           "count",  # count of pagos for that crédito
#     }

#     # Only keep columns that exist
#     valid_agg = {k: v for k, v in agg_dict.items() if k in df_cobros.columns}

#     df_grouped = (
#         df_cobros.groupby("idCredito", as_index=False)
#         .agg(valid_agg)
#     )

#     # Rename aggregation columns to match Power BI naming:
#     rename_agg = {}
#     if "montoCuota" in df_grouped.columns:
#         rename_agg["montoCuota"] = "montoCuota_sum"
#     if "montoCobrado" in df_grouped.columns:
#         rename_agg["montoCobrado"] = "montoCobrado_sum"
#     if "ID" in df_grouped.columns:
#         rename_agg["ID"] = "cantidadCobros"

#     df_grouped = df_grouped.rename(columns=rename_agg)

#     # Build totalFechasCobros (concatenated list of dates)
#     if "fechaCobro" in df_cobros.columns:
#         df_fechas = (
#             df_cobros
#             .dropna(subset=["fechaCobro"])
#             .assign(
#                 fechaCobro_str=lambda d: d["fechaCobro"].dt.strftime("%Y-%m-%d")
#             )
#             .groupby("idCredito", as_index=False)["fechaCobro_str"]
#             .agg(lambda x: ", ".join(x))
#             .rename(columns={"fechaCobro_str": "totalFechasCobros"})
#         )
#         df_grouped = df_grouped.merge(df_fechas, on="idCredito", how="left")
#     else:
#         df_grouped["totalFechasCobros"] = pd.NA

#     # ---------- Select desired Creditos columns ----------
#     columnas_deseadas = [
#         "ID",
#         "idJugador",
#         "nombreJugador",
#         "articulos",
#         "montoFinanciado",
#         "cantCuotas",
#         "montoCuota",
#         "fechaInicioTemp",
#         "finalizado",
#     ]

#     for c in columnas_deseadas:
#         if c not in df_credito.columns:
#             df_credito[c] = pd.NA

#     df_cred_sel = df_credito[columnas_deseadas + ["ID_str"]].copy()

#     # ---------- Merge Creditos + grouped Cobros ----------
#     df_final = pd.merge(
#         df_cred_sel,
#         df_grouped,
#         left_on="ID_str",
#         right_on="idCredito",
#         how="left",
#     )

#     # Drop helper join columns we don't want in final
#     drop_cols = [c for c in ["idCredito", "ID_str"] if c in df_final.columns]
#     if drop_cols:
#         df_final = df_final.drop(columns=drop_cols)

#     # Rename sums to total fields (Power BI naming)
#     df_final = df_final.rename(
#         columns={
#             "montoCuota_sum": "montoCuota_total",
#             "montoCobrado_sum": "montoCobrado_total",
#         }
#     )

#     # ---------- Join with Jugadores (main player info) ----------
#     # Map jugadores_raw to expected columns
#     df_jug = df_jugadores.rename(columns={"id": "ID"})
#     # Title is the name in your list -> we'll use it as Jugador
#     if "Title" in df_jug.columns:
#         df_jug = df_jug.rename(columns={"Title": "nombreJugador_Jugadores"})

#     jug_merge_cols = ["ID", "nombreJugador_Jugadores", "nombrePadreTutor", "categoria", "edad"]
#     jug_merge_cols = [c for c in jug_merge_cols if c in df_jug.columns]
#     df_jug_sel = df_jug[jug_merge_cols].copy()

#     df_final = df_final.merge(
#         df_jug_sel,
#         how="left",
#         left_on="idJugador",
#         right_on="ID",
#         suffixes=("", "_jug")
#     )

#     # Drop right-side ID (from Jugadores)
#     if "ID_jug" in df_final.columns:
#         df_final = df_final.drop(columns=["ID_jug"])

#     # Rename Jugadores columns to match M:
#     rename_jug = {}
#     if "nombreJugador_Jugadores" in df_final.columns:
#         rename_jug["nombreJugador_Jugadores"] = "Jugador"
#     if "nombrePadreTutor" in df_final.columns:
#         rename_jug["nombrePadreTutor"] = "TutorJugador"
#     if "categoria" in df_final.columns:
#         rename_jug["categoria"] = "Categoria"
#     if "edad" in df_final.columns:
#         rename_jug["edad"] = "Edad"

#     df_final = df_final.rename(columns=rename_jug)

#     # edadEtiqueta = Text.From([Edad]) & "-Años"
#     if "Edad" in df_final.columns:
#         df_final["edadEtiqueta"] = df_final["Edad"].apply(
#             lambda x: f"{int(x)}-Años" if pd.notnull(x) else pd.NA
#         )
#     else:
#         df_final["edadEtiqueta"] = pd.NA

#     # ---------- Join with estado_general_view (df_estado_general2) ----------
#     # df_estado has: ID, nombreJugador, totalCuotas, totalPagado, estadoGeneral
#     if "ID" in df_estado.columns:
#         df_final = df_final.merge(
#             df_estado[["ID", "estadoGeneral"]],
#             how="left",
#             on="ID",
#             suffixes=("", "_estado")
#         )
#         df_final = df_final.rename(columns={"estadoGeneral": "estaFinalGeneral"})
#     else:
#         df_final["estaFinalGeneral"] = pd.NA

#     # ---------- Join with data_jugadores (full outer) ----------
#     # We'll treat data_jugadores as jugadores_raw again.
#     df_dj = df_jugadores.rename(columns={"id": "ID"})
#     if "Title" in df_dj.columns:
#         df_dj = df_dj.rename(columns={"Title": "nombreJugador_dj"})

#     dj_cols = ["ID", "nombreJugador_dj", "categoria"]
#     dj_cols = [c for c in dj_cols if c in df_dj.columns]
#     df_dj_sel = df_dj[dj_cols].copy()

#     df_final = df_final.merge(
#         df_dj_sel,
#         how="outer",
#         left_on="idJugador",
#         right_on="ID",
#         suffixes=("", "_dj")
#     )

#     # Rename expanded data_jugadores columns to match Power BI:
#     # "data_jugadores.ID", "data_jugadores.nombreJugador", "data_jugadores.categoria"
#     if "ID_dj" in df_final.columns:
#         df_final = df_final.rename(columns={"ID_dj": "data_jugadores.ID"})
#     else:
#         df_final["data_jugadores.ID"] = pd.NA

#     if "nombreJugador_dj" in df_final.columns:
#         df_final = df_final.rename(columns={"nombreJugador_dj": "data_jugadores.nombreJugador"})
#     else:
#         df_final["data_jugadores.nombreJugador"] = pd.NA

#     if "categoria_dj" in df_final.columns:
#         df_final = df_final.rename(columns={"categoria_dj": "data_jugadores.categoria"})
#     else:
#         df_final["data_jugadores.categoria"] = pd.NA

#     # ---------- Save final view ----------
#     out_path = os.path.join(PROCESSED_DIR, "creditos_resumen_view.csv")
#     df_final.to_csv(out_path, index=False)
#     print(f"✔ creditos_resumen_view.csv written to {out_path} (rows={len(df_final)})")




def transform_creditos_resumen():
    """
    Build creditos_resumen_view.csv:

    - Aggregate Cobros by idCredito
    - Join with Creditos
    - Join with Jugadores (main info)
    - Join with estado_general_view
    - Full outer join with jugadores again (data_jugadores.*)
    - Back-fill player info for players with no credit
    """

    # ---------- Load raw / processed inputs ----------
    creditos_path  = os.path.join(RAW_DIR, "creditos_raw.csv")
    cobros_path    = os.path.join(RAW_DIR, "cobros_raw.csv")
    jugadores_path = os.path.join(RAW_DIR, "jugadores_raw.csv")
    estado_path    = os.path.join(PROCESSED_DIR, "estado_general_view.csv")

    df_credito   = pd.read_csv(creditos_path)
    df_cobros    = pd.read_csv(cobros_path)
    df_jugadores = pd.read_csv(jugadores_path)
    df_estado    = pd.read_csv(estado_path)

    # ---------- Normalize ID columns ----------
    df_credito = df_credito.rename(columns={"id": "ID"})   # credit ID
    df_cobros  = df_cobros.rename(columns={"id": "ID"})    # cobro row ID

    # idCredito in cobros -> string
    if "idCredito" in df_cobros.columns:
        df_cobros["idCredito"] = (
            df_cobros["idCredito"]
            .astype(str)
            .str.replace(".0", "", regex=False)
            .str.strip()
        )

    # Helper string version of creditos.ID
    df_credito["ID_str"] = (
        df_credito["ID"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    # ---------- Types in Cobros ----------
    if "fechaCobro" in df_cobros.columns:
        df_cobros["fechaCobro"] = pd.to_datetime(
            df_cobros["fechaCobro"], errors="coerce"
        )

    for col in ["montoCuota", "montoCobrado"]:
        if col in df_cobros.columns:
            df_cobros[col] = pd.to_numeric(df_cobros[col], errors="coerce")

    # ---------- Group Cobros by idCredito ----------
    agg_dict = {
        "montoCuota":   "sum",
        "montoCobrado": "sum",
        "ID":           "count",   # number of pagos
    }
    valid_agg = {k: v for k, v in agg_dict.items() if k in df_cobros.columns}

    df_grouped = (
        df_cobros.groupby("idCredito", as_index=False)
        .agg(valid_agg)
    )

    rename_agg = {}
    if "montoCuota" in df_grouped.columns:
        rename_agg["montoCuota"] = "montoCuota_sum"
    if "montoCobrado" in df_grouped.columns:
        rename_agg["montoCobrado"] = "montoCobrado_sum"
    if "ID" in df_grouped.columns:
        rename_agg["ID"] = "cantidadCobros"

    df_grouped = df_grouped.rename(columns=rename_agg)

    # totalFechasCobros
    if "fechaCobro" in df_cobros.columns:
        df_fechas = (
            df_cobros
            .dropna(subset=["fechaCobro"])
            .assign(fechaCobro_str=lambda d: d["fechaCobro"].dt.strftime("%Y-%m-%d"))
            .groupby("idCredito", as_index=False)["fechaCobro_str"]
            .agg(lambda x: ", ".join(x))
            .rename(columns={"fechaCobro_str": "totalFechasCobros"})
        )
        df_grouped = df_grouped.merge(df_fechas, on="idCredito", how="left")
    else:
        df_grouped["totalFechasCobros"] = pd.NA

    # ---------- Select Creditos columns ----------
    columnas_deseadas = [
        "ID",
        "idJugador",
        "nombreJugador",
        "articulos",
        "montoFinanciado",
        "cantCuotas",
        "montoCuota",
        "fechaInicioTemp",
        "finalizado",
    ]
    for c in columnas_deseadas:
        if c not in df_credito.columns:
            df_credito[c] = pd.NA

    df_cred_sel = df_credito[columnas_deseadas + ["ID_str"]].copy()

    # ---------- Merge Creditos + Cobros ----------
    df_final = pd.merge(
        df_cred_sel,
        df_grouped,
        left_on="ID_str",
        right_on="idCredito",
        how="left",
    )

    drop_cols = [c for c in ["idCredito", "ID_str"] if c in df_final.columns]
    if drop_cols:
        df_final = df_final.drop(columns=drop_cols)

    df_final = df_final.rename(
        columns={
            "montoCuota_sum": "montoCuota_total",
            "montoCobrado_sum": "montoCobrado_total",
        }
    )

    # ---------- Join with Jugadores (main info) ----------
    df_jug = df_jugadores.rename(columns={"id": "ID"})
    if "Title" in df_jug.columns:
        df_jug = df_jug.rename(columns={"Title": "nombreJugador_Jugadores"})

    jug_merge_cols = [
        "ID",
        "nombreJugador_Jugadores",
        "nombrePadreTutor",
        "categoria",
        "edad",
    ]
    jug_merge_cols = [c for c in jug_merge_cols if c in df_jug.columns]
    df_jug_sel = df_jug[jug_merge_cols].copy()

    df_final = df_final.merge(
        df_jug_sel,
        how="left",
        left_on="idJugador",
        right_on="ID",
        suffixes=("", "_jug"),
    )

    if "ID_jug" in df_final.columns:
        df_final = df_final.drop(columns=["ID_jug"])

    rename_jug = {}
    if "nombreJugador_Jugadores" in df_final.columns:
        rename_jug["nombreJugador_Jugadores"] = "Jugador"
    if "nombrePadreTutor" in df_final.columns:
        rename_jug["nombrePadreTutor"] = "TutorJugador"
    if "categoria" in df_final.columns:
        rename_jug["categoria"] = "Categoria"
    if "edad" in df_final.columns:
        rename_jug["edad"] = "Edad"

    df_final = df_final.rename(columns=rename_jug)

    # edadEtiqueta
    if "Edad" in df_final.columns:
        df_final["edadEtiqueta"] = df_final["Edad"].apply(
            lambda x: f"{int(x)}-Años" if pd.notnull(x) else pd.NA
        )
    else:
        df_final["edadEtiqueta"] = pd.NA

    # ---------- Join with estado_general_view ----------
    if "ID" in df_estado.columns:
        df_final = df_final.merge(
            df_estado[["ID", "estadoGeneral"]],
            how="left",
            on="ID",
            suffixes=("", "_estado"),
        )
        df_final = df_final.rename(columns={"estadoGeneral": "estaFinalGeneral"})
    else:
        df_final["estaFinalGeneral"] = pd.NA

    # ---------- Join with data_jugadores (full outer) ----------
    df_dj = df_jugadores.rename(columns={"id": "ID"})
    if "Title" in df_dj.columns:
        df_dj = df_dj.rename(columns={"Title": "nombreJugador_dj"})

    dj_cols = ["ID", "nombreJugador_dj", "nombrePadreTutor", "categoria", "edad"]
    dj_cols = [c for c in dj_cols if c in df_dj.columns]
    df_dj_sel = df_dj[dj_cols].copy()

    df_final = df_final.merge(
        df_dj_sel,
        how="outer",
        left_on="idJugador",
        right_on="ID",
        suffixes=("", "_dj"),
    )

    # data_jugadores.*
    if "ID_dj" in df_final.columns:
        df_final = df_final.rename(columns={"ID_dj": "data_jugadores.ID"})
    else:
        df_final["data_jugadores.ID"] = pd.NA

    if "nombreJugador_dj" in df_final.columns:
        df_final = df_final.rename(
            columns={"nombreJugador_dj": "data_jugadores.nombreJugador"}
        )
    else:
        df_final["data_jugadores.nombreJugador"] = pd.NA

    if "categoria_dj" in df_final.columns:
        df_final = df_final.rename(
            columns={"categoria_dj": "data_jugadores.categoria"}
        )
    else:
        df_final["data_jugadores.categoria"] = pd.NA

    # ---------- Back-fill from *_dj for all rows ----------
    # TutorJugador / Edad helpers from right side
    if "nombrePadreTutor_dj" in df_final.columns:
        df_final["TutorJugador"] = df_final.get("TutorJugador", pd.NA)
        df_final["TutorJugador"] = df_final["TutorJugador"].fillna(
            df_final["nombrePadreTutor_dj"]
        )

    if "edad_dj" in df_final.columns:
        df_final["Edad"] = df_final.get("Edad", pd.NA)
        df_final["Edad"] = df_final["Edad"].fillna(df_final["edad_dj"])

    # Rebuild edadEtiqueta from final Edad
    if "Edad" in df_final.columns:
        df_final["edadEtiqueta"] = df_final["Edad"].apply(
            lambda x: f"{int(x)}-Años" if pd.notnull(x) else pd.NA
        )

    # ---------- Duplicate base fields (nombrePadreTutor, categoria, edad) ----------
    # nombrePadreTutor column (besides TutorJugador)
    df_final["nombrePadreTutor"] = df_final.get("nombrePadreTutor", pd.NA)
    df_final["nombrePadreTutor"] = df_final["nombrePadreTutor"].fillna(
        df_final.get("TutorJugador")
    )

    # categoria / edad duplicates
    df_final["Categoria"] = df_final.get("Categoria", pd.NA)
    df_final["Edad"] = df_final.get("Edad", pd.NA)

    df_final["categoria"] = df_final.get("categoria", pd.NA)
    df_final["categoria"] = df_final["categoria"].fillna(df_final["Categoria"])

    df_final["edad"] = df_final.get("edad", pd.NA)
    df_final["edad"] = df_final["edad"].fillna(df_final["Edad"])

    # nombreJugador main column: use Jugador when missing
    df_final["Jugador"] = df_final.get("Jugador", pd.NA)
    df_final["nombreJugador"] = df_final.get("nombreJugador", pd.NA)
    df_final["nombreJugador"] = df_final["nombreJugador"].fillna(df_final["Jugador"])

    # ---------- Identify "no credit" rows ----------
    # No credit if credit ID is null but data_jugadores.ID exists
    mask_no_credit = df_final["ID"].isna() & df_final["data_jugadores.ID"].notna()

    # For those, if idJugador is empty, map it to player ID
    if "idJugador" in df_final.columns:
        df_final.loc[mask_no_credit, "idJugador"] = df_final.loc[
            mask_no_credit, "data_jugadores.ID"
        ]

    # Default estado final for no-credit players
    df_final.loc[mask_no_credit, "estaFinalGeneral"] = df_final.loc[
        mask_no_credit, "estaFinalGeneral"
    ].fillna("SIN CREDITO")

    # For no-credit players, also make sure all person fields are filled
    for col, source in [
        ("Jugador", "data_jugadores.nombreJugador"),
        ("nombreJugador", "data_jugadores.nombreJugador"),
        ("TutorJugador", "nombrePadreTutor"),
        ("Categoria", "data_jugadores.categoria"),
        ("categoria", "data_jugadores.categoria"),
        ("Edad", "edad"),
        ("edad", "edad"),
    ]:
        if col in df_final.columns and source in df_final.columns:
            df_final.loc[mask_no_credit, col] = df_final.loc[
                mask_no_credit, col
            ].fillna(df_final.loc[mask_no_credit, source])

    # Rebuild edadEtiqueta just in case
    if "Edad" in df_final.columns:
        df_final["edadEtiqueta"] = df_final["Edad"].apply(
            lambda x: f"{int(x)}-Años" if pd.notnull(x) else pd.NA
        )

    # ---------- Drop helper columns we no longer need ----------
    for col in ["nombrePadreTutor_dj", "edad_dj"]:
        if col in df_final.columns:
            df_final = df_final.drop(columns=[col])

    # ---------- Save ----------
    out_path = os.path.join(PROCESSED_DIR, "creditos_resumen_view.csv")
    df_final.to_csv(out_path, index=False)
    print(f"✔ creditos_resumen_view.csv written to {out_path} (rows={len(df_final)})")



def _parse_date_only(value):
    """
    Take a value like:
      - '2025-10-01T07:00:'
      - '2025-10-01T07:00:00Z'
      - '2025-10-01'
    and return a Python date (YYYY-MM-DD) or pd.NaT if it can't be parsed.
    """
    if pd.isna(value):
        return pd.NaT

    txt = str(value).strip()
    if not txt:
        return pd.NaT

    # If looks like ISO (has 'T'), keep only part before 'T'
    if "T" in txt:
        txt = txt.split("T", 1)[0]

    # Also trim a trailing colon if it exists (like '2025-10-01T07:00:')
    if txt.endswith(":"):
        txt = txt[:-1]

    try:
        return pd.to_datetime(txt, errors="raise").date()
    except Exception:
        return pd.NaT


def transform_jugadores_dates() -> None:
    """
    Clean Jugadores date-like columns from jugadores_raw.csv and
    write a jugadores_view.csv with safe YYYY-MM-DD dates.
    """

    jugadores_path = os.path.join(RAW_DIR, "jugadores_raw.csv")
    df = pd.read_csv(jugadores_path)

    # If you want, keep same column names as raw (Power BI will still see them),
    # or you can rename here to match your existing model:
    # df = df.rename(columns={"id": "ID", "Title": "nombreJugador"})

    # Date-like columns we care about
    date_cols = ["apertura", "cierre", "Created", "Modified"]

    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(_parse_date_only)

    # Optional: cast to string 'YYYY-MM-DD' instead of Python date objects
    # so Power BI treats them cleanly as dates when importing CSV:
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda d: d.strftime("%Y-%m-%d") if pd.notna(d) else ""
            )

    out_path = os.path.join(PROCESSED_DIR, "jugadores_view.csv")
    df.to_csv(out_path, index=False)
    print(f"✔ jugadores_view.csv written to {out_path} (rows={len(df)})")


def transform_categorias() -> None:
    """
    Clean categorias_raw.csv and write categorias_view.csv

    - Fix ISO datetime coming from SharePoint (Created / Modified / _ComplianceTagWrittenTime)
    - Rename:
        id   -> ID
        Title -> denominacion
    - Ensure the Power BI-friendly columns:
        ID, denominacion, Item Type, Path
    """

    categorias_path = os.path.join(RAW_DIR, "categorias_raw.csv")
    df = pd.read_csv(categorias_path)

    # --- Rename SharePoint columns to PB-friendly names ---
    df = df.rename(columns={
        "id": "ID",
        "Title": "denominacion",
    })

    # --- Fix datetime columns coming in as ISO strings ---
    for col in ["Created", "Modified", "_ComplianceTagWrittenTime"]:
        if col in df.columns:
            s = pd.to_datetime(df[col], errors="coerce", utc=True)
            # strip timezone → tz-naive
            try:
                s = s.dt.tz_convert(None)
            except TypeError:
                try:
                    s = s.dt.tz_localize(None)
                except TypeError:
                    pass
            df[col] = s

    # --- Ensure Power BI expected columns exist ---
    expected_cols = ["ID", "denominacion", "Item Type", "Path"]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Put expected columns first, keep the rest after
    other_cols = [c for c in df.columns if c not in expected_cols]
    df_out = df[expected_cols + other_cols]

    out_path = os.path.join(PROCESSED_DIR, "categorias_view.csv")
    df_out.to_csv(out_path, index=False)
    print(f"✔ categorias_view.csv written to {out_path} (rows={len(df_out)})")


def main():
    transform_cobros()
    transform_creditos()
    transform_estado_general()
    transform_creditos_resumen()
    transform_jugadores_dates()
    transform_categorias()
if __name__ == "__main__":
    main()

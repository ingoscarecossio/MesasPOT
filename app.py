# -*- coding: utf-8 -*-
"""
Buscador de Mesas — ULTRA PRO (GitHub-ready, standalone builder)
- Datos embebibles (Base64) y/o lectura automática desde repo / uploader.
- Delegaciones externas “mandatorias” para Conflictos (match exacto + tokens).
- Exportadores ICS (persona/aula/filtro), Gantt, Heatmap, Agenda, Resumen, Diagnóstico.
- Botón “Construir .py standalone (embebido)” para publicar sin archivos externos.
"""
import io, re, uuid, zipfile, base64, unicodedata, difflib, os, textwrap, inspect, sys
from datetime import datetime, date, time, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Set

import pandas as pd
import streamlit as st
import plotly.express as px

# ========== EMBEBIDOS (PÉGALOS AQUÍ PARA PUBLICAR SIN ARCHIVOS) ==========
# Deja vacíos para desarrollar desde archivos del repo; usa el botón "Construir .py standalone (embebido)"
# para generar y descargar un .py con estos campos rellenos automáticamente.
_EMBED_XLSX_B64 = ""        # STREAMLIT.xlsx (hoja principal p.ej. “Calendario”)
_EMBED_DELEG_B64 = ""       # DELEGACIONES.xlsx
_BG_B64 = ""                # Imagen de fondo opcional (png/jpg en base64)
_SHEET_CANDIDATES = ["Calendario", "Agenda", "Programación"]

# ========== CONFIG ==========
_DELEG_MARKER_REGEX = re.compile(r"\(\s*no disponible\s*,\s*asignar delegado\s*\)", re.IGNORECASE)
_SEP_REGEX = re.compile(r"[;,/]|\n|\r|\t|\||\u2022|·")

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("America/Bogota")
except Exception:
    TZ = timezone(timedelta(hours=-5))

st.set_page_config(page_title="Buscador de Mesas · ULTRA PRO", page_icon="🗂️", layout="wide", initial_sidebar_state="expanded")

# ========== UTILES UI ==========
def _keyns(section_name: str, base: str) -> str:
    safe = section_name.replace(" ", "_").lower() if isinstance(section_name, str) else "app"
    return f"{safe}__{base}"

def inject_base_css(dark: bool = True, shade: float = 0.75, density: str = "compacta"):
    if _BG_B64:
        bg_url = f"data:image/png;base64,{_BG_B64}"
        overlay = f"linear-gradient(rgba(0,0,0,{shade}), rgba(0,0,0,{shade}))"
        bg_css = f"background: {overlay}, url('{bg_url}') center center / cover fixed no-repeat;"
    else:
        bg_css = f"background: {'#0b1220' if dark else '#f7fafc'};"
    row_pad = {"compacta":"0.25rem","media":"0.5rem","amplia":"0.8rem"}[density]
    st.markdown(f"""
    <style>
    .stApp {{
        {bg_css}
        color: {"#e5e7eb" if dark else "#111827"} !important;
    }}
    .block-container {{ padding-top: 1.0rem; backdrop-filter: saturate(1.1); }}
    .gradient-title {{
        background: linear-gradient(90deg, #60a5fa 0%, #22d3ee 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800; letter-spacing: .2px;
    }}
    .card {{
        border-radius: 16px; padding: 1rem 1.2rem; 
        border: 1px solid {("#1f2937" if dark else "#e5e7eb")};
        background: {"rgba(17,24,39,0.82)" if dark else "rgba(255,255,255,0.93)"};
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
    }}
    .kpi {{ font-size: .9rem; color: {"#cbd5e1" if dark else "#6b7280"}; margin-bottom: .25rem; }}
    .kpi .value {{ display:block; font-size:1.6rem; font-weight:700; color: {"#f8fafc" if dark else "#111827"}; }}
    .small {{ font-size: .85rem; color: {"#cbd5e1" if dark else "#6b7280"}; }}
    .stDataFrame div[role='row'] {{ padding-top: {row_pad}; padding-bottom: {row_pad}; }}
    .dataframe th, .dataframe td {{ background: transparent !important; }}
    </style>
    """, unsafe_allow_html=True)

# ========== NORMALIZACIÓN ==========
COLUMN_ALIASES = {
    "Mesa": ["Mesa", "N° Mesa", "No Mesa", "Numero Mesa", "Número Mesa"],
    "Nombre de la mesa": ["Nombre de la mesa", "Nombre mesa", "Mesa - Nombre", "Titulo Mesa", "Título Mesa"],
    "Fecha": ["Fecha", "Día", "Dia"],
    "Inicio": ["Inicio", "Hora inicio", "Hora de inicio", "Start", "From"],
    "Fin": ["Fin", "Hora fin", "Hora de fin", "End", "To"],
    "Aula": ["Aula", "Lugar", "Sala", "Espacio"],
    "Participantes": ["Participantes", "Asistentes", "Invitados"],
    "Responsable": ["Responsable"],
    "Corresponsable": ["Corresponsable", "Co-responsable", "Co Responsable"],
    "Delegaciones": ["Delegaciones", "Delegation", "Delegado"]
}

def _safe_str(x): return "" if pd.isna(x) else str(x).strip()

def _norm(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize('NFKD', s).encode('ascii','ignore').decode('ascii')
    s = re.sub(r"\s+"," ", s)
    return s.lower().strip()

def _strip_delegate_marker(s: str) -> str:
    if not s: return s
    return _DELEG_MARKER_REGEX.sub("", s).strip()

def _has_delegate_marker(s: str) -> bool:
    return bool(s) and bool(_DELEG_MARKER_REGEX.search(s))

def find_col(df: pd.DataFrame, canonical: str):
    aliases = COLUMN_ALIASES.get(canonical, [canonical])
    for a in aliases:
        if a in df.columns: return a
    cols_low = {str(c).lower(): c for c in df.columns}
    for a in aliases:
        if a.lower() in cols_low: return cols_low[a.lower()]
    return None

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for canonical in COLUMN_ALIASES.keys():
        col = find_col(df, canonical)
        if col is None:
            if canonical == "Delegaciones": continue
            st.error(f"Falta la columna requerida: **{canonical}**"); st.stop()
        mapping[col] = canonical
    return df.rename(columns=mapping)

def _to_date(x):
    if isinstance(x, date) and not isinstance(x, datetime): return x
    if isinstance(x, datetime): return x.date()
    d = pd.to_datetime(x, errors="coerce", dayfirst=True)
    return None if pd.isna(d) else d.date()

def _to_time(x):
    if isinstance(x, time): return x
    if isinstance(x, datetime): return x.time().replace(microsecond=0)
    d = pd.to_datetime(x, errors="coerce")
    if not pd.isna(d) and isinstance(d, pd.Timestamp): return d.time().replace(microsecond=0)
    try:
        s = str(x).strip()
        if not s: return None
        hh, mm = s.split(":")[:2]
        return time(int(hh), int(mm))
    except Exception:
        return None

def combine_dt(fecha, hora):
    d = _to_date(fecha); t = _to_time(hora)
    if d is None or t is None: return None
    dt = datetime(d.year, d.month, d.day, t.hour, t.minute, t.second or 0)
    try: return dt.replace(tzinfo=TZ)
    except Exception: return dt

def ensure_sorted(df):
    df = df.copy()
    if "Fecha" in df.columns and "Inicio" in df.columns:
        df["_Fecha_dt"] = df["Fecha"].apply(_to_date)
        df["_Inicio_t"] = df["Inicio"].apply(_to_time)
        df.sort_values(by=["_Fecha_dt","_Inicio_t"], inplace=True, kind="mergesort")
        df.drop(columns=["_Fecha_dt","_Inicio_t"], inplace=True)
    return df

# ========== CARGA DE DATOS (robusta) ==========
def _read_excel_from_bytes(data: bytes, sheet_candidates=None) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(data))
    sheet = None
    if sheet_candidates:
        for cand in sheet_candidates:
            if cand in xls.sheet_names:
                sheet = cand; break
    if sheet is None: sheet = xls.sheet_names[0]
    return xls.parse(sheet)

@st.cache_data(show_spinner=False)
def _try_load_main(embed_b64: str):
    # Prioridad: uploader -> embebido -> archivo del repo
    if "upload_main" in st.session_state and st.session_state.upload_main is not None:
        file = st.session_state.upload_main
        return pd.ExcelFile(file).parse(0)
    if embed_b64:
        raw = base64.b64decode(embed_b64.encode("utf-8"))
        return _read_excel_from_bytes(raw, _SHEET_CANDIDATES)
    for path in ["STREAMLIT.xlsx","./data/STREAMLIT.xlsx","/mnt/data/STREAMLIT.xlsx"]:
        if os.path.exists(path):
            xls = pd.ExcelFile(path)
            sheet = "Calendario" if "Calendario" in xls.sheet_names else xls.sheet_names[0]
            return xls.parse(sheet)
    st.error("No se encontró el Excel principal. Carga **STREAMLIT.xlsx** o usa la versión embebida.")
    st.stop()

@st.cache_data(show_spinner=False)
def _try_load_deleg(embed_b64: str):
    if "upload_deleg" in st.session_state and st.session_state.upload_deleg is not None:
        file = st.session_state.upload_deleg
        return pd.ExcelFile(file).parse(0)
    if embed_b64:
        raw = base64.b64decode(embed_b64.encode("utf-8"))
        return _read_excel_from_bytes(raw, None)
    for path in ["DELEGACIONES.xlsx","./data/DELEGACIONES.xlsx","/mnt/data/DELEGACIONES.xlsx"]:
        if os.path.exists(path):
            return pd.ExcelFile(path).parse(0)
    return pd.DataFrame()

# ========== SIDEBAR ==========
if "dark" not in st.session_state:
    st.session_state.dark = True

with st.sidebar:
    st.session_state.dark = st.checkbox("Modo oscuro", value=st.session_state.dark, key="sidebar_dark")
    section = st.radio("Sección", ["Resumen","Consulta","Agenda","Gantt","Heatmap","Conflictos","Delegaciones","Diagnóstico","Acerca de"], index=0, key="sidebar_section")
    ui_dark = st.slider("Intensidad del fondo", 0.0, 1.0, 0.75, 0.05, key="sidebar_shade")
    densidad = st.select_slider("Densidad de tabla", options=["compacta","media","amplia"], value="compacta", key="sidebar_density")

    st.markdown("### 📦 Datos (carga rápida)")
    st.file_uploader("STREAMLIT.xlsx (principal)", type=["xlsx"], key="upload_main")
    st.file_uploader("DELEGACIONES.xlsx (opcional)", type=["xlsx"], key="upload_deleg")

inject_base_css(st.session_state.dark, ui_dark, densidad)
st.markdown("<h1 class='gradient-title'>🗂️ Buscador de Mesas · ULTRA PRO</h1>", unsafe_allow_html=True)
st.caption("GitHub-ready • Datos embebibles • Delegaciones robustas • Exportadores ICS")

# ========== LECTURA Y NORMALIZACIÓN ==========
raw = _try_load_main(_EMBED_XLSX_B64)
df0 = normalize_cols(raw)

def add_delegate_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["Participantes", "Responsable", "Corresponsable"]:
        if col in df.columns:
            df[f"__flag_{col}"] = df[col].fillna("").astype(str).apply(_has_delegate_marker)
            df[col] = df[col].fillna("").astype(str).apply(_strip_delegate_marker)
    flag_cols = [c for c in df.columns if c.startswith("__flag_")]
    df["Requiere Delegación"] = df[flag_cols].any(axis=1) if flag_cols else False
    df.drop(columns=flag_cols, inplace=True, errors="ignore")
    return df

df0 = add_delegate_flags(df0)
for col in ["Participantes","Responsable","Corresponsable","Aula","Nombre de la mesa","Mesa"]:
    if col in df0.columns: df0[f"__norm_{col}"] = df0[col].fillna("").astype(str).apply(_norm)
df0 = ensure_sorted(df0)

def _split_people(cell):
    if pd.isna(cell): return []
    parts = _SEP_REGEX.split(str(cell))
    clean = [p.strip() for p in parts if p and p.strip()]
    out = []
    for p in clean:
        if " y " in p: out.extend([x.strip() for x in p.split(" y ") if x.strip()])
        else: out.append(p)
    return out

def build_index(df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        part_list = _split_people(r.get("Participantes", ""))
        extra = []
        if _safe_str(r.get("Responsable")): extra.append(_safe_str(r.get("Responsable")))
        if _safe_str(r.get("Corresponsable")): extra.append(_safe_str(r.get("Corresponsable")))
        everyone = list(dict.fromkeys(extra + (part_list or [None])))
        if not everyone:
            rows.append({**r.to_dict(), "Participante_individual": None})
        else:
            for p in everyone:
                rows.append({**r.to_dict(), "Participante_individual": p})
    return ensure_sorted(pd.DataFrame(rows))

idx = build_index(df0)
for col in ["Responsable","Corresponsable","Aula","Nombre de la mesa","Participantes","Mesa"]:
    if col in idx.columns:
        idx[f"__norm_{col}"] = idx[col].fillna("").astype(str).apply(_norm)
idx["__norm_part"] = idx["Participante_individual"].fillna("").astype(str).apply(_norm)

# ========== DELEGACIONES EXTERNAS ==========
deleg_raw = _try_load_deleg(_EMBED_DELEG_B64)

def _prepare_deleg_map(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["__actor","__mesa","__fecha"])
    col_actor = None; col_mesa = None; col_fecha = None
    for c in df.columns:
        cl = str(c).lower()
        if col_actor is None and ("actor" in cl or "persona" in cl or "nombre" in cl):
            col_actor = c
        if col_mesa is None and "mesa" in cl:
            col_mesa = c
        if col_fecha is None and "fecha" in cl:
            col_fecha = c
    if col_actor is None or col_mesa is None or col_fecha is None:
        return pd.DataFrame(columns=["__actor","__mesa","__fecha"])
    out = pd.DataFrame({
        "__actor": df[col_actor].astype(str).map(_norm),
        "__mesa":  df[col_mesa].astype(str).map(_norm),
        "__fecha": pd.to_datetime(df[col_fecha], errors="coerce").dt.date
    }).dropna(subset=["__mesa","__fecha"])
    out = out[out["__actor"].astype(bool)]
    return out

deleg_map = _prepare_deleg_map(deleg_raw)

def _token_subset(a: str, b: str) -> bool:
    sa, sb = set(a.split()), set(b.split())
    if not sa or not sb: return False
    return sa.issubset(sb) if len(sa) <= len(sb) else sb.issubset(sa)

def annotate_delegations(idx: pd.DataFrame, dmap: pd.DataFrame) -> pd.DataFrame:
    idx = idx.copy()
    if dmap.empty or "Mesa" not in idx.columns:
        idx["__delegado_por_archivo"] = False
        return idx
    groups: Dict[Tuple[str, date], Set[str]] = {}
    for _, r in dmap.iterrows():
        key = (r["__mesa"], r["__fecha"])
        groups.setdefault(key, set()).add(r["__actor"])
    flags = []
    for _, r in idx.iterrows():
        key = (r.get("__norm_Mesa",""), _to_date(r.get("Fecha")))
        candidates = groups.get(key, set())
        actor = r.get("__norm_part","")
        ok = (actor in candidates) or any(_token_subset(actor, cand) for cand in candidates)
        flags.append(ok)
    idx["__delegado_por_archivo"] = flags
    return idx

idx = annotate_delegations(idx, deleg_map)

def mark_event_delegations(df: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if idx.empty: 
        df["Delegado por lista"] = False
        return df
    delegated_events = set((r["__norm_Mesa"], _to_date(r["Fecha"])) for _, r in idx[idx["__delegado_por_archivo"]==True].iterrows())
    df["Delegado por lista"] = [((row.get("__norm_Mesa",""), _to_date(row.get("Fecha"))) in delegated_events) for _, row in df.iterrows()]
    df["Requiere Delegación"] = df.get("Requiere Delegación", False) | df["Delegado por lista"]
    return df

df0 = mark_event_delegations(df0, idx)

# ========== MATCH FUZZY ==========
def _score(a: str, b: str) -> float:
    a = _norm(a); b = _norm(b)
    if not a or not b: return 0.0
    return 100.0 * difflib.SequenceMatcher(None, a, b).ratio()

def smart_match(series_norm: pd.Series, query: str, threshold: int = 80):
    q = _norm(query)
    if not q: return pd.Series([True]*len(series_norm), index=series_norm.index)
    return series_norm.apply(lambda s: _score(s, q) >= threshold)

# ========== CABECERA ==========
st.caption("Alto contraste, filtros avanzados, delegaciones obligatorias en conflictos y publicación sin archivos externos.")
st.divider()

# ========== SECCIONES ==========
if section == "Resumen":
    st.subheader("📈 Resumen ejecutivo")
    def make_stats(df: pd.DataFrame):
        n_mesas = df.shape[0]
        aulas = df["Aula"].dropna().astype(str).nunique() if "Aula" in df else 0
        dias = df["Fecha"].apply(_to_date).dropna().nunique() if "Fecha" in df else 0
        personas = set()
        personas.update(df["Responsable"].dropna().astype(str).tolist())
        personas.update(df["Corresponsable"].dropna().astype(str).tolist())
        for v in df["Participantes"].fillna("").astype(str).tolist():
            personas.update(_split_people(v))
        n_personas = len({x.strip() for x in personas if x and x.strip()})
        return n_mesas, aulas, dias, n_personas

    tm, na, nd, np = make_stats(df0)
    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(f"<div class='card'><div class='kpi'>Mesas</div><span class='value'>{tm}</span></div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='card'><div class='kpi'>Aulas</div><span class='value'>{na}</span></div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='card'><div class='kpi'>Días</div><span class='value'>{nd}</span></div>", unsafe_allow_html=True)
    with c4: st.markdown(f"<div class='card'><div class='kpi'>Personas únicas</div><span class='value'>{np}</span></div>", unsafe_allow_html=True)

    # Top personas / aulas
    all_people = []
    for v in df0["Participantes"].fillna("").astype(str).tolist(): all_people += _split_people(v)
    all_people += df0["Responsable"].dropna().astype(str).tolist()
    all_people += df0["Corresponsable"].dropna().astype(str).tolist()
    s = pd.Series([p.strip() for p in all_people if p and str(p).strip()])
    top_people = s.value_counts().head(10).rename_axis("Persona").reset_index(name="Conteo")
    uso_aula = df0.groupby("Aula")["Mesa"].count().sort_values(ascending=False).head(10).rename_axis("Aula").reset_index(name="Mesas")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Top 10 personas por participación**")
        if not top_people.empty:
            st.plotly_chart(px.bar(top_people, x="Conteo", y="Persona", orientation="h", height=400), use_container_width=True)
        else:
            st.info("Sin datos de personas.")
    with c2:
        st.markdown("**Aulas más usadas (Top 10)**")
        if not uso_aula.empty:
            st.plotly_chart(px.bar(uso_aula, x="Mesas", y="Aula", orientation="h", height=400), use_container_width=True)
        else:
            st.info("Sin datos de aulas.")

    # Distribuciones
    dfh = df0.copy()
    dfh["_fecha"] = dfh["Fecha"].apply(_to_date)
    dfh["_ini"] = dfh["Inicio"].apply(_to_time)
    dfh = dfh.dropna(subset=["_fecha"])
    dfh["Día semana"] = dfh["_fecha"].apply(lambda d: ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"][d.weekday()] if d else None)
    by_dow = dfh.groupby("Día semana")["Mesa"].count().reindex(["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"]).fillna(0).reset_index(name="Mesas")
    c3, c4 = st.columns(2)
    with c3:
        st.markdown("**Mesas por día de la semana**")
        st.plotly_chart(px.bar(by_dow, x="Día semana", y="Mesas", height=300), use_container_width=True)
    with c4:
        st.markdown("**Horas de inicio (histograma)**")
        hh = [t.hour for t in dfh["_ini"] if t is not None]
        if hh:
            st.plotly_chart(px.histogram(pd.DataFrame({"Hora": hh}), x="Hora", nbins=12, height=300), use_container_width=True)
        else:
            st.info("Sin horas de inicio válidas.")

    st.markdown("### ⬇️ Exportadores rápidos")
    # ICS por Persona / Aula
    people = sorted({p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                                    + df0["Responsable"].dropna().astype(str).tolist()
                                    + df0["Corresponsable"].dropna().astype(str).tolist()) if p})
    c1, c2 = st.columns(2)
    with c1:
        psel = st.selectbox("ICS por persona", options=["(ninguna)"]+people, index=0, key=_keyns(section,"res_ics_persona"))
        if psel and psel != "(ninguna)":
            m = (smart_match(idx["__norm_part"], psel, 90) |
                 smart_match(idx["__norm_Responsable"], psel, 90) |
                 smart_match(idx["__norm_Corresponsable"], psel, 90))
            rows = idx.loc[m, ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"]]
            rows = ensure_sorted(rows)
            if not rows.empty:
                st.download_button("Descargar ICS (persona)", data=build_ics(rows, calendar_name=f"Agenda — {psel}"),
                                   mime="text/calendar", file_name=f"agenda_{psel}.ics",
                                   key=_keyns(section,"btn_ics_persona"))
            else:
                st.info("Sin eventos para esta persona.")
    with c2:
        aulas = sorted(df0["Aula"].dropna().astype(str).unique().tolist())
        asel = st.selectbox("ICS por aula", options=["(ninguna)"]+aulas, index=0, key=_keyns(section,"res_ics_aula"))
        if asel and asel != "(ninguna)":
            rows = df0[df0["Aula"].astype(str)==asel][["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"]]
            rows = ensure_sorted(rows)
            if not rows.empty:
                st.download_button("Descargar ICS (aula)", data=build_ics(rows, calendar_name=f"Aula — {asel}"),
                                   mime="text/calendar", file_name=f"aula_{asel}.ics",
                                   key=_keyns(section,"btn_ics_aula"))
            else:
                st.info("Sin eventos para esta aula.")

elif section == "Consulta":
    with st.expander("⚙️ Filtros", expanded=False):
        c1, c2, c3 = st.columns(3)
        fechas_validas = [d for d in df0["Fecha"].apply(_to_date).dropna().tolist()]
        if fechas_validas: dmin, dmax = min(fechas_validas), max(fechas_validas)
        else:
            today = date.today(); dmin, dmax = today, today
        with c1:
            dr = st.date_input("Rango de fechas", value=(dmin, dmax), min_value=dmin, max_value=dmax, key=_keyns(section,"filtro_fechas"))
            fmin, fmax = (dr if isinstance(dr, tuple) and len(dr)==2 else (dmin, dmax))
            horas = st.slider("Rango de horas", 0, 23, (6, 20), key=_keyns(section,"filtro_horas"))
        with c2:
            aulas = sorted(df0["Aula"].dropna().astype(str).unique().tolist())
            aula_sel = st.multiselect("Aulas", ["(todas)"] + aulas, default=["(todas)"], key=_keyns(section,"filtro_aulas"))
            dow = st.multiselect("Días semana", ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"], default=["Lun","Mar","Mié","Jue","Vie"], key=_keyns(section,"filtro_dow"))
        with c3:
            responsables = sorted(df0["Responsable"].dropna().astype(str).unique().tolist())
            rsel = st.multiselect("Responsables", responsables, key=_keyns(section,"filtro_resp"))
            solo_deleg = st.checkbox("🔴 Solo mesas que requieren delegación", value=False, key=_keyns(section,"filtro_deleg"))

    modo = st.radio("Búsqueda", ["Seleccionar", "Texto"], index=0, horizontal=True, key=_keyns(section,"modo_busqueda"))
    people = sorted({p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                                    + df0["Responsable"].dropna().astype(str).tolist()
                                    + df0["Corresponsable"].dropna().astype(str).tolist()) if p})
    term = st.selectbox("Participante", options=[""]+people, index=0, key=_keyns(section,"sel_participante")) if modo=="Seleccionar" else st.text_input("Escriba parte del nombre", key=_keyns(section,"term_texto"))

    mask = pd.Series([True]*len(idx))
    fvals = idx["Fecha"].apply(_to_date)
    mask &= fvals.apply(lambda d: (d is not None) and (fmin <= d <= fmax))

    if aula_sel and not (len(aula_sel)==1 and aula_sel[0]=="(todas)"):
        allowed = set([a for a in aula_sel if a != "(todas)"])
        mask &= idx["Aula"].fillna("").astype(str).isin(allowed)

    dows = {"Lun":0,"Mar":1,"Mié":2,"Jue":3,"Vie":4,"Sáb":5,"Dom":6}
    selected_dows = [dows[x] for x in dow] if dow else list(dows.values())
    def _dow_ok(v):
        dd = _to_date(v)
        return dd is not None and dd.weekday() in selected_dows
    mask &= idx["Fecha"].apply(_dow_ok)

    hmin, hmax = horas
    def _hour_ok(v):
        t = _to_time(v)
        return (t is not None) and (hmin <= t.hour <= hmax)
    mask &= idx["Inicio"].apply(_hour_ok)

    if rsel:
        mask &= idx["Responsable"].fillna("").astype(str).isin(set(rsel))
    if solo_deleg:
        mask &= idx["Requiere Delegación"] == True
    if term:
        mask &= (smart_match(idx["__norm_part"], term) |
                 smart_match(idx["__norm_Responsable"], term) |
                 smart_match(idx["__norm_Corresponsable"], term))

    cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"]
    res = idx.loc[mask, cols].drop_duplicates().reset_index(drop=True)
    res = ensure_sorted(res)

    c1, c2, c3, c4 = st.columns(4)
    def make_stats(df: pd.DataFrame):
        n_mesas = df.shape[0]
        aulas = df["Aula"].dropna().astype(str).nunique() if "Aula" in df else 0
        dias = df["Fecha"].apply(_to_date).dropna().nunique() if "Fecha" in df else 0
        personas = set()
        personas.update(df["Responsable"].dropna().astype(str).tolist())
        personas.update(df["Corresponsable"].dropna().astype(str).tolist())
        for v in df["Participantes"].fillna("").astype(str).tolist():
            personas.update(_split_people(v))
        n_personas = len({x.strip() for x in personas if x and x.strip()})
        return n_mesas, aulas, dias, n_personas
    tm, na, nd, np = make_stats(res if not res.empty else df0)
    with c1: st.markdown(f"<div class='card'><div class='kpi'>Mesas</div><span class='value'>{tm}</span></div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='card'><div class='kpi'>Aulas</div><span class='value'>{na}</span></div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='card'><div class='kpi'>Días</div><span class='value'>{nd}</span></div>", unsafe_allow_html=True)
    with c4: st.markdown(f"<div class='card'><div class='kpi'>Personas únicas</div><span class='value'>{np}</span></div>", unsafe_allow_html=True)

    st.subheader("📋 Resultados")
    if term == "" and res.empty:
        st.info("Empiece escribiendo un nombre o elija uno de la lista.")
    elif res.empty:
        st.warning("Sin resultados.")
    else:
        rf = res.copy()
        if pd.api.types.is_datetime64_any_dtype(rf["Fecha"]):
            rf["Fecha"] = rf["Fecha"].dt.date.astype(str)
        else:
            rf["Fecha"] = rf["Fecha"].apply(lambda x: _to_date(x).isoformat() if _to_date(x) else _safe_str(x))
        for c in ["Inicio","Fin"]:
            def _fmt_hhmm(v):
                t = _to_time(v)
                return t.strftime("%H:%M") if t else _safe_str(v)
            rf[c] = rf[c].apply(_fmt_hhmm)

        rf.insert(0, "Delegación", rf["Requiere Delegación"].apply(lambda x: "🔴" if bool(x) else "—"))
        ordered = ["Delegación","Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"]
        rf = rf[[c for c in ordered if c in rf.columns]]
        st.dataframe(rf, use_container_width=True, hide_index=True)

        st.markdown("#### ⬇️ Descargas")
        st.download_button("CSV (filtro)", data=rf.to_csv(index=False).encode("utf-8-sig"),
                           mime="text/csv", file_name="resultados.csv", key=_keyns(section,"dl_csv"))
        xls_buf = io.BytesIO()
        with pd.ExcelWriter(xls_buf, engine="xlsxwriter") as w: rf.to_excel(w, sheet_name="Resultados", index=False)
        st.download_button("Excel (filtro)", data=xls_buf.getvalue(),
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           file_name="resultados.xlsx", key=_keyns(section,"dl_xlsx"))
        calname = f"Mesas — {term}" if term else "Mesas"
        st.download_button("ICS (todo en uno)", data=build_ics(rf, calendar_name=calname),
                           mime="text/calendar", file_name="mesas.ics", key=_keyns(section,"dl_ics_all"))
        st.download_button("ICS por mesa (ZIP)", data=zip_split_ics(rf),
                           mime="application/zip", file_name="mesas_por_mesa.zip", key=_keyns(section,"dl_zip"))

        st.markdown("#### 🔎 Detalle rápido")
        opciones = rf["Nombre de la mesa"].astype(str).unique().tolist()
        mesa_sel = st.selectbox("Seleccione una mesa", ["(ninguna)"] + opciones, index=0, key=_keyns(section,"detalle_mesa"))
        if mesa_sel != "(ninguna)":
            sub = df0[df0["Nombre de la mesa"].astype(str)==mesa_sel].head(1)
            detalle = sub.to_dict(orient="records")[0]
            def _find_col(cols, pattern):
                for c in cols:
                    if re.search(pattern, c, flags=re.IGNORECASE):
                        return c
                return None
            col_obj = _find_col(df0.columns, r"objetivo\s*general")
            col_est = _find_col(df0.columns, r"propuesta|estrat(é|e)g")
            objetivo = detalle.get(col_obj, "") if col_obj else ""
            estrategia = detalle.get(col_est, "") if col_est else ""
            st.markdown(f"""
            <div class='card'>
            <b>{detalle.get('Nombre de la mesa','')}</b><br>
            <span class='small'>Objetivo:</span> {objetivo}<br>
            <span class='small'>Estrategia:</span> {estrategia}<br>
            <span class='small'>Aula:</span> {detalle.get('Aula','')} · <span class='small'>Fecha:</span> {detalle.get('Fecha','')} · <span class='small'>Hora:</span> {detalle.get('Inicio','')}–{detalle.get('Fin','')}<br>
            <span class='small'>Resp.:</span> {detalle.get('Responsable','')} · <span class='small'>Co-resp.:</span> {detalle.get('Corresponsable','')}
            </div>
            """, unsafe_allow_html=True)

elif section == "Agenda":
    st.subheader("🗓️ Agenda por persona")
    people = sorted({p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                                    + df0["Responsable"].dropna().astype(str).tolist()
                                    + df0["Corresponsable"].dropna().astype(str).tolist()) if p})
    persona = st.selectbox("Seleccione persona", options=people, key=_keyns(section,"agenda_persona"))
    if persona:
        m = (smart_match(idx["__norm_part"], persona, 90) |
             smart_match(idx["__norm_Responsable"], persona, 90) |
             smart_match(idx["__norm_Corresponsable"], persona, 90))
        rows = idx.loc[m, ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"]].copy()
        rows = ensure_sorted(rows)
        if rows.empty:
            st.info("Sin eventos para esta persona.")
        else:
            for _, r in rows.iterrows():
                st.markdown(f"**{_safe_str(r['Nombre de la mesa'])}**  \n{_to_date(r['Fecha'])} • {_to_time(r['Inicio']).strftime('%H:%M') if _to_time(r['Inicio']) else ''}–{_to_time(r['Fin']).strftime('%H:%M') if _to_time(r['Fin']) else ''} • Aula: {_safe_str(r['Aula'])}  \n<span class='small'>Resp.: {_safe_str(r['Responsable'])} • Co-resp.: {_safe_str(r['Corresponsable'])} • Delegación: {'Sí' if r['Requiere Delegación'] else 'No'}</span>", unsafe_allow_html=True)
                st.divider()
            st.download_button("⬇️ ICS (Agenda)", data=build_ics(rows, calendar_name=f"Agenda — {persona}"),
                               mime="text/calendar", file_name=f"agenda_{persona}.ics", key=_keyns(section,"agenda_ics"))
            gantt_rows = []
            for _, r in rows.iterrows():
                start = combine_dt(r.get("Fecha"), r.get("Inicio")); end = combine_dt(r.get("Fecha"), r.get("Fin"))
                if not (start and end): continue
                gantt_rows.append({"Mesa": _safe_str(r.get("Nombre de la mesa")), "Aula": _safe_str(r.get("Aula")), "start": start, "end": end,
                                   "Delegación": "Sí" if r.get("Requiere Delegación") else "No"})
            if gantt_rows:
                dfg = pd.DataFrame(gantt_rows)
                fig = px.timeline(dfg, x_start="start", x_end="end", y="Aula", color="Delegación", hover_data=["Mesa"])
                fig.update_yaxes(autorange="reversed")
                fig.update_layout(height=420, margin=dict(l=10,r=10,t=30,b=20))
                st.plotly_chart(fig, use_container_width=True)

elif section == "Gantt":
    st.subheader("📊 Gantt (señala delegaciones)")
    rows = []
    for _, r in df0.iterrows():
        start = combine_dt(r.get("Fecha"), r.get("Inicio")); end = combine_dt(r.get("Fecha"), r.get("Fin"))
        if not (start and end): continue
        rows.append({"Mesa": _safe_str(r.get("Nombre de la mesa")), "Aula": _safe_str(r.get("Aula")),
                     "start": start, "end": end, "Delegación": "Sí" if r.get("Requiere Delegación") else "No"})
    if not rows:
        st.info("No hay datos para Gantt.")
    else:
        dfg = pd.DataFrame(rows)
        fig = px.timeline(dfg, x_start="start", x_end="end", y="Aula", color="Delegación", hover_data=["Mesa"])
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(height=550, margin=dict(l=10,r=10,t=30,b=20))
        st.plotly_chart(fig, use_container_width=True)

elif section == "Heatmap":
    st.subheader("🗺️ Heatmap de ocupación (Aula x Día)")
    dfh = df0.copy()
    dfh["Día"] = dfh["Fecha"].apply(_to_date)
    piv = pd.pivot_table(dfh, index="Aula", columns="Día", values="Mesa", aggfunc="count", fill_value=0)
    if piv.empty:
        st.info("No hay datos para el heatmap.")
    else:
        fig = px.imshow(piv, aspect="auto", labels=dict(color="Mesas"))
        fig.update_layout(height=500, margin=dict(l=10,r=10,t=30,b=20))
        st.plotly_chart(fig, use_container_width=True)

elif section == "Conflictos":
    st.subheader("🚦 Solapes por persona (con delegaciones)")
    people = sorted({p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                                    + df0["Responsable"].dropna().astype(str).tolist()
                                    + df0["Corresponsable"].dropna().astype(str).tolist()) if p})
    psel = st.multiselect("Personas a auditar", options=people, key=_keyns(section,"conf_personas"))
    aplicar_deleg = st.checkbox("Aplicar delegaciones (ignorar eventos delegados)", value=True, key=_keyns(section,"conf_aplicar_deleg"))

    if not psel:
        st.info("Seleccione una o más personas.")
    else:
        conf_rows = []
        for person in psel:
            m = (smart_match(idx["__norm_part"], person, 90) |
                 smart_match(idx["__norm_Responsable"], person, 90) |
                 smart_match(idx["__norm_Corresponsable"], person, 90))
            sel = idx.loc[m, ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","__delegado_por_archivo"]].copy()
            if aplicar_deleg:
                sel = sel[sel["__delegado_por_archivo"] == False]
            evs = []
            for _, r in sel.iterrows():
                f = combine_dt(r.get("Fecha"), r.get("Inicio")); t2 = combine_dt(r.get("Fecha"), r.get("Fin"))
                if f and t2: evs.append({"Mesa": _safe_str(r.get("Nombre de la mesa")), "Aula": _safe_str(r.get("Aula")), "start": f, "end": t2})
            evs = sorted(evs, key=lambda x: (x["start"], x["end"]))
            for i in range(len(evs)):
                for j in range(i+1, len(evs)):
                    a, b = evs[i], evs[j]
                    if a["end"] > b["start"] and a["start"].date()==b["start"].date():
                        conf_rows.append({
                            "Persona": person,
                            "Mesa A": a["Mesa"], "Aula A": a["Aula"], "Inicio A": a["start"], "Fin A": a["end"],
                            "Mesa B": b["Mesa"], "Aula B": b["Aula"], "Inicio B": b["start"], "Fin B": b["end"],
                        })
        if not conf_rows:
            st.success("Sin solapes detectados. ✅")
        else:
            dfc = pd.DataFrame(conf_rows)
            st.dataframe(dfc, use_container_width=True, hide_index=True)

elif section == "Delegaciones":
    st.subheader("🛟 Reporte de Delegaciones")
    cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"]
    rep = df0[df0["Requiere Delegación"]==True][cols].copy()
    if rep.empty:
        st.info("No hay mesas marcadas con 'Requiere Delegación'.")
    else:
        st.dataframe(rep, use_container_width=True, hide_index=True)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            rep.to_excel(w, sheet_name="Delegaciones", index=False)
        st.download_button("⬇️ Delegaciones (Excel)", data=buf.getvalue(),
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           file_name="delegaciones.xlsx", key=_keyns(section,"dl_deleg_xlsx"))

elif section == "Diagnóstico":
    st.subheader("🧪 Diagnóstico de calidad de datos")
    issues = []
    # Nulos clave
    for key in ["Nombre de la mesa","Fecha","Inicio","Fin"]:
        if key in df0.columns:
            n_null = int(pd.isna(df0[key]).sum())
            if n_null > 0: issues.append(f"Hay {n_null} nulos en '{key}'.")
    # Duración negativa
    if all(k in df0.columns for k in ["Fecha","Inicio","Fin"]):
        def dur(a,b):
            ta, tb = _to_time(a), _to_time(b)
            if ta is None or tb is None: return None
            return (datetime.combine(date(2000,1,1), tb) - datetime.combine(date(2000,1,1), ta)).total_seconds()/60.0
        dur_min = df0.apply(lambda r: dur(r["Inicio"], r["Fin"]), axis=1)
        if (pd.Series(dur_min).dropna() < 0).any():
            issues.append("Existen filas con duraciones negativas (Fin < Inicio).")
    # Duplicados exactos
    if all(k in df0.columns for k in ["Fecha","Inicio","Aula","Nombre de la mesa"]):
        n_dups = int(df0.duplicated(subset=["Fecha","Inicio","Aula","Nombre de la mesa"], keep=False).sum())
        if n_dups: issues.append(f"{n_dups} duplicados por (Fecha, Inicio, Aula, Nombre).")
    if not issues:
        st.success("Sin problemas críticos detectados. ✅")
    else:
        for it in issues: st.error("• " + it)

    st.markdown("—")
    st.markdown("**Vista rápida de campos clave**")
    cols = [c for c in ["Mesa","Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"] if c in df0.columns]
    st.dataframe(df0[cols].head(30), use_container_width=True, hide_index=True)

    st.markdown("—")
    st.subheader("🧩 Construir .py standalone (embebido)")
    st.caption("Convierte STREAMLIT.xlsx y DELEGACIONES.xlsx en Base64 y descarga este mismo script con los embebidos pegados.")
    with st.form("builder_embedded"):
        dev_streamlit_path = st.text_input("Ruta STREAMLIT.xlsx (repo)", "STREAMLIT.xlsx")
        dev_deleg_path = st.text_input("Ruta DELEGACIONES.xlsx (repo, opcional)", "DELEGACIONES.xlsx")
        submitted = st.form_submit_button("Construir y descargar .py embebido")
    if submitted:
        def _b64_or_empty(path):
            if path and os.path.exists(path):
                with open(path, "rb") as f: return base64.b64encode(f.read()).decode("utf-8")
            return ""
        b64_main = _b64_or_empty(dev_streamlit_path)
        b64_deleg = _b64_or_empty(dev_deleg_path)
        src = inspect.getsource(sys.modules[__name__])
        # Reemplazar los placeholders
        src = re.sub(r'_EMBED_XLSX_B64\s*=\s*""', f'_EMBED_XLSX_B64 = """{b64_main}"""', src, count=1)
        src = re.sub(r'_EMBED_DELEG_B64\s*=\s*""', f'_EMBED_DELEG_B64 = """{b64_deleg}"""', src, count=1)
        file_bytes = src.encode("utf-8")
        st.download_button("⬇️ Descargar app_embebida.py", data=file_bytes, mime="text/x-python", file_name="app_embebida.py")

else:
    st.subheader("ℹ️ Acerca de")
    st.markdown("Publicación: 13/09/2025 — ULTRA PRO")
    st.markdown("• Listo para GitHub/Streamlit Cloud.  \n• Datos embebibles con un clic.  \n• Delegaciones robustas (exact match + token subset).")

# ========== ICS ==========
def escape_text(val: str) -> str:
    return val.replace("\\","\\\\").replace(";","\\;").replace(",","\\,").replace("\\n","\\n").replace("\\r","\\r")

def dt_ics_utc(dt):
    if dt is None: return None
    if dt.tzinfo is None:
        try: dt = dt.replace(tzinfo=TZ)
        except Exception: pass
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def build_ics(rows: pd.DataFrame, calendar_name="Mesas"):
    now_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR","VERSION:2.0","PRODID:-//Planificador de Mesas//EN",
        "CALSCALE:GREGORIAN","METHOD:PUBLISH",
        "X-WR-CALNAME:"+calendar_name,"X-WR-TIMEZONE:America/Bogota",
    ]
    for _, r in rows.iterrows():
        f = combine_dt(r.get("Fecha"), r.get("Inicio")); t = combine_dt(r.get("Fecha"), r.get("Fin"))
        if f is None or t is None: continue
        uid = str(uuid.uuid4()) + "@mesas.local"
        nombre_mesa = _safe_str(r.get("Nombre de la mesa"))
        aula = _safe_str(r.get("Aula"))
        responsable = _safe_str(r.get("Responsable"))
        corresponsable = _safe_str(r.get("Corresponsable"))
        participantes = _safe_str(r.get("Participantes"))
        deleg = "Sí" if r.get("Requiere Delegación") else "No"
        summary = f"[DELEGAR] {nombre_mesa} — {aula}" if r.get("Requiere Delegación") else (f"{nombre_mesa} — {aula}" if aula else nombre_mesa)
        desc = "\n".join([
            f"Mesa: {nombre_mesa}",
            f"Aula: {aula}",
            f"Responsable: {responsable}",
            f"Corresponsable: {corresponsable}",
            f"Participantes: {participantes}",
            f"Requiere delegación: {deleg}"
        ])
        lines += [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{now_utc}",
            f"DTSTART:{dt_ics_utc(f)}",
            f"DTEND:{dt_ics_utc(t)}",
            f"SUMMARY:{escape_text(summary)}",
            f"LOCATION:{escape_text(aula)}",
            f"DESCRIPTION:{escape_text(desc)}",
            "END:VEVENT"
        ]
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines).encode("utf-8")

def zip_split_ics(rows: pd.DataFrame):
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for i, r in rows.reset_index(drop=True).iterrows():
            single_df = pd.DataFrame([r])
            nombre = _safe_str(r.get("Nombre de la mesa")) or f"mesa_{i+1}"
            ics_bytes = build_ics(single_df, calendar_name=nombre)
            safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", nombre)[:64] or f"mesa_{i+1}"
            zf.writestr(f"{safe}.ics", ics_bytes)
    mem.seek(0)
    return mem.getvalue()

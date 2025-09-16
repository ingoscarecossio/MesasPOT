# -*- coding: utf-8 -*-
"""
Cronograma Mesas POT — LIGHT (memoria reducida, profesional)
Secciones: Resumen, Consulta, Agenda, Gantt, Heatmap, Delegaciones

- Cache por hash con TTL + max_entries
- Índices/normalizaciones perezosos
- Downcast + category
- Modo ligero para datasets grandes (evita gráficos costosos)
- Límite de filas renderizadas (exportes completos aparte)
- Consulta híbrida (texto + selector)
- Agenda con 'Objetivo' (si existe)
- Delegaciones filtrando por 'Deben delegar'
"""

import io, re, base64, unicodedata, difflib, os, json, hashlib, glob, sys, gc
from datetime import datetime, date, time, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st


# ============= Config básica y apariencia =============
st.set_page_config(page_title="Cronograma Mesas POT — Light", page_icon="🗂️",
                   layout="wide", initial_sidebar_state="expanded")

def inject_base_css(dark: bool = True, shade: float = 0.75, density: str = "compacta"):
    row_pad = {"compacta":"0.25rem","media":"0.5rem","amplia":"0.8rem"}.get(density,"0.5rem")
    st.markdown(f"""
    <style>
    .block-container {{ padding-top: 0.9rem; }}
    .gradient-title {{
        background: linear-gradient(90deg,#60a5fa 0%,#22d3ee 100%);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent;
        font-weight:800; letter-spacing:.2px;
    }}
    .card {{ border-radius:14px; padding:.9rem 1.1rem;
        border:1px solid {("#1f2937" if dark else "#e5e7eb")};
        background: {"rgba(17,24,39,0.85)" if dark else "rgba(255,255,255,0.95)"};
        box-shadow:0 8px 24px rgba(0,0,0,0.20); }}
    .kpi {{ font-size:.85rem; color:{"#cbd5e1" if dark else "#6b7280"}; margin-bottom:.2rem; }}
    .kpi .value {{ display:block; font-size:1.5rem; font-weight:700; color:{"#f8fafc" if dark else "#111827"}; }}
    .small {{ font-size:.85rem; color:{"#cbd5e1" if dark else "#6b7280"}; }}
    .stDataFrame div[role='row'] {{ padding-top:{row_pad}; padding-bottom:{row_pad}; }}
    .dataframe th, .dataframe td {{ background:transparent !important; }}
    </style>
    """, unsafe_allow_html=True)


# ============= Constantes y utilidades =============
try:
    from zoneinfo import ZoneInfo
    TZ_DEFAULT = ZoneInfo("America/Bogota")
except Exception:
    TZ_DEFAULT = timezone(timedelta(hours=-5))

_SHEET_CANDIDATES = ["Calendario", "Agenda", "Programación", "Calendario_Mesas"]
_DELEG_MARKER_REGEX = re.compile(r"\(\s*no disponible\s*,\s*asignar delegado\s*\)", re.IGNORECASE)
_SEP_REGEX = re.compile(r"[;,/]|\n|\r|\t|\||\u2022|·")
MAX_ROWS = 1500

# Columnas canónicas: añade 'Objetivo' (opcional)
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
    "Delegaciones": ["Delegaciones", "Delegation", "Delegado"],  # opcional
    "Objetivo": ["Objetivo", "Objetivo de la reunión", "Objetivo de la reunion",
                 "Descripción", "Descripcion", "Observaciones"],  # opcional
}

OPTIONAL_COLS = {"Delegaciones", "Objetivo"}


# ===== Helpers =====
def _safe_str(x):
    try:
        return "" if (x is None or (isinstance(x, float) and np.isnan(x))) else str(x).strip()
    except Exception:
        return str(x)

def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"\s+"," ", s)
    return s.lower().strip()

def _strip_delegate_marker(s: str) -> str:
    if not s: return s
    return _DELEG_MARKER_REGEX.sub("", s).strip()

_MESA_RX_PAD   = re.compile(r'^\s*M\s*-\s*(\d+)\s*-\s*(\d+)\s*$', re.I)
_MESA_RX_SNUM  = re.compile(r'^\s*M\s*(\d+)\s*-\s*(\d+)\s*$', re.I)
_MESA_RX_S     = re.compile(r'^\s*M\s*(\d+)\s*-\s*S\s*(\d+)\s*$', re.I)
def _norm_mesa_code(x: str) -> str:
    s = _safe_str(x).upper().replace(" ", "")
    m = _MESA_RX_PAD.match(s)
    if m: return f"M{m.group(1)}-S{m.group(2)}"
    m = _MESA_RX_S.match(s)
    if m: return f"M{m.group(1)}-S{m.group(2)}"
    m = _MESA_RX_SNUM.match(s)
    if m: return f"M{m.group(1)}-S{m.group(2)}"
    return s

def _glob_candidates(prefix: str):
    pats = [
        f"{prefix}.xlsx", f"./{prefix}.xlsx", f"/mnt/data/{prefix}.xlsx",
        f"/mnt/data/{prefix}*.xlsx", f"./data/{prefix}.xlsx", f"./data/{prefix}*.xlsx",
    ]
    out = []
    for p in pats: out.extend(glob.glob(p))
    seen=set(); res=[]
    for x in out:
        if x not in seen and os.path.exists(x):
            seen.add(x); res.append(x)
    return res

REPO_CAND_MAIN  = _glob_candidates("STREAMLIT")
REPO_CAND_DELEG = _glob_candidates("DELEGACIONES")


# ===== Query Params (compat) =====
def _qp_get_all():
    try: return dict(st.query_params)
    except Exception:
        try: return st.experimental_get_query_params()
        except Exception: return {}
def _qp_get(key, default=None):
    qs = _qp_get_all()
    if key not in qs: return default
    v = qs[key];  return v[0] if isinstance(v,list) and v else v
def _qp_set(mapping: Dict[str, object]):
    m = {}
    for k, v in mapping.items():
        if v is None: continue
        m[k] = json.dumps(v, ensure_ascii=False) if isinstance(v,(list,tuple,dict)) else str(v)
    try: st.query_params.update(m)
    except Exception:
        try: base = _qp_get_all(); base.update(m); st.experimental_set_query_params(**base)
        except Exception: pass
def _qp_update_if_changed(mapping: Dict[str, object]):
    cur = _qp_get_all(); to_set={}
    for k, v in mapping.items():
        new_v = json.dumps(v, ensure_ascii=False) if isinstance(v,(list,tuple,dict)) else str(v)
        cur_v = cur.get(k, None); cur_v = cur_v[0] if isinstance(cur_v, list) and cur_v else cur_v
        if str(cur_v) != new_v: to_set[k] = v
    if to_set: _qp_set(to_set)


# ===== Conversión fecha/hora =====
def _to_date(x):
    if isinstance(x, date) and not isinstance(x, datetime): return x
    if isinstance(x, datetime): return x.date()
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    try: return datetime.strptime(str(x).strip(), "%Y-%m-%d").date()
    except Exception:
        d = pd.to_datetime(x, errors="coerce", utc=False)
        if pd.isna(d): return None
        if isinstance(d, pd.Timestamp): return d.date()
        return None

def _to_time(x):
    if isinstance(x, time): return x.replace(microsecond=0)
    if isinstance(x, datetime): return x.time().replace(microsecond=0)
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    d = pd.to_datetime(x, errors="coerce")
    if not pd.isna(d) and isinstance(d, pd.Timestamp): return d.time().replace(microsecond=0)
    try:
        s = str(x).strip();  hh, mm = s.split(":")[:2]
        return time(int(hh), int(mm))
    except Exception: return None

def combine_dt(fecha, hora, tz: Optional[timezone]=None):
    tz = tz or TZ_DEFAULT
    d = fecha if isinstance(fecha, date) and not isinstance(fecha, datetime) else _to_date(fecha)
    t = hora if isinstance(hora, time) else _to_time(hora)
    if d is None or t is None: return None
    sec = getattr(t, "second", 0) or 0
    return datetime(d.year, d.month, d.day, t.hour, t.minute, sec, tzinfo=tz)

def ensure_sorted(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "_fecha" in df.columns and "_ini" in df.columns:
        df.sort_values(by=["_fecha","_ini"], inplace=True, kind="mergesort")
    return df

def _select_existing(df: pd.DataFrame, cols: List[str]) -> List[str]:
    return [c for c in cols if c in df.columns]


# ===== Normalización columnas =====
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
            if canonical in OPTIONAL_COLS:
                continue
            st.error(f"Falta la columna requerida: **{canonical}**")
            st.stop()
        mapping[col] = canonical
    return df.rename(columns=mapping)


# ===== Carga de Excel (cache por hash con TTL) =====
def _file_hash(file_obj_or_path) -> str:
    h = hashlib.sha1()
    if hasattr(file_obj_or_path, "read"):  # UploadedFile
        pos = file_obj_or_path.tell(); file_obj_or_path.seek(0)
        h.update(file_obj_or_path.read()); file_obj_or_path.seek(pos)
    else:
        with open(file_obj_or_path, "rb") as f:
            for chunk in iter(lambda: f.read(1<<20), b""): h.update(chunk)
    return h.hexdigest()

def _strip_cache_prefix(key_or_path: str) -> str:
    if not isinstance(key_or_path, str): return key_or_path
    return key_or_path.split("::", 1)[1] if "::" in key_or_path else key_or_path

@st.cache_data(show_spinner=True, max_entries=4, ttl=1800)
def load_excel_from_src(src_key: str, bytes_data: bytes | None, sheet_candidates=None):
    try:
        if bytes_data:
            xls = pd.ExcelFile(io.BytesIO(bytes_data), engine="openpyxl")
        else:
            real_path = _strip_cache_prefix(src_key)
            if not real_path or not os.path.exists(real_path):
                raise FileNotFoundError(f"No existe el archivo: {real_path!r}")
            xls = pd.ExcelFile(real_path, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error abriendo Excel: {e}"); st.stop()

    try:
        if sheet_candidates:
            for cand in sheet_candidates:
                if cand in xls.sheet_names: return xls.parse(cand)
        return xls.parse(xls.sheet_names[0])
    except Exception as e:
        st.error(f"❌ Error leyendo la hoja del Excel: {e}"); st.stop()

def _resolve_main_df():
    bytes_main = None; src_key = None
    if st.session_state.get("upload_main"):
        h = _file_hash(st.session_state.upload_main)
        src_key = f"upload_main::{h}"
        st.session_state.upload_main.seek(0)
        bytes_main = st.session_state.upload_main.read()
    else:
        path = next((p for p in REPO_CAND_MAIN if os.path.exists(p)), None)
        if path: src_key = f"path_main::{path}"
    if not src_key:
        st.error("No se encontró el Excel principal. Carga **STREAMLIT.xlsx**.")
        st.stop()
    raw = load_excel_from_src(src_key, bytes_main, _SHEET_CANDIDATES)
    return normalize_cols(raw).copy()

def _resolve_deleg_df():
    bytes_d = None; src_key = None
    if st.session_state.get("upload_deleg"):
        h = _file_hash(st.session_state.upload_deleg)
        src_key = f"upload_deleg::{h}"
        st.session_state.upload_deleg.seek(0)
        bytes_d = st.session_state.upload_deleg.read()
    else:
        path = next((p for p in REPO_CAND_DELEG if os.path.exists(p)), None)
        if path: src_key = f"path_deleg::{path}"
    if not src_key:
        return pd.DataFrame()
    return load_excel_from_src(src_key, bytes_d, None)


# ===== Limpieza + tipos =====
def _split_people(cell):
    if pd.isna(cell): return []
    parts = _SEP_REGEX.split(str(cell))
    clean = [p.strip() for p in parts if p and p.strip()]
    out = []
    for p in clean:
        if " y " in p: out.extend([x.strip() for x in p.split(" y ") if x.strip()])
        else: out.append(p)
    return out

def clean_delegate_markers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["Participantes", "Responsable", "Corresponsable"]:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").apply(_strip_delegate_marker)
    df["Requiere Delegación"] = False
    return df

def _downcast_and_categorize(df: pd.DataFrame) -> pd.DataFrame:
    """Ajuste de memoria: category en textos repetidos y downcast numérico."""
    df = df.copy()
    for col in ["Aula","Responsable","Corresponsable"]:
        if col in df.columns:
            try: df[col] = df[col].astype("category")
            except Exception: pass
    for col in df.select_dtypes(include=["float64","int64"]).columns:
        try: df[col] = pd.to_numeric(df[col], downcast="integer")
        except Exception: pass
    return df

@st.cache_data(show_spinner=False, max_entries=4, ttl=1800)
def _dedup_events(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["_fecha","_ini","_fin","Aula","Nombre de la mesa"]
    if not all(c in df.columns for c in cols): return df.copy()
    try:
        return df.sort_values(cols, kind="mergesort").drop_duplicates(subset=cols, keep="first")
    except Exception:
        return df.copy()


# ===== Índice perezoso por persona =====
@st.cache_data(show_spinner=False, max_entries=4, ttl=1800)
def build_index_cached(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    use_cols = _select_existing(df, ["_fecha","_ini","_fin","Nombre de la mesa","Mesa",
                                     "Participantes","Responsable","Corresponsable","Aula","Objetivo"])
    it = df[use_cols].itertuples(index=False, name=None)
    for tup in it:
        rec = dict(zip(use_cols, tup))
        part_list = _split_people(rec.get("Participantes"))
        extra = []
        if _safe_str(rec.get("Responsable")):   extra.append(_safe_str(rec.get("Responsable")))
        if _safe_str(rec.get("Corresponsable")):extra.append(_safe_str(rec.get("Corresponsable")))
        everyone = list(dict.fromkeys(extra + (part_list or [None])))
        if not everyone:
            rows.append((rec.get("_fecha"),rec.get("_ini"),rec.get("_fin"),rec.get("Nombre de la mesa"),rec.get("Mesa"),
                         rec.get("Participantes"),rec.get("Responsable"),rec.get("Corresponsable"),rec.get("Aula"),
                         rec.get("Objetivo"), None))
        else:
            for p in everyone:
                rows.append((rec.get("_fecha"),rec.get("_ini"),rec.get("_fin"),rec.get("Nombre de la mesa"),rec.get("Mesa"),
                             rec.get("Participantes"),rec.get("Responsable"),rec.get("Corresponsable"),rec.get("Aula"),
                             rec.get("Objetivo"), p))
    out = pd.DataFrame(rows, columns=["_fecha","_ini","_fin","Nombre de la mesa","Mesa","Participantes",
                                      "Responsable","Corresponsable","Aula","Objetivo","Participante_individual"])
    out = ensure_sorted(out)
    for col in ["Responsable","Corresponsable","Aula","Nombre de la mesa","Participantes","Mesa","Objetivo"]:
        if col in out.columns: out[f"__norm_{col}"] = out[col].fillna("").astype(str).map(_norm)
    if "Participante_individual" in out.columns:
        out["__norm_part"] = out["Participante_individual"].fillna("").astype(str).map(_norm)
    else:
        out["__norm_part"] = ""
    for col in ["Aula","Responsable","Corresponsable","Nombre de la mesa","Participante_individual"]:
        if col in out.columns:
            try: out[col] = out[col].astype("category")
            except: pass
    return out


# ===== Delegaciones (mapa) =====
@st.cache_data(show_spinner=False, max_entries=4, ttl=1800)
def _prepare_deleg_map(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["__actor","__actor_raw","__mesa","__fecha","__ini","__fin","__ini_m","__fin_m"])
    col_actor = col_mesa = col_fecha = col_ini = col_fin = None
    for c in df.columns:
        cl = str(c).lower()
        if col_actor is None and ("actor" in cl): col_actor = c
        if col_mesa  is None and "mesa"  in cl:  col_mesa  = c
        if col_fecha is None and "fecha" in cl:  col_fecha = c
        if col_ini   is None and ("inicio" in cl or "hora inicio" in cl): col_ini = c
        if col_fin   is None and ("fin"    in cl or "hora fin"   in cl): col_fin = c
    if col_actor is None or col_mesa is None or col_fecha is None:
        return pd.DataFrame(columns=["__actor","__actor_raw","__mesa","__fecha","__ini","__fin","__ini_m","__fin_m"])

    def _to_t(x):
        d = pd.to_datetime(x, errors="coerce")
        if not pd.isna(d) and isinstance(d, pd.Timestamp): return d.time().replace(microsecond=0)
        try:
            s = str(x).strip()
            if not s: return None
            hh,mm = s.split(":")[:2]
            return time(int(hh), int(mm))
        except Exception:
            return None

    raw_actor = df[col_actor].astype(str).fillna("").map(str.strip)
    out = pd.DataFrame({
        "__actor":     raw_actor.map(_norm),
        "__actor_raw": raw_actor,
        "__mesa":      df[col_mesa].astype(str).map(_norm_mesa_code),
        "__fecha":     pd.to_datetime(df[col_fecha], errors="coerce").dt.date,
        "__ini":       df[col_ini].map(_to_t) if (col_ini is not None and col_ini in df.columns) else None,
        "__fin":       df[col_fin].map(_to_t) if (col_fin is not None and col_fin in df.columns) else None
    }).dropna(subset=["__mesa","__fecha"])
    def t2m(t):
        if pd.isna(t) or t is None: return np.nan
        return int(t.hour)*60 + int(t.minute)
    out["__ini_m"] = out["__ini"].map(t2m) if "__ini" in out.columns else np.nan
    out["__fin_m"] = out["__fin"].map(t2m) if "__fin" in out.columns else np.nan
    return out


# ===== Fuzzy search =====
try:
    from rapidfuzz.fuzz import partial_ratio
    def fuzzy_filter(series: pd.Series, q: str, thr=80) -> pd.Series:
        qn = _norm(q or "")
        if not qn: return pd.Series(True, index=series.index)
        fast = series.str.contains(qn, na=False)
        if fast.any(): return fast
        return series.map(lambda s: partial_ratio(s, qn) >= thr)
except Exception:
    def fuzzy_filter(series: pd.Series, q: str, thr=0.8) -> pd.Series:
        qn = _norm(q or "")
        if not qn: return pd.Series(True, index=series.index)
        fast = series.str.contains(qn, na=False)
        if fast.any(): return fast
        return series.map(lambda s: difflib.SequenceMatcher(None, s, qn).ratio() >= thr)


# ===== ICS =====
def escape_text(val: str) -> str:
    if val is None: return ""
    v = str(val)
    v = v.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
    v = v.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
    return v

def dt_ics_utc(dt):
    if dt is None: return None
    tz = TZ_DEFAULT
    if dt.tzinfo is None:
        try: dt = dt.replace(tzinfo=tz)
        except Exception: pass
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def build_ics(rows: pd.DataFrame, calendar_name="Cronograma Mesas POT"):
    now_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR","VERSION:2.0","PRODID:-//Cronograma Mesas POT//ES",
        "CALSCALE:GREGORIAN","METHOD:PUBLISH",
        f"X-WR-CALNAME:{escape_text(calendar_name)}",
        "X-WR-TIMEZONE:America/Bogota",
    ]
    for _, r in rows.iterrows():
        f = combine_dt(r.get("_fecha"), r.get("_ini"))
        t = combine_dt(r.get("_fecha"), r.get("_fin"))
        if f is None or t is None: continue
        nombre_mesa = _safe_str(r.get("Nombre de la mesa"))
        aula = _safe_str(r.get("Aula"))
        raw_uid = f"{_norm_mesa_code(r.get('Mesa') or nombre_mesa)}|{_to_date(r.get('_fecha'))}|{_to_time(r.get('_ini'))}|{aula}"
        uid = hashlib.sha1(raw_uid.encode("utf-8")).hexdigest() + "@mesas.local"
        lines += [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{now_utc}",
            f"DTSTART:{dt_ics_utc(f)}",
            f"DTEND:{dt_ics_utc(t)}",
            f"SUMMARY:{escape_text(nombre_mesa + (' — ' + aula if aula else '') )}",
            f"LOCATION:{escape_text(aula)}",
            "END:VEVENT",
        ]
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines).encode("utf-8")


# ============= Sidebar =============
if "dark" not in st.session_state: st.session_state.dark = True
with st.sidebar:
    st.session_state.dark = st.checkbox("Modo oscuro", value=st.session_state.dark, key="ui_dark")
    lite = st.toggle("🪶 Modo ligero (recom.)", value=True,
                     help="Evita gráficos y cómputos costosos hasta aplicar filtros.", key="ui_lite")
    densidad = st.select_slider("Densidad tabla", options=["compacta","media","amplia"], value="compacta", key="ui_dens")
    st.markdown("### 📦 Datos")
    st.file_uploader("STREAMLIT.xlsx",    type=["xlsx"], key="upload_main")
    st.file_uploader("DELEGACIONES.xlsx", type=["xlsx"], key="upload_deleg")
    if st.button("🧹 Limpiar cachés", key="house_gc_btn"):
        st.cache_data.clear()
        gc.collect()
        st.success("Cachés limpiadas y GC ejecutado.")

inject_base_css(st.session_state.dark, 0.75, densidad)

st.markdown("<h1 class='gradient-title'>🗂️ Cronograma Mesas POT — Light</h1>", unsafe_allow_html=True)
st.caption("Modo ligero • Cache por hash • Índices perezosos • Memoria optimizada")


# ============= Carga principal =============
df0 = _resolve_main_df()
df0 = clean_delegate_markers(df0)
df0["_fecha"] = df0["Fecha"].apply(_to_date)
df0["_ini"]   = df0["Inicio"].apply(_to_time)
df0["_fin"]   = df0["Fin"].apply(_to_time)
for col in ["Participantes","Responsable","Corresponsable","Aula","Nombre de la mesa","Mesa","Objetivo"]:
    if col in df0.columns:
        df0[col] = df0[col].astype(str)
        df0[f"__norm_{col}"] = df0[col].fillna("").astype(str).map(_norm)
df0 = _downcast_and_categorize(df0)
df0 = ensure_sorted(df0)

# Filtro temporal base — solo lun–vie y meses Sep–Oct (ajústalo si quieres ampliar)
def _is_weekday(d: Optional[date]) -> bool:
    return (d is not None) and (0 <= d.weekday() <= 4)
def _only_sep_oct_weekdays(d: Optional[date]) -> bool:
    return _is_weekday(d) and (d.month in (9,10))
DF = df0[df0["_fecha"].map(_only_sep_oct_weekdays)].copy()

if DF.empty:
    st.warning("No hay filas válidas (Lun–Vie, Sep–Oct). Sube un Excel o ajusta el filtro temporal desde el archivo fuente.")

# Secciones disponibles
sections = ["Resumen","Consulta","Agenda","Gantt","Heatmap","Delegaciones"]
sec = st.tabs(sections)


# ============= Sección: Resumen =============
with sec[0]:
    st.subheader("📈 Resumen ejecutivo")
    DFu = _dedup_events(DF)
    if lite and DFu.shape[0] > 6000:
        st.info("Modo ligero activo: desactívalo o filtra en Consulta para ver gráficos.")
    else:
        def make_stats(df):
            base = _dedup_events(df)
            n_mesas = base.shape[0]
            aulas = base["Aula"].dropna().astype(str).nunique() if "Aula" in base else 0
            dias  = base["_fecha"].dropna().nunique() if "_fecha" in base else 0
            allp = []
            for v in base["Participantes"].fillna("").astype(str).tolist():
                allp += _split_people(v)
            n_personas = len(pd.unique(pd.Series([p.strip() for p in allp if p]).astype(str)))
            return n_mesas, aulas, dias, n_personas

        tm, na, nd, npers = make_stats(DFu)
        c1,c2,c3,c4 = st.columns(4)
        with c1: st.markdown(f"<div class='card'><div class='kpi'>Mesas</div><span class='value'>{tm}</span></div>", unsafe_allow_html=True)
        with c2: st.markdown(f"<div class='card'><div class='kpi'>Aulas</div><span class='value'>{na}</span></div>", unsafe_allow_html=True)
        with c3: st.markdown(f"<div class='card'><div class='kpi'>Días</div><span class='value'>{nd}</span></div>", unsafe_allow_html=True)
        with c4: st.markdown(f"<div class='card'><div class='kpi'>Personas únicas</div><span class='value'>{npers}</span></div>", unsafe_allow_html=True)

        # Top personas y aulas (compacto)
        if DFu.shape[0] <= 20000 or not lite:
            all_people = []
            for v in DFu["Participantes"].fillna("").astype(str).tolist():
                all_people += _split_people(v)
            s = pd.Series([p.strip() for p in all_people if p and str(p).strip()])
            top_people = s.value_counts().head(10).rename_axis("Persona").reset_index(name="Conteo")
            uso_aula = DFu.groupby("Aula")["Nombre de la mesa"].count().sort_values(ascending=False).head(10)\
                          .rename_axis("Aula").reset_index(name="Mesas")
            c5, c6 = st.columns(2)
            with c5:
                st.markdown("**Top 10 personas por participación**")
                if not top_people.empty:
                    fig1 = px.bar(top_people, x="Conteo", y="Persona", orientation="h", height=360)
                    st.plotly_chart(fig1, use_container_width=True)
                else:
                    st.info("Sin datos.")
            with c6:
                st.markdown("**Aulas más usadas (Top 10)**")
                if not uso_aula.empty:
                    fig2 = px.bar(uso_aula, x="Mesas", y="Aula", orientation="h", height=360)
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.info("Sin datos.")
        else:
            st.caption("Se omitieron gráficos pesados por Modo ligero y tamaño de datos.")

        # ---------- Resúmenes rápidos (ligeros) ----------
        with st.expander("📌 Resúmenes rápidos (ligeros)", expanded=False):
            base = _dedup_events(DFu)

            def _to_minutes(row):
                ti, tf = row["_ini"], row["_fin"]
                if pd.isna(ti) or pd.isna(tf) or (ti is None) or (tf is None):
                    return np.nan
                return (datetime.combine(date(2000,1,1), tf) - datetime.combine(date(2000,1,1), ti)).total_seconds() / 60.0

            dur_mins = base.apply(_to_minutes, axis=1)
            dur_mean = float(np.nanmean(dur_mins)) if len(dur_mins) else np.nan

            mesas_por_dia = base.groupby("_fecha")["Nombre de la mesa"].count() if "_fecha" in base else pd.Series(dtype=int)
            prom_mesas_dia = float(mesas_por_dia.mean()) if len(mesas_por_dia) else 0.0
            top_dias = mesas_por_dia.sort_values(ascending=False).head(5).reset_index()
            if not top_dias.empty:
                top_dias.columns = ["Fecha", "Mesas"]
                top_dias["Fecha"] = top_dias["Fecha"].astype(str)

            if "_ini" in base:
                horas_ini = pd.Series([t.hour for t in base["_ini"] if pd.notna(t)])
                hora_pico = int(horas_ini.mode().iloc[0]) if not horas_ini.empty else None
                franja_maniana = int((horas_ini < 12).sum()) if not horas_ini.empty else 0
                franja_tarde   = int(((horas_ini >= 12) & (horas_ini < 18)).sum()) if not horas_ini.empty else 0
                total_franjas  = franja_maniana + franja_tarde
                p_maniana = round((franja_maniana / total_franjas * 100.0), 1) if total_franjas else 0.0
                p_tarde   = round((franja_tarde   / total_franjas * 100.0), 1) if total_franjas else 0.0
            else:
                hora_pico = None; p_maniana = p_tarde = 0.0

            top_aulas = (
                base.groupby("Aula")["Nombre de la mesa"].count()
                .sort_values(ascending=False).head(5).rename("Mesas")
                .reset_index()
                if "Aula" in base else pd.DataFrame(columns=["Aula","Mesas"])
            )

            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown(
                    f"<div class='card'><div class='kpi'>⏱️ Duración media</div>"
                    f"<span class='value'>{(round(dur_mean) if not np.isnan(dur_mean) else 'N/D')} min</span></div>",
                    unsafe_allow_html=True
                )
            with c2:
                st.markdown(
                    f"<div class='card'><div class='kpi'>📅 Mesas por día (prom.)</div>"
                    f"<span class='value'>{round(prom_mesas_dia,1)}</span></div>",
                    unsafe_allow_html=True
                )
            with c3:
                pico_txt = f"{hora_pico:02d}:00" if hora_pico is not None else "N/D"
                st.markdown(
                    f"<div class='card'><div class='kpi'>⏰ Hora pico de inicio</div>"
                    f"<span class='value'>{pico_txt}</span></div>",
                    unsafe_allow_html=True
                )

            st.caption(f"Distribución simple: Mañana {p_maniana}% · Tarde {p_tarde}%")

            t1, t2 = st.columns(2)
            with t1:
                st.markdown("**🗓️ Días más cargados (Top 5)**")
                if not top_dias.empty:
                    st.dataframe(top_dias, use_container_width=True, hide_index=True)
                else:
                    st.info("Sin datos suficientes.")
            with t2:
                st.markdown("**🏫 Aulas con más mesas (Top 5)**")
                if not top_aulas.empty:
                    st.dataframe(top_aulas, use_container_width=True, hide_index=True)
                else:
                    st.info("Sin datos suficientes.")


# ============= Sección: Consulta (híbrida) =============
with sec[1]:
    st.subheader("🔎 Consulta filtrada")
    @st.cache_data(show_spinner=False, max_entries=4, ttl=1800)
    def _get_idx(df: pd.DataFrame) -> pd.DataFrame:
        return build_index_cached(df)
    idx = _get_idx(DF)

    # Filtros compactos
    c1, c2, c3, c4 = st.columns([1,1,1,1])
    fechas_validas = [d for d in DF["_fecha"].dropna().tolist()]
    if fechas_validas: dmin, dmax = min(fechas_validas), max(fechas_validas)
    else: today = date.today(); dmin, dmax = today, today
    with c1:
        dr = st.date_input("Rango fechas", value=(dmin, dmax), min_value=dmin, max_value=dmax, key="consulta_rango")
        fmin, fmax = (dr if isinstance(dr, tuple) and len(dr)==2 else (dmin, dmax))
        st.slider("Rango horas", 0, 23, (6, 20), key="consulta_horas")
    with c2:
        aulas = sorted(DF.get("Aula", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        aula_sel = st.multiselect("Aulas", ["(todas)"] + aulas, default=["(todas)"], key="cons_aulas")
        dow_opts = ["Lun","Mar","Mié","Jue","Vie"]; dows = {"Lun":0,"Mar":1,"Mié":2,"Jue":3,"Vie":4}
        dow = st.multiselect("Días", dow_opts, default=dow_opts, key="cons_dow")
    with c3:
        responsables = sorted(DF.get("Responsable", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        rsel = st.multiselect("Responsables", responsables, default=[], key="cons_resp")
    with c4:
        # Entrada híbrida: texto y selector (el texto tiene prioridad)
        person_txt  = st.text_input("Persona (texto)", value=_qp_get("q",""), key="consulta_persona_txt")
        # Sugerencias para selector (si hay texto, filtra; si no, muestra todo)
        people_all = sorted({p for p in set(idx.get("Participante_individual", pd.Series(dtype=str)).dropna().astype(str).tolist()
                            + DF.get("Responsable", pd.Series(dtype=str)).dropna().astype(str).tolist()
                            + DF.get("Corresponsable", pd.Series(dtype=str)).dropna().astype(str).tolist()) if p})
        if person_txt:
            normq = _norm(person_txt)
            people_opts = [p for p in people_all if normq in _norm(p)]
            if not people_opts: people_opts = people_all
        else:
            people_opts = people_all
        person_pick = st.selectbox("…o selecciónala", options=[""] + people_opts, index=0, key="consulta_persona_pick")
        if st.button("Buscar", use_container_width=True, key="consulta_buscar_btn"):
            _qp_set({"q": person_txt or person_pick})

    # Valor efectivo de búsqueda
    term = (person_txt or person_pick or "").strip()

    # Máscara vectorizada (sin usar columnas inexistentes)
    idx = idx  # ya construido
    mask = pd.Series(True, index=idx.index, dtype=bool)
    mask &= idx["_fecha"].between(fmin, fmax, inclusive="both")
    dows = {"Lun":0,"Mar":1,"Mié":2,"Jue":3,"Vie":4}
    sel_dows = [dows[x] for x in dow] if dow else list(dows.values())
    mask &= idx["_fecha"].map(lambda d: d is not None and d.weekday() in sel_dows)
    hmin, hmax = st.session_state.get("consulta_horas",(6,20))
    mask &= idx["_ini"].map(lambda t: (t is not None) and (hmin <= t.hour <= hmax))
    if aula_sel and not (len(aula_sel)==1 and aula_sel[0]=="(todas)"):
        allowed = set([a for a in aula_sel if a != "(todas)"])
        mask &= idx["Aula"].astype(str).isin(allowed)
    if rsel: mask &= idx["Responsable"].astype(str).isin(set(rsel))
    if term:
        mask &= (fuzzy_filter(idx["__norm_part"], term) |
                 fuzzy_filter(idx["__norm_Responsable"], term) |
                 fuzzy_filter(idx["__norm_Corresponsable"], term))
    mask = mask.reindex(idx.index).fillna(False)

    cols     = ["_fecha","_ini","_fin","Aula","Nombre de la mesa","Responsable","Corresponsable","Participantes"]
    use_cols = _select_existing(idx, cols)
    res      = _dedup_events(idx.loc[mask, use_cols].copy())

    # KPIs
    tm = res.shape[0]
    na = res["Aula"].dropna().astype(str).nunique() if not res.empty else 0
    nd = res["_fecha"].dropna().nunique() if not res.empty else 0
    allp = []
    for v in res["Participantes"].fillna("").astype(str).tolist(): allp += _split_people(v)
    npersonas = len(pd.unique(pd.Series([p.strip() for p in allp if p]).astype(str))) if not res.empty else 0
    d1,d2,d3,d4 = st.columns(4)
    with d1: st.markdown(f"<div class='card'><div class='kpi'>Mesas</div><span class='value'>{tm}</span></div>", unsafe_allow_html=True)
    with d2: st.markdown(f"<div class='card'><div class='kpi'>Aulas</div><span class='value'>{na}</span></div>", unsafe_allow_html=True)
    with d3: st.markdown(f"<div class='card'><div class='kpi'>Días</div><span class='value'>{nd}</span></div>", unsafe_allow_html=True)
    with d4: st.markdown(f"<div class='card'><div class='kpi'>Personas únicas</div><span class='value'>{npersonas}</span></div>", unsafe_allow_html=True)

    st.markdown("#### 📋 Resultados")
    if res.empty:
        st.info("Sin resultados para los filtros actuales.")
    else:
        rf = res.copy()
        rf["Fecha"]  = rf["_fecha"].map(lambda d: d.isoformat() if d else "")
        rf["Inicio"] = rf["_ini"].map(lambda t: t.strftime("%H:%M") if t else "")
        rf["Fin"]    = rf["_fin"].map(lambda t: t.strftime("%H:%M") if t else "")
        view = rf.head(MAX_ROWS)
        if rf.shape[0] > MAX_ROWS:
            st.caption(f"Mostrando {MAX_ROWS} de {rf.shape[0]} filas. Usa la descarga para ver todo.")
        st.dataframe(view[["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes"]],
                     use_container_width=True, hide_index=True)

        st.markdown("##### ⬇️ Descargas")
        st.download_button("CSV (filtro)", data=rf.to_csv(index=False).encode("utf-8-sig"),
                           mime="text/csv", file_name="resultados.csv", key="cons_dl_csv")
        xls_buf = io.BytesIO()
        with pd.ExcelWriter(xls_buf, engine="xlsxwriter") as w:
            rf.to_excel(w, sheet_name="Resultados", index=False)
        st.download_button("Excel (filtro)", data=xls_buf.getvalue(),
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           file_name="resultados.xlsx", key="cons_dl_xlsx")
        st.download_button("ICS (todo en uno)", data=build_ics(res, calendar_name="Cronograma Mesas POT"),
                           mime="text/calendar", file_name="mesas.ics", key="cons_dl_ics")


# ============= Sección: Agenda (con Objetivo) =============
with sec[2]:
    st.subheader("🗓️ Agenda por persona")
    @st.cache_data(show_spinner=False, max_entries=4, ttl=1800)
    def _get_idx(df: pd.DataFrame) -> pd.DataFrame:
        return build_index_cached(df)
    idx = _get_idx(DF)

    people_all = sorted({p for p in set(idx.get("Participante_individual", pd.Series(dtype=str)).dropna().astype(str).tolist()
                        + DF.get("Responsable", pd.Series(dtype=str)).dropna().astype(str).tolist()
                        + DF.get("Corresponsable", pd.Series(dtype=str)).dropna().astype(str).tolist()) if p})
    q_txt = st.text_input("Buscar persona (texto)", value="", key="agenda_persona_txt")
    if q_txt:
        normq = _norm(q_txt)
        people_opts = [p for p in people_all if normq in _norm(p)] or people_all
    else:
        people_opts = people_all
    persona = st.selectbox("Seleccione persona", options=[""] + people_opts, index=0, key="agenda_persona_pick")

    if persona:
        m = (fuzzy_filter(idx["__norm_part"], persona, 0.9) |
             fuzzy_filter(idx["__norm_Responsable"], persona, 0.9) |
             fuzzy_filter(idx["__norm_Corresponsable"], persona, 0.9))
        cols_ag     = ["_fecha","_ini","_fin","Aula","Nombre de la mesa","Responsable",
                       "Corresponsable","Participantes","Objetivo"]
        use_cols_ag = _select_existing(idx, cols_ag)
        rows        = _dedup_events(idx.loc[m, use_cols_ag].copy())
        rows = ensure_sorted(rows)
        if rows.empty:
            st.info("Sin eventos para esta persona.")
        else:
            for _, r in rows.iterrows():
                s_ini = r["_ini"].strftime('%H:%M') if r["_ini"] else ""
                s_fin = r["_fin"].strftime('%H:%M') if r["_fin"] else ""
                objetivo = _safe_str(r.get("Objetivo"))
                st.markdown(
                    f"**{_safe_str(r['Nombre de la mesa'])}**  \n"
                    f"{r['_fecha'].isoformat() if r['_fecha'] else ''} • {s_ini}–{s_fin} • Aula: {_safe_str(r['Aula'])}",
                    unsafe_allow_html=True
                )
                if objetivo:
                    st.caption(f"🎯 Objetivo: {objetivo}")
                st.divider()
            st.download_button("⬇️ ICS (Agenda)",
                data=build_ics(rows, calendar_name=f"Agenda — {persona}"),
                mime="text/calendar", file_name=f"agenda_{persona}.ics", key="agenda_dl_ics")


# ============= Sección: Gantt =============
with sec[3]:
    st.subheader("📊 Gantt (Lun–Vie Sep–Oct)")
    DFu = _dedup_events(DF)
    if lite and DFu.shape[0] > 6000:
        st.info("Modo ligero activo: desactívalo o filtra en Consulta para ver el Gantt.")
    else:
        rows = []
        for _, r in DFu.iterrows():
            start = combine_dt(r["_fecha"], r["_ini"]); end = combine_dt(r["_fecha"], r["_fin"])
            if start and end:
                rows.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "Aula": _safe_str(r["Aula"]), "start": start, "end": end})
        if rows:
            dfg = pd.DataFrame(rows)
            fig = px.timeline(dfg, x_start="start", x_end="end", y="Aula", hover_data=["Mesa"])
            fig.update_yaxes(autorange="reversed")
            fig.update_layout(height=520, margin=dict(l=6,r=6,t=28,b=14))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos para Gantt.")


# ============= Sección: Heatmap =============
with sec[4]:
    st.subheader("🗺️ Heatmap (Aula × Día)")
    DFu = _dedup_events(DF)
    if lite and DFu.shape[0] > 6000:
        st.info("Modo ligero activo: desactívalo o filtra en Consulta para ver el heatmap.")
    else:
        if "Aula" not in DFu.columns or "_fecha" not in DFu.columns:
            st.info("Faltan columnas para el heatmap.")
        else:
            piv = pd.pivot_table(DFu, index="Aula", columns="_fecha",
                                 values="Nombre de la mesa", aggfunc="count", fill_value=0)
            if piv.empty:
                st.info("No hay datos para el heatmap.")
            else:
                try: piv = piv.astype(int)
                except Exception: pass
                fig = px.imshow(piv, aspect="auto", labels=dict(color="Mesas"))
                fig.update_layout(height=500, margin=dict(l=6,r=6,t=28,b=14))
                st.plotly_chart(fig, use_container_width=True)


# ============= Sección: Delegaciones (filtra por "Deben delegar") =============
with sec[5]:
    st.subheader("🛟 Delegaciones")
    deleg_raw = _resolve_deleg_df()
    deleg_map = _prepare_deleg_map(deleg_raw)
    DFu = _dedup_events(DF).copy()

    @st.cache_data(show_spinner=False, max_entries=4, ttl=1800)
    def _actors_for_event_batch(df_events: pd.DataFrame, deleg_map: pd.DataFrame) -> pd.DataFrame:
        if df_events is None or df_events.empty:
            out = df_events.copy() if df_events is not None else pd.DataFrame()
            if out is not None: out["__deleg_list"] = [[] for _ in range(len(out))]
            return out

        ev = df_events[["_fecha","_ini","_fin","Nombre de la mesa","Mesa","Aula","Responsable","Corresponsable","Participantes"]].copy()
        ev["mesa_norm"]  = ev["Mesa"].fillna(ev["Nombre de la mesa"]).astype(str).map(_norm_mesa_code)

        merged = ev.merge(deleg_map, left_on=["mesa_norm","_fecha"], right_on=["__mesa","__fecha"], how="left")
        if merged.empty:
            df = df_events.copy(); df["__deleg_list"] = [[] for _ in range(len(df))]
            return df

        def t2m(t):
            if pd.isna(t) or t is None: return np.nan
            return int(t.hour)*60 + int(t.minute)
        merged["_ini_m"] = merged["_ini"].map(t2m); merged["_fin_m"] = merged["_fin"].map(t2m)

        ini_ev = merged["_ini_m"].to_numpy(); fin_ev = merged["_fin_m"].to_numpy()
        ini_d  = merged["__ini_m"].to_numpy() if "__ini_m" in merged.columns else np.full(len(merged), np.nan)
        fin_d  = merged["__fin_m"].to_numpy() if "__fin_m" in merged.columns else np.full(len(merged), np.nan)
        has_ev = (~np.isnan(ini_ev)) & (~np.isnan(fin_ev))
        ini_d_f = np.where(np.isnan(ini_d), -1,   ini_d)
        fin_d_f = np.where(np.isnan(fin_d), 1e9,  fin_d)

        ok = has_ev & (np.maximum(ini_ev, ini_d_f) < np.minimum(fin_ev, fin_d_f))
        merged["__ok"] = ok | (np.isnan(ini_d) & np.isnan(fin_d) & has_ev)

        grouped = (
            merged[merged["__ok"]]
            .groupby(merged.index)["__actor_raw"]
            .apply(lambda s: [x for x in pd.unique(s.dropna())])
        )

        out = ev.copy()
        out["__deleg_list"] = grouped.reindex(ev.index).apply(lambda x: x if isinstance(x, list) else []).tolist()
        return out

    if deleg_map.empty:
        st.info("No hay datos en **DELEGACIONES.xlsx** o no coinciden con las mesas/fechas cargadas.")
    else:
        rep = _actors_for_event_batch(DFu, deleg_map)
        rep = rep[rep["__deleg_list"].map(len) > 0].copy()   # solo quienes deben delegar

        rep["Deben delegar"] = rep["__deleg_list"].map(lambda lst: ", ".join(lst))
        rep["Fecha"]  = rep["_fecha"].map(lambda d: d.isoformat() if d else "")
        rep["Inicio"] = rep["_ini"].map(lambda t: t.strftime("%H:%M") if t else "")
        rep["Fin"]    = rep["_fin"].map(lambda t: t.strftime("%H:%M") if t else "")

        # ---------- Filtros ----------
        c1, c2, c3 = st.columns([1, 1, 1])
        if not rep["_fecha"].dropna().empty:
            dmin, dmax = rep["_fecha"].min(), rep["_fecha"].max()
        else:
            today = date.today(); dmin, dmax = today, today
        with c1:
            rango = st.date_input("Rango fechas", value=(dmin, dmax), min_value=dmin, max_value=dmax, key="del_rango")
            fmin, fmax = (rango if isinstance(rango, tuple) and len(rango) == 2 else (dmin, dmax))
        with c2:
            aulas = sorted(rep.get("Aula", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
            aula_sel = st.multiselect("Aulas", ["(todas)"] + aulas, default=["(todas)"], key="del_aulas")
        with c3:
            actor_txt = st.text_input("Actor (texto)", value="", key="del_actor_txt")

        all_deleg = sorted({p for lst in rep["__deleg_list"] for p in lst})
        actor_pick = st.selectbox("…o selecciónalo", options=[""] + all_deleg, index=0, key="del_actor_pick")

        m = pd.Series(True, index=rep.index, dtype=bool)
        m &= rep["_fecha"].between(fmin, fmax, inclusive="both")
        if aula_sel and not (len(aula_sel) == 1 and aula_sel[0] == "(todas)"):
            allowed = set([a for a in aula_sel if a != "(todas)"])
            m &= rep["Aula"].astype(str).isin(allowed)

        def _in_deleg_list(row_list, query):
            if not query: return True
            qn = _norm(query)
            return any(qn in _norm(x) for x in row_list)

        if actor_txt:
            m &= rep["__deleg_list"].map(lambda lst: _in_deleg_list(lst, actor_txt))
        if actor_pick:
            m &= rep["__deleg_list"].map(lambda lst: any(_norm(actor_pick) == _norm(x) for x in lst))

        view_cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Deben delegar"]
        view = rep.loc[m, view_cols]

        # KPIs ligeros
        k1, k2 = st.columns(2)
        with k1:
            st.markdown(f"<div class='card'><div class='kpi'>Eventos con delegación</div><span class='value'>{view.shape[0]}</span></div>", unsafe_allow_html=True)
        with k2:
            actores_unicos = len({p for lst in rep.loc[m, "__deleg_list"] for p in lst})
            st.markdown(f"<div class='card'><div class='kpi'>Actores únicos (deben delegar)</div><span class='value'>{actores_unicos}</span></div>", unsafe_allow_html=True)

        if view.empty:
            st.info("No hay coincidencias con los filtros.")
        else:
            st.dataframe(view.head(MAX_ROWS), use_container_width=True, hide_index=True)
            if view.shape[0] > MAX_ROWS:
                st.caption(f"Mostrando {MAX_ROWS} de {view.shape[0]} filas. Usa la descarga para ver todo.")

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                view.to_excel(w, sheet_name="Delegaciones", index=False)
            st.download_button("⬇️ Delegaciones (Excel)", data=buf.getvalue(),
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               file_name="delegaciones.xlsx", key="del_dl_btn")

# -*- coding: utf-8 -*-
"""
Cronograma Mesas POT — versión optimizada, auditada y robusta (rendimiento + estabilidad)
- Carga de Excel cacheada por hash (upload/embebido/disk) con prefijo limpiado (FIX FileNotFoundError)
- Lazy index y delegaciones (solo cuando se usan)
- Omnibox con botón Buscar (debounce)
- Modo ligero para datasets grandes (evita gráficos costosos antes de filtrar)
- Navegación estable (URL delta-aware + session_state) compatible con Streamlit >=1.30 y anteriores
- Límite de filas visibles en tablas (exportes completos)
- Sección Diagnóstico ampliada (rutas, hojas, dependencias, tamaños, memoria aprox)
"""

import io, re, base64, unicodedata, difflib, os, json, hashlib, glob, sys, gc
from datetime import datetime, date, time, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ========= Embebidos opcionales =========
_EMBED_XLSX_B64 = ""        # STREAMLIT.xlsx (hoja candidata)
_EMBED_DELEG_B64 = ""       # DELEGACIONES.xlsx (primera hoja)
_BG_B64 = ""                # Imagen de fondo (base64) opcional
_SHEET_CANDIDATES = ["Calendario", "Agenda", "Programación", "Calendario_Mesas"]

# ========= Config & rutas repo =========
_DELEG_MARKER_REGEX = re.compile(r"\(\s*no disponible\s*,\s*asignar delegado\s*\)", re.IGNORECASE)
_SEP_REGEX = re.compile(r"[;,/]|\n|\r|\t|\||\u2022|·")

def _glob_candidates(prefix: str):
    pats = [
        f"{prefix}.xlsx", f"./{prefix}.xlsx", f"/mnt/data/{prefix}.xlsx",
        f"/mnt/data/{prefix}*.xlsx", f"./data/{prefix}.xlsx", f"./data/{prefix}*.xlsx",
    ]
    out = []
    for p in pats:
        out.extend(glob.glob(p))
    seen=set(); res=[]
    for x in out:
        if x not in seen and os.path.exists(x):
            seen.add(x); res.append(x)
    return res

REPO_CAND_MAIN  = _glob_candidates("STREAMLIT")
REPO_CAND_DELEG = _glob_candidates("DELEGACIONES")

# Incluye explícitamente rutas conocidas del entorno
for p in ["/mnt/data/STREAMLIT (2).xlsx", "/mnt/data/DELEGACIONES (1).xlsx"]:
    if os.path.exists(p):
        if "STREAMLIT" in p.upper() and p not in REPO_CAND_MAIN: REPO_CAND_MAIN.append(p)
        if "DELEGACIONES" in p.upper() and p not in REPO_CAND_DELEG: REPO_CAND_DELEG.append(p)

try:
    from zoneinfo import ZoneInfo
    TZ_DEFAULT = ZoneInfo("America/Bogota")
except Exception:
    TZ_DEFAULT = timezone(timedelta(hours=-5))

# ========= Query Params helpers (compat + delta-aware) =========
def _qp_get_all():
    try:
        return dict(st.query_params)
    except Exception:
        try:
            return st.experimental_get_query_params()
        except Exception:
            return {}

def _qp_get(key, default=None):
    qs = _qp_get_all()
    if key not in qs: return default
    v = qs[key]
    if isinstance(v, list): return v[0] if v else default
    return v

def _qp_set(mapping: Dict[str, object]):
    m = {}
    for k, v in mapping.items():
        if v is None: continue
        if isinstance(v, (list, tuple, dict)):
            m[k] = json.dumps(v, ensure_ascii=False)
        else:
            m[k] = str(v)
    try:
        st.query_params.update(m)  # >=1.32
    except Exception:
        try:
            base = _qp_get_all(); base.update(m)
            st.experimental_set_query_params(**base)
        except Exception:
            pass

def _qp_del(keys: List[str]):
    cur = _qp_get_all()
    for k in keys: cur.pop(k, None)
    try:
        st.query_params.update(cur)
    except Exception:
        try:
            st.experimental_set_query_params(**cur)
        except Exception:
            pass

def _qp_update_if_changed(mapping: Dict[str, object]):
    cur = _qp_get_all()
    to_set = {}
    for k, v in mapping.items():
        new_v = json.dumps(v, ensure_ascii=False) if isinstance(v,(list,tuple,dict)) else str(v)
        cur_v = cur.get(k, None)
        cur_v = cur_v[0] if isinstance(cur_v, list) and cur_v else cur_v
        if str(cur_v) != new_v:
            to_set[k] = v
    if to_set: _qp_set(to_set)

# ========= UI base =========
st.set_page_config(page_title="Cronograma Mesas POT", page_icon="🗂️",
                   layout="wide", initial_sidebar_state="expanded")

def inject_base_css(dark: bool = True, shade: float = 0.75, density: str = "compacta"):
    if _BG_B64:
        bg_url = f"data:image/png;base64,{_BG_B64}"
        overlay = f"linear-gradient(rgba(0,0,0,{shade}), rgba(0,0,0,{shade}))"
        bg_css = f"background: {overlay}, url('{bg_url}') center center / cover fixed no-repeat;"
    else:
        bg_css = f"background: {'#0b1220' if dark else '#f7fafc'};"
    row_pad = {"compacta":"0.25rem","media":"0.5rem","amplia":"0.8rem"}.get(density,"0.5rem")
    st.markdown(f"""
    <style>
    .stApp {{
        {bg_css}
        color: {"#e5e7eb" if dark else "#111827"} !important;
    }}
    .block-container {{ padding-top: 1.0rem; }}
    .gradient-title {{
        background: linear-gradient(90deg,#60a5fa 0%,#22d3ee 100%);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent;
        font-weight:800; letter-spacing:.2px;
    }}
    .card {{ border-radius:16px; padding:1rem 1.2rem;
        border:1px solid {("#1f2937" if dark else "#e5e7eb")};
        background: {"rgba(17,24,39,0.82)" if dark else "rgba(255,255,255,0.93)"};
        box-shadow:0 10px 30px rgba(0,0,0,0.25); }}
    .kpi {{ font-size:.9rem; color:{"#cbd5e1" if dark else "#6b7280"}; margin-bottom:.25rem; }}
    .kpi .value {{ display:block; font-size:1.6rem; font-weight:700; color:{"#f8fafc" if dark else "#111827"}; }}
    .small {{ font-size:.85rem; color:{"#cbd5e1" if dark else "#6b7280"}; }}
    .stDataFrame div[role='row'] {{ padding-top:{row_pad}; padding-bottom:{row_pad}; }}
    .dataframe th, .dataframe td {{ background:transparent !important; }}
    </style>
    """, unsafe_allow_html=True)

# ========= Utilidades =========
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

# Normalizador de código de Mesa: M-3-3 / M23-1 / M23-S1 -> M23-S1
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
    "Delegaciones": ["Delegaciones", "Delegation", "Delegado"],
}

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
            if canonical == "Delegaciones":  # opcional
                continue
            st.error(f"Falta la columna requerida: **{canonical}**")
            st.stop()
        mapping[col] = canonical
    return df.rename(columns=mapping)

def _to_date(x):
    if isinstance(x, date) and not isinstance(x, datetime): return x
    if isinstance(x, datetime): return x.date()
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    try:
        return datetime.strptime(str(x).strip(), "%Y-%m-%d").date()
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
    if not pd.isna(d) and isinstance(d, pd.Timestamp):
        return d.time().replace(microsecond=0)
    try:
        s = str(x).strip()
        if not s: return None
        parts = s.split(":")
        hh = int(parts[0]); mm = int(parts[1]) if len(parts) > 1 else 0
        return time(hh, mm)
    except Exception:
        return None

def combine_dt(fecha, hora, tz: Optional[timezone]=None):
    tz = tz or st.session_state.get("tz", TZ_DEFAULT)
    d = fecha if isinstance(fecha, date) and not isinstance(fecha, datetime) else _to_date(fecha)
    t = hora if isinstance(hora, time) else _to_time(hora)
    if d is None or t is None: return None
    sec = getattr(t, "second", 0) or 0
    return datetime(d.year, d.month, d.day, t.hour, t.minute, sec, tzinfo=tz)

def ensure_sorted(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "_fecha" in df.columns and "_ini" in df.columns:
        df.sort_values(by=["_fecha","_ini"], inplace=True, kind="mergesort")
    elif "Fecha" in df.columns and "Inicio" in df.columns:
        df["_Fecha_dt"]  = df["Fecha"].apply(_to_date)
        df["_Inicio_t"]  = df["Inicio"].apply(_to_time)
        df.sort_values(by=["_Fecha_dt","_Inicio_t"], inplace=True, kind="mergesort")
        df.drop(columns=["_Fecha_dt","_Inicio_t"], inplace=True, errors="ignore")
    return df

# ========= Cache por hash de archivo (FIX incluido) =========
def _file_hash(file_obj_or_path) -> str:
    h = hashlib.sha1()
    if hasattr(file_obj_or_path, "read"):  # UploadedFile
        pos = file_obj_or_path.tell()
        file_obj_or_path.seek(0)
        h.update(file_obj_or_path.read())
        file_obj_or_path.seek(pos)
    else:
        with open(file_obj_or_path, "rb") as f:
            for chunk in iter(lambda: f.read(1<<20), b""):
                h.update(chunk)
    return h.hexdigest()

def _strip_cache_prefix(key_or_path: str) -> str:
    """Convierte 'path_main::/x.xlsx' -> '/x.xlsx'. Si ya es path normal, lo deja igual."""
    if not isinstance(key_or_path, str):
        return key_or_path
    if "::" in key_or_path:
        return key_or_path.split("::", 1)[1]
    return key_or_path

@st.cache_data(show_spinner=True)
def load_excel_from_src(src_key: str, bytes_data: bytes | None, sheet_candidates=None):
    """
    src_key: usado SOLO para la clave de caché; si no hay bytes y es un path,
             puede venir con prefijos 'path_main::' o 'path_deleg::' (se limpian aquí).
    """
    try:
        # Forzamos openpyxl para evitar problemas en Cloud (añadir a requirements.txt)
        if bytes_data:
            xls = pd.ExcelFile(io.BytesIO(bytes_data), engine="openpyxl")
        else:
            real_path = _strip_cache_prefix(src_key)
            if not real_path or not os.path.exists(real_path):
                raise FileNotFoundError(f"No existe el archivo Excel en ruta: {real_path!r}")
            xls = pd.ExcelFile(real_path, engine="openpyxl")
    except FileNotFoundError as e:
        st.error(f"❌ No se encontró el archivo Excel.\n\nDetalles: {e}")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error abriendo el Excel: {e}")
        st.stop()

    # Selección de hoja
    try:
        if sheet_candidates:
            for cand in sheet_candidates:
                if cand in xls.sheet_names:
                    return xls.parse(cand)
            st.info(f"ℹ️ Ninguna hoja candidata encontrada {sheet_candidates}. Se usará la primera: {xls.sheet_names[0]!r}.")
        return xls.parse(xls.sheet_names[0])
    except Exception as e:
        st.error(f"❌ Error leyendo la hoja del Excel: {e}")
        st.stop()

def _resolve_main_df():
    bytes_main = None; src_key = None
    if st.session_state.get("upload_main"):
        h = _file_hash(st.session_state.upload_main)
        src_key = f"upload_main::{h}"     # clave de caché
        st.session_state.upload_main.seek(0)
        bytes_main = st.session_state.upload_main.read()
    elif _EMBED_XLSX_B64:
        src_key = f"embed_main::{hashlib.sha1(_EMBED_XLSX_B64.encode()).hexdigest()}"
        bytes_main = base64.b64decode(_EMBED_XLSX_B64)
    else:
        path = next((p for p in REPO_CAND_MAIN if os.path.exists(p)), None)
        if path: src_key = f"path_main::{path}"

    if not src_key:
        st.error("No se encontró el Excel principal. Carga **STREAMLIT.xlsx** (o variantes).")
        st.caption(f"Rutas probadas: {REPO_CAND_MAIN}")
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
    elif _EMBED_DELEG_B64:
        src_key = f"embed_deleg::{hashlib.sha1(_EMBED_DELEG_B64.encode()).hexdigest()}"
        bytes_d = base64.b64decode(_EMBED_DELEG_B64)
    else:
        path = next((p for p in REPO_CAND_DELEG if os.path.exists(p)), None)
        if path: src_key = f"path_deleg::{path}"

    if not src_key:
        # Delegaciones es opcional: devuelve DF vacío sin error.
        return pd.DataFrame()

    return load_excel_from_src(src_key, bytes_d, None)

# ========= Sidebar (estado estable + URL delta-aware) =========
if "dark" not in st.session_state:
    st.session_state.dark = True

sections = ["Resumen","Consulta","Agenda","Gantt","Heatmap","Conflictos",
            "Disponibilidad","Delegaciones","Calidad","Diferencias",
            "Recomendador","Diagnóstico","Acerca de"]

if "sec" not in st.session_state:
    sec_from_url = _qp_get("sec", "Resumen")
    st.session_state.sec = sec_from_url if sec_from_url in sections else "Resumen"

with st.sidebar:
    st.session_state.dark = st.checkbox("Modo oscuro", value=st.session_state.dark)
    section = st.radio("Sección", sections, index=sections.index(st.session_state.sec), key="sec")

    # Modo ligero (recomendado con muchos registros)
    lite = st.toggle("🪶 Modo ligero (recom.)", value=True,
                     help="Evita gráficos y cómputos costosos hasta aplicar filtros.")

    try:
        shade_from_url = float(_qp_get("shade", "0.75"))
    except Exception:
        shade_from_url = 0.75
    ui_dark = st.slider("Intensidad fondo", 0.0, 1.0, shade_from_url, 0.05)

    dens_default = _qp_get("dens","compacta")
    densidad = st.select_slider("Densidad tabla", options=["compacta","media","amplia"], value=dens_default)

    st.markdown("### 📦 Datos")
    st.file_uploader("STREAMLIT.xlsx",    type=["xlsx"], key="upload_main")
    st.file_uploader("DELEGACIONES.xlsx", type=["xlsx"], key="upload_deleg")

_qp_update_if_changed({"shade": ui_dark, "dens": densidad, "sec": st.session_state.sec})
inject_base_css(st.session_state.dark, ui_dark, densidad)

st.markdown("<h1 class='gradient-title'>🗂️ Cronograma Mesas POT</h1>", unsafe_allow_html=True)
st.caption("Omnibox (debounce) • Cache por hash • Lazy index/delegaciones • Modo ligero • Exportes")

# ========= Perfiles por URL =========
PROFILE  = (_qp_get("profile","lectura") or "lectura").lower()
IS_ADMIN = PROFILE == "admin"
IS_COORD = PROFILE == "coord"
READONLY = PROFILE == "lectura"
st.markdown(f"<div class='small'>Perfil activo: <b>{PROFILE}</b> {'💎' if IS_ADMIN else '🧭' if IS_COORD else '🔒'}</div>", unsafe_allow_html=True)

# ========= Lectura principal (cache por hash) =========
df0 = _resolve_main_df()

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

df0 = clean_delegate_markers(df0)
df0["_fecha"] = df0["Fecha"].apply(_to_date)
df0["_ini"]   = df0["Inicio"].apply(_to_time)
df0["_fin"]   = df0["Fin"].apply(_to_time)

for col in ["Participantes","Responsable","Corresponsable","Aula","Nombre de la mesa","Mesa"]:
    if col in df0.columns:
        df0[col] = df0[col].astype(str)
        df0[f"__norm_{col}"] = df0[col].fillna("").astype(str).map(_norm)

for cat_col in ["Aula","Responsable","Corresponsable"]:
    if cat_col in df0.columns:
        try: df0[cat_col] = df0[cat_col].astype("category")
        except Exception: pass

df0 = ensure_sorted(df0)

# Filtrado temporal base: Weekdays y meses Sep–Oct
def _is_weekday(d: Optional[date]) -> bool:
    return (d is not None) and (0 <= d.weekday() <= 4)
def _only_sep_oct_weekdays(d: Optional[date]) -> bool:
    return _is_weekday(d) and (d.month in (9,10))
DF = df0[df0["_fecha"].map(_only_sep_oct_weekdays)].copy()

@st.cache_data(show_spinner=False)
def _dedup_events(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["_fecha","_ini","_fin","Aula","Nombre de la mesa"]
    if not all(c in df.columns for c in cols):
        return df.copy()
    try:
        return df.sort_values(cols, kind="mergesort").drop_duplicates(subset=cols, keep="first")
    except Exception:
        return df.copy()

# ========= Lazy index & delegaciones =========
@st.cache_data(show_spinner=False)
def build_index_cached(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    it = df[["_fecha","_ini","_fin","Nombre de la mesa","Mesa","Participantes","Responsable","Corresponsable","Aula"]].itertuples(index=False, name=None)
    for _fecha,_ini,_fin,nom_mesa,mesa,participantes,respo,corres,aula in it:
        part_list = _split_people(participantes)
        extra = []
        if _safe_str(respo):    extra.append(_safe_str(respo))
        if _safe_str(corres):   extra.append(_safe_str(corres))
        everyone = list(dict.fromkeys(extra + (part_list or [None])))
        if not everyone:
            rows.append((_fecha,_ini,_fin,nom_mesa,mesa,participantes,respo,corres,aula,None))
        else:
            for p in everyone:
                rows.append((_fecha,_ini,_fin,nom_mesa,mesa,participantes,respo,corres,aula,p))
    out = pd.DataFrame(rows, columns=["_fecha","_ini","_fin","Nombre de la mesa","Mesa","Participantes","Responsable","Corresponsable","Aula","Participante_individual"])
    out = ensure_sorted(out)
    for col in ["Responsable","Corresponsable","Aula","Nombre de la mesa","Participantes","Mesa"]:
        if col in out.columns: out[f"__norm_{col}"] = out[col].fillna("").astype(str).map(_norm)
    if "Participante_individual" in out.columns:
        out["__norm_part"] = out["Participante_individual"].fillna("").astype(str).map(_norm)
    else:
        out["__norm_part"] = ""
    return out

deleg_raw = _resolve_deleg_df()

@st.cache_data(show_spinner=False)
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

deleg_map = _prepare_deleg_map(deleg_raw)

@st.cache_data(show_spinner=False)
def annotate_delegations_vectorized(idxf: pd.DataFrame, dmap: pd.DataFrame) -> pd.DataFrame:
    if dmap is None or dmap.empty or idxf is None or idxf.empty:
        out = idxf.copy(); out["__delegado_por_archivo"] = False; return out
    ev = idxf[["_fecha","_ini","_fin","Nombre de la mesa","Mesa","Participante_individual"]].copy()
    ev["ev_idx"]     = ev.index
    ev["mesa_norm"]  = ev["Mesa"].fillna(ev["Nombre de la mesa"]).astype(str).map(_norm_mesa_code)
    ev["actor_norm"] = ev["Participante_individual"].fillna("").astype(str).map(_norm)
    def t2m(t):
        if pd.isna(t) or t is None: return np.nan
        return int(t.hour)*60 + int(t.minute)
    ev["_ini_m"] = ev["_ini"].map(t2m)
    ev["_fin_m"] = ev["_fin"].map(t2m)
    merged = ev.merge(
        dmap, left_on=["mesa_norm","_fecha","actor_norm"],
        right_on=["__mesa","__fecha","__actor"], how="left", suffixes=("","_d")
    )
    ini_ev = merged["_ini_m"].to_numpy()
    fin_ev = merged["_fin_m"].to_numpy()
    ini_d  = merged["__ini_m"].to_numpy() if "__ini_m" in merged.columns else np.full(len(merged), np.nan)
    fin_d  = merged["__fin_m"].to_numpy() if "__fin_m" in merged.columns else np.full(len(merged), np.nan)
    has_ev_time = (~np.isnan(ini_ev)) & (~np.isnan(fin_ev))
    ini_d_f = np.where(np.isnan(ini_d), -1,    ini_d)
    fin_d_f = np.where(np.isnan(fin_d),  1e9,  fin_d)
    overlap = has_ev_time & (np.maximum(ini_ev, ini_d_f) < np.minimum(fin_ev, fin_d_f))
    no_hours = np.isnan(ini_d) & np.isnan(fin_d) & has_ev_time
    merged["__flag"] = overlap | no_hours
    flags = merged.groupby("ev_idx")["__flag"].any().reindex(idxf.index, fill_value=False)
    out = idxf.copy()
    out["__delegado_por_archivo"] = flags.values
    return out

# ========= Búsqueda borrosa (RapidFuzz si está disponible) =========
try:
    from rapidfuzz.fuzz import partial_ratio
    def fuzzy_filter(series: pd.Series, q: str, thr=80) -> pd.Series:
        qn = _norm(q or "")
        if not qn:
            return pd.Series(True, index=series.index)
        fast = series.str.contains(qn, na=False)
        if fast.any():
            return fast
        return series.map(lambda s: partial_ratio(s, qn) >= thr)
except Exception:
    def fuzzy_filter(series: pd.Series, q: str, thr=0.8) -> pd.Series:
        qn = _norm(q or "")
        if not qn:
            return pd.Series(True, index=series.index)
        fast = series.str.contains(qn, na=False)
        if fast.any():
            return fast
        return series.map(lambda s: difflib.SequenceMatcher(None, s, qn).ratio() >= thr)

# ========= ICS helpers =========
def escape_text(val: str) -> str:
    if val is None: return ""
    v = str(val)
    v = v.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
    v = v.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
    return v

def _fold_ical_line(line: str, limit: int = 75) -> str:
    if len(line) <= limit: return line
    chunks, s, first = [], line, True
    while s:
        take = limit if first else (limit - 1)
        chunk, s = s[:take], s[take:]
        chunks.append(chunk if first else " " + chunk)
        first = False
    return "\r\n".join(chunks)

def dt_ics_utc(dt):
    if dt is None: return None
    tz = st.session_state.get("tz", TZ_DEFAULT)
    if dt.tzinfo is None:
        try: dt = dt.replace(tzinfo=tz)
        except Exception: pass
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def build_ics(rows: pd.DataFrame, calendar_name="Cronograma Mesas POT"):
    now_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR","VERSION:2.0","PRODID:-//Cronograma Mesas POT//ES",
        "CALSCALE:GREGORIAN","METHOD:PUBLISH",
        _fold_ical_line(f"X-WR-CALNAME:{escape_text(calendar_name)}"),
        "X-WR-TIMEZONE:America/Bogota",
    ]
    for _, r in rows.iterrows():
        f = combine_dt(r.get("_fecha", r.get("Fecha")), r.get("_ini", r.get("Inicio")))
        t = combine_dt(r.get("_fecha", r.get("Fecha")), r.get("_fin", r.get("Fin")))
        if f is None or t is None: continue
        nombre_mesa = _safe_str(r.get("Nombre de la mesa"))
        aula = _safe_str(r.get("Aula"))
        raw_uid = f"{_norm_mesa_code(r.get('Mesa') or nombre_mesa)}|{_to_date(r.get('Fecha'))}|{_to_time(r.get('Inicio'))}|{aula}"
        uid = hashlib.sha1(raw_uid.encode("utf-8")).hexdigest() + "@mesas.local"
        lines += [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{now_utc}",
            f"DTSTART:{dt_ics_utc(f)}",
            f"DTEND:{dt_ics_utc(t)}",
            _fold_ical_line(f"SUMMARY:{escape_text(nombre_mesa + (' — ' + aula if aula else '') )}"),
            _fold_ical_line(f"LOCATION:{escape_text(aula)}"),
            "END:VEVENT",
        ]
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines).encode("utf-8")

# ========= Omnibox con debounce =========
col1, col2 = st.columns([1,0.18])
with col1:
    c_omni = st.text_input("🔎 Búsqueda (persona / mesa / aula)", value=_qp_get("q",""))
with col2:
    if st.button("Buscar", use_container_width=True):
        _qp_set({"sec":"Consulta","q":c_omni})
        st.rerun()

st.divider()

# ========= Secciones =========
section = st.session_state.sec
KEY_COLS = ["_fecha","_ini","_fin","Aula","Nombre de la mesa"]
MAX_ROWS = 2000  # límite de filas renderizadas en tablas (exportes completos)

if DF.empty:
    st.warning("No hay filas válidas (Lun–Vie, Sep–Oct). Sube un Excel o ajusta el filtro temporal desde el archivo fuente.")

# ---------------- Resumen ----------------
if section == "Resumen":
    st.subheader("📈 Resumen ejecutivo (Lun–Vie, Sep–Oct)")
    DFu = _dedup_events(DF)
    if lite and DFu.shape[0] > 5000:
        st.info("Modo ligero activo: desactívalo o aplica filtros (Consulta) para ver gráficos.")
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
                fig1 = px.bar(top_people, x="Conteo", y="Persona", orientation="h", height=380)
                st.plotly_chart(fig1, use_container_width=True)
            else:
                st.info("Sin datos.")
        with c6:
            st.markdown("**Aulas más usadas (Top 10)**")
            if not uso_aula.empty:
                fig2 = px.bar(uso_aula, x="Mesas", y="Aula", orientation="h", height=380)
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("Sin datos.")

        dfh = DFu.copy()
        def _dow(d):
            if not d: return None
            return ["Lun","Mar","Mié","Jue","Vie"][d.weekday()]
        dfh["Día semana"] = dfh["_fecha"].map(_dow)
        by_dow = dfh.groupby("Día semana")["Nombre de la mesa"].count()\
                    .reindex(["Lun","Mar","Mié","Jue","Vie"]).fillna(0).reset_index(name="Mesas")
        c7, c8 = st.columns(2)
        with c7:
            st.markdown("**Mesas por día de la semana**")
            fig3 = px.bar(by_dow, x="Día semana", y="Mesas", height=300)
            st.plotly_chart(fig3, use_container_width=True)
        with c8:
            st.markdown("**Horas de inicio (histograma)**")
            hh = [t.hour for t in DFu["_ini"] if t is not None]
            if hh:
                fig4 = px.histogram(pd.DataFrame({"Hora": hh}), x="Hora", nbins=12, height=300)
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.info("Sin horas válidas.")

# ---------------- Consulta ----------------
elif section == "Consulta":
    # Lazy index
    @st.cache_data(show_spinner=False)
    def _get_idx(df: pd.DataFrame) -> pd.DataFrame:
        return build_index_cached(df)
    idx = _get_idx(DF)

    # Filtros
    with st.expander("⚙️ Filtros (Lun–Vie, Sep–Oct)", expanded=False):
        c1, c2, c3, c4 = st.columns([1,1,1,0.6])
        fechas_validas = [d for d in DF["_fecha"].dropna().tolist()]
        if fechas_validas:
            dmin, dmax = min(fechas_validas), max(fechas_validas)
        else:
            today = date.today(); dmin, dmax = today, today
        if dmin > dmax: dmin, dmax = dmax, dmin

        with c1:
            qp_rng = None
            try:
                qp_rng = json.loads(_qp_get("rng")) if _qp_get("rng") else None
            except Exception:
                qp_rng = None
            def _parse_iso_date(s):
                try: return date.fromisoformat(str(s)[:10])
                except: return None
            def _safe_range_from_qp(rng, dmin, dmax):
                s, e = dmin, dmax
                if isinstance(rng, (list,tuple)) and len(rng)==2:
                    ps, pe = _parse_iso_date(rng[0]), _parse_iso_date(rng[1])
                    if ps: s = ps
                    if pe: e = pe
                if s > e: s, e = e, s
                s = max(dmin, min(s, dmax)); e = max(dmin, min(e, dmax))
                if s > e: s, e = dmin, dmax
                return s, e
            s_val, e_val = _safe_range_from_qp(qp_rng, dmin, dmax)
            dr = st.date_input("Rango de fechas", value=(s_val, e_val),
                               min_value=dmin, max_value=dmax, key="consulta_rango")
            fmin, fmax = (dr if isinstance(dr, tuple) and len(dr)==2 else (dmin, dmax))
            horas = st.slider("Rango de horas", 0, 23, (6, 20), key="consulta_horas")

        with c2:
            aulas = sorted(DF.get("Aula", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
            def _get_json(k, default):
                try: return json.loads(_qp_get(k)) if _qp_get(k) else default
                except Exception: return default
            aula_sel = st.multiselect("Aulas", ["(todas)"] + aulas,
                                      default=_get_json("aulas",["(todas)"]), key="consulta_aulas")
            dow_opts = ["Lun","Mar","Mié","Jue","Vie"]
            dow_default = ["Lun","Mar","Mié","Jue","Vie"]
            dow = st.multiselect("Días semana", dow_opts,
                                 default=_get_json("dows", dow_default), key="consulta_dow")
            dow = [d for d in dow if d in dow_opts]

        with c3:
            responsables = sorted(DF.get("Responsable", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
            rsel = st.multiselect("Responsables", responsables,
                                  default=_get_json("resp",[]), key="consulta_resp")
            sdel_default = _qp_get("sdel","false").lower() in ("true","1","yes")
            solo_deleg = st.checkbox("🔴 Solo mesas con delegaciones (archivo)", value=sdel_default,
                                     key="consulta_sdel")

        with c4:
            st.markdown("&nbsp;")
            if st.button("↺ Restablecer filtros", use_container_width=True):
                _qp_del(["rng","aulas","dows","resp","sdel","q","view"])
                st.rerun()

        st.caption(f"**Rango activo:** {fmin.isoformat()} → {fmax.isoformat()} · {(fmax - fmin).days + 1} días")
        _qp_update_if_changed({
            "rng": (fmin.isoformat(), fmax.isoformat()),
            "aulas": aula_sel, "dows": dow, "resp": rsel, "sdel": solo_deleg
        })

    # Vistas guardadas
    with st.expander("💾 Vistas guardadas"):
        if st.button("Guardar vista actual"):
            payload = {"rng": (fmin.isoformat(), fmax.isoformat()),
                       "aulas": aula_sel, "dows": dow, "resp": rsel,
                       "sdel": solo_deleg, "q": _qp_get("q","")}
            _qp_update_if_changed({"view": base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")})
            st.success("Vista guardada en la URL. Copia y compártela.")
        vw = _qp_get("view")
        if vw:
            try:
                payload = json.loads(base64.urlsafe_b64decode(vw.encode("utf-8")).decode("utf-8"))
                st.json(payload)
            except Exception:
                st.warning("Vista inválida en la URL.")

    # Delegaciones solo si se usan en esta sección
    if solo_deleg and not deleg_map.empty:
        idx = annotate_delegations_vectorized(idx, deleg_map)

    # Modo: seleccionar / texto
    modo = st.radio("Búsqueda", ["Seleccionar", "Texto"], index=0, horizontal=True, key="consulta_modo")
    people = sorted({p for p in set(idx.get("Participante_individual", pd.Series(dtype=str)).dropna().astype(str).tolist()
                       + DF.get("Responsable", pd.Series(dtype=str)).dropna().astype(str).tolist()
                       + DF.get("Corresponsable", pd.Series(dtype=str)).dropna().astype(str).tolist()) if p})
    term = (st.selectbox("Participante", options=[""]+people, index=0, key="consulta_part")
            if modo=="Seleccionar" else
            st.text_input("Escriba parte del nombre", value=_qp_get("q",""), key="consulta_term"))
    _qp_update_if_changed({"q": term})

    # Máscara vectorizada
    mask = pd.Series(True, index=idx.index, dtype=bool)
    mask &= idx["_fecha"].between(fmin, fmax, inclusive="both")
    if aula_sel and not (len(aula_sel)==1 and aula_sel[0]=="(todas)"):
        allowed = set([a for a in aula_sel if a != "(todas)"])
        mask &= idx["Aula"].astype(str).isin(allowed)
    dows = {"Lun":0,"Mar":1,"Mié":2,"Jue":3,"Vie":4}
    sel_dows = [dows[x] for x in dow] if dow else list(dows.values())
    mask &= idx["_fecha"].map(lambda d: d is not None and d.weekday() in sel_dows)
    hmin, hmax = st.session_state.get("consulta_horas",(6,20))
    mask &= idx["_ini"].map(lambda t: (t is not None) and (hmin <= t.hour <= hmax))
    if rsel: mask &= idx["Responsable"].astype(str).isin(set(rsel))
    if solo_deleg and "__delegado_por_archivo" in idx.columns:
        mask &= idx["__delegado_por_archivo"]
    if term:
        mask &= (fuzzy_filter(idx["__norm_part"], term) |
                 fuzzy_filter(idx["__norm_Responsable"], term) |
                 fuzzy_filter(idx["__norm_Corresponsable"], term))
    mask = mask.reindex(idx.index).fillna(False)

    cols   = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","_fecha","_ini","_fin"]
    res    = _dedup_events(idx.loc[mask, cols].copy())

    # KPIs
    tm = res.shape[0]
    na = res["Aula"].dropna().astype(str).nunique() if not res.empty else 0
    nd = res["_fecha"].dropna().nunique() if not res.empty else 0
    allp = []
    for v in res["Participantes"].fillna("").astype(str).tolist(): allp += _split_people(v)
    npersonas = len(pd.unique(pd.Series([p.strip() for p in allp if p]).astype(str))) if not res.empty else 0
    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(f"<div class='card'><div class='kpi'>Mesas</div><span class='value'>{tm}</span></div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='card'><div class='kpi'>Aulas</div><span class='value'>{na}</span></div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='card'><div class='kpi'>Días</div><span class='value'>{nd}</span></div>", unsafe_allow_html=True)
    with c4: st.markdown(f"<div class='card'><div class='kpi'>Personas únicas</div><span class='value'>{npersonas}</span></div>", unsafe_allow_html=True)

    st.subheader("📋 Resultados (fecha AAAA-MM-DD)")
    if term == "" and res.empty:
        st.info("Empiece escribiendo un nombre o elija uno de la lista.")
    elif res.empty:
        st.warning("Sin resultados.")
    else:
        rf = res.copy()
        rf["Fecha"]  = rf["_fecha"].map(lambda d: d.isoformat() if d else "")
        rf["Inicio"] = rf["_ini"].map(lambda t: t.strftime("%H:%M") if t else "")
        rf["Fin"]    = rf["_fin"].map(lambda t: t.strftime("%H:%M") if t else "")

        # Limitar filas renderizadas
        view = rf.head(MAX_ROWS)
        if rf.shape[0] > MAX_ROWS:
            st.caption(f"Mostrando {MAX_ROWS} de {rf.shape[0]} filas. Usa la descarga para ver todo.")
        st.dataframe(view[["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes"]],
                     use_container_width=True, hide_index=True)

        st.markdown("#### ⬇️ Descargas")
        st.download_button("CSV (filtro)", data=rf.to_csv(index=False).encode("utf-8-sig"),
                           mime="text/csv", file_name="resultados.csv")
        xls_buf = io.BytesIO()
        with pd.ExcelWriter(xls_buf, engine="xlsxwriter") as w:
            rf.to_excel(w, sheet_name="Resultados", index=False)
        st.download_button("Excel (filtro)", data=xls_buf.getvalue(),
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           file_name="resultados.xlsx")
        st.download_button("ICS (todo en uno)", data=build_ics(res, calendar_name="Cronograma Mesas POT"),
                           mime="text/calendar", file_name="mesas.ics")

# ---------------- Agenda ----------------
elif section == "Agenda":
    st.subheader("🗓️ Agenda por persona (Lun–Vie, Sep–Oct)")
    @st.cache_data(show_spinner=False)
    def _get_idx(df: pd.DataFrame) -> pd.DataFrame:
        return build_index_cached(df)
    idx = _get_idx(DF)

    people = sorted({p for p in set(idx.get("Participante_individual", pd.Series(dtype=str)).dropna().astype(str).tolist()
                        + DF.get("Responsable", pd.Series(dtype=str)).dropna().astype(str).tolist()
                        + DF.get("Corresponsable", pd.Series(dtype=str)).dropna().astype(str).tolist()) if p})
    persona = st.selectbox("Seleccione persona", options=people) if people else ""
    if persona:
        m = (fuzzy_filter(idx["__norm_part"], persona, 0.9) |
             fuzzy_filter(idx["__norm_Responsable"], persona, 0.9) |
             fuzzy_filter(idx["__norm_Corresponsable"], persona, 0.9))
        rows = _dedup_events(idx.loc[m, ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","_fecha","_ini","_fin"]].copy())
        rows = ensure_sorted(rows)
        if rows.empty:
            st.info("Sin eventos para esta persona.")
        else:
            for _, r in rows.iterrows():
                s_ini = r["_ini"].strftime('%H:%M') if r["_ini"] else ""
                s_fin = r["_fin"].strftime('%H:%M') if r["_fin"] else ""
                st.markdown(
                    f"**{_safe_str(r['Nombre de la mesa'])}**  \n"
                    f"{r['_fecha'].isoformat() if r['_fecha'] else ''} • {s_ini}–{s_fin} • Aula: {_safe_str(r['Aula'])}",
                    unsafe_allow_html=True
                )
                st.divider()
            st.download_button("⬇️ ICS (Agenda)",
                data=build_ics(rows, calendar_name=f"Agenda — {persona}"),
                mime="text/calendar", file_name=f"agenda_{persona}.ics")

# ---------------- Gantt ----------------
elif section == "Gantt":
    st.subheader("📊 Gantt — Lun–Vie Sep–Oct")
    DFu = _dedup_events(DF)
    if lite and DFu.shape[0] > 5000:
        st.info("Modo ligero activo: desactívalo o aplica filtros (Consulta) para ver el Gantt.")
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
            fig.update_layout(height=550, margin=dict(l=10,r=10,t=30,b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos para Gantt.")

# ---------------- Heatmap ----------------
elif section == "Heatmap":
    st.subheader("🗺️ Heatmap (Aula x Día) — Lun–Vie Sep–Oct")
    DFu = _dedup_events(DF)
    if lite and DFu.shape[0] > 5000:
        st.info("Modo ligero activo: desactívalo o aplica filtros (Consulta) para ver el heatmap.")
    else:
        if "Aula" not in DFu.columns or "_fecha" not in DFu.columns:
            st.info("Faltan columnas para construir el heatmap.")
        else:
            piv = pd.pivot_table(DFu, index="Aula", columns="_fecha",
                                 values="Nombre de la mesa", aggfunc="count", fill_value=0)
            if piv.empty:
                st.info("No hay datos para el heatmap.")
            else:
                try: piv = piv.astype(int)
                except Exception: pass
                fig = px.imshow(piv, aspect="auto", labels=dict(color="Mesas"))
                fig.update_layout(height=500, margin=dict(l=10,r=10,t=30,b=20))
                st.plotly_chart(fig, use_container_width=True)

# ---------------- Conflictos ----------------
elif section == "Conflictos":
    st.subheader("🚦 Solapes — Sweep line (Lun–Vie Sep–Oct)")
    @st.cache_data(show_spinner=False)
    def _get_idx(df: pd.DataFrame) -> pd.DataFrame:
        return build_index_cached(df)
    idx = _get_idx(DF)

    c1, c2, c3 = st.columns(3)
    with c1: scope = st.radio("Ámbito", ["Personas","Aulas"], horizontal=True)
    apply_qp = _qp_get("applydel", "true")
    gap_qp   = _qp_get("gap", "10")
    with c2:
        aplicar_deleg = True if READONLY else st.checkbox(
            "Aplicar DELEGACIONES.xlsx (ignorar actores delegados)", value=(apply_qp.lower() in ("true","1","yes")))
    with c3:
        try: gap_default = int(gap_qp)
        except Exception: gap_default = 10
        brecha = int(st.slider("Brecha mínima (min)", 0, 60, gap_default))
    _qp_update_if_changed({"applydel":aplicar_deleg, "gap":brecha})

    if aplicar_deleg and not deleg_map.empty:
        idx = annotate_delegations_vectorized(idx, deleg_map)

    def overlaps(events: List[Dict], gap_min=0):
        evs = sorted(events, key=lambda e: (e["start"], e["end"]))
        out, active = [], []
        for e in evs:
            active = [a for a in active if (a["end"] + timedelta(minutes=gap_min)) > e["start"]]
            for a in active:
                if (a["end"] + timedelta(minutes=gap_min)) > e["start"]:
                    out.append((a, e))
            active.append(e)
        return out

    dfc = pd.DataFrame()
    if scope == "Personas":
        people = sorted({p for p in set(idx.get("Participante_individual", pd.Series(dtype=str)).dropna().astype(str).tolist()
                           + DF.get("Responsable", pd.Series(dtype=str)).dropna().astype(str).tolist()
                           + DF.get("Corresponsable", pd.Series(dtype=str)).dropna().astype(str).tolist()) if p})
        psel = st.multiselect("Personas a auditar", options=people)
        if psel:
            conf_rows = []
            base_idx = idx if not aplicar_deleg else idx[idx["__delegado_por_archivo"] == False]
            for person in psel:
                m = (fuzzy_filter(base_idx["__norm_part"], person, 0.9) |
                     fuzzy_filter(base_idx["__norm_Responsable"], person, 0.9) |
                     fuzzy_filter(base_idx["__norm_Corresponsable"], person, 0.9))
                sel = _dedup_events(base_idx.loc[m, ["Nombre de la mesa","Aula","_fecha","_ini","_fin"]].copy())
                evs = []
                for _, r in sel.iterrows():
                    s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
                    if s and e: evs.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "Aula": _safe_str(r["Aula"]), "start": s, "end": e})
                for a,b in overlaps(evs, gap_min=brecha):
                    if a["start"].date() == b["start"].date():
                        conf_rows.append({
                            "Persona": person,
                            "Mesa A": a["Mesa"], "Aula A": a["Aula"], "Inicio A": a["start"], "Fin A": a["end"],
                            "Mesa B": b["Mesa"], "Aula B": b["Aula"], "Inicio B": b["start"], "Fin B": b["end"],
                        })
            dfc = pd.DataFrame(conf_rows)
        else:
            st.info("Seleccione una o más personas.")
    else:
        aulas = sorted(DF.get("Aula", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        asel  = st.multiselect("Aulas a auditar", options=aulas)
        if asel:
            conf_rows = []
            for aula in asel:
                sel = _dedup_events(DF[DF["Aula"].astype(str)==aula][["_fecha","_ini","_fin","Nombre de la mesa"]].copy())
                evs = []
                for _, r in sel.iterrows():
                    s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
                    if s and e: evs.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "start": s, "end": e})
                for a,b in overlaps(evs, gap_min=brecha):
                    if a["start"].date() == b["start"].date():
                        conf_rows.append({
                            "Aula": aula,
                            "Mesa A": a["Mesa"], "Inicio A": a["start"], "Fin A": a["end"],
                            "Mesa B": b["Mesa"], "Inicio B": b["start"], "Fin B": b["end"],
                        })
            dfc = pd.DataFrame(conf_rows)
        else:
            st.info("Seleccione una o más aulas.")

    if dfc.empty:
        st.success("Sin solapes detectados. ✅")
    else:
        st.dataframe(dfc, use_container_width=True, hide_index=True)

# ---------------- Disponibilidad ----------------
elif section == "Disponibilidad":
    st.subheader("🟢 Disponibilidad — usa el *Recomendador* para propuestas concretas.")

# ---------------- Delegaciones ----------------
elif section == "Delegaciones":
    st.subheader("🛟 Reporte de Delegaciones (Lun–Vie Sep–Oct)")
    DFu = _dedup_events(DF).copy()

    # Lazy: sólo construye lista si se usa esta sección
    @st.cache_data(show_spinner=False)
    def _actors_for_event_batch(df_events: pd.DataFrame, deleg_map: pd.DataFrame) -> pd.DataFrame:
        if df_events is None or df_events.empty:
            out = df_events.copy() if df_events is not None else pd.DataFrame()
            if out is not None:
                out["Deben delegar"] = [[] for _ in range(len(out))]
            return out
        ev = df_events[["_fecha","_ini","_fin","Nombre de la mesa","Mesa"]].copy()
        ev["mesa_norm"]  = ev["Mesa"].fillna(ev["Nombre de la mesa"]).astype(str).map(_norm_mesa_code)
        merged = ev.merge(deleg_map, left_on=["mesa_norm","_fecha"], right_on=["__mesa","__fecha"], how="left")
        if merged.empty:
            df = df_events.copy(); df["Deben delegar"] = [[] for _ in range(len(df))]
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
        grouped = merged[merged["__ok"]].groupby(merged.index)["__actor_raw"].apply(lambda s: [x for x in pd.unique(s.dropna())])
        out = df_events.copy()
        out["Deben delegar"] = grouped.reindex(df_events.index).apply(lambda x: x if isinstance(x,list) else []).tolist()
        return out

    rep = _actors_for_event_batch(DFu, deleg_map) if not deleg_map.empty else pd.DataFrame()
    rep = rep[rep["Deben delegar"].map(len)>0] if not rep.empty else rep

    if rep.empty:
        st.info("No hay delegaciones registradas en **DELEGACIONES.xlsx** para las mesas/fechas cargadas.")
    else:
        rep["Fecha"]  = rep["_fecha"].map(lambda d: d.isoformat() if d else "")
        rep["Inicio"] = rep["_ini"].map(lambda t: t.strftime("%H:%M") if t else "")
        rep["Fin"]    = rep["_fin"].map(lambda t: t.strftime("%H:%M") if t else "")
        rep["Deben delegar"] = rep["Deben delegar"].map(lambda lst: ", ".join(lst))
        view_cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Deben delegar"]
        view = rep[view_cols].head(MAX_ROWS)
        if rep.shape[0] > MAX_ROWS:
            st.caption(f"Mostrando {MAX_ROWS} de {rep.shape[0]} filas.")
        st.dataframe(view, use_container_width=True, hide_index=True)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            rep[view_cols].to_excel(w, sheet_name="Delegaciones", index=False)
        st.download_button("⬇️ Delegaciones (Excel)", data=buf.getvalue(),
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           file_name="delegaciones.xlsx")

# ---------------- Calidad ----------------
elif section == "Calidad":
    st.subheader("🧪 Panel de Calidad de Datos")
    DFu = _dedup_events(DF)
    bad_fecha = DF[DF["_fecha"].isna()]
    bad_ini   = DF[DF["_ini"].isna()]
    bad_fin   = DF[DF["_fin"].isna()]

    def _cmp_time(t):  # ancla horas al mismo día para comparar
        return datetime.combine(date(2000,1,1), t)

    wrong_order = DF[
        DF["_ini"].notna() & DF["_fin"].notna() &
        DF.apply(lambda r: _cmp_time(r["_fin"]) <= _cmp_time(r["_ini"]), axis=1)
    ]
    dups = DF[DF.duplicated(subset=KEY_COLS, keep=False)].sort_values(KEY_COLS)

    st.markdown("**Fechas faltantes**")
    if not bad_fecha.empty: st.dataframe(bad_fecha[["Nombre de la mesa","Fecha","Inicio","Fin","Aula"]], use_container_width=True, hide_index=True)
    else: st.success("OK")
    st.markdown("**Hora de inicio faltante**")
    if not bad_ini.empty: st.dataframe(bad_ini[["Nombre de la mesa","Fecha","Inicio","Aula"]], use_container_width=True, hide_index=True)
    else: st.success("OK")
    st.markdown("**Hora de fin faltante**")
    if not bad_fin.empty: st.dataframe(bad_fin[["Nombre de la mesa","Fecha","Fin","Aula"]], use_container_width=True, hide_index=True)
    else: st.success("OK")
    st.markdown("**Fin ≤ Inicio**")
    if not wrong_order.empty: st.dataframe(wrong_order[["Nombre de la mesa","Fecha","Inicio","Fin","Aula"]], use_container_width=True, hide_index=True)
    else: st.success("OK")
    st.markdown("**Duplicados por clave (Fecha, Inicio, Fin, Aula, Mesa)**")
    if not dups.empty: st.dataframe(dups[["Nombre de la mesa","Fecha","Inicio","Fin","Aula"]], use_container_width=True, hide_index=True)
    else: st.success("Sin duplicados.")

    # Reconciliación de Delegaciones (sin mesa/fecha correspondiente)
    st.subheader("🔎 Reconciliación Delegaciones")
    if not DF.empty and not deleg_map.empty:
        m_norm = DF["Mesa"].fillna(DF["Nombre de la mesa"]).astype(str).map(_norm_mesa_code)
        k_ev = set(zip(m_norm, DF["_fecha"]))
        no_match = deleg_map[~deleg_map.apply(lambda r: (r["__mesa"], r["__fecha"]) in k_ev, axis=1)]
        if not no_match.empty:
            st.warning("Delegaciones sin mesa/fecha coincidente:")
            st.dataframe(no_match[["__actor_raw","__mesa","__fecha","__ini","__fin"]], use_container_width=True, hide_index=True)
        else:
            st.success("Todas las delegaciones referencian mesa/fecha existente.")
    else:
        st.info("Sin datos suficientes para reconciliar delegaciones.")

# ---------------- Diferencias ----------------
elif section == "Diferencias":
    st.subheader("🧭 Diferencias entre archivos")
    a = st.file_uploader("Archivo A (.xlsx)", type=["xlsx"], key="diff_a")
    b = st.file_uploader("Archivo B (.xlsx)", type=["xlsx"], key="diff_b")
    if a and b:
        A = normalize_cols(pd.ExcelFile(a, engine="openpyxl").parse(0)); B = normalize_cols(pd.ExcelFile(b, engine="openpyxl").parse(0))
        for df in (A,B):
            df["_fecha"] = df["Fecha"].apply(_to_date); df["_ini"] = df["Inicio"].apply(_to_time); df["_fin"] = df["Fin"].apply(_to_time)
        KEY = ["_fecha","_ini","_fin","Aula","Nombre de la mesa"]
        Akey = set(map(tuple, _dedup_events(A)[KEY].dropna().to_numpy()))
        Bkey = set(map(tuple, _dedup_events(B)[KEY].dropna().to_numpy()))
        add = Bkey - Akey; rem = Akey - Bkey; common = Akey & Bkey
        st.markdown("**Altas (en B y no en A)**"); st.write(len(add))
        st.markdown("**Bajas (en A y no en B)**"); st.write(len(rem))
        st.markdown("**Eventos comunes**");         st.write(len(common))

# ---------------- Recomendador ----------------
elif section == "Recomendador":
    st.subheader("🧠 Recomendador de horario (Top 5)")
    DFu = _dedup_events(DF)
    if lite and DFu.shape[0] > 5000:
        st.info("Modo ligero activo: reduce rango o desactívalo para calcular propuestas.")
    else:
        nombre = st.text_input("Nombre de la mesa nueva / a reubicar")
        responsables = sorted(DF.get("Responsable", pd.Series(dtype=str)).dropna().astype(str).unique())
        resp_sel = st.multiselect("Responsables implicados", responsables)
        participantes_libre = st.text_area("Participantes (separados por coma) — opcional")
        personas = [p.strip() for p in (resp_sel + _split_people(participantes_libre)) if p.strip()]
        aulas = sorted(DF.get("Aula", pd.Series(dtype=str)).dropna().astype(str).unique())
        aulas_sel = st.multiselect("Aulas posibles", aulas, default=aulas)
        dur_min = st.slider("Duración (min)", 30, 240, 120, 15)
        paso    = st.slider("Paso de búsqueda (min)", 15, 60, 30, 15)
        fechas_validas = sorted(DFu["_fecha"].dropna().unique().tolist())
        dmin, dmax = (min(fechas_validas), max(fechas_validas)) if fechas_validas else (date.today(), date.today())
        dr = st.date_input("Rango de búsqueda", value=(dmin, dmax), min_value=dmin, max_value=dmax)
        fmin, fmax = (dr if isinstance(dr, tuple) and len(dr)==2 else (dmin, dmax))

        @st.cache_data(show_spinner=False)
        def person_busy_map(df: pd.DataFrame) -> Dict[str, List[Tuple[datetime, datetime]]]:
            mp: Dict[str, List[Tuple[datetime, datetime]]] = {}
            exp = build_index_cached(df)
            for _, r in exp.iterrows():
                p = r.get("Participante_individual")
                if not p: continue
                s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
                if s and e: mp.setdefault(_norm(p), []).append((s,e))
            return {k: sorted(v) for k,v in mp.items()}

        busy_by_person = person_busy_map(DFu)
        busy_by_room   = {}
        for _, r in DFu.iterrows():
            s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
            if s and e:
                busy_by_room.setdefault(_norm(_safe_str(r.get("Aula",""))), []).append((s,e))
        for k in list(busy_by_room.keys()): busy_by_room[k] = sorted(busy_by_room[k])

        def overlaps_list(win: Tuple[datetime,datetime], intervals: List[Tuple[datetime,datetime]]) -> int:
            s, e = win; return sum(1 for a,b in intervals if max(s,a) < min(e,b))
        def gap_cost(win: Tuple[datetime,datetime], intervals: List[Tuple[datetime,datetime]]) -> float:
            s, e = win; mins=[]
            for a,b in intervals:
                if b <= s: mins.append((s-b).total_seconds()/60.0)
                elif a >= e: mins.append((a-e).total_seconds()/60.0)
            if not mins: return 0.0
            m = min(mins); return 0.0 if m >= 30 else (30 - m)

        if st.button("Calcular propuestas"):
            proposals = []
            day = fmin
            while day <= fmax:
                if day.weekday() <= 4:
                    t_cursor = time(8,0)
                    while t_cursor < time(18,0):
                        start = datetime(day.year, day.month, day.day, t_cursor.hour, t_cursor.minute, tzinfo=TZ_DEFAULT)
                        end   = start + timedelta(minutes=dur_min)
                        win   = (start, end)
                        # costo aula
                        c_room = overlaps_list(win, busy_by_room.get(_norm(aulas_sel[0]), [])) if aulas_sel else 0
                        # costo personas
                        c_people = 0; pegado = 0.0
                        for p in personas:
                            iv = busy_by_person.get(_norm(p), [])
                            c_people += overlaps_list(win, iv)
                            pegado   += gap_cost(win, iv)
                        cost = (10*c_room) + (5*c_people) + (0.1*pegado)
                        if c_room==0 and c_people==0:
                            for aula in aulas_sel:
                                proposals.append({"Fecha": day.isoformat(),
                                                  "Inicio": start.strftime("%H:%M"),
                                                  "Fin": end.strftime("%H:%M"),
                                                  "Aula": aula,
                                                  "Conflictos": c_room+c_people,
                                                  "Pegado(min)": round(pegado,1),
                                                  "Costo": round(cost,2)})
                        # avanzar
                        t_cursor = (datetime.combine(day, t_cursor) + timedelta(minutes=paso)).time()
                day += timedelta(days=1)
            if not proposals:
                st.warning("No encontré opciones sin conflictos. Amplía rango o reduce restricciones.")
            else:
                dfp = pd.DataFrame(proposals).sort_values(by=["Costo","Fecha","Inicio"]).head(5)
                st.dataframe(dfp, use_container_width=True, hide_index=True)
                if not dfp.empty:
                    dummy = pd.DataFrame([{
                        "Nombre de la mesa": nombre or "Propuesta",
                        "_fecha": date.fromisoformat(dfp.iloc[0]["Fecha"]),
                        "_ini": _to_time(dfp.iloc[0]["Inicio"]),
                        "_fin": _to_time(dfp.iloc[0]["Fin"]),
                        "Aula": dfp.iloc[0]["Aula"]
                    }])
                    st.download_button("⬇️ ICS (mejor opción)", data=build_ics(dummy, calendar_name="Propuesta"),
                                       mime="text/calendar", file_name="propuesta.ics")

# ---------------- Diagnóstico ----------------
elif section == "Diagnóstico":
    st.subheader("🧪 Diagnóstico (Lun–Vie Sep–Oct)")
    tz_opt = st.selectbox("Zona horaria ICS",
        options=["America/Bogota","America/Lima","America/Mexico_City","UTC"], index=0)
    try:
        from zoneinfo import ZoneInfo
        st.session_state.tz = ZoneInfo(tz_opt) if tz_opt!="UTC" else timezone.utc
    except Exception:
        st.session_state.tz = TZ_DEFAULT

    # Información de entorno y archivos
    st.markdown("### 🧷 Fuentes y hojas detectadas")
    st.write({"Candidatos STREAMLIT": REPO_CAND_MAIN, "Candidatos DELEGACIONES": REPO_CAND_DELEG})
    try:
        src_info = _qp_get("src_dbg", "")
        if src_info: st.code(src_info)
    except Exception:
        pass

    # Hoja(s) del principal (re-abre vía loader para listar)
    try:
        # Reutiliza la cache key más reciente posible
        _ = None
        if st.session_state.get("upload_main"):
            _ = f"upload_main::{_file_hash(st.session_state.upload_main)}"
        elif _EMBED_XLSX_B64:
            _ = f"embed_main::{hashlib.sha1(_EMBED_XLSX_B64.encode()).hexdigest()}"
        elif REPO_CAND_MAIN:
            _ = f"path_main::{REPO_CAND_MAIN[0]}"
        if _:
            rp = _strip_cache_prefix(_)
            st.caption(f"Ruta principal usada (diagnóstico): {rp}")
            if os.path.exists(rp):
                xl = pd.ExcelFile(rp, engine="openpyxl")
                st.write({"Hojas disponibles": xl.sheet_names})
    except Exception as e:
        st.warning(f"No se pudieron listar las hojas: {e}")

    # Tamaños y memoria
    st.markdown("### 📦 Tamaños y memoria (aprox)")
    def _mem_df(df: pd.DataFrame) -> str:
        try:
            return f"{df.memory_usage(deep=True).sum()/1024/1024:.2f} MB"
        except Exception:
            return "N/D"
    st.write({
        "Filas DF (Sep–Oct, Lun–Vie)": int(DF.shape[0]),
        "Memoria DF": _mem_df(DF),
        "Filas índice (si se usa)": "cacheado",
    })
    if 'rapidfuzz' in sys.modules:
        st.success("RapidFuzz activo (búsqueda borrosa acelerada).")
    else:
        st.info("Usando difflib estándar para búsqueda borrosa.")

    # Chequeos rápidos
    DFu = _dedup_events(DF)
    issues=[]
    def _err(row,col,msg): issues.append(f"Fila {int(row)+2} — {col}: {msg}")
    for i, r in DFu.iterrows():
        if r.get("_fecha") is None: _err(i,"Fecha", f"Inválida/vacía (‘{_safe_str(r.get('Fecha'))[:24]}’)")
        t1, t2 = r.get("_ini"), r.get("_fin")
        if t1 is None: _err(i,"Inicio", f"Hora inválida/vacía (‘{_safe_str(r.get('Inicio'))[:24]}’)")
        if t2 is None: _err(i,"Fin",    f"Hora inválida/vacía (‘{_safe_str(r.get('Fin'))[:24]}’)")
        if t1 and t2 and (datetime.combine(date(2000,1,1), t2) <= datetime.combine(date(2000,1,1), t1)):
            _err(i,"Fin", f"Fin ≤ Inicio ({t1} -> {t2})")
    if all(c in DFu.columns for c in KEY_COLS):
        n_dups = int(DFu.duplicated(subset=KEY_COLS, keep=False).sum())
        if n_dups: issues.append(f"{n_dups} duplicados por clave {KEY_COLS}.")
    if not issues:
        st.success("Sin problemas críticos. ✅")
    else:
        for it in issues:
            st.error("• " + it)

    # Botón para liberar memoria de caches
    if st.button("🧹 Limpiar cachés y recolector de basura"):
        st.cache_data.clear()
        gc.collect()
        st.success("Cachés limpiados.")

# ---------------- Acerca de ----------------
else:
    st.subheader("ℹ️ Acerca de")
    st.markdown("Publicación: 2025-09-15 — Cronograma Mesas POT (cache por hash, lazy, modo ligero, navegación estable)")

# -*- coding: utf-8 -*-
"""
Mesas · INIMAGINABLE — versión productiva
- Fechas AAAA-MM-DD, sin sábados ni domingos (Lun–Vie) y solo Sept–Oct
- Filtros robustos (date_input con clamp y orden), estado en URL y “↺ Restablecer”
- Máscara alineada (sin IndexingError)
- Conteos sobre eventos únicos (Fecha+Inicio+Fin+Aula+Nombre)
- ICS robusto (escape + folding + UID determinístico)
- Delegaciones: 'Deben delegar' tomado exclusivamente de DELEGACIONES.xlsx (columna 'actor')
- Conflictos con sweep-line (O(n log n)) que ignoran actores delegados si se desea
"""
import io, re, base64, unicodedata, difflib, os, json, hashlib
from datetime import datetime, date, time, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

# ========= Embebidos opcionales (si quieres empaquetar todo en un solo archivo) =========
_EMBED_XLSX_B64 = ""        # STREAMLIT.xlsx (hoja “Calendario” o primera)
_EMBED_DELEG_B64 = ""       # DELEGACIONES.xlsx (primera hoja)
_BG_B64 = ""                # Imagen de fondo opcional en base64
_SHEET_CANDIDATES = ["Calendario", "Agenda", "Programación"]

# ========= Config & rutas repo =========
_DELEG_MARKER_REGEX = re.compile(r"\(\s*no disponible\s*,\s*asignar delegado\s*\)", re.IGNORECASE)
_SEP_REGEX = re.compile(r"[;,/]|\n|\r|\t|\||\u2022|·")
REPO_CAND_MAIN = ["STREAMLIT.xlsx","./data/STREAMLIT.xlsx","/mnt/data/STREAMLIT.xlsx"]
REPO_CAND_DELEG = ["DELEGACIONES.xlsx","./data/DELEGACIONES.xlsx","/mnt/data/DELEGACIONES.xlsx"]

try:
    from zoneinfo import ZoneInfo
    TZ_DEFAULT = ZoneInfo("America/Bogota")
except Exception:
    TZ_DEFAULT = timezone(timedelta(hours=-5))

# ========= UI base =========
st.set_page_config(page_title="Mesas · INIMAGINABLE", page_icon="🗂️", layout="wide", initial_sidebar_state="expanded")

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
    .gradient-title {{ background: linear-gradient(90deg,#60a5fa 0%,#22d3ee 100%); -webkit-background-clip:text; -webkit-text-fill-color:transparent; font-weight:800; letter-spacing:.2px; }}
    .card {{ border-radius:16px; padding:1rem 1.2rem; border:1px solid {("#1f2937" if dark else "#e5e7eb")}; background: {"rgba(17,24,39,0.82)" if dark else "rgba(255,255,255,0.93)"}; box-shadow:0 10px 30px rgba(0,0,0,0.25); }}
    .kpi {{ font-size:.9rem; color:{"#cbd5e1" if dark else "#6b7280"}; margin-bottom:.25rem; }}
    .kpi .value {{ display:block; font-size:1.6rem; font-weight:700; color:{"#f8fafc" if dark else "#111827"}; }}
    .small {{ font-size:.85rem; color:{"#cbd5e1" if dark else "#6b7280"}; }}
    .stDataFrame div[role='row'] {{ padding-top:{row_pad}; padding-bottom:{row_pad}; }}
    .dataframe th, .dataframe td {{ background:transparent !important; }}
    </style>
    """, unsafe_allow_html=True)

# ========= Utilidades =========
def _safe_str(x): return "" if pd.isna(x) else str(x).strip()

def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii','ignore').decode('ascii')
    s = re.sub(r"\s+"," ", s)
    return s.lower().strip()

def _strip_delegate_marker(s: str) -> str:
    if not s: return s
    return _DELEG_MARKER_REGEX.sub("", s).strip()

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

# ▶︎ Fechas estrictas AAAA-MM-DD
def _to_date(x):
    if isinstance(x, date) and not isinstance(x, datetime): return x
    if isinstance(x, datetime): return x.date()
    if pd.isna(x): return None
    try:
        return datetime.strptime(str(x).strip(), "%Y-%m-%d").date()
    except Exception:
        pass
    d = pd.to_datetime(x, errors="coerce", utc=False)
    return None if pd.isna(d) else (d.date() if isinstance(d, pd.Timestamp) else None)

def _to_time(x):
    if isinstance(x, time): return x
    if isinstance(x, datetime): return x.time().replace(microsecond=0)
    d = pd.to_datetime(x, errors="coerce")
    if not pd.isna(d) and isinstance(d, pd.Timestamp): return d.time().replace(microsecond=0)
    try:
        hh, mm = str(x).strip().split(":")[:2]
        return time(int(hh), int(mm))
    except Exception:
        return None

def combine_dt(fecha, hora, tz: Optional[timezone]=None):
    tz = tz or st.session_state.get("tz", TZ_DEFAULT)
    d = fecha if isinstance(fecha, date) and not isinstance(fecha, datetime) else _to_date(fecha)
    t = hora if isinstance(hora, time) else _to_time(hora)
    if d is None or t is None: return None
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second or 0, tzinfo=tz)

def ensure_sorted(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "_fecha" in df.columns and "_ini" in df.columns:
        df.sort_values(by=["_fecha","_ini"], inplace=True, kind="mergesort")
    elif "Fecha" in df.columns and "Inicio" in df.columns:
        df["_Fecha_dt"] = df["Fecha"].apply(_to_date)
        df["_Inicio_t"] = df["Inicio"].apply(_to_time)
        df.sort_values(by=["_Fecha_dt","_Inicio_t"], inplace=True, kind="mergesort")
        df.drop(columns=["_Fecha_dt","_Inicio_t"], inplace=True)
    return df

# ========= Carga de datos =========
def _read_excel_from_bytes(data: bytes, sheet_candidates=None) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(data))
    sheet = None
    if sheet_candidates:
        for cand in sheet_candidates:
            if cand in xls.sheet_names: sheet = cand; break
    if sheet is None: sheet = xls.sheet_names[0]
    return xls.parse(sheet)

@st.cache_data(show_spinner=False)
def _try_load_main(embed_b64: str):
    if "upload_main" in st.session_state and st.session_state.upload_main is not None:
        return pd.ExcelFile(st.session_state.upload_main).parse(0)
    if embed_b64:
        raw = base64.b64decode(embed_b64.encode("utf-8"))
        return _read_excel_from_bytes(raw, _SHEET_CANDIDATES)
    for path in REPO_CAND_MAIN:
        if os.path.exists(path):
            xls = pd.ExcelFile(path)
            sheet = "Calendario" if "Calendario" in xls.sheet_names else xls.sheet_names[0]
            return xls.parse(sheet)
    st.error("No se encontró el Excel principal. Carga **STREAMLIT.xlsx** o usa la versión embebida."); st.stop()

@st.cache_data(show_spinner=False)
def _try_load_deleg(embed_b64: str):
    if "upload_deleg" in st.session_state and st.session_state.upload_deleg is not None:
        return pd.ExcelFile(st.session_state.upload_deleg).parse(0)
    if embed_b64:
        raw = base64.b64decode(embed_b64.encode("utf-8"))
        return _read_excel_from_bytes(raw, None)
    for path in REPO_CAND_DELEG:
        if os.path.exists(path):
            return pd.ExcelFile(path).parse(0)
    return pd.DataFrame()

# ========= Query params =========
def set_qp(**kwargs):
    qp = st.query_params
    for k,v in kwargs.items():
        if v is None:
            if k in qp: del qp[k]
        else:
            qp[k] = json.dumps(v, ensure_ascii=False) if isinstance(v,(list,tuple,dict)) else str(v)
def get_qp(key, default=None, parse_json=False):
    qp = st.query_params
    if key not in qp: return default
    val = qp[key]
    if parse_json:
        try: return json.loads(val)
        except Exception: return default
    return val

# ========= Sidebar =========
if "dark" not in st.session_state: st.session_state.dark = True
with st.sidebar:
    st.session_state.dark = st.checkbox("Modo oscuro", value=st.session_state.dark)
    section = st.radio("Sección", ["Resumen","Consulta","Agenda","Gantt","Heatmap","Conflictos","Disponibilidad","Delegaciones","Diagnóstico","Acerca de"], index=0)
    ui_dark = st.slider("Intensidad fondo", 0.0, 1.0, float(get_qp("shade",0.75)), 0.05)
    densidad = st.select_slider("Densidad tabla", options=["compacta","media","amplia"], value=get_qp("dens","compacta"))
    set_qp(shade=ui_dark, dens=densidad, sec=section)
    st.markdown("### 📦 Datos")
    st.file_uploader("STREAMLIT.xlsx", type=["xlsx"], key="upload_main")
    st.file_uploader("DELEGACIONES.xlsx", type=["xlsx"], key="upload_deleg")

inject_base_css(st.session_state.dark, ui_dark, densidad)

st.markdown("<h1 class='gradient-title'>🗂️ Mesas · INIMAGINABLE</h1>", unsafe_allow_html=True)
st.caption("Omnibox • Weekdays-only • Delegaciones desde archivo • Conflictos sweep-line • Exportes completos")

# ========= Perfiles =========
PROFILE = st.query_params.get("profile","lectura").lower()
IS_ADMIN = PROFILE == "admin"
IS_COORD = PROFILE == "coord"
READONLY = PROFILE == "lectura"
st.markdown(f"<div class='small'>Perfil activo: <b>{PROFILE}</b> {'💎' if IS_ADMIN else '🧭' if IS_COORD else '🔒'}</div>", unsafe_allow_html=True)

# ========= Lectura principal =========
raw = _try_load_main(_EMBED_XLSX_B64)
df0 = normalize_cols(raw)

# ====== Limpieza visual de marcadores del principal (no define “deben delegar”) ======
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
            df[col] = df[col].fillna("").astype(str).apply(_strip_delegate_marker)
    df["Requiere Delegación"] = False  # se define desde DELEGACIONES.xlsx
    return df

df0 = clean_delegate_markers(df0)

# Precalcular
df0["_fecha"] = df0["Fecha"].apply(_to_date)
df0["_ini"]   = df0["Inicio"].apply(_to_time)
df0["_fin"]   = df0["Fin"].apply(_to_time)

for col in ["Participantes","Responsable","Corresponsable","Aula","Nombre de la mesa","Mesa"]:
    if col in df0.columns: df0[f"__norm_{col}"] = df0[col].fillna("").astype(str).apply(_norm)
df0 = ensure_sorted(df0)

# Weekdays only y meses 9–10
def _is_weekday(d: Optional[date]) -> bool:
    return (d is not None) and (0 <= d.weekday() <= 4)
def _only_sep_oct_weekdays(d: Optional[date]) -> bool:
    return _is_weekday(d) and (d.month in (9, 10))
DF = df0[df0["_fecha"].apply(_only_sep_oct_weekdays)].copy()

# Index expandido (personas)
def build_index(df: pd.DataFrame) -> pd.DataFrame:
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

idx = build_index(DF)
for col in ["Responsable","Corresponsable","Aula","Nombre de la mesa","Participantes","Mesa"]:
    if col in idx.columns: idx[f"__norm_{col}"] = idx[col].fillna("").astype(str).apply(_norm)
idx["__norm_part"] = idx["Participante_individual"].fillna("").astype(str).apply(_norm)

# ========= Delegaciones desde archivo =========
deleg_raw = _try_load_deleg(_EMBED_DELEG_B64)

def _prepare_deleg_map(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve DF con: __actor (norm), __actor_raw (original), __mesa (norm), __fecha, __ini, __fin"""
    if df is None or df.empty:
        return pd.DataFrame(columns=["__actor","__actor_raw","__mesa","__fecha","__ini","__fin"])
    col_actor = col_mesa = col_fecha = col_ini = col_fin = None
    for c in df.columns:
        cl = str(c).lower()
        if col_actor is None and ("actor" in cl): col_actor = c
        if col_mesa  is None and "mesa" in cl:   col_mesa = c
        if col_fecha is None and "fecha" in cl:  col_fecha = c
        if col_ini   is None and ("inicio" in cl or "hora inicio" in cl): col_ini = c
        if col_fin   is None and ("fin" in cl or "hora fin" in cl):       col_fin = c
    if col_actor is None or col_mesa is None or col_fecha is None:
        return pd.DataFrame(columns=["__actor","__actor_raw","__mesa","__fecha","__ini","__fin"])

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
        "__mesa":      df[col_mesa].astype(str).map(_norm),
        "__fecha":     pd.to_datetime(df[col_fecha], errors="coerce").dt.date,
        "__ini":       df[col_ini].map(_to_t) if col_ini in df.columns else None,
        "__fin":       df[col_fin].map(_to_t) if col_fin in df.columns else None
    }).dropna(subset=["__mesa","__fecha"])
    out = out[out["__actor"].astype(bool)]
    return out

deleg_map = _prepare_deleg_map(deleg_raw)

def _token_subset(a: str, b: str) -> bool:
    sa, sb = set(a.split()), set(b.split())
    if not sa or not sb: return False
    return sa.issubset(sb) if len(sa) <= len(sb) else sb.issubset(sa)

def _build_deleg_groups(dmap: pd.DataFrame):
    # key: (mesa_norm, fecha) -> list of (actor_norm, actor_raw, ini, fin)
    groups: Dict[Tuple[str, date], List[Tuple[str, str, Optional[time], Optional[time]]]] = {}
    for _, r in dmap.iterrows():
        key = (r["__mesa"], r["__fecha"])
        groups.setdefault(key, []).append((r["__actor"], r["__actor_raw"], r.get("__ini"), r.get("__fin")))
    return groups

DELEG_GROUPS = _build_deleg_groups(deleg_map)

def annotate_delegations(idxf: pd.DataFrame, groups) -> pd.DataFrame:
    """Marca idxf['__delegado_por_archivo']=True si ese participante figura como actor en DELEGACIONES.xlsx para esa mesa/fecha (y hora si aplica)."""
    idxf = idxf.copy()
    flags = []
    for _, r in idxf.iterrows():
        mesa_norm = _norm(_safe_str(r.get("Mesa") or r.get("Nombre de la mesa")))
        key = (mesa_norm, r.get("_fecha"))
        cands = groups.get(key, [])
        actor_norm = _norm(r.get("Participante_individual",""))
        ini_r, fin_r = r.get("_ini"), r.get("_fin")
        ok = False
        for act_norm, _act_raw, ini_d, fin_d in cands:
            name_ok = (actor_norm == act_norm) or _token_subset(actor_norm, act_norm)
            if not name_ok: 
                continue
            if ini_d and fin_d and ini_r and fin_r:
                ok |= (max(ini_r, ini_d) < min(fin_r, fin_d))
            else:
                ok |= True
        flags.append(ok)
    idxf["__delegado_por_archivo"] = flags
    return idxf

idx = annotate_delegations(idx, DELEG_GROUPS)

# ========= ICS =========
def escape_text(val: str) -> str:
    if val is None: return ""
    v = str(val)
    v = v.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
    v = v.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")
    return v
def _fold_ical_line(line: str, limit: int = 75) -> str:
    if len(line) <= limit:
        return line
    chunks = []
    s = line
    first = True
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
def build_ics(rows: pd.DataFrame, calendar_name="Mesas"):
    now_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR","VERSION:2.0","PRODID:-//Planificador de Mesas//ES",
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
        raw_uid = f"{nombre_mesa}|{_to_date(r.get('Fecha'))}|{_to_time(r.get('Inicio'))}|{aula}"
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

# ========= Omnibox =========
c_omni = st.text_input("🔎 Búsqueda rápida (persona / mesa / aula)", value=st.query_params.get("q",""))
if c_omni:
    st.query_params["sec"] = "Consulta"
    st.query_params["q"] = c_omni
    st.rerun()

st.divider()

# ========= Clave única de evento =========
KEY_COLS = ["_fecha","_ini","_fin","Aula","Nombre de la mesa"]
def _dedup_events(df: pd.DataFrame) -> pd.DataFrame:
    if not all(c in df.columns for c in KEY_COLS): return df.copy()
    return df.sort_values(KEY_COLS, kind="mergesort").drop_duplicates(subset=KEY_COLS, keep="first")

# ========= Utilidades de fecha para widgets =========
def _parse_iso_date(s) -> Optional[date]:
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None
def _safe_range_from_qp(qp_rng, dmin: date, dmax: date) -> Tuple[date, date]:
    s, e = dmin, dmax
    if isinstance(qp_rng, (list, tuple)) and len(qp_rng) == 2:
        ps, pe = _parse_iso_date(qp_rng[0]), _parse_iso_date(qp_rng[1])
        if ps: s = ps
        if pe: e = pe
    if s > e: s, e = e, s
    s = max(dmin, min(s, dmax))
    e = max(dmin, min(e, dmax))
    if s > e: s, e = dmin, dmax
    return s, e

# ========= Secciones =========
if section == "Resumen":
    st.subheader("📈 Resumen ejecutivo (Lun–Vie, Sep–Oct)")
    DFu = _dedup_events(DF)

    def make_stats(df):
        base = _dedup_events(df)
        n_mesas = base.shape[0]
        aulas = base["Aula"].dropna().astype(str).nunique() if "Aula" in base else 0
        dias = base["_fecha"].dropna().nunique() if "_fecha" in base else 0
        # personas únicas solo por participantes para no inflar (puedes sumar resp/co si lo deseas)
        allp = []
        for v in base["Participantes"].fillna("").astype(str).tolist(): allp += _split_people(v)
        n_personas = len(pd.unique(pd.Series([p.strip() for p in allp if p]).astype(str)))
        return n_mesas, aulas, dias, n_personas

    tm, na, nd, np = make_stats(DFu)
    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(f"<div class='card'><div class='kpi'>Mesas</div><span class='value'>{tm}</span></div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='card'><div class='kpi'>Aulas</div><span class='value'>{na}</span></div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='card'><div class='kpi'>Días</div><span class='value'>{nd}</span></div>", unsafe_allow_html=True)
    with c4: st.markdown(f"<div class='card'><div class='kpi'>Personas únicas</div><span class='value'>{np}</span></div>", unsafe_allow_html=True)

elif section == "Consulta":
    with st.expander("⚙️ Filtros (Lun–Vie, Sep–Oct)", expanded=False):
        c1, c2, c3, c4 = st.columns([1,1,1,0.6])

        fechas_validas = [d for d in DF["_fecha"].dropna().tolist()]
        if fechas_validas:
            dmin, dmax = min(fechas_validas), max(fechas_validas)
        else:
            today = date.today(); dmin, dmax = today, today
        if dmin > dmax: dmin, dmax = dmax, dmin

        with c1:
            qp_rng = get_qp("rng", default=None, parse_json=True) if "rng" in st.query_params else None
            s_val, e_val = _safe_range_from_qp(qp_rng, dmin, dmax)
            dr = st.date_input("Rango de fechas", value=(s_val, e_val),
                               min_value=dmin, max_value=dmax, key="consulta_rango")
            fmin, fmax = (dr if isinstance(dr, tuple) and len(dr)==2 else (dmin, dmax))
            horas = st.slider("Rango de horas", 0, 23, (6, 20), key="consulta_horas")

        with c2:
            aulas = sorted(DF["Aula"].dropna().astype(str).unique().tolist())
            aula_sel = st.multiselect("Aulas", ["(todas)"] + aulas,
                                      default=get_qp("aulas",["(todas)"],True), key="consulta_aulas")
            dow_opts = ["Lun","Mar","Mié","Jue","Vie"]
            dow_default = ["Lun","Mar","Mié","Jue","Vie"]
            dow = st.multiselect("Días semana", dow_opts,
                                 default=get_qp("dows", dow_default, True), key="consulta_dow")
            dow = [d for d in dow if d in dow_opts]

        with c3:
            responsables = sorted(DF["Responsable"].dropna().astype(str).unique().tolist())
            rsel = st.multiselect("Responsables", responsables,
                                  default=get_qp("resp",[],True), key="consulta_resp")
            solo_deleg = st.checkbox("🔴 Solo mesas con delegaciones (archivo)",
                                     value=bool(get_qp("sdel","false") in ("true","True","1")),
                                     key="consulta_sdel")

        with c4:
            st.markdown("&nbsp;")
            if st.button("↺ Restablecer filtros", use_container_width=True):
                for k in ["rng","aulas","dows","resp","sdel","q"]:
                    if k in st.query_params: del st.query_params[k]
                st.rerun()

        st.caption(f"**Rango activo:** {fmin.isoformat()} → {fmax.isoformat()} · {(fmax - fmin).days + 1} días")
        set_qp(rng=(fmin.isoformat(), fmax.isoformat()),
               aulas=aula_sel, dows=dow, resp=rsel, sdel=solo_deleg)

    modo = st.radio("Búsqueda", ["Seleccionar", "Texto"], index=0, horizontal=True, key="consulta_modo")
    people = sorted({
        p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                       + DF["Responsable"].dropna().astype(str).tolist()
                       + DF["Corresponsable"].dropna().astype(str).tolist()) if p
    })
    term = (
        st.selectbox("Participante", options=[""]+people, index=0, key="consulta_part")
        if modo=="Seleccionar" else
        st.text_input("Escriba parte del nombre", value=st.query_params.get("q",""), key="consulta_term")
    )
    set_qp(q=term)

    # Máscara alineada
    mask = pd.Series(True, index=idx.index, dtype=bool)

    mask &= idx["_fecha"].apply(lambda d: (d is not None) and (fmin <= d <= fmax))

    if aula_sel and not (len(aula_sel)==1 and aula_sel[0]=="(todas)"):
        allowed = set([a for a in aula_sel if a != "(todas)"])
        mask &= idx["Aula"].fillna("").astype(str).isin(allowed)

    dows = {"Lun":0,"Mar":1,"Mié":2,"Jue":3,"Vie":4}
    selected_dows = [dows[x] for x in dow] if dow else list(dows.values())
    mask &= idx["_fecha"].apply(lambda dd: dd is not None and dd.weekday() in selected_dows)

    hmin, hmax = horas
    mask &= idx["_ini"].apply(lambda t: (t is not None) and (hmin <= t.hour <= hmax))

    if rsel:
        mask &= idx["Responsable"].fillna("").astype(str).isin(set(rsel))

    if solo_deleg:
        mask &= idx["__delegado_por_archivo"] == True

    if term:
        mask &= (
            smart_match := (  # operador morsel para claridad
                (idx["__norm_part"].str.contains(_norm(term)))  # fallback por si rapidfuzz no está
            )
        ) | (
            (idx["__norm_part"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(term)).ratio() >= 0.8)) |
            (idx["__norm_Responsable"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(term)).ratio() >= 0.8)) |
            (idx["__norm_Corresponsable"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(term)).ratio() >= 0.8))
        )

    mask = mask.reindex(idx.index).fillna(False)

    cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula",
            "Responsable","Corresponsable","Participantes",
            "_fecha","_ini","_fin"]

    res_idx = idx.loc[mask, cols].copy()
    res = _dedup_events(res_idx)

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    tm = res.shape[0]
    na = res["Aula"].dropna().astype(str).nunique() if not res.empty else 0
    nd = res["_fecha"].dropna().nunique() if not res.empty else 0
    allp = []
    for v in res["Participantes"].fillna("").astype(str).tolist(): allp += _split_people(v)
    npersonas = len(pd.unique(pd.Series([p.strip() for p in allp if p]).astype(str))) if not res.empty else 0
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
        rf["Fecha"]  = rf["_fecha"].apply(lambda d: d.isoformat() if d else "")
        rf["Inicio"] = rf["_ini"].apply(lambda t: t.strftime("%H:%M") if t else "")
        rf["Fin"]    = rf["_fin"].apply(lambda t: t.strftime("%H:%M") if t else "")
        st.dataframe(rf[["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes"]],
                     use_container_width=True, hide_index=True)

        st.markdown("#### ⬇️ Descargas")
        st.download_button("CSV (filtro)", data=rf.to_csv(index=False).encode("utf-8-sig"),
                           mime="text/csv", file_name="resultados.csv")
        xls_buf = io.BytesIO()
        with pd.ExcelWriter(xls_buf, engine="xlsxwriter") as w: rf.to_excel(w, sheet_name="Resultados", index=False)
        st.download_button("Excel (filtro)", data=xls_buf.getvalue(),
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           file_name="resultados.xlsx")
        st.download_button("ICS (todo en uno)", data=build_ics(res, calendar_name="Mesas"),
                           mime="text/calendar", file_name="mesas.ics")

elif section == "Agenda":
    st.subheader("🗓️ Agenda por persona (Lun–Vie, Sep–Oct)")
    people = sorted({
        p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                       + DF["Responsable"].dropna().astype(str).tolist()
                       + DF["Corresponsable"].dropna().astype(str).tolist()) if p
    })
    persona = st.selectbox("Seleccione persona", options=people)
    if persona:
        m = (
            (idx["__norm_part"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(persona)).ratio() >= 0.9)) |
            (idx["__norm_Responsable"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(persona)).ratio() >= 0.9)) |
            (idx["__norm_Corresponsable"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(persona)).ratio() >= 0.9))
        )
        rows = _dedup_events(idx.loc[m, ["Nombre de la mesa","Fecha","Inicio","Fin","Aula",
                                         "Responsable","Corresponsable","Participantes",
                                         "_fecha","_ini","_fin"]].copy())
        rows = ensure_sorted(rows)
        if rows.empty:
            st.info("Sin eventos para esta persona.")
        else:
            for _, r in rows.iterrows():
                s_ini = r["_ini"].strftime('%H:%M') if r["_ini"] else ""
                s_fin = r["_fin"].strftime('%H:%M') if r["_fin"] else ""
                st.markdown(
                    f"**{_safe_str(r['Nombre de la mesa'])}**  \n"
                    f"{r['_fecha'].isoformat() if r['_fecha'] else ''} • {s_ini}–{s_fin} • Aula: {_safe_str(r['Aula'])}  \n"
                    f"<span class='small'>Resp.: {_safe_str(r['Responsable'])} • Co-resp.: {_safe_str(r['Corresponsable'])}</span>",
                    unsafe_allow_html=True
                )
                st.divider()
            st.download_button("⬇️ ICS (Agenda)", data=build_ics(rows, calendar_name=f"Agenda — {persona}"),
                               mime="text/calendar", file_name=f"agenda_{persona}.ics")

elif section == "Gantt":
    st.subheader("📊 Gantt — Lun–Vie Sep–Oct")
    rows = []
    DFu = _dedup_events(DF)
    for _, r in DFu.iterrows():
        start = combine_dt(r["_fecha"], r["_ini"]); end = combine_dt(r["_fecha"], r["_fin"])
        if not (start and end): continue
        rows.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "Aula": _safe_str(r["Aula"]), "start": start, "end": end})
    if rows:
        dfg = pd.DataFrame(rows)
        fig = px.timeline(dfg, x_start="start", x_end="end", y="Aula", hover_data=["Mesa"])
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(height=550, margin=dict(l=10,r=10,t=30,b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay datos para Gantt.")

elif section == "Heatmap":
    st.subheader("🗺️ Heatmap (Aula x Día) — Lun–Vie Sep–Oct")
    DFu = _dedup_events(DF)
    piv = pd.pivot_table(DFu, index="Aula", columns="_fecha", values="Nombre de la mesa", aggfunc="count", fill_value=0)
    if piv.empty:
        st.info("No hay datos para el heatmap.")
    else:
        fig = px.imshow(piv, aspect="auto", labels=dict(color="Mesas"))
        fig.update_layout(height=500, margin=dict(l=10,r=10,t=30,b=20))
        st.plotly_chart(fig, use_container_width=True)

elif section == "Conflictos":
    st.subheader("🚦 Solapes — Sweep line (Lun–Vie Sep–Oct)")
    c1, c2, c3 = st.columns(3)
    with c1: scope = st.radio("Ámbito", ["Personas","Aulas"], horizontal=True)
    apply_qp = st.query_params.get("applydel", "true")
    gap_qp = st.query_params.get("gap", "10")
    with c2:
        aplicar_deleg = True if READONLY else st.checkbox(
            "Aplicar DELEGACIONES.xlsx (ignorar actores delegados en el análisis)",
            value=(apply_qp.lower() in ("true","1","yes"))
        )
    with c3:
        try: gap_default = int(gap_qp)
        except Exception: gap_default = 10
        brecha = st.slider("Brecha mínima (min)", 0, 60, gap_default)
    set_qp(applydel=aplicar_deleg, gap=brecha)

    def overlaps(events: List[Dict], gap_min=0):
        evs = sorted(events, key=lambda e: (e["start"], e["end"]))
        out = []; active = []
        for e in evs:
            active = [a for a in active if (a["end"] + timedelta(minutes=gap_min)) > e["start"]]
            for a in active:
                if (a["end"] + timedelta(minutes=gap_min)) > e["start"]:
                    out.append((a, e))
            active.append(e)
        return out

    dfc = pd.DataFrame()
    if scope == "Personas":
        people = sorted({
            p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                           + DF["Responsable"].dropna().astype(str).tolist()
                           + DF["Corresponsable"].dropna().astype(str).tolist()) if p
        })
        psel = st.multiselect("Personas a auditar", options=people)
        if psel:
            conf_rows = []
            base_idx = idx if not aplicar_deleg else idx[idx["__delegado_por_archivo"] == False]
            for person in psel:
                m = (
                    (base_idx["__norm_part"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(person)).ratio() >= 0.9)) |
                    (base_idx["__norm_Responsable"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(person)).ratio() >= 0.9)) |
                    (base_idx["__norm_Corresponsable"].apply(lambda s: difflib.SequenceMatcher(None, s, _norm(person)).ratio() >= 0.9))
                )
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
        aulas = sorted(DF["Aula"].dropna().astype(str).unique().tolist())
        asel = st.multiselect("Aulas a auditar", options=aulas)
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

    if dfc.empty: st.success("Sin solapes detectados. ✅")
    else: st.dataframe(dfc, use_container_width=True, hide_index=True)

elif section == "Disponibilidad":
    st.subheader("🟢 Disponibilidad (personas / aulas) — Lun–Vie Sep–Oct")
    c1, c2, c3 = st.columns(3)
    with c1: mode = st.radio("Modo", ["Personas","Aulas"], horizontal=True)
    with c2: ventana = st.slider("Duración mínima (min)", 15, 240, 60, 15)
    with c3: margen = st.slider("Margen (min)", 0, 60, 10, 5)
    fechas_validas = [d for d in DF["_fecha"].dropna().tolist()]
    if fechas_validas: dmin, dmax = min(fechas_validas), max(fechas_validas)
    else: today = date.today(); dmin, dmax = today, today
    if dmin > dmax: dmin, dmax = dmax, dmin
    dr = st.date_input("Rango de fechas", value=(dmin, dmax), min_value=dmin, max_value=dmax)
    st.info("Selecciona personas/aulas y calcula huecos libres (pendiente de activación según tu flujo).")

elif section == "Delegaciones":
    st.subheader("🛟 Reporte de Delegaciones (Lun–Vie Sep–Oct)")

    DFu = _dedup_events(DF).copy()

    # ---- actores por evento (desde DELEGACIONES.xlsx) ----
    def _actors_for_event(r) -> List[str]:
        mesa_norm = _norm(_safe_str(r.get("Mesa") or r.get("Nombre de la mesa")))
        key = (mesa_norm, r.get("_fecha"))
        cands = DELEG_GROUPS.get(key, [])
        ini_r, fin_r = r.get("_ini"), r.get("_fin")
        out = []
        for act_norm, act_raw, ini_d, fin_d in cands:
            if ini_r and fin_r and ini_d and fin_d:
                if max(ini_r, ini_d) < min(fin_r, fin_d):
                    out.append(act_raw)
            else:
                out.append(act_raw)
        # únicos preservando orden
        seen=set(); ret=[]
        for a in out:
            if a and a not in seen:
                seen.add(a); ret.append(a)
        return ret

    DFu["Deben delegar"] = DFu.apply(_actors_for_event, axis=1)
    rep = DFu[DFu["Deben delegar"].apply(lambda x: len(x)>0)].copy()

    if rep.empty:
        st.info("No hay delegaciones registradas en **DELEGACIONES.xlsx** para las mesas/fechas cargadas.")
    else:
        rep["Fecha"]  = rep["_fecha"].apply(lambda d: d.isoformat() if d else "")
        rep["Inicio"] = rep["_ini"].apply(lambda t: t.strftime("%H:%M") if t else "")
        rep["Fin"]    = rep["_fin"].apply(lambda t: t.strftime("%H:%M") if t else "")
        rep["Deben delegar"] = rep["Deben delegar"].apply(lambda lst: ", ".join(lst))
        view_cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula",
                     "Responsable","Corresponsable","Participantes","Deben delegar"]
        st.dataframe(rep[view_cols], use_container_width=True, hide_index=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            rep[view_cols].to_excel(w, sheet_name="Delegaciones", index=False)
        st.download_button("⬇️ Delegaciones (Excel)", data=buf.getvalue(),
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           file_name="delegaciones.xlsx")

elif section == "Diagnóstico":
    st.subheader("🧪 Diagnóstico (Lun–Vie Sep–Oct)")
    tz_opt = st.selectbox("Zona horaria ICS", options=["America/Bogota","America/Lima","America/Mexico_City","UTC"], index=0)
    try:
        from zoneinfo import ZoneInfo
        st.session_state.tz = ZoneInfo(tz_opt) if tz_opt!="UTC" else timezone.utc
    except Exception:
        st.session_state.tz = TZ_DEFAULT

    DFu = _dedup_events(DF)
    issues = []
    def _err(row, col, msg): issues.append(f"Fila {int(row)+2} — {col}: {msg}")

    for i, r in DFu.iterrows():
        if r.get("_fecha") is None: _err(i,"Fecha", f"Inválida/vacía (valor='{_safe_str(r.get('Fecha'))[:24]}')")
        t1, t2 = r.get("_ini"), r.get("_fin")
        if t1 is None: _err(i,"Inicio", f"Hora inválida/vacía (valor='{_safe_str(r.get('Inicio'))[:24]}')")
        if t2 is None: _err(i,"Fin",    f"Hora inválida/vacía (valor='{_safe_str(r.get('Fin'))[:24]}')")
        if t1 and t2 and (datetime.combine(date(2000,1,1), t2) <= datetime.combine(date(2000,1,1), t1)):
            _err(i,"Fin", f"Fin ≤ Inicio ({t1} -> {t2})")

    if all(c in DFu.columns for c in KEY_COLS[0:4] + ["Nombre de la mesa"]):
        n_dups = int(DFu.duplicated(subset=KEY_COLS, keep=False).sum())
        if n_dups: issues.append(f"{n_dups} duplicados por clave de evento {KEY_COLS}.")

    if not issues:
        st.success("Sin problemas críticos detectados. ✅")
    else:
        for it in issues: st.error("• " + it)

else:
    st.subheader("ℹ️ Acerca de")
    st.markdown("Publicación: 13/09/2025 — INIMAGINABLE (Delegaciones desde archivo)")

# -*- coding: utf-8 -*-
"""
Planificador de Mesas y Agenda POT
- Fechas AAAA-MM-DD, sin sábados ni domingos (Lun–Vie) y solo Sept–Oct
- KPIs + Gráficos en Resumen
- Vistas guardadas (shareable URL) en Consulta
- Panel de Calidad & Reconciliación de Delegaciones
- Diff entre versiones (altas/bajas/cambios)
- Recomendador de horario (top-5) sin romper lo anterior
- ICS robusto, Conflictos sweep-line, Delegaciones desde DELEGACIONES.xlsx (columna 'actor')
"""
import io, re, base64, unicodedata, difflib, os, json, hashlib, math
from datetime import datetime, date, time, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

# ========= Embebidos opcionales =========
_EMBED_XLSX_B64 = ""        # STREAMLIT.xlsx (hoja “Calendario” o primera)
_EMBED_DELEG_B64 = ""       # DELEGACIONES.xlsx (primera hoja)
_BG_B64 = ""                # Imagen de fondo (base64) opcional
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
    .block-container {{ padding-top: 1.0rem; }}
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

# Fechas AAAA-MM-DD
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
    section = st.radio("Sección", ["Resumen","Consulta","Agenda","Gantt","Heatmap","Conflictos","Disponibilidad","Delegaciones","Calidad","Diferencias","Recomendador","Diagnóstico","Acerca de"], index=0)
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
    df["Requiere Delegación"] = False  # la “obligación” viene del archivo DELEGACIONES.xlsx
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
    groups: Dict[Tuple[str, date], List[Tuple[str, str, Optional[time], Optional[time]]]] = {}
    for _, r in dmap.iterrows():
        key = (r["__mesa"], r["__fecha"])
        groups.setdefault(key, []).append((r["__actor"], r["__actor_raw"], r.get("__ini"), r.get("__fin")))
    return groups
DELEG_GROUPS = _build_deleg_groups(deleg_map)

def annotate_delegations(idxf: pd.DataFrame, groups) -> pd.DataFrame:
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

# ========= Búsqueda rápida (Omnibox) =========
def _score(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, _norm(a), _norm(b)).ratio()
def fuzzy_filter(series: pd.Series, q: str, thr=0.8) -> pd.Series:
    qn = _norm(q)
    if not qn: return pd.Series(True, index=series.index)
    return series.apply(lambda s: _score(s, qn) >= thr)

# ========= ICS =========
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

# ========= Utilidad fechas para widgets =========
def _parse_iso_date(s) -> Optional[date]:
    try: return date.fromisoformat(str(s)[:10])
    except Exception: return None
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

# ================================== SECCIONES ==================================

# ---------------- Resumen ----------------
if section == "Resumen":
    st.subheader("📈 Resumen ejecutivo (Lun–Vie, Sep–Oct)")

    DFu = _dedup_events(DF)

    # KPIs
    def make_stats(df):
        base = _dedup_events(df)
        n_mesas = base.shape[0]
        aulas = base["Aula"].dropna().astype(str).nunique() if "Aula" in base else 0
        dias = base["_fecha"].dropna().nunique() if "_fecha" in base else 0
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

    # Gráficos (nuevo)
    all_people = []
    for v in DFu["Participantes"].fillna("").astype(str).tolist(): all_people += _split_people(v)
    s = pd.Series([p.strip() for p in all_people if p and str(p).strip()])
    top_people = s.value_counts().head(10).rename_axis("Persona").reset_index(name="Conteo")
    uso_aula = DFu.groupby("Aula")["Nombre de la mesa"].count().sort_values(ascending=False).head(10).rename_axis("Aula").reset_index(name="Mesas")

    c5, c6 = st.columns(2)
    with c5:
        st.markdown("**Top 10 personas por participación**")
        if not top_people.empty:
            st.plotly_chart(px.bar(top_people, x="Conteo", y="Persona", orientation="h", height=380), use_container_width=True)
        else:
            st.info("Sin datos de personas.")
    with c6:
        st.markdown("**Aulas más usadas (Top 10)**")
        if not uso_aula.empty:
            st.plotly_chart(px.bar(uso_aula, x="Mesas", y="Aula", orientation="h", height=380), use_container_width=True)
        else:
            st.info("Sin datos de aulas.")

    dfh = DFu.copy()
    dfh["Día semana"] = dfh["_fecha"].apply(lambda d: ["Lun","Mar","Mié","Jue","Vie"][d.weekday()] if d else None)
    by_dow = dfh.groupby("Día semana")["Nombre de la mesa"].count().reindex(["Lun","Mar","Mié","Jue","Vie"]).fillna(0).reset_index(name="Mesas")
    c7, c8 = st.columns(2)
    with c7:
        st.markdown("**Mesas por día de la semana**")
        st.plotly_chart(px.bar(by_dow, x="Día semana", y="Mesas", height=300), use_container_width=True)
    with c8:
        st.markdown("**Horas de inicio (histograma)**")
        hh = [t.hour for t in DFu["_ini"] if t is not None]
        if hh:
            st.plotly_chart(px.histogram(pd.DataFrame({"Hora": hh}), x="Hora", nbins=12, height=300), use_container_width=True)
        else:
            st.info("Sin horas de inicio válidas.")

# ---------------- Consulta ----------------
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
            dr = st.date_input("Rango de fechas", value=(s_val, e_val), min_value=dmin, max_value=dmax, key="consulta_rango")
            fmin, fmax = (dr if isinstance(dr, tuple) and len(dr)==2 else (dmin, dmax))
            horas = st.slider("Rango de horas", 0, 23, (6, 20), key="consulta_horas")

        with c2:
            aulas = sorted(DF["Aula"].dropna().astype(str).unique().tolist())
            aula_sel = st.multiselect("Aulas", ["(todas)"] + aulas, default=get_qp("aulas",["(todas)"],True), key="consulta_aulas")
            dow_opts = ["Lun","Mar","Mié","Jue","Vie"]
            dow_default = ["Lun","Mar","Mié","Jue","Vie"]
            dow = st.multiselect("Días semana", dow_opts, default=get_qp("dows", dow_default, True), key="consulta_dow")
            dow = [d for d in dow if d in dow_opts]

        with c3:
            responsables = sorted(DF["Responsable"].dropna().astype(str).unique().tolist())
            rsel = st.multiselect("Responsables", responsables, default=get_qp("resp",[],True), key="consulta_resp")
            solo_deleg = st.checkbox("🔴 Solo mesas con delegaciones (archivo)", value=bool(get_qp("sdel","false") in ("true","True","1")), key="consulta_sdel")

        with c4:
            st.markdown("&nbsp;")
            if st.button("↺ Restablecer filtros", use_container_width=True):
                for k in ["rng","aulas","dows","resp","sdel","q","view"]:
                    if k in st.query_params: del st.query_params[k]
                st.rerun()

        st.caption(f"**Rango activo:** {fmin.isoformat()} → {fmax.isoformat()} · {(fmax - fmin).days + 1} días")
        set_qp(rng=(fmin.isoformat(), fmax.isoformat()), aulas=aula_sel, dows=dow, resp=rsel, sdel=solo_deleg)

    # Vistas guardadas (URL)
    with st.expander("💾 Vistas guardadas"):
        vista_nombre = st.text_input("Nombre de la vista")
        if st.button("Guardar vista actual"):
            payload = {"rng": (fmin.isoformat(), fmax.isoformat()), "aulas": aula_sel, "dows": dow, "resp": rsel, "sdel": solo_deleg, "q": st.query_params.get("q","")}
            st.query_params["view"] = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
            st.success("Vista guardada en la URL. Copia y compártela.")
        if "view" in st.query_params:
            try:
                payload = json.loads(base64.urlsafe_b64decode(st.query_params["view"].encode("utf-8")).decode("utf-8"))
                st.json(payload)
            except Exception:
                st.warning("Vista inválida en la URL.")

    modo = st.radio("Búsqueda", ["Seleccionar", "Texto"], index=0, horizontal=True, key="consulta_modo")
    people = sorted({
        p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                       + DF["Responsable"].dropna().astype(str).tolist()
                       + DF["Corresponsable"].dropna().astype(str).tolist()) if p
    })
    term = st.selectbox("Participante", options=[""]+people, index=0, key="consulta_part") if modo=="Seleccionar" else st.text_input("Escriba parte del nombre", value=st.query_params.get("q",""), key="consulta_term")
    set_qp(q=term)

    # Máscara
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
    if rsel: mask &= idx["Responsable"].fillna("").astype(str).isin(set(rsel))
    if solo_deleg: mask &= idx["__delegado_por_archivo"] == True
    if term:
        mask &= (fuzzy_filter(idx["__norm_part"], term) | fuzzy_filter(idx["__norm_Responsable"], term) | fuzzy_filter(idx["__norm_Corresponsable"], term))
    mask = mask.reindex(idx.index).fillna(False)

    cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","_fecha","_ini","_fin"]
    res_idx = idx.loc[mask, cols].copy()
    res = _dedup_events(res_idx)

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    tm = res.shape[0]; na = res["Aula"].dropna().astype(str).nunique() if not res.empty else 0
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
        st.dataframe(rf[["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes"]], use_container_width=True, hide_index=True)

        st.markdown("#### ⬇️ Descargas")
        st.download_button("CSV (filtro)", data=rf.to_csv(index=False).encode("utf-8-sig"), mime="text/csv", file_name="resultados.csv")
        xls_buf = io.BytesIO()
        with pd.ExcelWriter(xls_buf, engine="xlsxwriter") as w: rf.to_excel(w, sheet_name="Resultados", index=False)
        st.download_button("Excel (filtro)", data=xls_buf.getvalue(), mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", file_name="resultados.xlsx")
        st.download_button("ICS (todo en uno)", data=build_ics(res, calendar_name="Mesas"), mime="text/calendar", file_name="mesas.ics")

# ---------------- Agenda ----------------
elif section == "Agenda":
    st.subheader("🗓️ Agenda por persona (Lun–Vie, Sep–Oct)")
    people = sorted({p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                        + DF["Responsable"].dropna().astype(str).tolist()
                        + DF["Corresponsable"].dropna().astype(str).tolist()) if p})
    persona = st.selectbox("Seleccione persona", options=people)
    if persona:
        m = (fuzzy_filter(idx["__norm_part"], persona, 0.9) | fuzzy_filter(idx["__norm_Responsable"], persona, 0.9) | fuzzy_filter(idx["__norm_Corresponsable"], persona, 0.9))
        rows = _dedup_events(idx.loc[m, ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","_fecha","_ini","_fin"]].copy())
        rows = ensure_sorted(rows)
        if rows.empty:
            st.info("Sin eventos para esta persona.")
        else:
            for _, r in rows.iterrows():
                s_ini = r["_ini"].strftime('%H:%M') if r["_ini"] else ""
                s_fin = r["_fin"].strftime('%H:%M') if r["_fin"] else ""
                st.markdown(f"**{_safe_str(r['Nombre de la mesa'])}**  \n{r['_fecha'].isoformat() if r['_fecha'] else ''} • {s_ini}–{s_fin} • Aula: {_safe_str(r['Aula'])}", unsafe_allow_html=True)
                st.divider()
            st.download_button("⬇️ ICS (Agenda)", data=build_ics(rows, calendar_name=f"Agenda — {persona}"), mime="text/calendar", file_name=f"agenda_{persona}.ics")

# ---------------- Gantt ----------------
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

# ---------------- Heatmap ----------------
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

# ---------------- Conflictos ----------------
elif section == "Conflictos":
    st.subheader("🚦 Solapes — Sweep line (Lun–Vie Sep–Oct)")
    c1, c2, c3 = st.columns(3)
    with c1: scope = st.radio("Ámbito", ["Personas","Aulas"], horizontal=True)
    apply_qp = st.query_params.get("applydel", "true")
    gap_qp = st.query_params.get("gap", "10")
    with c2:
        aplicar_deleg = True if READONLY else st.checkbox("Aplicar DELEGACIONES.xlsx (ignorar actores delegados)", value=(apply_qp.lower() in ("true","1","yes")))
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
        people = sorted({p for p in set(idx["Participante_individual"].dropna().astype(str).tolist()
                           + DF["Responsable"].dropna().astype(str).tolist()
                           + DF["Corresponsable"].dropna().astype(str).tolist()) if p})
        psel = st.multiselect("Personas a auditar", options=people)
        if psel:
            conf_rows = []
            base_idx = idx if not aplicar_deleg else idx[idx["__delegado_por_archivo"] == False]
            for person in psel:
                m = (fuzzy_filter(base_idx["__norm_part"], person, 0.9) | fuzzy_filter(base_idx["__norm_Responsable"], person, 0.9) | fuzzy_filter(base_idx["__norm_Corresponsable"], person, 0.9))
                sel = _dedup_events(base_idx.loc[m, ["Nombre de la mesa","Aula","_fecha","_ini","_fin"]].copy())
                evs = []
                for _, r in sel.iterrows():
                    s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
                    if s and e: evs.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "Aula": _safe_str(r["Aula"]), "start": s, "end": e})
                for a,b in overlaps(evs, gap_min=brecha):
                    if a["start"].date() == b["start"].date():
                        conf_rows.append({"Persona": person,"Mesa A": a["Mesa"], "Aula A": a["Aula"], "Inicio A": a["start"], "Fin A": a["end"],"Mesa B": b["Mesa"], "Aula B": b["Aula"], "Inicio B": b["start"], "Fin B": b["end"]})
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
                        conf_rows.append({"Aula": aula,"Mesa A": a["Mesa"], "Inicio A": a["start"], "Fin A": a["end"],"Mesa B": b["Mesa"], "Inicio B": b["start"], "Fin B": b["end"]})
            dfc = pd.DataFrame(conf_rows)
        else:
            st.info("Seleccione una o más aulas.")
    if dfc.empty: st.success("Sin solapes detectados. ✅")
    else: st.dataframe(dfc, use_container_width=True, hide_index=True)

# ---------------- Disponibilidad (placeholder operativo) ----------------
elif section == "Disponibilidad":
    st.subheader("🟢 Disponibilidad (personas / aulas) — Lun–Vie Sep–Oct")
    st.info("Usa el *Recomendador* para propuestas concretas de horario.")

# ---------------- Delegaciones (con columna “Deben delegar”) ----------------
elif section == "Delegaciones":
    st.subheader("🛟 Reporte de Delegaciones (Lun–Vie Sep–Oct)")
    DFu = _dedup_events(DF).copy()

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
        view_cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Deben delegar"]
        st.dataframe(rep[view_cols], use_container_width=True, hide_index=True)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            rep[view_cols].to_excel(w, sheet_name="Delegaciones", index=False)
        st.download_button("⬇️ Delegaciones (Excel)", data=buf.getvalue(), mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", file_name="delegaciones.xlsx")

# ---------------- Calidad (nuevo) ----------------
elif section == "Calidad":
    st.subheader("🧪 Panel de Calidad de Datos")
    DFu = _dedup_events(DF)

    issues = []
    # Fechas/Horas inválidas
    bad_fecha = DF[DF["_fecha"].isna()]
    bad_ini   = DF[DF["_ini"].isna()]
    bad_fin   = DF[DF["_fin"].isna()]
    # Fin <= Inicio
    wrong_order = DF[DF.apply(lambda r: r["_ini"] and r["_fin"] and (datetime.combine(date(2000,1,1), r["_fin"]) <= datetime.combine(date(2000,1,1), r["_ini"])), axis=1)]
    # Duplicados por clave
    dups = DF[DF.duplicated(subset=KEY_COLS, keep=False)].sort_values(KEY_COLS)

    st.markdown("**Fechas faltantes**"); st.dataframe(bad_fecha[["Nombre de la mesa","Fecha","Inicio","Fin","Aula"]], use_container_width=True, hide_index=True) if not bad_fecha.empty else st.success("OK")
    st.markdown("**Hora de inicio faltante**"); st.dataframe(bad_ini[["Nombre de la mesa","Fecha","Inicio","Aula"]], use_container_width=True, hide_index=True) if not bad_ini.empty else st.success("OK")
    st.markdown("**Hora de fin faltante**"); st.dataframe(bad_fin[["Nombre de la mesa","Fecha","Fin","Aula"]], use_container_width=True, hide_index=True) if not bad_fin.empty else st.success("OK")
    st.markdown("**Fin ≤ Inicio**"); st.dataframe(wrong_order[["Nombre de la mesa","Fecha","Inicio","Fin","Aula"]], use_container_width=True, hide_index=True) if not wrong_order.empty else st.success("OK")
    st.markdown("**Duplicados por clave (Fecha, Inicio, Fin, Aula, Mesa)**")
    st.dataframe(dups[["Nombre de la mesa","Fecha","Inicio","Fin","Aula"]], use_container_width=True, hide_index=True) if not dups.empty else st.success("Sin duplicados.")

    # Reconciliación de Delegaciones
    st.subheader("🔎 Reconciliación Delegaciones")
    # Registros de delegaciones sin match de mesa/fecha
    no_match = []
    for key, cands in DELEG_GROUPS.items():
        if _dedup_events(DF[(_norm(DF["Nombre de la mesa"].fillna("").astype(str))==key[0]) & (DF["_fecha"]==key[1])]).empty:
            for c in cands:
                no_match.append({"Mesa(fecha)": key, "Actor": c[1]})
    if no_match:
        st.warning("Delegaciones sin mesa/fecha coincidente:")
        st.dataframe(pd.DataFrame(no_match), use_container_width=True, hide_index=True)
    else:
        st.success("Todas las delegaciones referencian mesa/fecha existente.")

# ---------------- Diferencias (nuevo) ----------------
elif section == "Diferencias":
    st.subheader("🧭 Diferencias entre archivos")
    a = st.file_uploader("Archivo A (.xlsx)", type=["xlsx"], key="diff_a")
    b = st.file_uploader("Archivo B (.xlsx)", type=["xlsx"], key="diff_b")
    if a and b:
        A = normalize_cols(pd.ExcelFile(a).parse(0))
        B = normalize_cols(pd.ExcelFile(b).parse(0))
        for df in (A,B):
            df["_fecha"] = df["Fecha"].apply(_to_date)
            df["_ini"]   = df["Inicio"].apply(_to_time)
            df["_fin"]   = df["Fin"].apply(_to_time)
        KEY = ["_fecha","_ini","_fin","Aula","Nombre de la mesa"]
        Akey = set(tuple(x) for x in _dedup_events(A)[KEY].dropna().to_records(index=False))
        Bkey = set(tuple(x) for x in _dedup_events(B)[KEY].dropna().to_records(index=False))
        add = Bkey - Akey
        rem = Akey - Bkey
        st.markdown("**Altas (en B y no en A)**")
        st.write(len(add))
        st.markdown("**Bajas (en A y no en B)**")
        st.write(len(rem))
        # Cambios: misma clave pero columnas diferentes
        common = Akey & Bkey
        # (por simplicidad mostramos conteo; extender a diff de columnas si lo quieres más detallado)
        st.markdown("**Eventos comunes**"); st.write(len(common))

# ---------------- Recomendador (nuevo) ----------------
elif section == "Recomendador":
    st.subheader("🧠 Recomendador de horario (Top 5)")
    DFu = _dedup_events(DF)
    nombre = st.text_input("Nombre de la mesa nueva / a reubicar")
    responsables = sorted(DF["Responsable"].dropna().astype(str).unique())
    resp_sel = st.multiselect("Responsables implicados", responsables)
    participantes_libre = st.text_area("Participantes (separados por coma) — opcional")
    personas = [p.strip() for p in (resp_sel + _split_people(participantes_libre)) if p.strip()]
    aulas = sorted(DF["Aula"].dropna().astype(str).unique())
    aulas_sel = st.multiselect("Aulas posibles", aulas, default=aulas)
    dur_min = st.slider("Duración (min)", 30, 240, 120, 15)
    paso = st.slider("Paso de búsqueda (min)", 15, 60, 30, 15)

    fechas_validas = sorted(DFu["_fecha"].dropna().unique().tolist())
    if fechas_validas:
        dmin, dmax = min(fechas_validas), max(fechas_validas)
    else:
        today = date.today(); dmin, dmax = today, today
    dr = st.date_input("Rango de búsqueda", value=(dmin, dmax), min_value=dmin, max_value=dmax)
    fmin, fmax = (dr if isinstance(dr, tuple) and len(dr)==2 else (dmin, dmax))

    def person_busy_map(df: pd.DataFrame) -> Dict[str, List[Tuple[datetime, datetime]]]:
        mp: Dict[str, List[Tuple[datetime, datetime]]] = {}
        for _, r in build_index(df).iterrows():
            p = r["Participante_individual"]
            if not p: continue
            s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
            if not (s and e): continue
            mp.setdefault(_norm(p), []).append((s,e))
        return {k: sorted(v) for k,v in mp.items()}

    busy_by_person = person_busy_map(DFu)
    busy_by_room   = {}
    for _, r in DFu.iterrows():
        s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
        if not (s and e): continue
        busy_by_room.setdefault(_norm(_safe_str(r["Aula"])), []).append((s,e))
    for k in busy_by_room: busy_by_room[k] = sorted(busy_by_room[k])

    def overlaps_list(win: Tuple[datetime,datetime], intervals: List[Tuple[datetime,datetime]]) -> int:
        s, e = win
        cnt = 0
        for a,b in intervals:
            if max(s,a) < min(e,b): cnt += 1
        return cnt

    def gap_cost(win: Tuple[datetime,datetime], intervals: List[Tuple[datetime,datetime]]) -> float:
        # penaliza quedar "pegado" (menos de 30 min) a otra sesión
        s, e = win
        mins = []
        for a,b in intervals:
            if b <= s: mins.append((s - b).total_seconds()/60.0)
            elif a >= e: mins.append((a - e).total_seconds()/60.0)
        if not mins: return 0.0
        m = min(mins)
        return 0.0 if m >= 30 else (30 - m)  # 0 si hay buen respiro, >0 si queda pegado

    if st.button("Calcular propuestas"):
        proposals = []
        day = fmin
        while day <= fmax:
            if day.weekday() <= 4:  # Lun–Vie
                for aula in aulas_sel:
                    room_key = _norm(aula)
                    # búsqueda entre 08:00 y 18:00
                    t = time(8,0)
                    while t < time(18,0):
                        start = datetime(day.year, day.month, day.day, t.hour, t.minute, tzinfo=TZ_DEFAULT)
                        end   = start + timedelta(minutes=dur_min)
                        win   = (start, end)
                        # conflictos por aula
                        c_room = overlaps_list(win, busy_by_room.get(room_key, []))
                        # conflictos por personas
                        c_people = 0
                        pegado = 0.0
                        for p in personas:
                            iv = busy_by_person.get(_norm(p), [])
                            c_people += overlaps_list(win, iv)
                            pegado   += gap_cost(win, iv)
                        cost = (10*c_room) + (5*c_people) + (0.1*pegado)
                        if c_room==0 and c_people==0:
                            proposals.append({"Fecha": day.isoformat(), "Inicio": start.strftime("%H:%M"), "Fin": end.strftime("%H:%M"), "Aula": aula, "Conflictos": c_room+c_people, "Pegado(min)": round(pegado,1), "Costo": round(cost,2)})
                        t = (datetime.combine(day, t) + timedelta(minutes=paso)).time()
            day += timedelta(days=1)
        if not proposals:
            st.warning("No encontré opciones sin conflictos en el rango. Amplía el rango o reduce restricciones.")
        else:
            dfp = pd.DataFrame(proposals).sort_values(by=["Costo","Fecha","Inicio"]).head(5)
            st.dataframe(dfp, use_container_width=True, hide_index=True)
            # ICS sugerencia
            if not dfp.empty:
                dummy = pd.DataFrame([{
                    "Nombre de la mesa": nombre or "Propuesta",
                    "_fecha": date.fromisoformat(dfp.iloc[0]["Fecha"]),
                    "_ini": _to_time(dfp.iloc[0]["Inicio"]),
                    "_fin": _to_time(dfp.iloc[0]["Fin"]),
                    "Aula": dfp.iloc[0]["Aula"]
                }])
                st.download_button("⬇️ ICS (mejor opción)", data=build_ics(dummy, calendar_name="Propuesta"), mime="text/calendar", file_name="propuesta.ics")

# ---------------- Diagnóstico ----------------
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
        if n_dups: issues.append(f"{n_dups} duplicados por {KEY_COLS}.")
    if not issues: st.success("Sin problemas críticos detectados. ✅")
    else:
        for it in issues: st.error("• " + it)

# ---------------- Acerca de ----------------
else:
    st.subheader("ℹ️ Acerca de")
    st.markdown("Publicación: 13/09/2025 — INIMAGINABLE+ (Resumen con gráficos • Vistas guardadas • Calidad • Diff • Recomendador)")

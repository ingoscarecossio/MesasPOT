# -*- coding: utf-8 -*-
"""
Mesas · INIMAGINABLE — versión auditada/optimizada
- Omnibox + deep-link
- Perfiles (admin/coord/lectura), modo auditoría
- Delegaciones 2.0 (por día/rango) + aliases opcionales (hoja "Aliases")
- Conflictos con sweep-line O(n log n) y brecha persistente en URL
- Disponibilidad personas/aulas
- Export CSV/Excel/ICS/ZIP + PDF (o HTML imprimible)
- Diagnóstico con auditoría estricta y empaquetado embebido
- ICS con escape correcto y folding RFC 5545 + UID determinístico
- **Ajustes pedidos**: sin domingo en gráficas/controles; solo septiembre y octubre; fechas AAAA-MM-DD
"""
import io, re, uuid, zipfile, base64, unicodedata, difflib, os, inspect, sys, json, hashlib
from datetime import datetime, date, time, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

# ========= Embebidos opcionales =========
_EMBED_XLSX_B64 = ""        # STREAMLIT.xlsx (hoja “Calendario” o primera)
_EMBED_DELEG_B64 = ""       # DELEGACIONES.xlsx (primera hoja)
_BG_B64 = ""                # Fondo opcional (base64)
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

# ========= Helpers =========
def _safe_str(x): return "" if pd.isna(x) else str(x).strip()

def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii','ignore').decode('ascii')
    s = re.sub(r"\s+"," ", s)
    return s.lower().strip()

def _strip_delegate_marker(s: str) -> str:
    if not s: return s
    return _DELEG_MARKER_REGEX.sub("", s).strip()

def _has_delegate_marker(s: str) -> bool:
    return bool(s) and bool(_DELEG_MARKER_REGEX.search(s))

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

def combine_dt(fecha, hora, tz: Optional[timezone]=None):
    tz = tz or st.session_state.get("tz", TZ_DEFAULT)
    d = fecha if isinstance(fecha, date) and not isinstance(fecha, datetime) else _to_date(fecha)
    t = hora if isinstance(hora, time) else _to_time(hora)
    if d is None or t is None: return None
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second or 0, tzinfo=tz)

def ensure_sorted(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "Fecha" in df.columns and "Inicio" in df.columns:
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
st.caption("Omnibox • Perfiles • Delegaciones 2.0 • Conflictos sweep-line • Disponibilidad • Exportes completos")

# ========= Perfiles =========
PROFILE = st.query_params.get("profile","lectura").lower()
IS_ADMIN = PROFILE == "admin"
IS_COORD = PROFILE == "coord"
READONLY = PROFILE == "lectura"
st.markdown(f"<div class='small'>Perfil activo: <b>{PROFILE}</b> {'💎' if IS_ADMIN else '🧭' if IS_COORD else '🔒'}</div>", unsafe_allow_html=True)

# ========= Lectura principal =========
raw = _try_load_main(_EMBED_XLSX_B64)
df0 = normalize_cols(raw)

# Aliases opcionales (hoja "Aliases")
aliases_map: Dict[str, Dict[str,str]] = {}
try:
    def _read_aliases():
        if _EMBED_XLSX_B64:
            rawb = base64.b64decode(_EMBED_XLSX_B64.encode("utf-8"))
            xls = pd.ExcelFile(io.BytesIO(rawb))
        else:
            for path in REPO_CAND_MAIN:
                if os.path.exists(path):
                    xls = pd.ExcelFile(path); break
        return xls.parse("Aliases") if "Aliases" in xls.sheet_names else pd.DataFrame()
    _al = _read_aliases()
    if not _al.empty:
        def _mkmap(df, cold, coln):
            m = {}
            if cold in df and coln in df:
                for a,b in zip(df[cold], df[coln]):
                    a2, b2 = _norm(a), str(b)
                    if a2 and b2: m[a2] = b2
            return m
        aliases_map["person"] = _mkmap(_al, "Person_old", "Person_new")
        aliases_map["aula"]   = _mkmap(_al, "Aula_old", "Aula_new")
except Exception:
    aliases_map = {}

def _apply_alias(s: str, kind: str) -> str:
    n = _norm(s)
    return aliases_map.get(kind, {}).get(n, n)

# Limpiar marcadores delegación y flags
def add_delegate_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["Participantes", "Responsable", "Corresponsable"]:
        if col in df.columns:
            df[f"__flag_{col}"] = df[col].fillna("").astype(str).apply(_has_delegate_marker)
            df[col] = df[col].fillna("").astype(str).apply(_strip_delegate_marker)
    flags = [c for c in df.columns if c.startswith("__flag_")]
    df["Requiere Delegación"] = df[flags].any(axis=1) if flags else False
    df.drop(columns=flags, inplace=True, errors="ignore")
    return df

df0 = add_delegate_flags(df0)

# Precalcular para performance
df0["_fecha"] = df0["Fecha"].apply(_to_date)
df0["_ini"]   = df0["Inicio"].apply(_to_time)
df0["_fin"]   = df0["Fin"].apply(_to_time)

for col in ["Participantes","Responsable","Corresponsable","Aula","Nombre de la mesa","Mesa"]:
    if col in df0.columns: df0[f"__norm_{col}"] = df0[col].fillna("").astype(str).apply(_norm)
if "Aula" in df0.columns:
    df0["__norm_Aula"] = df0["Aula"].fillna("").astype(str).map(lambda x:_apply_alias(x,"aula"))
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

idx = build_index(df0)
for col in ["Responsable","Corresponsable","Aula","Nombre de la mesa","Participantes","Mesa"]:
    if col in idx.columns: idx[f"__norm_{col}"] = idx[col].fillna("").astype(str).apply(_norm)
if "Aula" in idx.columns:
    idx["__norm_Aula"] = idx["Aula"].fillna("").astype(str).map(lambda x:_apply_alias(x,"aula"))
else:
    idx["__norm_Aula"] = ""
idx["__norm_part"] = idx["Participante_individual"].fillna("").astype(str).apply(lambda s:_apply_alias(s,"person"))

# ========= Delegaciones 2.0 =========
deleg_raw = _try_load_deleg(_EMBED_DELEG_B64)
def _prepare_deleg_map(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["__actor","__mesa","__fecha","__ini","__fin"])
    col_actor = col_mesa = col_fecha = col_ini = col_fin = None
    for c in df.columns:
        cl = str(c).lower()
        if col_actor is None and ("actor" in cl or "persona" in cl or "nombre" in cl): col_actor = c
        if col_mesa  is None and "mesa" in cl:  col_mesa = c
        if col_fecha is None and "fecha" in cl: col_fecha = c
        if col_ini   is None and ("inicio" in cl or "hora inicio" in cl): col_ini = c
        if col_fin   is None and ("fin" in cl or "hora fin" in cl): col_fin = c
    if col_actor is None or col_mesa is None or col_fecha is None:
        return pd.DataFrame(columns=["__actor","__mesa","__fecha","__ini","__fin"])
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
    out = pd.DataFrame({
        "__actor": df[col_actor].astype(str).map(lambda s:_apply_alias(s,"person")),
        "__mesa":  df[col_mesa].astype(str).map(_norm),
        "__fecha": pd.to_datetime(df[col_fecha], errors="coerce").dt.date,
        "__ini": df[col_ini].map(_to_t) if col_ini in df.columns else None,
        "__fin": df[col_fin].map(_to_t) if col_fin in df.columns else None
    }).dropna(subset=["__mesa","__fecha"])
    out = out[out["__actor"].astype(bool)]
    return out
deleg_map = _prepare_deleg_map(deleg_raw)

def _token_subset(a: str, b: str) -> bool:
    sa, sb = set(a.split()), set(b.split())
    if not sa or not sb: return False
    return sa.issubset(sb) if len(sa) <= len(sb) else sb.issubset(sa)

def annotate_delegations(idxf: pd.DataFrame, dmap: pd.DataFrame) -> pd.DataFrame:
    idxf = idxf.copy()
    if dmap.empty or "Mesa" not in idxf.columns:
        idxf["__delegado_por_archivo"] = False
        return idxf
    groups: Dict[Tuple[str, date], List[Tuple[str, Optional[time], Optional[time]]]] = {}
    for _, r in dmap.iterrows():
        key = (r["__mesa"], r["__fecha"])
        groups.setdefault(key, []).append((r["__actor"], r.get("__ini"), r.get("__fin")))
    flags = []
    for _, r in idxf.iterrows():
        key = (r.get("__norm_Mesa",""), r.get("_fecha"))
        candidates = groups.get(key, [])
        actor = r.get("__norm_part","")
        ini_r, fin_r = r.get("_ini"), r.get("_fin")
        ok_any = False
        for cand_actor, ini_d, fin_d in candidates:
            name_ok = (actor == cand_actor) or _token_subset(actor, cand_actor)
            if not name_ok: continue
            if ini_d and fin_d and ini_r and fin_r:
                ok_any |= (max(ini_r, ini_d) < min(fin_r, fin_d))
            else:
                ok_any |= True
        flags.append(ok_any)
    idxf["__delegado_por_archivo"] = flags
    return idxf

idx = annotate_delegations(idx, deleg_map)

def mark_event_delegations(df: pd.DataFrame, idxf: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if idxf.empty:
        df["Delegado por lista"] = False
        return df
    delegated_events = set((r["__norm_Mesa"], r["_fecha"]) for _, r in idxf[idxf["__delegado_por_archivo"]==True].iterrows())
    df["Delegado por lista"] = [((row.get("__norm_Mesa",""), row.get("_fecha")) in delegated_events) for _, row in df.iterrows()]
    df["Requiere Delegación"] = df.get("Requiere Delegación", False) | df["Delegado por lista"]
    return df

df0 = mark_event_delegations(df0, idx)

# ========= Fuzzy =========
def _score(a: str, b: str) -> float:
    try:
        from rapidfuzz.fuzz import ratio  # opcional
        return float(ratio(_norm(a), _norm(b)))
    except Exception:
        return 100.0 * difflib.SequenceMatcher(None, _norm(a), _norm(b)).ratio()
def smart_match(series_norm: pd.Series, query: str, threshold: int = 80):
    q = _norm(query)
    if not q: return pd.Series([True]*len(series_norm), index=series_norm.index)
    return series_norm.apply(lambda s: _score(s, q) >= threshold)

# ========= ICS seguro (escape + folding + UID determinístico) =========
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
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Planificador de Mesas//ES",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        _fold_ical_line(f"X-WR-CALNAME:{escape_text(calendar_name)}"),
        "X-WR-TIMEZONE:America/Bogota",
    ]
    for _, r in rows.iterrows():
        f = combine_dt(r.get("_fecha", r.get("Fecha")), r.get("_ini", r.get("Inicio")))
        t = combine_dt(r.get("_fecha", r.get("Fecha")), r.get("_fin", r.get("Fin")))
        if f is None or t is None: continue
        nombre_mesa = _safe_str(r.get("Nombre de la mesa"))
        aula = _safe_str(r.get("Aula"))
        responsable = _safe_str(r.get("Responsable"))
        corresponsable = _safe_str(r.get("Corresponsable"))
        participantes = _safe_str(r.get("Participantes"))
        deleg = "Sí" if r.get("Requiere Delegación") else "No"
        # UID determinístico
        raw_uid = f"{nombre_mesa}|{_to_date(r.get('Fecha'))}|{_to_time(r.get('Inicio'))}|{aula}"
        uid = hashlib.sha1(raw_uid.encode("utf-8")).hexdigest() + "@mesas.local"
        summary = f"[DELEGAR] {nombre_mesa} — {aula}" if r.get("Requiere Delegación") else (f"{nombre_mesa} — {aula}" if aula else nombre_mesa)
        desc = "\n".join([
            f"Mesa: {nombre_mesa}",
            f"Aula: {aula}",
            f"Responsable: {responsable}",
            f"Corresponsable: {corresponsable}",
            f"Participantes: {participantes}",
            f"Requiere delegación: {deleg}"
        ])
        props = [
            ("UID", uid),
            ("DTSTAMP", now_utc),
            ("DTSTART", dt_ics_utc(f)),
            ("DTEND", dt_ics_utc(t)),
            ("SUMMARY", escape_text(summary)),
            ("LOCATION", escape_text(aula)),
            ("DESCRIPTION", escape_text(desc)),
        ]
        lines.append("BEGIN:VEVENT")
        for k, v in props:
            if v is None: continue
            lines.append(_fold_ical_line(f"{k}:{v}"))
        lines.append("END:VEVENT")
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

def _has_reportlab():
    try:
        import reportlab  # type: ignore
        return True
    except Exception:
        return False

def export_pdf_simple(title: str, rows: pd.DataFrame, filename: str="reporte.pdf"):
    if rows is None or rows.empty:
        st.info("No hay datos para exportar."); return
    if not _has_reportlab():
        html = "<h2>"+title+"</h2>" + rows.to_html(index=False)
        st.download_button("⬇️ HTML (imprimir a PDF)", data=html.encode("utf-8"), mime="text/html", file_name=filename.replace(".pdf",".html"))
        st.info("Instala 'reportlab' para PDF nativo: `pip install reportlab`"); return
    from reportlab.lib.pagesizes import A4  # type: ignore
    from reportlab.pdfgen import canvas    # type: ignore
    from reportlab.lib.units import cm     # type: ignore
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    W, H = A4
    y = H - 2*cm
    c.setFont("Helvetica-Bold", 14); c.drawString(2*cm, y, title); y -= 1*cm
    c.setFont("Helvetica", 8)
    cols = list(rows.columns); colw = (W-4*cm)/max(len(cols),1)
    c.setFont("Helvetica-Bold", 8)
    for i, h in enumerate(cols): c.drawString(2*cm+i*colw, y, str(h)[:22])
    y -= 0.6*cm; c.setFont("Helvetica", 8)
    for _, r in rows.iterrows():
        for i, k in enumerate(cols):
            c.drawString(2*cm+i*colw, y, str(r.get(k,""))[:22])
        y -= 0.48*cm
        if y < 2*cm:
            c.showPage(); y = H - 2*cm; c.setFont("Helvetica", 8)
            c.setFont("Helvetica-Bold", 8)
            for i, h in enumerate(cols): c.drawString(2*cm+i*colw, y, str(h)[:22])
            y -= 0.6*cm; c.setFont("Helvetica", 8)
    c.save(); buf.seek(0)
    st.download_button("⬇️ PDF", data=buf.getvalue(), mime="application/pdf", file_name=filename)

# ========= Omnibox =========
c_omni = st.text_input("🔎 Búsqueda rápida (persona / mesa / aula)", value=st.query_params.get("q",""))
if c_omni:
    qn = _norm(c_omni)
    aulas_known = sorted(df0["Aula"].dropna().astype(str).unique().tolist())
    if any(_norm(a)==qn for a in aulas_known):
        st.query_params["sec"] = "Consulta"
        st.query_params["aulas"] = json.dumps([next(a for a in aulas_known if _norm(a)==qn)])
    else:
        st.query_params["sec"] = "Consulta"
        st.query_params["q"] = c_omni
    st.rerun()

st.divider()

# ========= FILTRO CENTRAL: SOLO SEPTIEMBRE Y OCTUBRE =========
def _only_sep_oct(d: Optional[date]) -> bool:
    return (d is not None) and (d.month in (9, 10))

# Vista principal restringida a septiembre y octubre
DF = df0[df0["_fecha"].apply(_only_sep_oct)].copy()
IDX = idx[idx["_fecha"].apply(_only_sep_oct)].copy()

# ========= Secciones =========
if section == "Resumen":
    st.subheader("📈 Resumen ejecutivo (solo septiembre y octubre)")
    def make_stats(df):
        n_mesas = df.shape[0]
        aulas = df["Aula"].dropna().astype(str).nunique() if "Aula" in df else 0
        dias = df["_fecha"].dropna().nunique() if "_fecha" in df else 0
        personas = set()
        personas.update(df["Responsable"].dropna().astype(str).tolist())
        personas.update(df["Corresponsable"].dropna().astype(str).tolist())
        for v in df["Participantes"].fillna("").astype(str).tolist(): personas.update(_split_people(v))
        n_personas = len({x.strip() for x in personas if x and x.strip()})
        return n_mesas, aulas, dias, n_personas
    tm, na, nd, np = make_stats(DF)
    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(f"<div class='card'><div class='kpi'>Mesas</div><span class='value'>{tm}</span></div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='card'><div class='kpi'>Aulas</div><span class='value'>{na}</span></div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='card'><div class='kpi'>Días</div><span class='value'>{nd}</span></div>", unsafe_allow_html=True)
    with c4: st.markdown(f"<div class='card'><div class='kpi'>Personas únicas</div><span class='value'>{np}</span></div>", unsafe_allow_html=True)

    all_people = []
    for v in DF["Participantes"].fillna("").astype(str).tolist(): all_people += _split_people(v)
    all_people += DF["Responsable"].dropna().astype(str).tolist()
    all_people += DF["Corresponsable"].dropna().astype(str).tolist()
    s = pd.Series([p.strip() for p in all_people if p and str(p).strip()])
    top_people = s.value_counts().head(10).rename_axis("Persona").reset_index(name="Conteo")
    uso_aula = DF.groupby("Aula")["Mesa"].count().sort_values(ascending=False).head(10).rename_axis("Aula").reset_index(name="Mesas")
    c1, c2 = st.columns(2)
    with c1: st.markdown("**Top 10 personas por participación**"); st.plotly_chart(px.bar(top_people, x="Conteo", y="Persona", orientation="h", height=400), use_container_width=True)
    with c2: st.markdown("**Aulas más usadas (Top 10)**"); st.plotly_chart(px.bar(uso_aula, x="Mesas", y="Aula", orientation="h", height=400), use_container_width=True)

    # Distribución por día de semana (sin domingo)
    dfh = DF.dropna(subset=["_fecha"]).copy()
    dfh["Día semana"] = dfh["_fecha"].apply(lambda d: ["Lun","Mar","Mié","Jue","Vie","Sáb"][d.weekday()] if d.weekday() < 6 else None)
    dfh = dfh.dropna(subset=["Día semana"])
    by_dow = dfh.groupby("Día semana")["Mesa"].count().reindex(["Lun","Mar","Mié","Jue","Vie","Sáb"]).fillna(0).reset_index(name="Mesas")
    c3, c4 = st.columns(2)
    with c3: st.markdown("**Mesas por día de la semana (sin domingo)**"); st.plotly_chart(px.bar(by_dow, x="Día semana", y="Mesas", height=300), use_container_width=True)
    with c4:
        st.markdown("**Horas de inicio (histograma)**")
        hh = [t.hour for t in DF["_ini"] if t is not None]
        st.plotly_chart(px.histogram(pd.DataFrame({"Hora": hh}), x="Hora", nbins=12, height=300), use_container_width=True)

    try:
        _cols = [c for c in ["Mesa","Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable"] if c in DF.columns]
        # En tablas: fecha AAAA-MM-DD
        tmp = DF[_cols].head(50).copy()
        if "Fecha" in tmp: tmp["Fecha"] = DF["_fecha"].head(50).apply(lambda d: d.isoformat() if d else "")
        export_pdf_simple("Resumen de Mesas (Sep-Oct)", tmp, "resumen_mesas.pdf")
    except Exception: pass

    st.markdown("### ⬇️ Exportadores rápidos")
    people = sorted({p for p in set(IDX["Participante_individual"].dropna().astype(str).tolist()
                                    + DF["Responsable"].dropna().astype(str).tolist()
                                    + DF["Corresponsable"].dropna().astype(str).tolist()) if p})
    c1, c2 = st.columns(2)
    with c1:
        psel = st.selectbox("ICS por persona", options=["(ninguna)"]+people, index=0)
        if psel and psel != "(ninguna)":
            m = (smart_match(IDX["__norm_part"], psel, 90) |
                 smart_match(IDX["__norm_Responsable"], psel, 90) |
                 smart_match(IDX["__norm_Corresponsable"], psel, 90))
            rows = IDX.loc[m, ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación","_fecha","_ini","_fin"]]
            rows = ensure_sorted(rows)
            if not rows.empty:
                st.download_button("Descargar ICS (persona)", data=build_ics(rows, calendar_name=f"Agenda — {psel}"),
                                   mime="text/calendar", file_name=f"agenda_{psel}.ics")
    with c2:
        aulas = sorted(DF["Aula"].dropna().astype(str).unique().tolist())
        asel = st.selectbox("ICS por aula", options=["(ninguna)"]+aulas, index=0)
        if asel and asel != "(ninguna)":
            rows = DF[DF["Aula"].astype(str)==asel][["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación","_fecha","_ini","_fin"]]
            rows = ensure_sorted(rows)
            if not rows.empty:
                st.download_button("Descargar ICS (aula)", data=build_ics(rows, calendar_name=f"Aula — {asel}"),
                                   mime="text/calendar", file_name=f"aula_{asel}.ics")

elif section == "Consulta":
    with st.expander("⚙️ Filtros (solo septiembre y octubre)", expanded=False):
        c1, c2, c3 = st.columns(3)
        fechas_validas = [d for d in DF["_fecha"].dropna().tolist()]
        if fechas_validas: dmin, dmax = min(fechas_validas), max(fechas_validas)
        else: today = date.today(); dmin, dmax = today, today
        with c1:
            qp_rng = get_qp("rng", default=None, parse_json=True)
            dr = st.date_input("Rango de fechas", value=(dmin, dmax) if not qp_rng else (date.fromisoformat(qp_rng[0]), date.fromisoformat(qp_rng[1])), min_value=dmin, max_value=dmax)
            fmin, fmax = (dr if isinstance(dr, tuple) and len(dr)==2 else (dmin, dmax))
            horas = st.slider("Rango de horas", 0, 23, (6, 20))
        with c2:
            aulas = sorted(DF["Aula"].dropna().astype(str).unique().tolist())
            aula_sel = st.multiselect("Aulas", ["(todas)"] + aulas, default=get_qp("aulas",["(todas)"],True))
            # SIN DOMINGO en selector
            dow_opts = ["Lun","Mar","Mié","Jue","Vie","Sáb"]
            dow = st.multiselect("Días semana", dow_opts, default=get_qp("dows",["Lun","Mar","Mié","Jue","Vie"],True))
            # Clamp a opciones válidas por si venía "Dom" en URL
            dow = [d for d in dow if d in dow_opts]
        with c3:
            responsables = sorted(DF["Responsable"].dropna().astype(str).unique().tolist())
            rsel = st.multiselect("Responsables", responsables, default=get_qp("resp",[],True))
            solo_deleg = st.checkbox("🔴 Solo mesas que requieren delegación", value=bool(get_qp("sdel","false") in ("true","True","1")))
        set_qp(rng=(fmin.isoformat(), fmax.isoformat()), aulas=aula_sel, dows=dow, resp=rsel, sdel=solo_deleg)

    modo = st.radio("Búsqueda", ["Seleccionar", "Texto"], index=0, horizontal=True)
    people = sorted({p for p in set(IDX["Participante_individual"].dropna().astype(str).tolist()
                                    + DF["Responsable"].dropna().astype(str).tolist()
                                    + DF["Corresponsable"].dropna().astype(str).tolist()) if p})
    term = st.selectbox("Participante", options=[""]+people, index=0) if modo=="Seleccionar" else st.text_input("Escriba parte del nombre", value=st.query_params.get("q",""))
    set_qp(q=term)

    mask = pd.Series([True]*len(IDX))
    mask &= IDX["_fecha"].apply(lambda d: (d is not None) and (fmin <= d <= fmax))
    if aula_sel and not (len(aula_sel)==1 and aula_sel[0]=="(todas)"):
        allowed = set([a for a in aula_sel if a != "(todas)"])
        mask &= IDX["Aula"].fillna("").astype(str).isin(allowed)
    dows = {"Lun":0,"Mar":1,"Mié":2,"Jue":3,"Vie":4,"Sáb":5}  # SIN "Dom"
    selected_dows = [dows[x] for x in dow] if dow else list(dows.values())
    mask &= IDX["_fecha"].apply(lambda dd: dd is not None and dd.weekday() in selected_dows)
    hmin, hmax = horas
    mask &= IDX["_ini"].apply(lambda t: (t is not None) and (hmin <= t.hour <= hmax))
    if rsel:
        mask &= IDX["Responsable"].fillna("").astype(str).isin(set(rsel))
    if solo_deleg:
        mask &= IDX["Requiere Delegación"] == True
    if term:
        mask &= (smart_match(IDX["__norm_part"], term) |
                 smart_match(IDX["__norm_Responsable"], term) |
                 smart_match(IDX["__norm_Corresponsable"], term))

    cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación","_fecha","_ini","_fin"]
    res = IDX.loc[mask, cols].drop_duplicates().reset_index(drop=True)
    res = ensure_sorted(res)

    c1, c2, c3, c4 = st.columns(4)
    def make_stats(df):
        n_mesas = df.shape[0]
        aulas = df["Aula"].dropna().astype(str).nunique() if "Aula" in df else 0
        dias = df["_fecha"].dropna().nunique() if "_fecha" in df else 0
        personas = set()
        personas.update(df["Responsable"].dropna().astype(str).tolist())
        personas.update(df["Corresponsable"].dropna().astype(str).tolist())
        for v in df["Participantes"].fillna("").astype(str).tolist(): personas.update(_split_people(v))
        n_personas = len({x.strip() for x in personas if x and x.strip()})
        return n_mesas, aulas, dias, n_personas
    tm, na, nd, np = make_stats(res if not res.empty else DF)
    with c1: st.markdown(f"<div class='card'><div class='kpi'>Mesas</div><span class='value'>{tm}</span></div>", unsafe_allow_html=True)
    with c2: st.markdown(f"<div class='card'><div class='kpi'>Aulas</div><span class='value'>{na}</span></div>", unsafe_allow_html=True)
    with c3: st.markdown(f"<div class='card'><div class='kpi'>Días</div><span class='value'>{nd}</span></div>", unsafe_allow_html=True)
    with c4: st.markdown(f"<div class='card'><div class='kpi'>Personas únicas</div><span class='value'>{np}</span></div>", unsafe_allow_html=True)

    st.subheader("📋 Resultados (fecha AAAA-MM-DD)")
    if term == "" and res.empty:
        st.info("Empiece escribiendo un nombre o elija uno de la lista.")
    elif res.empty:
        st.warning("Sin resultados.")
    else:
        rf = res.copy()
        rf["Fecha"] = rf["_fecha"].apply(lambda d: d.isoformat() if d else "")
        for c in ["Inicio","Fin"]:
            rf[c] = rf[f"_{'ini' if c=='Inicio' else 'fin'}"].apply(lambda t: t.strftime("%H:%M") if t else "")
        rf.insert(0,"Delegación", rf["Requiere Delegación"].apply(lambda x: "🔴" if bool(x) else "—"))
        ordered = ["Delegación","Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"]
        rf = rf[[c for c in ordered if c in rf.columns]]
        st.dataframe(rf, use_container_width=True, hide_index=True)

        st.markdown("#### ⬇️ Descargas")
        st.download_button("CSV (filtro)", data=rf.to_csv(index=False).encode("utf-8-sig"), mime="text/csv", file_name="resultados.csv")
        xls_buf = io.BytesIO()
        with pd.ExcelWriter(xls_buf, engine="xlsxwriter") as w: rf.to_excel(w, sheet_name="Resultados", index=False)
        st.download_button("Excel (filtro)", data=xls_buf.getvalue(), mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", file_name="resultados.xlsx")
        calname = f"Mesas — {term}" if term else "Mesas"
        st.download_button("ICS (todo en uno)", data=build_ics(res, calendar_name=calname), mime="text/calendar", file_name="mesas.ics")
        st.download_button("ICS por mesa (ZIP)", data=zip_split_ics(res), mime="application/zip", file_name="mesas_por_mesa.zip")
        try: export_pdf_simple("Consulta — Resultados (Sep-Oct)", rf, "consulta_resultados.pdf")
        except Exception: pass

elif section == "Agenda":
    st.subheader("🗓️ Agenda por persona (Sep-Oct)")
    people = sorted({p for p in set(IDX["Participante_individual"].dropna().astype(str).tolist()
                                    + DF["Responsable"].dropna().astype(str).tolist()
                                    + DF["Corresponsable"].dropna().astype(str).tolist()) if p})
    persona = st.selectbox("Seleccione persona", options=people)
    if persona:
        m = (smart_match(IDX["__norm_part"], persona, 90) |
             smart_match(IDX["__norm_Responsable"], persona, 90) |
             smart_match(IDX["__norm_Corresponsable"], persona, 90))
        rows = IDX.loc[m, ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación","_fecha","_ini","_fin"]].copy()
        rows = ensure_sorted(rows)
        if rows.empty:
            st.info("Sin eventos para esta persona.")
        else:
            for _, r in rows.iterrows():
                s_ini = r["_ini"].strftime('%H:%M') if r["_ini"] else ""
                s_fin = r["_fin"].strftime('%H:%M') if r["_fin"] else ""
                st.markdown(f"**{_safe_str(r['Nombre de la mesa'])}**  \n{r['_fecha'].isoformat() if r['_fecha'] else ''} • {s_ini}–{s_fin} • Aula: {_safe_str(r['Aula'])}  \n<span class='small'>Resp.: {_safe_str(r['Responsable'])} • Co-resp.: {_safe_str(r['Corresponsable'])} • Delegación: {'Sí' if r['Requiere Delegación'] else 'No'}</span>", unsafe_allow_html=True)
                st.divider()
            st.download_button("⬇️ ICS (Agenda)", data=build_ics(rows, calendar_name=f"Agenda — {persona}"), mime="text/calendar", file_name=f"agenda_{persona}.ics")
            gantt_rows = []
            for _, r in rows.iterrows():
                start = combine_dt(r["_fecha"], r["_ini"]); end = combine_dt(r["_fecha"], r["_fin"])
                if not (start and end): continue
                gantt_rows.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "Aula": _safe_str(r["Aula"]), "start": start, "end": end, "Delegación": "Sí" if r["Requiere Delegación"] else "No"})
            if gantt_rows:
                dfg = pd.DataFrame(gantt_rows)
                fig = px.timeline(dfg, x_start="start", x_end="end", y="Aula", color="Delegación", hover_data=["Mesa"])
                fig.update_yaxes(autorange="reversed")
                fig.update_layout(height=420, margin=dict(l=10,r=10,t=30,b=20))
                st.plotly_chart(fig, use_container_width=True)

elif section == "Gantt":
    st.subheader("📊 Gantt (señala delegaciones) — Sep-Oct")
    rows = []
    for _, r in DF.iterrows():
        start = combine_dt(r["_fecha"], r["_ini"]); end = combine_dt(r["_fecha"], r["_fin"])
        if not (start and end): continue
        rows.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "Aula": _safe_str(r["Aula"]), "start": start, "end": end, "Delegación": "Sí" if r["Requiere Delegación"] else "No"})
    if not rows: st.info("No hay datos para Gantt.")
    else:
        dfg = pd.DataFrame(rows)
        fig = px.timeline(dfg, x_start="start", x_end="end", y="Aula", color="Delegación", hover_data=["Mesa"])
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(height=550, margin=dict(l=10,r=10,t=30,b=20))
        st.plotly_chart(fig, use_container_width=True)

elif section == "Heatmap":
    st.subheader("🗺️ Heatmap de ocupación (Aula x Día) — Sep-Oct")
    dfh = DF.copy()
    piv = pd.pivot_table(dfh, index="Aula", columns="_fecha", values="Mesa", aggfunc="count", fill_value=0)
    if piv.empty: st.info("No hay datos para el heatmap.")
    else:
        fig = px.imshow(piv, aspect="auto", labels=dict(color="Mesas"))
        fig.update_layout(height=500, margin=dict(l=10,r=10,t=30,b=20))
        st.plotly_chart(fig, use_container_width=True)

elif section == "Conflictos":
    st.subheader("🚦 Solapes (personas / aulas) — Sweep line (Sep-Oct)")
    c1, c2, c3 = st.columns(3)
    with c1: scope = st.radio("Ámbito", ["Personas","Aulas"], horizontal=True)
    # Persistencia en URL
    apply_qp = st.query_params.get("applydel", "true")
    gap_qp = st.query_params.get("gap", "10")
    with c2:
        if READONLY:
            st.caption("Perfil lectura: las delegaciones se respetan siempre.")
            aplicar_deleg = True
        else:
            aplicar_deleg = st.checkbox("Aplicar delegaciones (ignorar eventos delegados)", value=(apply_qp.lower() in ("true","1","yes")))
    with c3:
        try: gap_default = int(gap_qp)
        except Exception: gap_default = 10
        brecha = st.slider("Brecha mínima (min)", 0, 60, gap_default)
    set_qp(applydel=aplicar_deleg, gap=brecha)

    def find_overlaps_sweepline(events: List[Dict], gap_min=0):
        if not events: return []
        evs = sorted(events, key=lambda e: (e["start"], e["end"]))
        out = []
        active = []
        for e in evs:
            # purgar activos que ya no chocan
            active = [a for a in active if (a["end"] + timedelta(minutes=gap_min)) > e["start"]]
            for a in active:
                if (a["end"] + timedelta(minutes=gap_min)) > e["start"]:
                    out.append((a, e))
            active.append(e)
        return out

    dfc = pd.DataFrame()
    if scope == "Personas":
        people = sorted({p for p in set(IDX["Participante_individual"].dropna().astype(str).tolist()
                                        + DF["Responsable"].dropna().astype(str).tolist()
                                        + DF["Corresponsable"].dropna().astype(str).tolist()) if p})
        psel = st.multiselect("Personas a auditar", options=people)
        if not psel: st.info("Seleccione una o más personas.")
        else:
            conf_rows = []
            for person in psel:
                m = (smart_match(IDX["__norm_part"], person, 90) |
                     smart_match(IDX["__norm_Responsable"], person, 90) |
                     smart_match(IDX["__norm_Corresponsable"], person, 90))
                sel = IDX.loc[m, ["Nombre de la mesa","Aula","_fecha","_ini","_fin","__delegado_por_archivo"]].copy()
                if aplicar_deleg: sel = sel[sel["__delegado_por_archivo"] == False]
                evs = []
                for _, r in sel.iterrows():
                    s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
                    if s and e: evs.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "Aula": _safe_str(r["Aula"]), "start": s, "end": e})
                for a,b in find_overlaps_sweepline(evs, gap_min=brecha):
                    if a["start"].date() == b["start"].date():
                        # No hace falta excluir domingo: al estar filtrado DF/IDX a Sep-Oct y sin domingo en filtros, no aparecerá
                        conf_rows.append({
                            "Persona": person,
                            "Mesa A": a["Mesa"], "Aula A": a["Aula"], "Inicio A": a["start"], "Fin A": a["end"],
                            "Mesa B": b["Mesa"], "Aula B": b["Aula"], "Inicio B": b["start"], "Fin B": b["end"],
                        })
            dfc = pd.DataFrame(conf_rows)
    else:
        aulas = sorted(DF["Aula"].dropna().astype(str).unique().tolist())
        asel = st.multiselect("Aulas a auditar", options=aulas)
        if not asel: st.info("Seleccione una o más aulas.")
        else:
            conf_rows = []
            for aula in asel:
                sel = DF[DF["Aula"].astype(str)==aula][["_fecha","_ini","_fin","Nombre de la mesa"]].copy()
                evs = []
                for _, r in sel.iterrows():
                    s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
                    if s and e: evs.append({"Mesa": _safe_str(r["Nombre de la mesa"]), "start": s, "end": e})
                for a,b in find_overlaps_sweepline(evs, gap_min=brecha):
                    if a["start"].date() == b["start"].date():
                        conf_rows.append({
                            "Aula": aula,
                            "Mesa A": a["Mesa"], "Inicio A": a["start"], "Fin A": a["end"],
                            "Mesa B": b["Mesa"], "Inicio B": b["start"], "Fin B": b["end"],
                        })
            dfc = pd.DataFrame(conf_rows)

    if dfc.empty:
        st.success("Sin solapes detectados. ✅")
    else:
        st.dataframe(dfc, use_container_width=True, hide_index=True)
        try:
            if "Persona" in dfc.columns:
                who = sorted(dfc["Persona"].unique().tolist())[0]
                focus = []
                for _, r in dfc.iterrows():
                    focus += [
                        {"Etiqueta":"A", "Nombre": r["Mesa A"], "start": r["Inicio A"], "end": r["Fin A"]},
                        {"Etiqueta":"B", "Nombre": r["Mesa B"], "start": r["Inicio B"], "end": r["Fin B"]},
                    ]
                dff = pd.DataFrame(focus)
                fig = px.timeline(dff, x_start="start", x_end="end", y="Etiqueta", color="Nombre", hover_data=["Nombre"])
                fig.update_yaxes(autorange="reversed")
                fig.update_layout(height=320, margin=dict(l=10,r=10,t=10,b=10), title=f"Solapes visuales — {who}")
                st.plotly_chart(fig, use_container_width=True)
            elif "Aula" in dfc.columns:
                aula = sorted(dfc["Aula"].unique().tolist())[0]
                focus = []
                for _, r in dfc.iterrows():
                    focus += [
                        {"Etiqueta":"A", "Nombre": r["Mesa A"], "start": r["Inicio A"], "end": r["Fin A"]},
                        {"Etiqueta":"B", "Nombre": r["Mesa B"], "start": r["Inicio B"], "end": r["Fin B"]},
                    ]
                dff = pd.DataFrame(focus)
                fig = px.timeline(dff, x_start="start", x_end="end", y="Etiqueta", color="Nombre", hover_data=["Nombre"])
                fig.update_yaxes(autorange="reversed")
                fig.update_layout(height=320, margin=dict(l=10,r=10,t=10,b=10), title=f"Solapes visuales — Aula {aula}")
                st.plotly_chart(fig, use_container_width=True)
        except Exception:
            pass
        try: export_pdf_simple("Reporte de Conflictos (Sep-Oct)", dfc, "conflictos.pdf")
        except Exception: pass

elif section == "Disponibilidad":
    st.subheader("🟢 Buscador de disponibilidad (personas / aulas) — Sep-Oct")
    c1, c2, c3 = st.columns(3)
    with c1: mode = st.radio("Modo", ["Personas","Aulas"], horizontal=True)
    with c2: ventana = st.slider("Duración mínima (min)", 15, 240, 60, 15)
    with c3: margen = st.slider("Margen (min)", 0, 60, 10, 5, help="Colchón a cada lado de los eventos ocupados.")
    fechas_validas = [d for d in DF["_fecha"].dropna().tolist()]
    if fechas_validas: dmin, dmax = min(fechas_validas), max(fechas_validas)
    else: today = date.today(); dmin, dmax = today, today
    dr = st.date_input("Rango de fechas", value=(dmin, dmax), min_value=dmin, max_value=dmax)

    def _slots_free(events: List[Tuple[datetime, datetime]], day: date):
        tz = st.session_state.get("tz", TZ_DEFAULT)
        start_day = datetime(day.year, day.month, day.day, 6, 0, tzinfo=tz)
        end_day   = datetime(day.year, day.month, day.day, 22,0, tzinfo=tz)
        evs = sorted(events, key=lambda x: x[0])
        free = []; cur = start_day
        for s,e in evs:
            s2 = s - timedelta(minutes=margen)
            if s2 > cur and (s2 - cur).total_seconds()/60 >= ventana:
                free.append((cur, s2))
            cur = max(cur, e + timedelta(minutes=margen))
        if cur < end_day and (end_day - cur).total_seconds()/60 >= ventana:
            free.append((cur, end_day))
        return free

    if isinstance(dr, tuple) and len(dr)==2:
        fmin, fmax = dr
        rows_out = []
        if mode == "Personas":
            people = sorted({p for p in set(IDX["Participante_individual"].dropna().astype(str).tolist()
                                            + DF["Responsable"].dropna().astype(str).tolist()
                                            + DF["Corresponsable"].dropna().astype(str).tolist()) if p})
            sel = st.multiselect("Personas", options=people)
            for person in sel:
                m = (smart_match(IDX["__norm_part"], person, 90) |
                     smart_match(IDX["__norm_Responsable"], person, 90) |
                     smart_match(IDX["__norm_Corresponsable"], person, 90))
                sel_idx = IDX.loc[m, ["_fecha","_ini","_fin"]]
                by_day: Dict[date, List[Tuple[datetime,datetime]]] = {}
                for _, r in sel_idx.iterrows():
                    d0 = r["_fecha"]; s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
                    if not (d0 and s and e): continue
                    if not (fmin <= d0 <= fmax): continue
                    by_day.setdefault(d0, []).append((s,e))
                for d0, events in by_day.items():
                    # Excluir domingo por si acaso
                    if d0.weekday() == 6: 
                        continue
                    for s,e in _slots_free(events, d0):
                        rows_out.append({"Tipo":"Persona","Nombre": person, "Día": d0, "Libre desde": s, "Libre hasta": e, "Minutos": int((e-s).total_seconds()/60)})
        else:
            aulas = sorted(DF["Aula"].dropna().astype(str).unique().tolist())
            sel = st.multiselect("Aulas", options=aulas)
            for aula in sel:
                sel_df = DF[DF["Aula"].astype(str)==aula][["_fecha","_ini","_fin"]]
                by_day: Dict[date, List[Tuple[datetime,datetime]]] = {}
                for _, r in sel_df.iterrows():
                    d0 = r["_fecha"]; s = combine_dt(r["_fecha"], r["_ini"]); e = combine_dt(r["_fecha"], r["_fin"])
                    if not (d0 and s and e): continue
                    if not (fmin <= d0 <= fmax): continue
                    by_day.setdefault(d0, []).append((s,e))
                for d0, events in by_day.items():
                    if d0.weekday() == 6: 
                        continue
                    for s,e in _slots_free(events, d0):
                        rows_out.append({"Tipo":"Aula","Nombre": aula, "Día": d0, "Libre desde": s, "Libre hasta": e, "Minutos": int((e-s).total_seconds()/60)})
        if rows_out:
            out = pd.DataFrame(rows_out).sort_values(by=["Nombre","Día","Libre desde"])
            # Fechas AAAA-MM-DD en display
            out["Día"] = out["Día"].apply(lambda d: d.isoformat() if isinstance(d, date) else d)
            st.dataframe(out, use_container_width=True, hide_index=True)
            st.download_button("⬇️ CSV (disponibilidad)", data=out.to_csv(index=False).encode("utf-8-sig"), mime="text/csv", file_name="disponibilidad.csv")
        else:
            st.info("No se encontraron huecos con esos parámetros.")

elif section == "Delegaciones":
    st.subheader("🛟 Reporte de Delegaciones (Sep-Oct)")
    cols = ["Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"]
    rep = DF[DF["Requiere Delegación"]==True][cols].copy()
    if rep.empty: st.info("No hay mesas marcadas con 'Requiere Delegación'.")
    else:
        # Fechas AAAA-MM-DD
        rep["Fecha"] = DF.loc[rep.index, "_fecha"].apply(lambda d: d.isoformat() if d else "")
        st.dataframe(rep, use_container_width=True, hide_index=True)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w: rep.to_excel(w, sheet_name="Delegaciones", index=False)
        st.download_button("⬇️ Delegaciones (Excel)", data=buf.getvalue(), mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", file_name="delegaciones.xlsx")

elif section == "Diagnóstico":
    st.subheader("🧪 Diagnóstico y construcción (Sep-Oct)")
    tz_opt = st.selectbox("Zona horaria ICS", options=["America/Bogota","America/Lima","America/Mexico_City","UTC"], index=0)
    try:
        from zoneinfo import ZoneInfo
        st.session_state.tz = ZoneInfo(tz_opt) if tz_opt!="UTC" else timezone.utc
    except Exception:
        st.session_state.tz = TZ_DEFAULT

    issues = []
    audit_rows = []
    def _err(row, col, msg): audit_rows.append({"Fila": int(row)+2, "Columna": col, "Detalle": msg})
    for i, r in DF.iterrows():
        if r.get("_fecha") is None:
            _err(i,"Fecha", f"Inválida/vacía (valor='{_safe_str(r.get('Fecha'))[:24]}')")
        t1, t2 = r.get("_ini"), r.get("_fin")
        if t1 is None: _err(i,"Inicio", f"Hora inválida/vacía (valor='{_safe_str(r.get('Inicio'))[:24]}')")
        if t2 is None: _err(i,"Fin",    f"Hora inválida/vacía (valor='{_safe_str(r.get('Fin'))[:24]}')")
        if t1 and t2 and (datetime.combine(date(2000,1,1), t2) <= datetime.combine(date(2000,1,1), t1)):
            _err(i,"Fin", f"Fin ≤ Inicio ({t1} -> {t2})")
        if not _safe_str(r.get("Aula")): _err(i,"Aula","Vacía")
    if audit_rows: issues.append("Se hallaron problemas puntuales (ver tabla).")

    if all(k in DF.columns for k in ["Fecha","Inicio","Fin"]):
        def dur(a,b):
            ta, tb = _to_time(a), _to_time(b)
            if ta is None or tb is None: return None
            return (datetime.combine(date(2000,1,1), tb) - datetime.combine(date(2000,1,1), ta)).total_seconds()/60.0
        dur_min = DF.apply(lambda r: dur(r["Inicio"], r["Fin"]), axis=1)
        if (pd.Series(dur_min).dropna() < 0).any():
            issues.append("Existen filas con duraciones negativas (Fin < Inicio).")

    if all(k in DF.columns for k in ["Fecha","Inicio","Aula","Nombre de la mesa"]):
        n_dups = int(DF.duplicated(subset=["Fecha","Inicio","Aula","Nombre de la mesa"], keep=False).sum())
        if n_dups: issues.append(f"{n_dups} duplicados por (Fecha, Inicio, Aula, Nombre).")

    st.markdown("**Resultado**")
    if not issues: st.success("Sin problemas críticos detectados. ✅")
    else:
        for it in issues: st.error("• " + it)
    if audit_rows:
        st.markdown("**Auditoría de filas/columnas**")
        df_aud = pd.DataFrame(audit_rows).head(500)
        # Fechas AAAA-MM-DD si hubiera columna 'Fecha' en auditoría futura
        st.dataframe(df_aud, use_container_width=True, hide_index=True)

    st.markdown("—")
    st.markdown("**Vista rápida (Sep-Oct, AAAA-MM-DD)**")
    cols = [c for c in ["Mesa","Nombre de la mesa","Fecha","Inicio","Fin","Aula","Responsable","Corresponsable","Participantes","Requiere Delegación"] if c in DF.columns]
    quick = DF[cols].head(30).copy()
    if "Fecha" in quick: quick["Fecha"] = DF["_fecha"].head(30).apply(lambda d: d.isoformat() if d else "")
    st.dataframe(quick, use_container_width=True, hide_index=True)

    st.markdown("—")
    st.subheader("🧩 Construir .py embebido y ZIP")
    with st.form("builder_embedded"):
        dev_streamlit_path = st.text_input("Ruta STREAMLIT.xlsx (repo)", REPO_CAND_MAIN[0])
        dev_deleg_path = st.text_input("Ruta DELEGACIONES.xlsx (repo, opcional)", REPO_CAND_DELEG[0])
        submitted = st.form_submit_button("Construir y descargar")
    if submitted:
        def _b64_or_empty(path):
            if path and os.path.exists(path):
                with open(path, "rb") as f: return base64.b64encode(f.read()).decode("utf-8")
            return ""
        b64_main = _b64_or_empty(dev_streamlit_path)
        b64_deleg = _b64_or_empty(dev_deleg_path)
        src = inspect.getsource(sys.modules[__name__])
        src = re.sub(r'_EMBED_XLSX_B64\s*=\s*""', f'_EMBED_XLSX_B64 = """{b64_main}"""', src, count=1)
        src = re.sub(r'_EMBED_DELEG_B64\s*=\s*""', f'_EMBED_DELEG_B64 = """{b64_deleg}"""', src, count=1)
        file_bytes = src.encode("utf-8")
        st.download_button("⬇️ app_embebida.py", data=file_bytes, mime="text/x-python", file_name="app_embebida.py")
        ics_example = build_ics(DF.head(3), calendar_name="Ejemplo")
        mem = io.BytesIO()
        with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("app_embebida.py", file_bytes)
            z.writestr("README_minimo.md", "# Mesas — INIMAGINABLE\nEjecuta: `streamlit run app_embebida.py`.\n")
            z.writestr("ejemplo.ics", ics_example)
        mem.seek(0)
        st.download_button("⬇️ paquete.zip", data=mem.getvalue(), mime="application/zip", file_name="paquete_inimaginable.zip")

    st.markdown("—")
    st.markdown("### 📦 Snapshots")
    def snapshot_state_export():
        data = dict(st.query_params)
        blob = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        st.download_button("⬇️ Snapshot (.json)", data=blob, mime="application/json", file_name="snapshot_mesas.json")
    def snapshot_state_import():
        up = st.file_uploader("Cargar snapshot (.json)", type=["json"], key="snapup")
        if up is not None:
            try:
                js = json.load(up)
                st.query_params.clear()
                for k,v in js.items(): st.query_params[k] = v
                st.success("Snapshot cargado. Refrescando…"); st.rerun()
            except Exception as e:
                st.error(f"No se pudo cargar el snapshot: {e}")
    colA, colB = st.columns(2)
    with colA: snapshot_state_export()
    with colB: snapshot_state_import()

else:
    st.subheader("ℹ️ Acerca de")
    st.markdown("Publicación: 13/09/2025 — INIMAGINABLE (auditada)")
    st.markdown("• ICS robusto (escape + folding + UID determinístico)\n• Solapes O(n log n)\n• Pre-cálculos de fecha/hora\n• Persistencia de estado en URL\n• Delegaciones 2.0 y exportes completos\n• Sin domingo en UI/gráficas y restringido a septiembre/octubre con fechas AAAA-MM-DD\n")

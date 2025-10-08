from __future__ import annotations

# ==================== Imports ====================
import os, re, json, hashlib, unicodedata, difflib
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import httpx
from fastapi import FastAPI, Response, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from dotenv import load_dotenv

# ==================== ENV ====================
load_dotenv()

DROPBOX_APP_KEY       = os.getenv("DROPBOX_APP_KEY", "")
DROPBOX_APP_SECRET    = os.getenv("DROPBOX_APP_SECRET", "")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN", "")

DROPBOX_FILE_PATH     = os.getenv("DROPBOX_FILE_PATH", "")
EXCEL_SHEET           = os.getenv("EXCEL_SHEET", "Produtos utilizados")
EXCEL_HEADER_ROW      = int(os.getenv("EXCEL_HEADER_ROW", "4"))

CATALOGO_PATH         = os.getenv("CATALOGO_PATH", "").strip()
CATALOGO_DROPBOX_PATH = os.getenv("CATALOGO_DROPBOX_PATH", "").strip()

FUZZY_THRESHOLD   = float(os.getenv("FUZZY_THRESHOLD", "0.93"))
MAX_LEN_DIFF      = int(os.getenv("MAX_LEN_DIFF", "2"))
REQUIRE_SAME_INIT = os.getenv("REQUIRE_SAME_INITIAL", "1") == "1"

# ==================== App & Static ====================
app = FastAPI(title="Petrachi — Estoque & Agrofit")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ==================== Utils ====================
def make_etag_from_bytes(*chunks: bytes) -> str:
    md5 = hashlib.md5()
    for c in chunks:
        md5.update(c)
    return md5.hexdigest()

def _normalize_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    for ch in ("-", "_", "/", ".", ","):
        s = s.replace(ch, " ")
    return " ".join(s.split())

FORM_CODES = {"ec","wg","sl","sc","cs","od","wp","gr","dc","hc","pm","ds","fs","se","ew"}
def _simplify_product_name(s: str) -> str:
    """
    Remove tokens de formulação (EC/WG/SL/SC...) e números/concentrações.
    Ex.: "Reglone 200 SL" -> "reglone"
    """
    n = _normalize_name(s)
    if not n:
        return ""
    tokens = [t for t in n.split() if t not in FORM_CODES and not re.fullmatch(r"\d+[.,]?\d*", t)]
    return " ".join(tokens)

def to_dec(s: Any) -> Decimal:
    """
    Converte strings pt-BR/EN para Decimal.
    Ex.: "1.020,00" -> 1020.00 ; "-94,80" -> -94.80 ; "15" -> 15 ; "-15" -> -15
    """
    if s is None:
        return Decimal("0")
    txt = str(s).strip().replace(" ", "")
    if txt == "":
        return Decimal("0")
    if "." in txt and "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    elif "," in txt:
        txt = txt.replace(",", ".")
    m = re.search(r"^[+-]?\d+(?:\.\d+)?", txt)
    if m:
        txt = m.group(0)
    try:
        return Decimal(txt)
    except Exception:
        return Decimal("0")

# ==================== Front ====================
@app.get("/", response_class=HTMLResponse)
def serve_index():
    return FileResponse(str(STATIC_DIR / "index.html"))

@app.get("/health")
def health():
    return {"status": "ok"}

# ==================== Dropbox ====================
async def dropbox_get_access_token() -> str:
    if not (DROPBOX_APP_KEY and DROPBOX_APP_SECRET and DROPBOX_REFRESH_TOKEN):
        raise RuntimeError("Credenciais do Dropbox ausentes no .env")
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://api.dropboxapi.com/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": DROPBOX_REFRESH_TOKEN,
                "client_id": DROPBOX_APP_KEY,
                "client_secret": DROPBOX_APP_SECRET,
            },
        )
        r.raise_for_status()
        return r.json()["access_token"]

async def dropbox_download(path: str) -> bytes:
    token = await dropbox_get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Dropbox-API-Arg": json.dumps({"path": path}),
    }
    url = "https://content.dropboxapi.com/2/files/download"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(url, headers=headers)
        r.raise_for_status()
        return r.content

# ==================== Parse & mapping (movimentações) ====================
def map_row_estoque(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Lê UMA movimentação. A quantidade já vem com sinal (ex.: -15 para saída).
    """
    lower_map = {str(k).strip().lower(): ("" if v is None else str(v).strip()) for k, v in row.items()}

    def get(keys: List[str], default: str = "") -> str:
        for k in keys:
            v = lower_map.get(k.lower())
            if v not in ("", None):
                return v
        return default

    nome = get(["nome_comercial","nome comercial","nome","produto"])
    und  = get(["unidade","un","und","u.m.","u.m"])
    qtd_field = get(["quantidade","qtd","estoque","saldo"], "0")
    qtd = to_dec(qtd_field)  # usa direto o sinal do valor
    data = get(["data","dt","emissão","emissao","data movimento","data_movimento"])

    return {
        "nome": nome,
        "qtd": float(qtd),
        "und": und,
        "ia": get(["ia","ingrediente_ativo","ingrediente ativo","ias"]),
        "pragas": get(["pragas","praga","alvos","pragas_alvo"]),
        "registro": get(["registro","numero_registro","n_registro","nº registro"]),
        "empresa": get(["empresa","titular_registro","registrante","fornecedor"]),
        "formulacao": get(["formulacao","formulação","form"]),
        "data": data,
    }

def sniff_header_row(df_no_header: pd.DataFrame, search_limit: int = 8) -> int:
    target_keys = {"produto","nome","nome comercial","quantidade","unidade"}
    best_row, best_score = 0, -1
    max_row = min(search_limit, len(df_no_header))
    for i in range(max_row):
        row_vals = [str(x).strip().lower() for x in list(df_no_header.iloc[i].values)]
        score = sum(1 for v in row_vals if v in target_keys)
        if score > best_score:
            best_score = score
            best_row = i
    return best_row

def read_xlsx_safely(xls_bytes: bytes, sheet_name: str, header_row_1based: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any], pd.DataFrame]:
    meta = {"used_sheet": sheet_name, "used_header_row_1based": header_row_1based, "columns": [], "sheets": []}
    x = pd.ExcelFile(BytesIO(xls_bytes), engine="openpyxl")
    meta["sheets"] = list(x.sheet_names)
    sheet_to_use = sheet_name if sheet_name in x.sheet_names else x.sheet_names[0]

    df = pd.read_excel(BytesIO(xls_bytes), sheet_name=sheet_to_use, header=header_row_1based - 1,
                       dtype=str, engine="openpyxl").fillna("")
    meta["used_sheet"] = sheet_to_use
    meta["columns"] = list(df.columns)

    unnamed_ratio = sum(str(c).lower().startswith("unnamed") for c in df.columns) / max(1, len(df.columns))
    if unnamed_ratio >= 0.6:
        df_raw = pd.read_excel(BytesIO(xls_bytes), sheet_name=sheet_to_use, header=None,
                               dtype=str, engine="openpyxl").fillna("")
        guess = sniff_header_row(df_raw)
        df = pd.read_excel(BytesIO(xls_bytes), sheet_name=sheet_to_use, header=guess,
                           dtype=str, engine="openpyxl").fillna("")
        meta["used_header_row_1based"] = guess + 1
        meta["columns"] = list(df.columns)

    rows = df.to_dict(orient="records")
    mapped = [map_row_estoque(r) for r in rows]
    mapped = [r for r in mapped if r["nome"]]
    return mapped, meta, df

# ==================== Catálogo (Agrofit) ====================

def coerce_list(x):
    """Converte qualquer coisa em lista de strings (suporta ['A','B'], 'A, B', etc.)."""
    if x is None or x == "":
        return []
    if isinstance(x, list):
        out = []
        for it in x:
            s = str(it).strip()
            if s:
                out.append(s)
        return out
    if isinstance(x, dict):
        vals = []
        for k in ("nome", "name", "valor", "value", "label", "texto", "text"):
            if k in x and x[k]:
                vals.append(str(x[k]).strip())
        return [v for v in vals if v]
    s = str(x).strip()
    # tenta JSON de lista
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        try:
            j = json.loads(s.replace("'", '"'))
            return coerce_list(j)
        except Exception:
            pass
    # separadores comuns
    parts = re.split(r"[;,|/]+", s)
    return [p.strip() for p in parts if p.strip()]

def map_catalog_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte o catálogo para {nome, ia, pragas, registro, empresa, formulacao,
    pragas_list, pragas_por_cultura}.
    - Aceita múltiplos nomes/aliases em campos variados
    - IA formatada: 'ingrediente (grupo) (conc UNID)'
    - Extrai pragas por cultura a partir de 'indicacao_uso'
    """
    lower_map: Dict[str, Any] = {str(k).strip().lower(): v for k, v in row.items()}

    def get_first(keys: List[str], default: Any = "") -> Any:
        for k in keys:
            if k in lower_map and lower_map[k] not in ("", None, ""):
                return lower_map[k]
        return default

    def coerce_list(x):
        if x is None or x == "": return []
        if isinstance(x, list): return [str(it).strip() for it in x if str(it).strip()]
        if isinstance(x, dict):
            vals = []
            for k in ("nome","name","valor","value","label","texto","text"):
                if k in x and x[k]: vals.append(str(x[k]).strip())
            return [v for v in vals if v]
        s = str(x).strip()
        # tenta json/serializado
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                j = json.loads(s.replace("'", '"'))
                return coerce_list(j)
            except Exception:
                pass
        return [p.strip() for p in re.split(r"[;,|/]+", s) if p.strip()]

    # ---------- nomes / aliases ----------
    cand_nomes = []
    for k in ["marca comercial","marca_comercial","nome comercial","nome_comercial",
              "produto comercial","produto_comercial","nome do produto","nome_do_produto",
              "produto","produto nome","produto_nome","nome"]:
        if k in lower_map and lower_map[k]:
            cand_nomes += coerce_list(lower_map[k])
    for k in ["sinonimos","sinônimos","aliases","alias","nomes_alternativos","marcas equivalentes","nomes equivalentes"]:
        if k in lower_map and lower_map[k]:
            cand_nomes += coerce_list(lower_map[k])

    seen = set()
    aliases = []
    for s in cand_nomes:
        key = s.lower()
        if key not in seen:
            seen.add(key)
            aliases.append(s)
    nome = aliases[0] if aliases else ""

    # ---------- helpers IA ----------
    def _fmt_unidade(unidade: str) -> str:
        u = (unidade or "").strip().lower()
        mapping = {
            "gramas por litros": "g/L", "gramas por litro": "g/L", "grama por litro": "g/L",
            "gramas por quilo": "g/kg", "grama por quilo": "g/kg",
            "miligramas por litro": "mg/L", "miligrama por litro": "mg/L",
            "quilogramas por hectare": "kg/ha", "litros por hectare": "L/ha",
        }
        return (mapping.get(u) or (unidade or "")).strip()

    def _fmt_num(x: Any) -> str:
        s = str(x or "").strip().replace(",", ".")
        try:
            f = float(s)
            return str(int(f)) if abs(f - int(f)) < 1e-9 else s
        except:
            return s

    def _fmt_ia_item(item: Any) -> str:
        if isinstance(item, str):
            return item.strip()
        if isinstance(item, dict):
            nome_i = (item.get("ingrediente_ativo") or item.get("nome") or "").strip()
            grupo  = (item.get("grupo_quimico") or item.get("grupo químico") or "").strip()
            conc   = _fmt_num(item.get("concentracao") or item.get("concentração") or "")
            unid   = _fmt_unidade(item.get("unidade_medida") or item.get("unidade de medida") or "")
            parts = []
            if nome_i: parts.append(nome_i)
            if grupo:  parts.append(f"({grupo})")
            if conc or unid:
                parts.append(f"({conc} {unid})".strip() if conc and unid else f"({conc or unid})")
            return " ".join(parts).strip()
        if isinstance(item, list):
            for it in item:
                s = _fmt_ia_item(it)
                if s: return s
            return ""
        return ""

    def _to_list(x: Any) -> List[Any]:
        if x is None or x == "": return []
        if isinstance(x, list): return x
        if isinstance(x, dict): return [x]
        s = str(x).strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                j = json.loads(s.replace("'", '"'))
                return j if isinstance(j, list) else [j]
            except Exception:
                pass
        return [s]

    # ---------- IA ----------
    ia_det_raw = get_first(["ingrediente_ativo_detalhado","ingredientes ativos","ingredientes_ativos"])
    det_items = _to_list(ia_det_raw)
    if det_items:
        ia_fmt, sset = [], set()
        for item in det_items:
            s = _fmt_ia_item(item).strip()
            if s:
                k = s.lower()
                if k not in sset:
                    sset.add(k)
                    ia_fmt.append(s)
        ia_clean = ", ".join(ia_fmt)
    else:
        ia_simples = get_first(["ia","ingrediente ativo","ingrediente_ativo","i.a.","i.a"])
        ia_clean = str(ia_simples or "").strip()
        m = re.match(r"^\[\s*'([^']+)'\s*\]$", ia_clean) or re.match(r'^\[\s*"([^"]+)"\s*\]$', ia_clean)
        if m:
            ia_clean = m.group(1).strip()

    # ---------- PRAGAS ----------
    def _as_list_str(x) -> list[str]:
        if x is None: return []
        if isinstance(x, list):
            out = []
            for it in x:
                s = str(it).strip()
                if s: out.append(s)
            return out
        s = str(x).strip()
        return [p.strip() for p in re.split(r"[;,|/]+", s) if p.strip()]

    pragas_pairs = []
    pragas_val = get_first(["pragas","praga","alvos","pragas_alvo","pragas alvo"])
    for p in _as_list_str(pragas_val):
        pragas_pairs.append(("", p))

    indic = get_first(["indicacao_uso","indicação de uso","indicacao de uso","indicacoes de uso","indicações de uso"])
    for it in _to_list(indic):
        if not isinstance(it, dict):
            for p in _as_list_str(it):
                pragas_pairs.append(("", p))
            continue
        cultura = str(it.get("cultura", "")).strip()
        nomes = it.get("praga_nome_comum") or it.get("praga nome comum") or it.get("alvos") or it.get("pragas")
        for p in _as_list_str(nomes):
            pragas_pairs.append((cultura, p))

    from collections import defaultdict as _dd
    pragas_por_cultura = _dd(set)
    for cultura, praga in pragas_pairs:
        c = (cultura or "").strip()
        p = (praga or "").strip()
        if p:
            pragas_por_cultura[c].add(p)

    pragas_list = sorted({p for ps in pragas_por_cultura.values() for p in ps})
    pragas_clean = "; ".join(pragas_list)

    registro   = str(get_first(["numero_registro","número de registro","registro","n_registro","nº registro","nº de registro"]) or "").strip()
    empresa    = str(get_first(["titular_registro","registrante","empresa","titular do registro","titular do produto"]) or "").strip()
    formulacao = str(get_first(["formulacao","formulação","form","formulacao comercial","formulação comercial"]) or "").strip()

    return {
        "nome": nome,
        "ia": ia_clean,
        "pragas": pragas_clean,
        "registro": registro,
        "empresa": empresa,
        "formulacao": formulacao,
        "pragas_list": pragas_list,
        "pragas_por_cultura": {k: sorted(v) for k, v in pragas_por_cultura.items()},
    }





async def load_catalog_rows_raw() -> List[Dict[str, Any]]:
    # Prioridade: Dropbox -> local -> []
    if CATALOGO_DROPBOX_PATH:
        try:
            data = await dropbox_download(CATALOGO_DROPBOX_PATH)
            try:
                df = pd.read_excel(BytesIO(data), dtype=str, engine="openpyxl").fillna("")
                return df.to_dict(orient="records")
            except Exception:
                rows = json.loads(data.decode("utf-8"))
                return rows if isinstance(rows, list) else []
        except Exception as e:
            print("[CATALOGO] falha dropbox:", e)
    if CATALOGO_PATH and os.path.exists(CATALOGO_PATH):
        try:
            if CATALOGO_PATH.lower().endswith(".json"):
                with open(CATALOGO_PATH, "rb") as f:
                    return json.load(f)
            else:
                df = pd.read_excel(CATALOGO_PATH, dtype=str, engine="openpyxl").fillna("")
                return df.to_dict(orient="records")
        except Exception as e:
            print("[CATALOGO] falha local:", e)
    return []


def _build_catalog_index(catalog_rows: List[Dict[str, Any]]):
    """
    Cria índices por:
      - exact_idx: nome_normalizado -> row
      - simple_idx: nome_simplificado -> row
      - buckets: para fuzzy (por inicial e tamanho)
    Indexa TODOS os aliases do produto.
    """
    exact_idx: Dict[str, Dict[str, Any]] = {}
    simple_idx: Dict[str, Dict[str, Any]] = {}
    buckets: Dict[Tuple[str, int], List[Tuple[str, Dict[str, Any]]]] = {}

    for raw in catalog_rows:
        r = map_catalog_row(raw)
        # pega a lista completa de nomes/aliases; se não existir, cai para o campo nome
        names = r.get("aliases") or ([r.get("nome")] if r.get("nome") else [])
        if not names:
            continue

        for n in names:
            if not n:
                continue
            nn = _normalize_name(n)
            if not nn:
                continue

            # exact
            if nn not in exact_idx:
                exact_idx[nn] = r

            # simple (remove formulação/códigos e números tipo “200 SL”)
            sn = _simplify_product_name(n)
            if sn:
                simple_idx.setdefault(sn, r)

            # buckets para fuzzy
            key = (nn[:1] or "#", len(nn))
            buckets.setdefault(key, []).append((nn, r))

    return exact_idx, simple_idx, buckets



def _fuzzy_match_from_buckets(q: str,
                              exact_idx: Dict[str, Dict[str, Any]],
                              buckets,
                              threshold: float,
                              max_len_diff: int,
                              require_same_init: bool) -> Optional[Dict[str, Any]]:
    qn = _normalize_name(q)
    if not qn:
        return None
    if qn in exact_idx:
        return exact_idx[qn]

    first = qn[:1] or "#"
    qlen = len(qn)
    best = None
    best_ratio = 0.0

    initials = [first] if require_same_init else list({k[0] for k in buckets.keys()})
    for init in initials:
        for L in range(max(1, qlen - max_len_diff), qlen + max_len_diff + 1):
            cand_list = buckets.get((init, L), [])
            if not cand_list:
                continue
            for cand_norm, cand_row in cand_list:
                q_tokens = set(qn.split())
                c_tokens = set(cand_norm.split())
                if q_tokens and c_tokens:
                    overlap = len(q_tokens & c_tokens) / max(1, len(q_tokens))
                    if overlap < 0.6:
                        continue
                ratio = difflib.SequenceMatcher(None, qn, cand_norm).ratio()
                if ratio >= threshold and ratio > best_ratio:
                    best_ratio = ratio
                    best = cand_row
    return best


def enrich_with_catalog(estoque_rows: List[Dict[str, Any]],
                        catalog_rows: List[Dict[str, Any]],
                        threshold: float = FUZZY_THRESHOLD,
                        max_len_diff: int = MAX_LEN_DIFF,
                        require_same_init: bool = REQUIRE_SAME_INIT) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    exact_idx, simple_idx, buckets = _build_catalog_index(catalog_rows)
    total, exact_hits, approx_hits = len(estoque_rows), 0, 0
    out: List[Dict[str, Any]] = []

    for it in estoque_rows:
        base = it.copy()
        nome = base.get("nome", "")
        nn = _normalize_name(nome)
        cat = None

        if nn in exact_idx:
            cat = exact_idx[nn]
            exact_hits += 1
            match_type = "exact"
        else:
            sn = _simplify_product_name(nome)
            if sn and sn in simple_idx:
                cat = simple_idx[sn]
                approx_hits += 1
                match_type = "simple"
            else:
                cand = _fuzzy_match_from_buckets(
                    nome, exact_idx, buckets,
                    threshold=threshold, max_len_diff=max_len_diff,
                    require_same_init=require_same_init
                )
                if cand:
                    cat = cand
                    approx_hits += 1
                    match_type = "fuzzy"
                else:
                    match_type = "none"

        if cat:
            # completa campos vazios a partir do catálogo
            for field in ["ia","pragas","registro","empresa","formulacao"]:
                if not str(base.get(field) or "").strip():
                    base[field] = cat.get(field, "")

            # estruturas auxiliares
            if "pragas_list" not in base or not base["pragas_list"]:
                base["pragas_list"] = cat.get("pragas_list", [])
            if "pragas_por_cultura" not in base or not base.get("pragas_por_cultura"):
                base["pragas_por_cultura"] = cat.get("pragas_por_cultura", {})

        # 👉 FALTAVA ISTO:
        base["catalog_match"] = match_type
        out.append(base)

    meta = {
        "total": total,
        "exact_hits": exact_hits,
        "fuzzy_or_simple_hits": approx_hits,
        "match_rate": round((exact_hits + approx_hits) / total * 100, 2) if total else 0.0,
        "threshold": threshold,
        "max_len_diff": max_len_diff,
        "require_same_initial": require_same_init,
        "catalog_size": len(catalog_rows),
    }
    return out, meta



# ==================== Agregações ====================
def aggregate_estoque(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Agrega por nome comercial → saldo (somatório com sinal), unidade mais comum
    e campos enriquecidos.
    """
    buckets: Dict[str, Dict[str, Any]] = {}
    und_counts: Dict[str, Counter] = defaultdict(Counter)

    for r in rows:
        nome = r.get("nome") or ""
        if not nome:
            continue
        b = buckets.setdefault(nome, {
            "nome": nome,
            "qtd": Decimal("0"),
            "und": "",
            "ia": "", "pragas": "", "registro": "", "empresa": "", "formulacao": "",
        })

        b["qtd"] += to_dec(r.get("qtd"))

        und = (r.get("und") or "").strip()
        if und:
            und_counts[nome][und] += 1

        for k in ["ia","pragas","registro","empresa","formulacao"]:
            if not b[k] and (r.get(k) or "").strip():
                b[k] = (r.get(k) or "").strip()

        # >>> NEW: acumula pragas
        if r.get("pragas_list"):
            b.setdefault("pragas_list", set()).update(r["pragas_list"])
        if r.get("pragas_por_cultura"):
            b.setdefault("pragas_por_cultura", {})
            for cul, plist in r["pragas_por_cultura"].items():
                b["pragas_por_cultura"].setdefault(cul, set()).update(plist)
        # <<< NEW

    for nome, b in buckets.items():
        if und_counts[nome]:
            b["und"] = und_counts[nome].most_common(1)[0][0]
        b["qtd"] = float(b["qtd"].quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))

        # converte estruturas e monta resumo de pragas
        if "pragas_list" in b and isinstance(b["pragas_list"], set):
            b["pragas_list"] = sorted(b["pragas_list"])
        if "pragas_por_cultura" in b:
            b["pragas_por_cultura"] = {k: sorted(list(v)) for k, v in b["pragas_por_cultura"].items()}
        b["pragas_count"] = len(b.get("pragas_list", []))
        if b["pragas_count"] > 0 and not b.get("pragas"):
            top = b["pragas_list"][:5]
            extra = b["pragas_count"] - len(top)
            b["pragas"] = "; ".join(top) + (f" (+{extra})" if extra > 0 else "")



    out = list(buckets.values())
    out.sort(key=lambda x: x["nome"].lower())
    return out

def resumo_entradas_saidas(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    acc: Dict[str, Dict[str, Any]] = {}
    und_counts: Dict[str, Counter] = defaultdict(Counter)

    for r in rows:
        nome = r.get("nome") or ""
        if not nome:
            continue
        d = acc.setdefault(nome, {"nome": nome, "entradas": 0.0, "saidas": 0.0, "saldo": 0.0, "und": ""})
        q = float(to_dec(r.get("qtd")))
        if q >= 0:
            d["entradas"] += q
        else:
            d["saidas"] += abs(q)
        d["saldo"] += q

        und = (r.get("und") or "").strip()
        if und:
            und_counts[nome][und] += 1

    for nome, d in acc.items():
        if und_counts[nome]:
            d["und"] = und_counts[nome].most_common(1)[0][0]

    out = list(acc.values())
    out.sort(key=lambda x: x["nome"].lower())
    return out

# ==================== APIs ====================
@app.get("/api/estoque")
async def api_estoque(
    if_none_match: Optional[str] = None,
    culturas: Optional[str] = Query(None, description="CSV de culturas desejadas (ex: milho,soja,trigo,batata)"),
    qpraga: Optional[str] = Query(None, description="Texto para filtrar por praga (case-insensitive)")
):
    # 1) baixa estoque
    try:
        xls = await dropbox_download(DROPBOX_FILE_PATH)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Dropbox download falhou: {e}")

    # 2) baixa catálogo (para compor ETag)
    cat_bytes = b""
    try:
        if CATALOGO_DROPBOX_PATH:
            cat_bytes = await dropbox_download(CATALOGO_DROPBOX_PATH)
        elif CATALOGO_PATH and os.path.exists(CATALOGO_PATH):
            with open(CATALOGO_PATH, "rb") as f:
                cat_bytes = f.read()
    except Exception:
        cat_bytes = b""

    combined_etag = make_etag_from_bytes(xls, cat_bytes or b"")
    if if_none_match and combined_etag.replace('"','') == if_none_match.replace('"',''):
        return Response(status_code=304, headers={"ETag": f'"{combined_etag}"'})

    # 3) parse e enriquecer
    try:
        mov_rows, _meta, _df = read_xlsx_safely(xls, EXCEL_SHEET, EXCEL_HEADER_ROW)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro lendo planilha: {e}")

    catalog_raw = await load_catalog_rows_raw()
    enriched_rows, _emeta = enrich_with_catalog(mov_rows, catalog_raw)
    aggregated = aggregate_estoque(enriched_rows)

    # 4) filtros cultura/praga
    cul_set = None
    if culturas:
        cul_set = {c.strip().lower() for c in culturas.split(",") if c.strip()}
    q = (qpraga or "").strip().lower() or None

    filtered = []
    for item in aggregated:
        pragas_list = item.get("pragas_list", [])
        pragas_by_cul = item.get("pragas_por_cultura", {})

        if cul_set:
            keep = []
            for cul, plist in pragas_by_cul.items():
                if cul and cul.strip().lower() in cul_set:
                    keep.extend(plist)
            if "" in pragas_by_cul and not keep:
                keep.extend(pragas_by_cul[""])
            pragas_list = sorted(set(keep))

        if q and not any(q in p.lower() for p in pragas_list):
            continue

        out = dict(item)
        out["pragas_list"] = pragas_list
        out["pragas_count"] = len(pragas_list)
        if out["pragas_count"] > 0:
            top = pragas_list[:5]
            extra = out["pragas_count"] - len(top)
            out["pragas"] = "; ".join(top) + (f" (+{extra})" if extra > 0 else "")
        else:
            out["pragas"] = ""

        filtered.append(out)

    body = json.dumps(filtered, ensure_ascii=False).encode("utf-8")
    return Response(content=body, media_type="application/json", headers={"ETag": f'"{combined_etag}"'})


@app.get("/api/estoque/resumo")
async def api_estoque_resumo():
    try:
        xls = await dropbox_download(DROPBOX_FILE_PATH)
        mov_rows, _meta, _df = read_xlsx_safely(xls, EXCEL_SHEET, EXCEL_HEADER_ROW)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro lendo planilha: {e}")
    return resumo_entradas_saidas(mov_rows)

@app.get("/api/estoque/movimentos")
async def api_estoque_movimentos(
    nome: str = Query(..., description="Nome comercial exatamente como na planilha"),
    limit: int = Query(200, ge=1, le=5000),
):
    try:
        xls = await dropbox_download(DROPBOX_FILE_PATH)
        mov_rows, _meta, df = read_xlsx_safely(xls, EXCEL_SHEET, EXCEL_HEADER_ROW)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro lendo planilha: {e}")

    rows = [r for r in mov_rows if (r.get("nome") or "") == nome]

    def parse_date(s: str) -> Tuple[int, str]:
        if not s: return (0, "")
        m = re.match(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
        if m:
            try: return (int(m.group(1))*10000 + int(m.group(2))*100 + int(m.group(3)), s)
            except: return (0, s)
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
        if m:
            try: return (int(m.group(3))*10000 + int(m.group(2))*100 + int(m.group(1)), s)
            except: return (0, s)
        return (0, s)

    rows.sort(key=lambda r: parse_date(r.get("data",""))[0], reverse=True)

    entradas = sum(max(0.0, float(to_dec(r.get("qtd")))) for r in rows)
    saidas   = sum(max(0.0, -float(to_dec(r.get("qtd")))) for r in rows)
    saldo    = entradas - saidas
    und      = next((r.get("und","").strip() for r in rows if (r.get("und") or "").strip()), "")

    hist = [{"data": r.get("data",""), "qtd": r.get("qtd",0.0), "und": r.get("und","")} for r in rows[:limit]]
    return {"nome": nome, "entradas": entradas, "saidas": saidas, "saldo": saldo, "und": und, "movimentos": hist, "count": len(rows)}

@app.get("/api/estoque/debug")
async def api_estoque_debug():
    try:
        xls = await dropbox_download(DROPBOX_FILE_PATH)
        rows, meta, df = read_xlsx_safely(xls, EXCEL_SHEET, EXCEL_HEADER_ROW)
        preview_raw = df.head(5).to_dict(orient="records")
        return {"meta": meta, "count_mapped": len(rows), "preview_raw": preview_raw, "preview_mapped": rows[:10]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/catalogo")
async def api_catalogo():
    try:
        rows = await load_catalog_rows_raw()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro lendo catálogo: {e}")

@app.get("/api/enriquecimento/debug")
async def api_enriquecimento_debug():
    try:
        xls = await dropbox_download(DROPBOX_FILE_PATH)
        base_rows, meta, _df = read_xlsx_safely(xls, EXCEL_SHEET, EXCEL_HEADER_ROW)
        catalog_raw = await load_catalog_rows_raw()
        enriched, emeta = enrich_with_catalog(base_rows, catalog_raw)

        not_matched = [r for r in enriched if not r.get("ia") and not r.get("registro")]
        sample_nf = not_matched[:15]
        return {
            "sheet_meta": meta,
            "catalog_raw_len": len(catalog_raw),
            "enrichment_meta": emeta,
            "sample_unmatched": sample_nf,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


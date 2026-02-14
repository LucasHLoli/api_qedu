#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
==============================================================================
GERADOR UNIFICADO DE RELATÓRIOS — QEDU  (API + CSV IDEB)
==============================================================================
Input:  código IBGE (7 dígitos)
Output: 5 TXTs — Aprendizado, Infraestrutura, Censo, IDEB, Taxa Rendimento

Coleta dados via API QEdu + CSV IDEB e gera TXTs idênticos aos originais.
Anos detectados DINAMICAMENTE.
==============================================================================
"""

import pathlib, time, sys, re
from datetime import datetime
from typing import Any, Optional, Tuple, Dict, List

try:
    import numpy as np
except ImportError:
    np = None

try:
    import requests
except ImportError:
    print("❌  pip install requests"); sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("❌  pip install pandas"); sys.exit(1)

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
BASE_DIR   = pathlib.Path(__file__).parent
DADOS_DIR  = BASE_DIR / "dados"
OUTPUT_DIR = BASE_DIR / "output"

IDEB_MUN_CSV = DADOS_DIR / "ideb_saeb_municipios_28_07_final 1.csv"
IDEB_UF_CSV  = DADOS_DIR / "ideb_saeb_estados_28_07_final 1.csv"

BASE_URL  = "https://qedu.org.br/api/v1"
ANO_ATUAL = datetime.now().year
LINE      = "=" * 80
SUBLINE   = "-" * 80

# ---------- headers obrigatórios (API retorna 403 sem eles) ----------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://qedu.org.br/",
}

# ---------- mapeamentos ----------
DEPENDENCIAS = {0: "Todas as redes", 1: "Federal", 2: "Estadual",
                3: "Municipal", 4: "Privada", 5: "Pública"}

CICLOS = {"AI": "Anos Iniciais (1º ao 5º)",
          "AF": "Anos Finais (6º ao 9º)",
          "EM": "Ensino Médio"}

DISCIPLINAS = {"lp": "Língua Portuguesa", "mt": "Matemática"}

NIVEIS = [
    ("adequado",    "Adequado (Proficiente + Avançado)"),
    ("avancado",    "Avançado"),
    ("proficiente", "Proficiente"),
    ("basico",      "Básico"),
    ("insuficiente","Insuficiente"),
]

ITENS_INFRA_RELEVANTES = [
    "Biblioteca*", "Láb. Informática", "Láb. Ciências",
    "Sala de Leitura", "Quadra de Esportes", "Internet", "Banda Larga",
]

CAMPOS_MATRICULA = [
    ("matriculas_creche",            "Creche"),
    ("matriculas_pre_escolar",       "Pré-Escola"),
    ("matriculas_anos_iniciais",     "Anos Iniciais (1º ao 5º)"),
    ("matriculas_anos_finais",       "Anos Finais (6º ao 9º)"),
    ("matriculas_ensino_medio",      "Ensino Médio"),
    ("matriculas_eja",               "EJA"),
    ("matriculas_educacao_especial", "Educação Especial"),
]

CAMPOS_SERIES = [
    ("matriculas_1ano", "1º Ano", "Anos Iniciais"),
    ("matriculas_2ano", "2º Ano", "Anos Iniciais"),
    ("matriculas_3ano", "3º Ano", "Anos Iniciais"),
    ("matriculas_4ano", "4º Ano", "Anos Iniciais"),
    ("matriculas_5ano", "5º Ano", "Anos Iniciais"),
    ("matriculas_6ano", "6º Ano", "Anos Finais"),
    ("matriculas_7ano", "7º Ano", "Anos Finais"),
    ("matriculas_8ano", "8º Ano", "Anos Finais"),
    ("matriculas_9ano", "9º Ano", "Anos Finais"),
]

SEGMENTOS_DISPLAY = {
    "anos iniciais": "ANOS INICIAIS",
    "anos finais":   "ANOS FINAIS",
    "ensino medio":  "ENSINO MEDIO",
}

# ---------- códigos UF (IBGE) → (nome, sigla) ----------
UF_CODES = {
    "11": ("Rondônia", "RO"), "12": ("Acre", "AC"), "13": ("Amazonas", "AM"),
    "14": ("Roraima", "RR"), "15": ("Pará", "PA"), "16": ("Amapá", "AP"),
    "17": ("Tocantins", "TO"), "21": ("Maranhão", "MA"), "22": ("Piauí", "PI"),
    "23": ("Ceará", "CE"), "24": ("Rio Grande do Norte", "RN"),
    "25": ("Paraíba", "PB"), "26": ("Pernambuco", "PE"), "27": ("Alagoas", "AL"),
    "28": ("Sergipe", "SE"), "29": ("Bahia", "BA"), "31": ("Minas Gerais", "MG"),
    "32": ("Espírito Santo", "ES"), "33": ("Rio de Janeiro", "RJ"),
    "35": ("São Paulo", "SP"), "41": ("Paraná", "PR"), "42": ("Santa Catarina", "SC"),
    "43": ("Rio Grande do Sul", "RS"), "50": ("Mato Grosso do Sul", "MS"),
    "51": ("Mato Grosso", "MT"), "52": ("Goiás", "GO"), "53": ("Distrito Federal", "DF"),
}


def is_estado(codigo):
    """Retorna True se o código é de estado (2 dígitos)."""
    return str(codigo).strip() in UF_CODES


# =============================================================================
# DETECÇÃO DINÂMICA DE ANOS
# =============================================================================
def _anos_candidatos(n: int = 6) -> list:
    """Retorna [ano_atual, ano-1, ..., ano-n+1] — fallback amplo para garantir dados.

    Sempre tenta do ano atual para trás. Com n=6 e ANO_ATUAL=2026:
    [2026, 2025, 2024, 2023, 2022, 2021] — censo/infra disponível em 2024,
    taxa em 2023, garantia máxima de encontrar dados.
    """
    return [ANO_ATUAL - i for i in range(n)]


def _anos_saeb() -> list:
    """SAEB é bienal ímpar: 2023, 2021, 2019 ... Tenta do mais recente."""
    a = ANO_ATUAL if ANO_ATUAL % 2 == 1 else ANO_ATUAL - 1
    return [a - 2 * i for i in range(5)]


# =============================================================================
# HTTP  (com cache por sessão — evita chamadas duplicadas)
# =============================================================================
_FETCH_CACHE: Dict[tuple, Any] = {}


def _clear_cache():
    _FETCH_CACHE.clear()


def fetch_json(url: str, params: dict = None, tentativas: int = 3) -> Any:
    cache_key = (url, tuple(sorted((params or {}).items())))
    if cache_key in _FETCH_CACHE:
        return _FETCH_CACHE[cache_key]
    for i in range(tentativas):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            r.raise_for_status()
            result = r.json()
            _FETCH_CACHE[cache_key] = result
            return result
        except Exception:
            if i == tentativas - 1:
                _FETCH_CACHE[cache_key] = None
                return None
            time.sleep(0.5)
    return None


# =============================================================================
# COLETA — com fallback de anos
# =============================================================================
def fetch_censo(ibge, dep_id, ano=None, loc=0, oferta=0):
    for a in ([ano] if ano else _anos_candidatos()):
        d = fetch_json(f"{BASE_URL}/censo/territorios/matriculas",
                       {"ibge_id": ibge, "ano": a, "dependencia_id": dep_id,
                        "localizacao_id": loc, "oferta_id": oferta})
        if d and d.get("censo"):
            return d, a
    return None, 0


def fetch_infra(ibge, dep_id, ano=None):
    for a in ([ano] if ano else _anos_candidatos()):
        d = fetch_json(f"{BASE_URL}/infra/{ibge}/comparativo",
                       {"dependencia_id": dep_id, "ano": a})
        if d and isinstance(d, list):
            for s in d:
                for it in s.get("items", []):
                    if it.get("values"):
                        return d, a
    return None, 0


def fetch_aprendizado(ibge, dep_id, ciclo):
    return fetch_json(f"{BASE_URL}/aprendizado/{ibge}/ultimos-comparativo",
                      {"dependencia_id": dep_id, "ciclo_id": ciclo})


def _normalizar_taxa_keys(d):
    """Normaliza keys da API de taxa rendimento.

    API pode retornar 'entidade'/'parent' ou 'municipio'/'estado'.
    Padroniza para 'municipio'/'estado'/'brasil'.
    """
    if not isinstance(d, dict):
        return d
    return {
        "municipio": d.get("entidade") or d.get("municipio") or [],
        "estado":    d.get("parent")   or d.get("estado")    or [],
        "brasil":    d.get("brasil")   or [],
    }


def fetch_taxa(ibge, ciclo, dep_id=0, ano=None, loc=0):
    for a in ([ano] if ano else _anos_candidatos()):
        d = fetch_json(
            f"{BASE_URL}/taxa-rendimento/taxa-rendimento/{ibge}/comparacao",
            {"dependencia_id": dep_id, "ano": a,
             "ciclo_id": ciclo, "localizacao_id": loc})
        if d and (d.get("entidade") or d.get("municipio") or d.get("brasil")):
            norm = _normalizar_taxa_keys(d)
            # Detectar ano real mais recente nos dados (API pode ignorar param ano)
            ano_real = 0
            for regs in norm.values():
                if isinstance(regs, list):
                    for r in regs:
                        ra = r.get("ano")
                        if ra and ra > ano_real:
                            ano_real = ra
            return norm, ano_real if ano_real else a
    return None, 0


def fetch_taxa_historico(ibge, ciclo, dep_id=0, loc=0):
    """Busca últimos 3 anos de taxa para evolução histórica."""
    resultados = {}
    for a in _anos_candidatos(8):
        d = fetch_json(
            f"{BASE_URL}/taxa-rendimento/taxa-rendimento/{ibge}/comparacao",
            {"dependencia_id": dep_id, "ano": a,
             "ciclo_id": ciclo, "localizacao_id": loc})
        if d and (d.get("entidade") or d.get("municipio") or d.get("brasil")):
            resultados[a] = _normalizar_taxa_keys(d)
        if len(resultados) >= 3:
            break
    return resultados


# =============================================================================
# IDEB (CSV)
# =============================================================================
def _normalizar_segmento(s):
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    s = s.replace("ã", "a").replace("é", "e").replace("í", "i").replace("ê", "e")
    s = s.replace("ensino médio", "ensino medio").replace("medio", "ensino medio") \
         if "medio" in s and "ensino" not in s else s
    for old, new in [("ano iniciais", "anos iniciais"), ("anos inicias", "anos iniciais"),
                     ("ano finais", "anos finais")]:
        s = s.replace(old, new)
    return s.strip()


def load_ideb(ibge):
    """Retorna (df_mun, df_uf, brasil_stats) ou (None, None, None)."""
    if not IDEB_MUN_CSV.exists() or not IDEB_UF_CSV.exists():
        return None, None, None

    mun_df = pd.read_csv(IDEB_MUN_CSV, sep=",", dtype={"codigo_ibge": str})
    uf_df  = pd.read_csv(IDEB_UF_CSV, sep=";")

    mun_df.columns = [c.strip().lower() for c in mun_df.columns]
    uf_df.columns  = [c.strip().lower() for c in uf_df.columns]

    # normalizar nomes de colunas
    if "valor" in uf_df.columns and "valor_numerico" not in uf_df.columns:
        uf_df.rename(columns={"valor": "valor_numerico"}, inplace=True)
    if "valor" in mun_df.columns and "valor_numerico" not in mun_df.columns:
        mun_df.rename(columns={"valor": "valor_numerico"}, inplace=True)

    # normalizar segmentos
    for df in [mun_df, uf_df]:
        if "segmento" in df.columns:
            df["segmento"] = df["segmento"].apply(_normalizar_segmento)

    # converter valor_numerico
    for df in [mun_df, uf_df]:
        if "valor_numerico" in df.columns:
            df["valor_numerico"] = pd.to_numeric(df["valor_numerico"], errors="coerce")

    df_mun = mun_df[mun_df["codigo_ibge"] == str(ibge)].copy()

    # --- Estado (2 dígitos): usa CSV de estados como dados primários ---
    if is_estado(ibge):
        _, uf_sigla = UF_CODES.get(str(ibge), ("", ""))
        df_estado = uf_df[uf_df["indicador_uf"] == uf_sigla].copy() if uf_sigla else pd.DataFrame()
        if df_estado.empty:
            return None, None, None
        # Brasil = stats dos estados
        brasil_st = None
        if "valor_numerico" in uf_df.columns and "indicador_tipo_nome" in uf_df.columns:
            cols_group = ["indicador_tipo_nome", "ano"]
            if "segmento" in uf_df.columns:
                cols_group.append("segmento")
            brasil_st = (uf_df.groupby(cols_group)["valor_numerico"]
                         .agg(["mean", "median", "std", "min", "max", "count"])
                         .reset_index())
        # Retorna estado como df_mun (primário), None como df_uf, e brasil_stats
        return df_estado, None, brasil_st

    if df_mun.empty:
        return None, None, None

    uf_sigla = df_mun["indicador_uf"].iloc[0] if "indicador_uf" in df_mun.columns else None
    df_uf = uf_df[uf_df["indicador_uf"] == uf_sigla].copy() if uf_sigla else pd.DataFrame()

    # Brasil = stats dos estados
    brasil_stats = None
    if "valor_numerico" in uf_df.columns and "indicador_tipo_nome" in uf_df.columns:
        cols_group = ["indicador_tipo_nome", "ano"]
        if "segmento" in uf_df.columns:
            cols_group.append("segmento")
        brasil_stats = (uf_df.groupby(cols_group)["valor_numerico"]
                        .agg(["mean", "median", "std", "min", "max", "count"])
                        .reset_index())

    return df_mun, df_uf, brasil_stats


# =============================================================================
# DESCOBRIR MUNICÍPIO
# =============================================================================
def descobrir_municipio(ibge):
    """Descobre nome do município/estado e UF via API ou CSV."""
    ibge = str(ibge).strip()

    # Estado (2 dígitos) — retorna direto do mapa
    if ibge in UF_CODES:
        nome, sigla = UF_CODES[ibge]
        return nome, sigla

    # 1) Tentar via taxa rendimento — resposta contém territorio.nome
    for ciclo in ["AI", "AF"]:
        for ano_t in _anos_candidatos():
            raw = fetch_json(
                f"{BASE_URL}/taxa-rendimento/taxa-rendimento/{ibge}/comparacao",
                {"dependencia_id": 0, "ano": ano_t, "ciclo_id": ciclo,
                 "localizacao_id": 0})
            if not raw:
                continue
            ent = raw.get("entidade") or raw.get("municipio") or []
            par = raw.get("parent") or raw.get("estado") or []
            if ent and isinstance(ent, list) and len(ent) > 0:
                rend = ent[0].get("rendimento", ent[0])
                nome = rend.get("territorio", {}).get("nome")
                uf = "??"
                if par and isinstance(par, list) and len(par) > 0:
                    rend_p = par[0].get("rendimento", par[0])
                    uf = rend_p.get("territorio", {}).get("sigla", "??")
                if nome:
                    return nome, uf
            break  # se tem resposta mas sem nome, não precisa tentar outro ano

    # 2) Tentar via censo — territorio pode existir em versões mais antigas
    d, _ = fetch_censo(ibge, dep_id=5)
    if d and "censo" in d:
        c = d["censo"]
        t = c.get("territorio", {})
        if t and t.get("nome"):
            p = t.get("parent", {})
            return t.get("nome", f"IBGE_{ibge}"), (p.get("sigla", "??") if p else "??")

    # 3) Fallback CSV IDEB
    if IDEB_MUN_CSV.exists():
        try:
            df = pd.read_csv(IDEB_MUN_CSV, sep=",",
                             dtype={"codigo_ibge": str}, nrows=300000)
            df.columns = [c.strip().lower() for c in df.columns]
            r = df[df["codigo_ibge"] == str(ibge)]
            if not r.empty:
                return (r.iloc[0].get("indicador_municipio", f"IBGE_{ibge}"),
                        r.iloc[0].get("indicador_uf", "??"))
        except Exception:
            pass
    return f"IBGE_{ibge}", "??"


# =============================================================================
# FORMATAÇÃO
# =============================================================================
def _pct(v):
    """0.655 → '65.5%'  |  65.5 → '65.5%'"""
    if v is None:
        return "sem dados"
    val = v * 100 if abs(v) <= 1.01 else v
    return f"{val:.1f}%"


def _pp(v, decimais=1):
    """Diferença em pontos percentuais."""
    if v is None:
        return "sem dados"
    val = v * 100 if abs(v) <= 1.01 else v
    fmt = f"{{:+.{decimais}f}}pp"
    return fmt.format(val)


def _val(v, fmt=".2f"):
    return f"{v:{fmt}}" if v is not None else "N/D"


def _slug(nome):
    return (nome.replace(" ", "_").replace("'", "").replace("/", "_")
            .replace("ã", "a").replace("é", "e").replace("ç", "c")
            .replace("í", "i").replace("ó", "o").replace("ú", "u")
            .replace("â", "a").replace("ê", "e").replace("ô", "o"))


# =============================================================================
# CABEÇALHO / RODAPÉ
# =============================================================================
def _hdr(titulo, mun, **kw):
    t = f"{LINE}\n{titulo}\n{LINE}\n\n"
    t += f"📍 Território: {mun}\n"
    if kw.get("rede"):    t += f"🏫 Rede: {kw['rede']}\n"
    if kw.get("ciclo"):   t += f"📚 Ciclo: {kw['ciclo']}\n"
    if kw.get("ano"):     t += f"📅 Ano de referência: {kw['ano']}\n"
    if kw.get("periodo"): t += f"📅 Período histórico: {kw['periodo']}\n"
    t += f"📅 Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
    return t


def _footer(fonte="QEdu (qedu.org.br)"):
    return f"\n\n{LINE}\nFonte: {fonte}\n{LINE}\n"


# #############################################################################
#
#  1. APRENDIZADO  (SAEB)
#
# #############################################################################

def _classificar_desempenho(pct_adequado):
    """Classifica % adequado (escala 0-1)."""
    if pct_adequado is None:
        return "Dados não disponíveis", "⚪"
    p = pct_adequado * 100 if pct_adequado <= 1 else pct_adequado
    if p >= 70:
        return "Bom desempenho", "✅"
    if p >= 50:
        return "Desempenho intermediário", "⚠️"
    return "Desempenho crítico - oportunidade de atuação", "🔴"


def _adeq(rec, disc):
    """Retorna % adequado (proficiente+avançado) de um registro."""
    v = rec.get(f"{disc}_adequado")
    if v is not None:
        return v
    vp = rec.get(f"{disc}_proficiente")
    va = rec.get(f"{disc}_avancado")
    if vp is not None or va is not None:
        return (vp or 0) + (va or 0)
    return None


def _extrair_territorios(dados, ibge):
    """Separa registros API aprendizado por território."""
    mun, est, br = [], [], []
    if not dados or not isinstance(dados, list):
        return mun, est, br
    for grupo in dados:
        if not isinstance(grupo, list):
            continue
        for r in grupo:
            if not isinstance(r, dict):
                continue
            rid = r.get("territorio", {}).get("ibge_id")
            pid = r.get("territorio", {}).get("parent_id")
            if str(rid) == str(ibge):
                mun.append(r)
            elif rid == 7:
                br.append(r)
            elif pid is not None and pid <= 27:
                est.append(r)
    return mun, est, br


def gerar_txt_aprendizado(ibge, mun, uf):
    """Gera relatório de Aprendizado (SAEB) — idêntico ao original."""
    dep_id = 5  # Pública (todas)
    txt_final = ""

    for cid, cnome in CICLOS.items():
        dados = fetch_aprendizado(ibge, dep_id, cid)
        recs_mun, recs_est, recs_br = _extrair_territorios(dados, ibge)

        if not recs_mun:
            continue

        ciclo_label = cnome
        if cid in ("AI", "AF"):
            ciclo_label = f"{cnome.split('(')[0].strip()} do Ensino Fundamental ({cnome.split('(')[1]}" if "(" in cnome else cnome

        bloco = _hdr("RELATÓRIO COMPLETO DE APRENDIZADO - DADOS QEDU",
                      mun, rede="Pública (todas as redes)", ciclo=ciclo_label)
        bloco += "\n"

        recs_mun.sort(key=lambda x: x.get("ano", 0))
        anos_disp = [r.get("ano") for r in recs_mun]

        # =================================================================
        # PARTE 1: EVOLUÇÃO TEMPORAL
        # =================================================================
        bloco += f"\n{'*'*80}\nPARTE 1: EVOLUÇÃO TEMPORAL DOS INDICADORES\n{'*'*80}\n\n"

        col_anos = "".join(f"{a:>7}" for a in anos_disp)
        bloco += f"{'Disciplina':>18} {'Nível':>40} {col_anos} {'Variação':>10}\n"

        for disc, disc_nome in DISCIPLINAS.items():
            for niv_key, niv_label in NIVEIS:
                vals = []
                for r in recs_mun:
                    if niv_key == "adequado":
                        v = _adeq(r, disc)
                    else:
                        v = r.get(f"{disc}_{niv_key}")
                    vals.append(v)

                vals_str = "".join(f"{(v*100 if v else 0):>6.1f}%" for v in vals)
                var_str = ""
                if len(vals) >= 2 and vals[0] is not None and vals[-1] is not None:
                    var = (vals[-1] - vals[0]) * 100
                    var_str = f"{var:+.2f}pp"
                bloco += f"{disc_nome:>18} {niv_label:>40} {vals_str} {var_str:>10}\n"

        # =================================================================
        # PARTE 2: COMPARATIVO
        # =================================================================
        bloco += f"\n\n{'*'*80}\nPARTE 2: COMPARATIVO COM MUNICÍPIOS SEMELHANTES E BRASIL\n{'*'*80}\n"

        ultimo_mun = recs_mun[-1]
        ultimo_br  = sorted(recs_br, key=lambda x: x.get("ano", 0))[-1] if recs_br else None
        # "Municípios semelhantes" = primeiro grupo que não é o município nem Brasil
        ultimo_sem = sorted(recs_est, key=lambda x: x.get("ano", 0))[-1] if recs_est else None

        # Resumo adequado
        bloco += f"\nRESUMO - % de Alunos com Aprendizado Adequado:\n\n"
        h_sem = "Municípios semelhantes" if ultimo_sem else "Estado"
        bloco += (f"{'Disciplina':>18} {'Município':>10} {h_sem:>23} "
                  f"{'Brasil':>7} {'vs Semelhantes':>15} {'vs Brasil':>10}\n")

        for disc, disc_nome in DISCIPLINAS.items():
            vm = _adeq(ultimo_mun, disc)
            vb = _adeq(ultimo_br, disc) if ultimo_br else None
            vs = _adeq(ultimo_sem, disc) if ultimo_sem else None

            d_sem = f"{(vm-vs)*100:+.1f}pp" if vm is not None and vs is not None else ""
            d_br  = f"{(vm-vb)*100:+.1f}pp" if vm is not None and vb is not None else ""
            bloco += (f"{disc_nome:>18} {_pct(vm):>10} {_pct(vs):>23} "
                      f"{_pct(vb):>7} {d_sem:>15} {d_br:>10}\n")

        # Detalhamento por nível
        bloco += f"\n\nDETALHAMENTO POR NÍVEL:\n\n"
        bloco += (f"{'Disciplina':>18} {'Nível':>40} {'Município':>10} "
                  f"{h_sem:>23} {'Brasil':>7} {'vs Semelhantes':>15} {'vs Brasil':>10}\n")

        for disc, disc_nome in DISCIPLINAS.items():
            for niv_key, niv_label in NIVEIS:
                if niv_key == "adequado":
                    vm = _adeq(ultimo_mun, disc)
                    vb = _adeq(ultimo_br, disc) if ultimo_br else None
                    vs = _adeq(ultimo_sem, disc) if ultimo_sem else None
                else:
                    vm = ultimo_mun.get(f"{disc}_{niv_key}")
                    vb = ultimo_br.get(f"{disc}_{niv_key}") if ultimo_br else None
                    vs = ultimo_sem.get(f"{disc}_{niv_key}") if ultimo_sem else None

                d_sem = f"{(vm-vs)*100:+.1f}pp" if vm is not None and vs is not None else ""
                d_br  = f"{(vm-vb)*100:+.1f}pp" if vm is not None and vb is not None else ""
                bloco += (f"{disc_nome:>18} {niv_label:>40} {_pct(vm):>10} "
                          f"{_pct(vs):>23} {_pct(vb):>7} {d_sem:>15} {d_br:>10}\n")

        # =================================================================
        # PARTE 3: ANÁLISE QUALITATIVA
        # =================================================================
        bloco += f"\n\n{'*'*80}\nPARTE 3: ANÁLISE QUALITATIVA\n{'*'*80}\n"
        bloco += f"\n{LINE}\nANÁLISE QUALITATIVA - EVOLUÇÃO DO APRENDIZADO\n{LINE}\n"
        bloco += f"\n📍 Território: {mun}\n🏫 Rede: Pública (todas as redes)\n"
        bloco += f"📚 Ciclo: {ciclo_label}\n"
        bloco += f"📅 Período analisado: {anos_disp[0]} a {anos_disp[-1]}\n"

        bloco += f"\n{SUBLINE}\nDIAGNÓSTICO ATUAL POR DISCIPLINA\n{SUBLINE}\n"

        alertas_criticos = 0
        abaixo_brasil = 0
        oportunidades = []

        for disc, disc_nome in DISCIPLINAS.items():
            adeq = _adeq(ultimo_mun, disc)
            classif, emoji = _classificar_desempenho(adeq)

            prof  = ultimo_mun.get(f"{disc}_proficiente")
            avanc = ultimo_mun.get(f"{disc}_avancado")
            basico = ultimo_mun.get(f"{disc}_basico")
            insuf = ultimo_mun.get(f"{disc}_insuficiente")
            inad = ((basico or 0) + (insuf or 0))

            bloco += f"\n📘 {disc_nome.upper()}\n\n"
            bloco += f"   {emoji} Situação atual: {classif}\n"
            bloco += f"   • Alunos com aprendizado adequado: {_pct(adeq)}\n"
            bloco += f"      - Avançado: {_pct(avanc)}\n"
            bloco += f"      - Proficiente: {_pct(prof)}\n"
            bloco += f"   • Alunos com aprendizado inadequado: {_pct(inad)}\n"
            bloco += f"      - Básico: {_pct(basico)}\n"
            bloco += f"      - Insuficiente: {_pct(insuf)}\n"

            # Evolução
            adeq_primeiro = _adeq(recs_mun[0], disc)
            if adeq is not None and adeq_primeiro is not None:
                var = (adeq - adeq_primeiro) * 100
                e = "📈 Melhora" if var > 0 else "📉 Piora" if var < 0 else "➡️ Estável"
                bloco += f"   • Evolução ({anos_disp[0]}-{anos_disp[-1]}): {e} ({var:+.1f}pp)\n"

            if adeq and adeq < 0.5:
                alertas_criticos += 1
                oportunidades.append((disc_nome, adeq, insuf))

            vb_adeq = _adeq(ultimo_br, disc) if ultimo_br else None
            if adeq and vb_adeq and adeq < vb_adeq:
                abaixo_brasil += 1

        # Oportunidades
        bloco += f"\n{SUBLINE}\n🎯 OPORTUNIDADES IDENTIFICADAS\n{SUBLINE}\n"
        if not oportunidades:
            bloco += "\n   ✅ Sem oportunidades críticas identificadas.\n"
        for disc_nome, adeq, insuf in oportunidades:
            bloco += f"\n   🔴 {disc_nome}: Apenas {_pct(adeq)} com aprendizado adequado\n"
            bloco += f"      → {_pct(insuf)} em nível insuficiente\n"
            bloco += f"      → Potencial para: reforço escolar, materiais de nivelamento\n"

        # Pandemia
        anos_pan = {r.get("ano"): r for r in recs_mun}
        if 2019 in anos_pan and 2021 in anos_pan and 2023 in anos_pan:
            bloco += f"\n{SUBLINE}\n📉 IMPACTO DA PANDEMIA E RECUPERAÇÃO\n{SUBLINE}\n"
            for disc, disc_nome in DISCIPLINAS.items():
                a19 = _adeq(anos_pan[2019], disc)
                a21 = _adeq(anos_pan[2021], disc)
                a23 = _adeq(anos_pan[2023], disc)
                if a19 and a21 and a23:
                    queda = (a21 - a19) * 100
                    recup = (a23 - a21) * 100
                    saldo = (a23 - a19) * 100
                    bloco += f"\n   📘 {disc_nome}:\n"
                    bloco += f"      • 2019→2021 (pandemia): {queda:+.1f}pp\n"
                    bloco += f"      • 2021→2023 (recuperação): {recup:+.1f}pp\n"
                    bloco += f"      • Saldo total (2019→2023): {saldo:+.1f}pp\n"
                    if a23 >= a19:
                        bloco += f"      ✅ RECUPEROU o patamar pré-pandemia\n"
                    else:
                        bloco += f"      ⚠️ Ainda {abs(saldo):.1f}pp ABAIXO do nível pré-pandemia\n"

        # Comparativo qualitativo
        bloco += f"\n{LINE}\nANÁLISE QUALITATIVA - COMPARATIVO COM SEMELHANTES E BRASIL\n{LINE}\n"
        bloco += f"\n📊 Comparação de {mun} com municípios semelhantes e média nacional\n"

        cats = {"abaixo_br": [], "abaixo_sem": [], "acima": []}
        for disc, disc_nome in DISCIPLINAS.items():
            vm_adeq = _adeq(ultimo_mun, disc)
            vb_adeq = _adeq(ultimo_br, disc) if ultimo_br else None
            vs_adeq = _adeq(ultimo_sem, disc) if ultimo_sem else None
            if vm_adeq is None:
                continue
            item = {"disc": disc_nome, "mun": vm_adeq, "br": vb_adeq, "sem": vs_adeq}
            if vb_adeq and vm_adeq < vb_adeq:
                cats["abaixo_br"].append(item)
            elif vs_adeq and vm_adeq < vs_adeq:
                cats["abaixo_sem"].append(item)
            else:
                cats["acima"].append(item)

        bloco += f"\n{SUBLINE}\n🔴 ABAIXO DA MÉDIA NACIONAL (BRASIL)\n{SUBLINE}\n"
        if not cats["abaixo_br"]:
            bloco += "   ✅ Nenhum indicador abaixo da média nacional.\n"
        for it in cats["abaixo_br"]:
            d = (it["mun"] - it["br"]) * 100
            bloco += f"\n   ❌ {it['disc']} - Adequado\n"
            bloco += f"      Município: {_pct(it['mun'])} | Brasil: {_pct(it['br'])} → {d:+.1f}pp\n"

        bloco += f"\n{SUBLINE}\n🟡 ABAIXO DE MUNICÍPIOS SEMELHANTES (mas acima do Brasil)\n{SUBLINE}\n"
        if not cats["abaixo_sem"]:
            pass  # vazio igual ao original
        for it in cats["abaixo_sem"]:
            d = (it["mun"] - (it["sem"] or 0)) * 100
            bloco += f"\n   ⚠️ {it['disc']} - Adequado\n"
            bloco += f"      Município: {_pct(it['mun'])} | Semelhantes: {_pct(it['sem'])} → {d:+.1f}pp\n"

        bloco += f"\n{SUBLINE}\n🟢 ACIMA DAS MÉDIAS (Semelhantes e Brasil)\n{SUBLINE}\n"
        for it in cats["acima"]:
            d_br = (it["mun"] - (it["br"] or 0)) * 100
            d_sem = (it["mun"] - (it["sem"] or 0)) * 100
            bloco += f"\n   ✅ {it['disc']} - Adequado\n"
            bloco += f"      Município: {_pct(it['mun'])} | Semelhantes: {_pct(it.get('sem'))} | Brasil: {_pct(it['br'])}\n"
            bloco += f"      → {d_br:+.1f}pp vs Brasil | {d_sem:+.1f}pp vs Semelhantes\n"

        # Resumo comparativo
        bloco += f"\n{LINE}\n📋 RESUMO COMPARATIVO\n{LINE}\n\n"
        bloco += f"   🔴 Abaixo do Brasil:          {len(cats['abaixo_br'])} disciplina(s)\n"
        bloco += f"   🟡 Abaixo de Semelhantes:     {len(cats['abaixo_sem'])} disciplina(s)\n"
        bloco += f"   🟢 Acima de ambos:            {len(cats['acima'])} disciplina(s)\n"

        # Conclusão
        bloco += f"\n{LINE}\n💡 CONCLUSÃO E RECOMENDAÇÕES PARA ABORDAGEM COMERCIAL\n{LINE}\n"
        bloco += f"\n📍 {mun.upper()}\n"

        if alertas_criticos > 0 or abaixo_brasil > 0:
            bloco += "   🔴 SITUAÇÃO: CRÍTICA\n"
            bloco += "   POTENCIAL DE MERCADO: ALTO\n"
            bloco += "   → Recomendação: reforço escolar, recuperação, materiais de nivelamento\n"
        elif len(cats["abaixo_sem"]) > 0:
            bloco += "   🟡 SITUAÇÃO: ATENÇÃO\n"
            bloco += "   POTENCIAL DE MERCADO: MÉDIO-ALTO\n"
            bloco += "   → Recomendação: soluções para alcançar patamar de municípios semelhantes\n"
        else:
            bloco += "   🟢 SITUAÇÃO: POSITIVA\n"
            bloco += "   POTENCIAL DE MERCADO: MÉDIO\n"
            bloco += "   → Recomendação: soluções de excelência e enriquecimento curricular\n"

        bloco += _footer("QEdu (qedu.org.br)")
        txt_final += bloco

    if not txt_final:
        txt_final = _hdr("RELATÓRIO COMPLETO DE APRENDIZADO - DADOS QEDU", mun)
        txt_final += "\n  ⚠️ Sem dados de aprendizado disponíveis.\n"
        txt_final += _footer()
    return txt_final


# #############################################################################
#
#  2. INFRAESTRUTURA
#
# #############################################################################

def gerar_txt_infra(ibge, mun, uf):
    """Gera relatório de infraestrutura — idêntico ao original."""
    dep_id = 3  # Municipal
    dados, ano = fetch_infra(ibge, dep_id)

    if not dados:
        return (_hdr("RELATÓRIO DE INFRAESTRUTURA ESCOLAR - DADOS QEDU", mun, ano="N/D")
                + "\n  ⚠️ Sem dados.\n" + _footer())

    bloco = _hdr("RELATÓRIO DE INFRAESTRUTURA ESCOLAR - DADOS QEDU",
                  mun, rede="Municipal", ano=ano)

    # Extrair itens
    items_data = []
    for sec in dados:
        for item in sec.get("items", []):
            label = item.get("label", "")
            vals = item.get("values", [])
            if not vals:
                continue
            vm = ve = vb = None
            for v in vals:
                ent = v.get("entidade", "")
                if ent == "Municipio":
                    vm = v.get("value")
                elif ent == "Estado":
                    ve = v.get("value")
                elif ent == "Brasil":
                    vb = v.get("value")
            if vm is not None:
                items_data.append({"label": label, "mun": vm, "est": ve, "br": vb})

    # --- Estado: se não achou "Municipio", usa "Estado" como entidade principal ---
    if not items_data:
        for sec in dados:
            for item in sec.get("items", []):
                label = item.get("label", "")
                vals = item.get("values", [])
                if not vals:
                    continue
                ve = vb = None
                for v in vals:
                    ent = v.get("entidade", "")
                    if ent == "Estado":
                        ve = v.get("value")
                    elif ent == "Brasil":
                        vb = v.get("value")
                if ve is not None:
                    items_data.append({"label": label, "mun": ve, "est": None, "br": vb})

    # Filtrar relevantes
    items_rel = [i for i in items_data if i["label"] in ITENS_INFRA_RELEVANTES]
    if not items_rel:
        items_rel = items_data[:10]

    # PARTE 1: Tabela
    bloco += f"\n\nPARTE 1: TABELA COMPARATIVA\n{SUBLINE}\n\n"
    bloco += f"{'Indicador':>20} {'Município':>10} {'Estado':>7} {'Brasil':>7} {'vs Brasil':>10} {'vs Estado':>10}\n"

    for it in items_rel:
        d_br = f"{(it['mun']-it['br'])*100:+.1f}pp" if it["br"] is not None else ""
        d_est = f"{(it['mun']-it['est'])*100:+.1f}pp" if it["est"] is not None else ""
        bloco += (f"{it['label']:>20} {_pct(it['mun']):>10} {_pct(it['est']):>7} "
                  f"{_pct(it['br']):>7} {d_br:>10} {d_est:>10}\n")

    # PANORAMA COMPARATIVO
    bloco += f"\n\n\n{LINE}\nPANORAMA COMPARATIVO - ANÁLISE QUALITATIVA\n{LINE}\n"
    bloco += f"\n📊 Análise comparativa de {mun} em relação ao Estado e Brasil\n"

    abaixo_br = [i for i in items_rel if i["br"] is not None and i["mun"] < i["br"]]
    abaixo_est = [i for i in items_rel
                  if i["br"] is not None and i["mun"] >= i["br"]
                  and i["est"] is not None and i["mun"] < i["est"]]
    acima = [i for i in items_rel if i not in abaixo_br and i not in abaixo_est]

    bloco += f"\n{SUBLINE}\n🔴 INDICADORES ABAIXO DA MÉDIA NACIONAL (BRASIL)\n{SUBLINE}\n\n"
    if not abaixo_br:
        bloco += "   ✅ Nenhum indicador abaixo da média nacional.\n"
    for it in sorted(abaixo_br, key=lambda x: (x["mun"] - x["br"])):
        d = (it["mun"] - it["br"]) * 100
        bloco += f"\n   ❌ {it['label']}\n"
        bloco += f"      Município: {_pct(it['mun'])} | Estado: {_pct(it['est'])} | Brasil: {_pct(it['br'])}\n"
        bloco += f"      → {d:+.1f}pp vs Brasil\n"

    bloco += f"\n{SUBLINE}\n🟡 INDICADORES ABAIXO DA MÉDIA ESTADUAL (mas acima do Brasil)\n{SUBLINE}\n\n"
    if not abaixo_est:
        bloco += "   ✅ Nenhum indicador abaixo da média estadual (que esteja acima da nacional).\n"
    for it in abaixo_est:
        bloco += f"\n   ⚠️ {it['label']}\n"
        bloco += f"      Município: {_pct(it['mun'])} | Estado: {_pct(it['est'])} | Brasil: {_pct(it['br'])}\n"

    bloco += f"\n{SUBLINE}\n🟢 INDICADORES ACIMA DAS MÉDIAS ESTADUAL E NACIONAL\n{SUBLINE}\n\n"
    for it in acima:
        d_br = (it["mun"] - (it["br"] or 0)) * 100
        d_est = (it["mun"] - (it["est"] or 0)) * 100
        bloco += f"   ✅ {it['label']}\n"
        bloco += f"      Município: {_pct(it['mun'])} | Estado: {_pct(it['est'])} | Brasil: {_pct(it['br'])}\n"
        bloco += f"      → {d_br:+.1f}pp vs Brasil | {d_est:+.1f}pp vs Estado\n\n"

    # Resumo
    total = len(items_rel)
    bloco += f"{LINE}\n📋 RESUMO DO PANORAMA\n{LINE}\n\n"
    bloco += f"   Total de indicadores analisados: {total}\n\n"
    bloco += f"   🔴 Abaixo do Brasil:           {len(abaixo_br)} indicador(es)\n"
    bloco += f"   🟡 Abaixo do Estado:           {len(abaixo_est)} indicador(es)\n"
    bloco += f"   🟢 Acima de ambos:             {len(acima)} indicador(es)\n"

    # Conclusão
    bloco += f"\n{SUBLINE}\n💬 CONCLUSÃO\n{SUBLINE}\n\n"
    n_abr = len(abaixo_br)
    if n_abr == 0 and len(abaixo_est) == 0:
        bloco += f"   {mun} apresenta EXCELENTE infraestrutura escolar nos indicadores\n"
        bloco += f"   analisados, estando ACIMA das médias estadual e nacional em todos os itens.\n\n"
        bloco += f"   💡 Recomendação: Focar em soluções de ATUALIZAÇÃO e MODERNIZAÇÃO,\n"
        bloco += f"   já que a infraestrutura básica está bem estabelecida.\n"
    elif n_abr == 0:
        bloco += f"   {mun} apresenta BOA infraestrutura, acima da média nacional,\n"
        bloco += f"   mas com oportunidade de alcançar o patamar estadual em alguns itens.\n\n"
        bloco += f"   💡 Recomendação: Focar em equiparar ao patamar estadual.\n"
    elif n_abr <= 2:
        bloco += f"   {mun} apresenta infraestrutura PARCIALMENTE adequada,\n"
        bloco += f"   com {n_abr} indicador(es) abaixo da média nacional.\n\n"
        bloco += f"   💡 Recomendação: oportunidade de melhoria rápida.\n"
    else:
        bloco += f"   {mun} apresenta DÉFICIT significativo de infraestrutura,\n"
        bloco += f"   com {n_abr} indicadores abaixo da média nacional.\n\n"
        bloco += f"   💡 Recomendação: Priorizar INFRAESTRUTURA BÁSICA — grande potencial de mercado.\n"

    bloco += _footer("QEdu (qedu.org.br)")
    return bloco


# #############################################################################
#
#  3. CENSO ESCOLAR
#
# #############################################################################

def gerar_txt_censo(ibge, mun, uf):
    """Gera relatório de Censo Escolar — idêntico ao original."""
    dep_id = 3  # Municipal
    dados, ano = fetch_censo(ibge, dep_id)

    if not dados or "censo" not in dados:
        return (_hdr("RELATÓRIO DO CENSO ESCOLAR - DADOS QEDU", mun)
                + "\n  ⚠️ Sem dados.\n" + _footer())

    c = dados["censo"]
    qtd_escolas = c.get("qtd_escolas", 0)

    # Matrículas por etapa
    mat_etapas = {}
    total_mat = 0
    for campo, label in CAMPOS_MATRICULA:
        v = c.get(campo)
        if v is not None:
            mat_etapas[label] = v
            total_mat += v

    media_alunos = total_mat / qtd_escolas if qtd_escolas else 0

    bloco = _hdr("RELATÓRIO DO CENSO ESCOLAR - DADOS QEDU", mun)
    bloco += "\n"

    # PARTE 1: Resumo geral
    bloco += f"\n{'*'*80}\nPARTE 1: RESUMO GERAL\n{'*'*80}\n\n"
    bloco += f"{'Indicador':>25} {'Valor':>22}\n"
    bloco += f"{'Número de Escolas':>25} {qtd_escolas:>22,}\n"
    bloco += f"{'Total de Matrículas':>25} {total_mat:>22,}\n"
    bloco += f"{'Média de Alunos por Escola':>25} {media_alunos:>22.1f}\n"
    bloco += f"{'Rede':>25} {'Municipal':>22}\n"
    bloco += f"{'Localização':>25} {'Urbana e Rural (todas)':>22}\n"
    bloco += f"{'Ano de Referência':>25} {ano:>22}\n"

    # PARTE 2: Matrículas por etapa
    bloco += f"\n\n{'*'*80}\nPARTE 2: MATRÍCULAS POR ETAPA DE ENSINO\n{'*'*80}\n\n"
    bloco += f"{'Etapa de Ensino':>25} {'Matrículas':>11} {'% do Total':>11} {'Média por Escola':>17}\n"
    for label, v in mat_etapas.items():
        p = v / total_mat * 100 if total_mat else 0
        m = v / qtd_escolas if qtd_escolas else 0
        bloco += f"{label:>25} {v:>11,} {p:>10.1f}% {m:>17.1f}\n"
    bloco += f"{'TOTAL':>25} {total_mat:>11,} {'100%':>11} {media_alunos:>17.1f}\n"

    # PARTE 3: Matrículas por série
    bloco += f"\n\n{'*'*80}\nPARTE 3: MATRÍCULAS POR SÉRIE/ANO\n{'*'*80}\n\n"
    bloco += f"{'Ciclo':>15} {'Série/Ano':>10} {'Matrículas':>11}\n"
    subtotais = {}
    for campo, label, ciclo in CAMPOS_SERIES:
        v = c.get(campo)
        if v is not None:
            bloco += f"{ciclo:>15} {label:>10} {v:>11,}\n"
            subtotais[ciclo] = subtotais.get(ciclo, 0) + v
    for ciclo, sub in subtotais.items():
        bloco += f"{ciclo:>15} {'Subtotal':>10} {sub:>11,}\n"

    # ANÁLISE QUALITATIVA
    bloco += f"\n\n{LINE}\nANÁLISE QUALITATIVA - CENSO ESCOLAR\n{LINE}\n"
    bloco += f"\n📍 Território: {mun}\n🏫 Rede: Municipal\n"
    bloco += f"📍 Localização: Urbana e Rural (todas)\n"
    bloco += f"📅 Ano de referência: {ano}\n"

    bloco += f"\n{SUBLINE}\n📊 VISÃO GERAL\n{SUBLINE}\n\n"
    bloco += f"   • Total de Escolas: {qtd_escolas:,}\n"
    bloco += f"   • Total de Matrículas: {total_mat:,}\n"
    bloco += f"   • Média de alunos por escola: {media_alunos:.1f}\n"

    bloco += f"\n{SUBLINE}\n📚 DISTRIBUIÇÃO POR ETAPA DE ENSINO\n{SUBLINE}\n\n"
    for label, v in sorted(mat_etapas.items(), key=lambda x: -x[1]):
        p = v / total_mat * 100 if total_mat else 0
        bloco += f"   • {label}: {v:,} matrículas ({p:.1f}%)\n"
    if mat_etapas:
        maior_etapa = max(mat_etapas, key=mat_etapas.get)
        bloco += f"\n   📌 Maior concentração: {maior_etapa}\n"
        bloco += f"      com {mat_etapas[maior_etapa]:,} matrículas\n"

    # Insights comerciais
    bloco += f"\n{SUBLINE}\n💡 INSIGHTS PARA ABORDAGEM COMERCIAL\n{SUBLINE}\n\n"
    mat_infantil = (c.get("matriculas_creche") or 0) + (c.get("matriculas_pre_escolar") or c.get("matriculas_pre_escola") or 0)
    mat_fund = (c.get("matriculas_anos_iniciais") or 0) + (c.get("matriculas_anos_finais") or 0)
    mat_em = c.get("matriculas_ensino_medio") or 0
    mat_eja = c.get("matriculas_eja") or 0
    mat_especial = c.get("matriculas_educacao_especial") or 0

    if mat_infantil:
        bloco += f"   👶 EDUCAÇÃO INFANTIL: {mat_infantil:,} matrículas\n"
        bloco += f"      → Potencial para: materiais lúdicos, livros infantis, brinquedos educativos\n\n"
    if mat_fund:
        bloco += f"   📖 ENSINO FUNDAMENTAL: {mat_fund:,} matrículas\n"
        bloco += f"      • Anos Iniciais: {c.get('matriculas_anos_iniciais', 0):,}\n"
        bloco += f"      • Anos Finais: {c.get('matriculas_anos_finais', 0):,}\n"
        bloco += f"      → Potencial para: livros didáticos, paradidáticos, materiais de alfabetização\n\n"
    if mat_em:
        bloco += f"   🎓 ENSINO MÉDIO: {mat_em:,} matrículas\n"
        bloco += f"      → Potencial para: materiais preparatórios ENEM/vestibular, livros técnicos\n\n"
    if mat_eja:
        bloco += f"   📚 EJA: {mat_eja:,} matrículas\n"
        bloco += f"      → Potencial para: materiais específicos para jovens e adultos\n\n"
    if mat_especial:
        bloco += f"   ♿ EDUCAÇÃO ESPECIAL: {mat_especial:,} matrículas\n"
        bloco += f"      → Potencial para: materiais adaptados, recursos de acessibilidade\n\n"

    bloco += _footer("QEdu - Censo Escolar (qedu.org.br)")
    return bloco


# #############################################################################
#
#  4. IDEB  (CSV)
#
# #############################################################################

def _trend_slope(anos, valores):
    """Tendência linear (pts/ano)."""
    if np is not None and len(anos) >= 2:
        try:
            return float(np.polyfit(anos, valores, 1)[0])
        except Exception:
            pass
    if len(anos) >= 2:
        return (valores[-1] - valores[0]) / (anos[-1] - anos[0])
    return 0.0


def gerar_txt_ideb(ibge, mun, uf):
    """Gera relatório IDEB — idêntico ao original (CSV-based)."""
    df_mun, df_uf, brasil_stats = load_ideb(ibge)

    if df_mun is None or df_mun.empty:
        return (_hdr("RELATÓRIO DE ANÁLISE IDEB", mun)
                + "\n  ⚠️ Sem dados IDEB disponíveis.\n" + _footer("IDEB/SAEB - INEP/MEC"))

    txt = f"{LINE}\nRELATÓRIO DE ANÁLISE IDEB\n"
    txt += f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n{LINE}\n"
    txt += f"\n📍 ESCOPO DA ANÁLISE\n{'-'*40}\n"
    txt += f"Município: {mun}\nEstado: {uf}\n"
    txt += f"Comparativo: Município vs Estado vs Brasil\n"
    txt += f"Nota: Ensino Médio usa dados da rede estadual (não há IDEB municipal para EM)\n"

    segmentos = ["anos iniciais", "anos finais", "ensino medio"]
    esferas   = ["municipal", "estadual"]

    for esfera in esferas:
        df_esf = df_mun[df_mun["esfera"] == esfera] if "esfera" in df_mun.columns else pd.DataFrame()
        if df_esf.empty:
            continue

        # Para estadual, só EM
        segs_usar = segmentos if esfera == "municipal" else ["ensino medio"]

        has_data = False
        for seg in segs_usar:
            df_seg = df_esf[df_esf["segmento"] == seg] if "segmento" in df_esf.columns else pd.DataFrame()
            df_ideb = df_seg[df_seg["indicador_tipo_nome"] == "IDEB"].sort_values("ano") if not df_seg.empty else pd.DataFrame()
            if not df_ideb.empty:
                has_data = True
                break
        if not has_data:
            continue

        insights_esfera = []

        txt += f"\n📊 HISTÓRICO IDEB POR SEGMENTO\n{'-'*40}\n"

        for seg in segs_usar:
            df_seg = df_esf[df_esf["segmento"] == seg] if "segmento" in df_esf.columns else pd.DataFrame()
            df_ideb = df_seg[df_seg["indicador_tipo_nome"] == "IDEB"].sort_values("ano") if not df_seg.empty else pd.DataFrame()

            if df_ideb.empty:
                continue

            seg_display = SEGMENTOS_DISPLAY.get(seg, seg.upper())
            if esfera == "estadual" and seg == "ensino medio":
                seg_display = "ENSINO MEDIO (REDE ESTADUAL)"

            txt += f"\n▶ {seg_display}\n{SUBLINE}\n"

            # Dados do estado
            df_uf_seg = pd.DataFrame()
            if df_uf is not None and not df_uf.empty:
                mask_uf = (df_uf["indicador_tipo_nome"] == "IDEB")
                if "segmento" in df_uf.columns:
                    mask_uf = mask_uf & (df_uf["segmento"] == seg)
                df_uf_seg = df_uf[mask_uf].sort_values("ano")

            # Dados do Brasil
            df_br_seg = pd.DataFrame()
            if brasil_stats is not None and not brasil_stats.empty:
                mask_br = (brasil_stats["indicador_tipo_nome"] == "IDEB")
                if "segmento" in brasil_stats.columns:
                    mask_br = mask_br & (brasil_stats["segmento"] == seg)
                df_br_seg = brasil_stats[mask_br].sort_values("ano")

            # Cabeçalho da tabela depende se é municipal (Município col) ou estadual (Estado col)
            if esfera == "municipal":
                txt += f"{'Ano':<8} {'Município':<12} {'Estado':<12} {'Brasil(M)':<12} {'vs Estado':<12} {'vs Brasil':<12}\n"
            else:
                txt += f"{'Ano':<8} {'Estado':<12} {'Brasil(M)':<12} {'Brasil(Md)':<12} {'vs Média':<12} {'vs Mediana':<12}\n"
            txt += f"{SUBLINE}\n"

            anos_list = []
            vals_list = []

            for _, row in df_ideb.iterrows():
                a = int(row["ano"])
                vm = row["valor_numerico"]
                anos_list.append(a)
                vals_list.append(float(vm) if pd.notna(vm) else 0)

                ve = None
                if not df_uf_seg.empty:
                    uf_row = df_uf_seg[df_uf_seg["ano"] == a]
                    if not uf_row.empty:
                        ve = uf_row.iloc[0]["valor_numerico"]

                vb_mean = vb_med = None
                if not df_br_seg.empty:
                    br_row = df_br_seg[df_br_seg["ano"] == a]
                    if not br_row.empty:
                        vb_mean = br_row.iloc[0]["mean"]
                        vb_med = br_row.iloc[0]["median"] if "median" in br_row.columns else None

                if esfera == "municipal":
                    d_est = f"{vm-ve:+.2f}" if ve is not None and pd.notna(vm) else ""
                    d_br  = f"{vm-vb_mean:+.2f}" if vb_mean is not None and pd.notna(vm) else ""
                    txt += f"{a:<8} {_val(vm):<12} {_val(ve):<12} {_val(vb_mean):<12} {d_est:<12} {d_br:<12}\n"
                else:
                    d_mean = f"{vm-vb_mean:+.2f}" if vb_mean is not None and pd.notna(vm) else ""
                    d_med  = f"{vm-vb_med:+.2f}" if vb_med is not None and pd.notna(vm) else ""
                    txt += f"{a:<8} {_val(vm):<12} {_val(vb_mean):<12} {_val(vb_med):<12} {d_mean:<12} {d_med:<12}\n"

            # Calcular insights
            vals_clean = [v for v in vals_list if v > 0]
            anos_clean = [anos_list[i] for i, v in enumerate(vals_list) if v > 0]

            if len(vals_clean) >= 2:
                variacao = ((vals_clean[-1] - vals_clean[0]) / vals_clean[0]) * 100 if vals_clean[0] != 0 else 0
                trend = _trend_slope(anos_clean, vals_clean)

                insight_lines = []

                # vs Estado (municipal only)
                if esfera == "municipal" and not df_uf_seg.empty:
                    merged = pd.merge(df_ideb[["ano", "valor_numerico"]],
                                      df_uf_seg[["ano", "valor_numerico"]],
                                      on="ano", suffixes=("_mun", "_est"))
                    if not merged.empty:
                        diff_est = (merged["valor_numerico_mun"] - merged["valor_numerico_est"]).mean()
                        if diff_est > 0.3:
                            insight_lines.append(f"  ✅ Município supera média estadual em {diff_est:.2f} pontos")
                        elif diff_est < -0.3:
                            insight_lines.append(f"  ⚠️ Município está {abs(diff_est):.2f} pontos abaixo do estado")
                        else:
                            insight_lines.append(f"  ➡️ Município próximo do estado ({diff_est:+.2f} pontos)")

                # vs Brasil
                if not df_br_seg.empty:
                    if esfera == "municipal":
                        merged_br = pd.merge(df_ideb[["ano", "valor_numerico"]],
                                             df_br_seg[["ano", "mean"]], on="ano")
                        if not merged_br.empty:
                            diff_br = (merged_br["valor_numerico"] - merged_br["mean"]).mean()
                            if diff_br > 0.3:
                                insight_lines.append(f"  ✅ Município supera média nacional em {diff_br:.2f} pontos")
                            elif diff_br < -0.3:
                                insight_lines.append(f"  ⚠️ Município está {abs(diff_br):.2f} pontos abaixo da média nacional")
                    else:
                        merged_br = pd.merge(df_ideb[["ano", "valor_numerico"]],
                                             df_br_seg[["ano", "mean", "median"]], on="ano")
                        if not merged_br.empty:
                            diff_mean = (merged_br["valor_numerico"] - merged_br["mean"]).mean()
                            diff_med = (merged_br["valor_numerico"] - merged_br["median"]).mean()
                            if diff_mean > 0.3:
                                insight_lines.append(f"  ✅ Supera média nacional em {diff_mean:.2f} pontos")
                            if diff_med > 0.3:
                                insight_lines.append(f"  ✅ Supera mediana nacional em {diff_med:.2f} pontos")

                # Tendência
                if trend > 0.05:
                    insight_lines.append(f"  📈 Tendência de crescimento (+{trend:.3f}/ano)")
                elif trend < -0.05:
                    insight_lines.append(f"  📉 Tendência de queda ({trend:.3f}/ano)")
                else:
                    insight_lines.append(f"  ➡️ Tendência estável ({trend:+.3f}/ano)")

                # Crescimento expressivo
                if variacao > 20:
                    insight_lines.append(f"  🚀 Crescimento expressivo de {variacao:.1f}% no período")
                elif variacao < -10:
                    insight_lines.append(f"  🔻 Queda de {abs(variacao):.1f}% no período")

                # Pandemia
                anos_dict = dict(zip(anos_clean, vals_clean))
                if 2019 in anos_dict and 2021 in anos_dict:
                    d_pan = anos_dict[2021] - anos_dict[2019]
                    if d_pan < -0.3:
                        insight_lines.append(f"  🦠 Impacto da pandemia detectado ({d_pan:+.1f} pontos 2019→2021)")
                    elif d_pan > 0.3:
                        insight_lines.append(f"  💪 Resiliência na pandemia (crescimento de {d_pan:.1f} pontos 2019→2021)")

                # Recuperação
                if 2021 in anos_dict and 2023 in anos_dict:
                    d_rec = anos_dict[2023] - anos_dict[2021]
                    if d_rec > 0.2:
                        insight_lines.append(f"  🔄 {seg_display} : Recuperação pós-pandemia (+{d_rec:.1f} pontos 2021→2023)")
                    elif d_rec < -0.2:
                        insight_lines.append(f"  ⚠️ {seg_display} : Continuidade de queda pós-pandemia ({d_rec:+.1f} pontos 2021→2023)")

                stats_dict = {
                    "variacao": variacao,
                    "trend": trend,
                    "max": max(vals_clean),
                    "min": min(vals_clean),
                }
                # Adicionar médias vs referência
                if esfera == "municipal" and not df_uf_seg.empty:
                    merged_tmp = pd.merge(df_ideb[["ano", "valor_numerico"]],
                                          df_uf_seg[["ano", "valor_numerico"]],
                                          on="ano", suffixes=("_mun", "_est"))
                    if not merged_tmp.empty:
                        stats_dict["mun_vs_estado"] = (merged_tmp["valor_numerico_mun"] - merged_tmp["valor_numerico_est"]).mean()
                if not df_br_seg.empty:
                    merged_tmp2 = pd.merge(df_ideb[["ano", "valor_numerico"]],
                                           df_br_seg[["ano", "mean"]], on="ano")
                    if not merged_tmp2.empty:
                        stats_dict["mun_vs_brasil"] = (merged_tmp2["valor_numerico"] - merged_tmp2["mean"]).mean()

                insights_esfera.append((seg_display, insight_lines, stats_dict))

        # Bloco de insights
        if insights_esfera:
            txt += f"\n\n💡 INSIGHTS E OBSERVAÇÕES\n{LINE}\n"
            for seg_label, lines, _ in insights_esfera:
                txt += f"\n▶ {seg_label}\n{'-'*40}\n"
                for l in lines:
                    txt += l + "\n"

            txt += f"\n\n📈 ESTATÍSTICAS ADICIONAIS\n{LINE}\n"
            for seg_label, _, stats in insights_esfera:
                txt += f"\n▶ {seg_label}\n{'-'*40}\n"
                txt += f"  • Variação total (%): {stats['variacao']:.2f}\n"
                txt += f"  • Tendência (pts/ano): {stats['trend']:.2f}\n"
                if "mun_vs_estado" in stats:
                    txt += f"  • Município vs Estado: {stats['mun_vs_estado']:.2f}\n"
                if "mun_vs_brasil" in stats:
                    txt += f"  • Município vs Brasil: {stats['mun_vs_brasil']:.2f}\n"
                txt += f"  • Maior valor: {stats['max']:.2f}\n"
                txt += f"  • Menor valor: {stats['min']:.2f}\n"

    txt += f"\n{LINE}\nFim do Relatório\n"
    return txt


# #############################################################################
#
#  5. TAXA DE RENDIMENTO
#
# #############################################################################

def _classificar_taxa(valor, tipo):
    """Classifica taxa (valor em percentual 0-100 ou decimal 0-1)."""
    if valor is None:
        return "Sem dados", "⚪"
    v = valor * 100 if abs(valor) <= 1.01 else valor
    if tipo == "aprovacao":
        if v >= 98: return "Excelente", "✅"
        if v >= 95: return "Bom", "🟢"
        if v >= 90: return "Regular", "🟡"
        return "Crítico", "🔴"
    elif tipo == "reprovacao":
        if v <= 1:  return "Excelente", "✅"
        if v <= 3:  return "Bom", "🟢"
        if v <= 5:  return "Regular", "🟡"
        return "Crítico", "🔴"
    else:  # abandono
        if v <= 0.5: return "Excelente", "✅"
        if v <= 1.5: return "Bom", "🟢"
        if v <= 3:   return "Regular", "🟡"
        return "Crítico", "🔴"


def _safe_taxa(v):
    """Converte taxa para % string."""
    if v is None:
        return "sem dados"
    val = v * 100 if abs(v) <= 1.01 else v
    return f"{val:.1f}%"


def _get_rendimento(reg):
    """Extrai aprovados/reprovados/abandonos do registro de taxa."""
    r = reg.get("rendimento", reg)
    aprov  = r.get("aprovados")
    reprov = r.get("reprovados")
    aband  = r.get("abandonos")
    return aprov, reprov, aband


def _get_ultimo_reg(regs):
    """Retorna último registro (por ano) de uma lista."""
    if not regs or not isinstance(regs, list):
        return None
    return sorted(regs, key=lambda x: x.get("ano", 0))[-1]


def _get_nome_estado(dados):
    """Tenta extrair nome do estado dos dados de taxa."""
    regs = dados.get("estado") or dados.get("parent") or []
    if isinstance(regs, list) and regs:
        r = regs[0]
        rend = r.get("rendimento", r)
        terr = rend.get("territorio", {})
        return terr.get("nome", "Estado")
    return "Estado"


def gerar_txt_taxa(ibge, mun, uf):
    """Gera relatório de Taxa de Rendimento — idêntico ao original."""

    # Coletar dados para todos os ciclos
    etapas_dados = {}
    ano_ref = 0

    for cid, cnome in CICLOS.items():
        dados, a = fetch_taxa(ibge, cid)
        if not dados:
            etapas_dados[cid] = None
            continue
        if a > ano_ref:
            ano_ref = a

        reg_mun = _get_ultimo_reg(dados.get("municipio", []))
        aprov, reprov, aband = _get_rendimento(reg_mun) if reg_mun else (None, None, None)

        etapas_dados[cid] = {
            "aprovados": aprov, "reprovados": reprov, "abandonos": aband,
            "nome": cnome, "dados_full": dados, "ano": a,
        }

    if not ano_ref:
        return (_hdr("RELATÓRIO DE TAXAS DE RENDIMENTO - DADOS QEDU", mun)
                + "\n  ⚠️ Sem dados.\n" + _footer())

    # Detectar período histórico
    anos_hist = set()
    for ed in etapas_dados.values():
        if ed and ed.get("dados_full"):
            for r in ed["dados_full"].get("municipio", []):
                if isinstance(r, dict) and r.get("ano"):
                    anos_hist.add(r["ano"])
    anos_hist = sorted(anos_hist)
    periodo = f"{anos_hist[0]} a {anos_hist[-1]}" if len(anos_hist) >= 2 else str(ano_ref)

    bloco = _hdr("RELATÓRIO DE TAXAS DE RENDIMENTO - DADOS QEDU",
                  mun, ano=ano_ref, periodo=periodo)
    bloco += "\n"

    # PARTE 1: Taxas por etapa
    bloco += f"\n{'*'*80}\nPARTE 1: TAXAS DE RENDIMENTO POR ETAPA\n{'*'*80}\n\n"
    bloco += f"{'Etapa':>28} {'Aprovação':>10} {'Reprovação':>11} {'Abandono':>9}\n"

    for cid, cnome in CICLOS.items():
        ed = etapas_dados.get(cid)
        if ed and ed.get("aprovados") is not None:
            bloco += f"{cnome:>28} {_safe_taxa(ed['aprovados']):>10} {_safe_taxa(ed['reprovados']):>11} {_safe_taxa(ed['abandonos']):>9}\n"
        else:
            bloco += f"{cnome:>28} {'sem dados':>10} {'sem dados':>11} {'sem dados':>9}\n"

    # PARTE 2: Comparativo
    bloco += f"\n\n{'*'*80}\nPARTE 2: COMPARATIVO {ano_ref} - MUNICÍPIO vs ESTADO vs BRASIL\n{'*'*80}\n\n"

    # Usar primeiro ciclo com dados
    nome_estado = "Estado"
    for cid in CICLOS:
        ed = etapas_dados.get(cid)
        if ed and ed.get("dados_full"):
            dados_comp = ed["dados_full"]
            nome_estado = _get_nome_estado(dados_comp)

            rm = _get_ultimo_reg(dados_comp.get("municipio", []))
            re_ = _get_ultimo_reg(dados_comp.get("estado", []))
            rb = _get_ultimo_reg(dados_comp.get("brasil", []))

            if rm:
                bloco += f"{'Indicador':>10} {'Município':>10} {'Estado':>7} {'Brasil':>7} {'vs Estado':>10} {'vs Brasil':>10}\n"
                am, rpm, abm = _get_rendimento(rm)
                ae, rpe, abe = _get_rendimento(re_) if re_ else (None, None, None)
                ab_, rpb, abb = _get_rendimento(rb) if rb else (None, None, None)

                for ind, vm, ve, vb in [("Aprovação", am, ae, ab_),
                                        ("Reprovação", rpm, rpe, rpb),
                                        ("Abandono", abm, abe, abb)]:
                    d_est = _pp(vm - ve if vm is not None and ve is not None else None, 2)
                    d_br  = _pp(vm - vb if vm is not None and vb is not None else None, 2)
                    bloco += f"{ind:>10} {_safe_taxa(vm):>10} {_safe_taxa(ve):>7} {_safe_taxa(vb):>7} {d_est:>10} {d_br:>10}\n"
            break

    # PARTE 3: Evolução histórica
    bloco += f"\n\n{'*'*80}\nPARTE 3: EVOLUÇÃO HISTÓRICA ({periodo})\n{'*'*80}\n\n"

    # Usar primeiro ciclo com dados completos
    for cid in CICLOS:
        ed = etapas_dados.get(cid)
        if not ed or not ed.get("dados_full"):
            continue
        dados_hist = ed["dados_full"]

        # anos disponíveis
        all_anos = sorted(set(
            r.get("ano") for r in dados_hist.get("municipio", [])
            if isinstance(r, dict) and r.get("ano")
        ))
        if len(all_anos) > 5:
            all_anos = all_anos[-3:]  # últimos 3

        if len(all_anos) < 2:
            continue

        col_anos = "".join(f"{a:>6}" for a in all_anos)
        bloco += f"{'Indicador':>10} {'Entidade':>10} {col_anos} {'Variação':>10}\n"

        for ind, campo in [("Aprovação", "aprovados"), ("Reprovação", "reprovados"), ("Abandono", "abandonos")]:
            for escopo, nome_ent in [("municipio", "Município"), ("estado", nome_estado), ("brasil", "Brasil")]:
                regs = dados_hist.get(escopo, [])
                if not isinstance(regs, list):
                    continue
                regs_dict = {}
                for reg in regs:
                    _, _, _ = _get_rendimento(reg)
                    r = reg.get("rendimento", reg)
                    regs_dict[reg.get("ano")] = r.get(campo)

                vals_str = ""
                first_v = last_v = None
                for a in all_anos:
                    v = regs_dict.get(a)
                    vals_str += f"{_safe_taxa(v):>6}"
                    if v is not None:
                        if first_v is None:
                            first_v = v
                        last_v = v

                var_str = ""
                if first_v is not None and last_v is not None:
                    var_str = _pp(last_v - first_v, 2)

                bloco += f"{ind:>10} {nome_ent:>10} {vals_str} {var_str:>10}\n"
        break

    # ANÁLISE QUALITATIVA
    bloco += f"\n\n{LINE}\nANÁLISE QUALITATIVA - TAXAS DE RENDIMENTO\n{LINE}\n"

    alertas = []
    destaques = []

    # Diagnóstico por etapa
    bloco += f"\n{SUBLINE}\n📊 DIAGNÓSTICO POR ETAPA DE ENSINO\n{SUBLINE}\n"

    for cid, cnome in CICLOS.items():
        ed = etapas_dados.get(cid)
        if not ed or ed.get("aprovados") is None:
            bloco += f"\n   📌 {cnome.upper()}: sem dados disponíveis\n"
            continue

        aprov = ed["aprovados"]
        reprov = ed["reprovados"]
        aband = ed["abandonos"]

        bloco += f"\n   📌 {cnome.upper()}\n\n"
        ac, ae = _classificar_taxa(aprov, "aprovacao")
        rc, re2 = _classificar_taxa(reprov, "reprovacao")
        bc, be = _classificar_taxa(aband, "abandono")

        bloco += f"      {ae} Aprovação: {_safe_taxa(aprov)} - {ac}\n"
        bloco += f"      {re2} Reprovação: {_safe_taxa(reprov)} - {rc}\n"
        bloco += f"      {be} Abandono: {_safe_taxa(aband)} - {bc}\n"

        # Alertas
        rv = reprov * 100 if reprov and abs(reprov) <= 1.01 else (reprov or 0)
        av = aband * 100 if aband and abs(aband) <= 1.01 else (aband or 0)
        apv = aprov * 100 if aprov and abs(aprov) <= 1.01 else (aprov or 0)
        if rv > 5:
            alertas.append(f"{cnome}: Alta reprovação ({rv:.1f}%)")
        if av > 3:
            alertas.append(f"{cnome}: Alto abandono ({av:.1f}%)")
        if apv >= 98:
            destaques.append(f"{cnome}: Excelente aprovação ({apv:.1f}%)")

    # Comparativo qualitativo
    bloco += f"\n{SUBLINE}\n📈 COMPARATIVO {ano_ref}: {mun.upper()} vs {nome_estado.upper()} vs BRASIL\n{SUBLINE}\n"

    for cid in CICLOS:
        ed = etapas_dados.get(cid)
        if not ed or not ed.get("dados_full"):
            continue
        dados_comp = ed["dados_full"]

        rm = _get_ultimo_reg(dados_comp.get("municipio", []))
        re_ = _get_ultimo_reg(dados_comp.get("estado", []))
        rb = _get_ultimo_reg(dados_comp.get("brasil", []))
        if not rm:
            continue

        am, rpm, abm = _get_rendimento(rm)
        ae_, rpe, abe = _get_rendimento(re_) if re_ else (None, None, None)
        ab_, rpb, abb = _get_rendimento(rb) if rb else (None, None, None)

        for ind, vm, ve, vb, campo in [
            ("Aprovação", am, ae_, ab_, "aprovados"),
            ("Reprovação", rpm, rpe, rpb, "reprovados"),
            ("Abandono", abm, abe, abb, "abandonos"),
        ]:
            bloco += f"\n   {ind}:\n"
            bloco += f"      • {mun}: {_safe_taxa(vm)}\n"
            if ve is not None:
                bloco += f"      • {nome_estado}: {_safe_taxa(ve)}\n"
            if vb is not None:
                bloco += f"      • Brasil: {_safe_taxa(vb)}\n"

            if vm is not None and ve is not None:
                d = vm - ve
                dpp = d * 100 if abs(d) <= 1.01 else d
                if campo == "aprovados":
                    e = "✅ acima" if dpp > 0.005 else "🔴 abaixo" if dpp < -0.005 else "➡️ igual"
                else:
                    e = "✅ melhor" if dpp < -0.005 else "🔴 pior" if dpp > 0.005 else "➡️ igual"
                bloco += f"      → {e} do estado ({dpp:+.2f}pp)\n"
            if vm is not None and vb is not None:
                d = vm - vb
                dpp = d * 100 if abs(d) <= 1.01 else d
                if campo == "aprovados":
                    e = "✅ acima" if dpp > 0.005 else "🔴 abaixo" if dpp < -0.005 else "➡️ igual"
                else:
                    e = "✅ melhor" if dpp < -0.005 else "🔴 pior" if dpp > 0.005 else "➡️ igual"
                bloco += f"      → {e} do Brasil ({dpp:+.2f}pp)\n"
        break

    # Evolução temporal qualitativa
    for cid in CICLOS:
        ed = etapas_dados.get(cid)
        if not ed or not ed.get("dados_full"):
            continue
        regs = sorted(ed["dados_full"].get("municipio", []),
                      key=lambda x: x.get("ano", 0))
        if len(regs) >= 2:
            a_first, r_first, ab_first = _get_rendimento(regs[0])
            a_last, r_last, ab_last   = _get_rendimento(regs[-1])

            bloco += f"\n{SUBLINE}\n📅 EVOLUÇÃO TEMPORAL ({periodo})\n{SUBLINE}\n"

            if a_first is not None and a_last is not None:
                d = (a_last - a_first)
                dpp = d * 100 if abs(d) <= 1.01 else d
                e = "📈 Melhora" if dpp > 0.005 else "📉 Piora" if dpp < -0.005 else "➡️ Estável"
                bloco += f"\n   Aprovação: {_safe_taxa(a_first)} → {_safe_taxa(a_last)} ({dpp:+.2f}pp) {e}\n"

            if r_first is not None and r_last is not None:
                d = (r_last - r_first)
                dpp = d * 100 if abs(d) <= 1.01 else d
                e = "📈 Melhora" if dpp < -0.005 else "📉 Piora" if dpp > 0.005 else "➡️ Estável"
                bloco += f"   Reprovação: {_safe_taxa(r_first)} → {_safe_taxa(r_last)} ({dpp:+.2f}pp) {e}\n"

            if ab_first is not None and ab_last is not None:
                d = (ab_last - ab_first)
                dpp = d * 100 if abs(d) <= 1.01 else d
                e = "📈 Melhora" if dpp < -0.005 else "📉 Piora" if dpp > 0.005 else "➡️ Estável"
                bloco += f"   Abandono: {_safe_taxa(ab_first)} → {_safe_taxa(ab_last)} ({dpp:+.2f}pp) {e}\n"
        break

    # Alertas
    if alertas:
        bloco += f"\n{SUBLINE}\n🚨 ALERTAS\n{SUBLINE}\n\n"
        for al in alertas:
            bloco += f"   ⚠️ {al}\n"

    # Destaques
    if destaques:
        bloco += f"\n{SUBLINE}\n🌟 DESTAQUES POSITIVOS\n{SUBLINE}\n\n"
        for d in destaques:
            bloco += f"   ✅ {d}\n"

    # Conclusão
    bloco += f"\n{SUBLINE}\n💡 CONCLUSÃO E RECOMENDAÇÕES\n{SUBLINE}\n\n"
    if len(alertas) == 0:
        bloco += f"   ✅ {mun} apresenta EXCELENTES taxas de rendimento escolar.\n"
        bloco += f"   O fluxo escolar está saudável, com baixa reprovação e abandono.\n\n"
        bloco += f"   💼 Abordagem comercial: Focar em soluções de EXCELÊNCIA\n"
        bloco += f"   e enriquecimento curricular para manter os bons indicadores.\n"
    elif len(alertas) <= 2:
        bloco += f"   ⚠️ {mun} apresenta BOAS taxas, com pontos de atenção.\n\n"
        bloco += f"   💼 Abordagem: soluções direcionadas para etapas problemáticas.\n"
    else:
        bloco += f"   🔴 {mun} apresenta DESAFIOS no fluxo escolar.\n\n"
        bloco += f"   💼 Abordagem: RECUPERAÇÃO e reforço escolar. Grande potencial de mercado.\n"

    bloco += _footer("QEdu - Taxas de Rendimento / INEP (qedu.org.br)")
    return bloco


# #############################################################################
#
#  DADOS ESTRUTURADOS (JSON-friendly para IA)
#
# #############################################################################

def coletar_dados_estruturados(ibge, mun, uf):
    """Coleta dados numéricos estruturados — reutiliza cache das chamadas já feitas."""
    dados = {"entidade": mun, "uf": uf, "tipo": "estado" if is_estado(ibge) else "municipio"}

    # --- Aprendizado ---
    dep_id = 5
    aprendizado = {}
    for cid, cnome in CICLOS.items():
        raw = fetch_aprendizado(ibge, dep_id, cid)
        mun_recs, est_recs, br_recs = _extrair_territorios(raw, ibge)
        if not mun_recs:
            continue
        ultimo = sorted(mun_recs, key=lambda x: x.get("ano", 0))[-1]
        ultimo_br = sorted(br_recs, key=lambda x: x.get("ano", 0))[-1] if br_recs else None
        ciclo_d = {"ano": ultimo.get("ano"), "disciplinas": {}}
        for disc, disc_nome in DISCIPLINAS.items():
            ent_d = {}
            for nk, _ in NIVEIS:
                v = _adeq(ultimo, disc) if nk == "adequado" else ultimo.get(f"{disc}_{nk}")
                if v is not None:
                    ent_d[nk] = round(v * 100 if abs(v) <= 1.01 else v, 2)
            br_d = {}
            if ultimo_br:
                for nk, _ in NIVEIS:
                    v = _adeq(ultimo_br, disc) if nk == "adequado" else ultimo_br.get(f"{disc}_{nk}")
                    if v is not None:
                        br_d[nk] = round(v * 100 if abs(v) <= 1.01 else v, 2)
            ciclo_d["disciplinas"][disc_nome] = {"entidade": ent_d, "brasil": br_d}
        aprendizado[cid] = ciclo_d
    if aprendizado:
        dados["aprendizado"] = aprendizado

    # --- Censo ---
    dep_id = 3
    raw_c, ano_c = fetch_censo(ibge, dep_id)
    if raw_c and "censo" in raw_c:
        c = raw_c["censo"]
        censo_d = {"ano": ano_c, "qtd_escolas": c.get("qtd_escolas"), "matriculas": {}}
        total = 0
        for campo, label in CAMPOS_MATRICULA:
            v = c.get(campo)
            if v is not None:
                censo_d["matriculas"][label] = v
                total += v
        censo_d["total_matriculas"] = total
        dados["censo"] = censo_d

    # --- Infra ---
    dep_id = 3
    raw_i, ano_i = fetch_infra(ibge, dep_id)
    if raw_i:
        infra_d = {"ano": ano_i, "indicadores": {}}
        for sec in raw_i:
            for item in sec.get("items", []):
                label = item.get("label", "")
                for v in item.get("values", []):
                    ent = v.get("entidade", "")
                    val = v.get("value")
                    if val is not None:
                        if label not in infra_d["indicadores"]:
                            infra_d["indicadores"][label] = {}
                        key = ent.lower()
                        infra_d["indicadores"][label][key] = round(val * 100, 2)
        dados["infra"] = infra_d

    # --- Taxa de Rendimento ---
    taxa_d = {}
    for cid, cnome in CICLOS.items():
        raw_t, ano_t = fetch_taxa(ibge, cid)
        if raw_t:
            reg = _get_ultimo_reg(raw_t.get("municipio", []))
            if reg:
                ap, rp, ab = _get_rendimento(reg)
                taxa_d[cid] = {
                    "nome": cnome, "ano": ano_t,
                    "aprovacao_pct": round(ap * 100, 2) if ap else None,
                    "reprovacao_pct": round(rp * 100, 2) if rp else None,
                    "abandono_pct": round(ab * 100, 2) if ab else None,
                }
    if taxa_d:
        dados["taxa_rendimento"] = taxa_d

    return dados


# #############################################################################
#
#  GERAÇÃO COMPLETA
#
# #############################################################################

def gerar_todos(ibge, output_dir=None):
    """Gera os 5 relatórios TXT para um município ou estado."""
    _clear_cache()  # limpa cache de requests para nova entidade

    mun, uf_sigla = descobrir_municipio(ibge)
    slug = _slug(mun)

    geradores = [
        ("aprendizado",     gerar_txt_aprendizado),
        ("infra",           gerar_txt_infra),
        ("censo",           gerar_txt_censo),
        ("ideb",            gerar_txt_ideb),
        ("taxa_rendimento", gerar_txt_taxa),
    ]

    arquivos = {}
    for nome, fn in geradores:
        try:
            txt = fn(ibge, mun, uf_sigla)
        except Exception as e:
            txt = f"❌ Erro ao gerar {nome}: {e}"
        fname = f"{slug}_{nome}.txt"
        arquivos[fname] = txt

    # Dados estruturados (JSON-friendly) — reutiliza cache, custo zero
    dados_estruturados = coletar_dados_estruturados(ibge, mun, uf_sigla)

    if output_dir:
        output_dir = pathlib.Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        for fname, txt in arquivos.items():
            (output_dir / fname).write_text(txt, encoding="utf-8")

    return {"municipio": mun, "uf": uf_sigla, "ibge": ibge,
            "arquivos": arquivos, "dados_estruturados": dados_estruturados}


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Gerador QEDU — CLI")
    parser.add_argument("ibge", help="Código IBGE (7 dígitos para município, 2 dígitos para estado)")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    out = pathlib.Path(args.output) if args.output else OUTPUT_DIR / args.ibge
    print(f"\n🔄 Gerando relatórios para IBGE {args.ibge}...")
    res = gerar_todos(args.ibge, out)
    print(f"✅ {res['municipio']} ({res['uf']}) — {len(res['arquivos'])} arquivos em {out}")
    for f in res["arquivos"]:
        print(f"   📄 {f}")

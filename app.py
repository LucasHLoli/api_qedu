#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
==============================================================================
API QEDU — Flask para Render + n8n  (WSGI — compatível com gunicorn)
==============================================================================
GET  /gerar?ibge=2304400        →  JSON com 5 relatórios TXT (município)
GET  /gerar?ibge=23              →  JSON com 5 relatórios TXT (estado)
GET  /gerar/<ibge>              →  idem (path param)
GET  /relatorio?ibge=2304400&tipo=censo  →  TXT puro de 1 relatório
GET  /municipio?ibge=2304400    →  nome + UF
GET  /health                    →  status
==============================================================================
"""

import os
import logging
import traceback
from datetime import datetime
from flask import Flask, request, jsonify, Response

from gerador import gerar_todos, descobrir_municipio, is_estado, OUTPUT_DIR

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api_qedu")

# =============================================================================
# APP
# =============================================================================
app = Flask(__name__)

# Tipos de relatório válidos
TIPOS_VALIDOS = ["aprendizado", "infra", "censo", "ideb", "taxa_rendimento"]


# =============================================================================
# CORS — libera n8n e qualquer frontend
# =============================================================================
@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response


# =============================================================================
# HELPERS
# =============================================================================

def _validar_ibge(ibge: str):
    """Valida e limpa código IBGE. Aceita 2 dígitos (estado) ou 7 (município)."""
    ibge = ibge.strip()
    if not ibge.isdigit() or len(ibge) not in (2, 7):
        return None, (
            jsonify({"erro": f"Código IBGE inválido: '{ibge}'. Use 7 dígitos (município) ou 2 dígitos (estado)."}),
            400,
        )
    return ibge, None


def _gerar(ibge: str):
    """Roda o gerador e retorna (dict, None) ou (None, erro_response)."""
    ibge, erro = _validar_ibge(ibge)
    if erro:
        return None, erro

    log.info(f"Gerando relatórios para IBGE {ibge}...")

    try:
        out_dir = OUTPUT_DIR / ibge
        resultado = gerar_todos(ibge, out_dir)
    except Exception as e:
        log.error(f"Erro ao gerar IBGE {ibge}: {e}\n{traceback.format_exc()}")
        return None, (jsonify({"erro": f"Erro ao gerar: {str(e)}", "ibge": ibge}), 500)

    # Organiza por tipo
    relatorios = {}
    for fname, conteudo in resultado.get("arquivos", {}).items():
        for tipo in TIPOS_VALIDOS:
            if tipo in fname.lower():
                relatorios[tipo] = conteudo
                break

    log.info(f"OK: {resultado['municipio']} ({resultado['uf']}) — {len(relatorios)} relatórios")

    resp = {
        "municipio": resultado["municipio"],
        "uf": resultado["uf"],
        "ibge": ibge,
        "tipo": "estado" if is_estado(ibge) else "municipio",
        "relatorios": relatorios,
    }

    # Dados estruturados (JSON limpo para IA)
    dados_est = resultado.get("dados_estruturados")
    if dados_est:
        resp["dados"] = dados_est

    return resp, None


# =============================================================================
# ENDPOINTS
# =============================================================================

# ---------- HEALTH ----------

@app.route("/")
@app.route("/health")
def health():
    """Health check — Render usa /health para saber se está vivo."""
    return jsonify(
        status="ok",
        version="2.0.0",
        timestamp=datetime.now().isoformat(),
        tipos_disponiveis=TIPOS_VALIDOS,
    )


# ---------- GERAR TODOS (endpoint principal pro n8n) ----------

@app.route("/gerar")
def gerar_query():
    """GET /gerar?ibge=2304400 — formato preferido pro n8n."""
    ibge = request.args.get("ibge", "").strip()
    if not ibge:
        return jsonify({"erro": "Parâmetro 'ibge' obrigatório. Ex: /gerar?ibge=2304400"}), 400

    r, erro = _gerar(ibge)
    if erro:
        return erro

    r["gerado_em"] = datetime.now().isoformat()
    r["total_relatorios"] = len(r["relatorios"])
    return jsonify(r)


@app.route("/gerar/<ibge>")
def gerar_path(ibge):
    """GET /gerar/2304400 — atalho via URL."""
    r, erro = _gerar(ibge)
    if erro:
        return erro

    r["gerado_em"] = datetime.now().isoformat()
    r["total_relatorios"] = len(r["relatorios"])
    return jsonify(r)


# ---------- RELATÓRIO INDIVIDUAL (texto puro) ----------

@app.route("/relatorio")
def relatorio_individual():
    """GET /relatorio?ibge=2304400&tipo=censo → texto puro de 1 relatório."""
    ibge = request.args.get("ibge", "").strip()
    tipo = request.args.get("tipo", "").strip().lower()

    if not ibge:
        return jsonify({"erro": "Parâmetro 'ibge' obrigatório."}), 400
    if tipo not in TIPOS_VALIDOS:
        return jsonify({"erro": f"Tipo inválido. Use: {TIPOS_VALIDOS}"}), 400

    r, erro = _gerar(ibge)
    if erro:
        return erro

    txt = r["relatorios"].get(tipo)
    if not txt:
        return jsonify({"erro": f"Relatório '{tipo}' não gerado para IBGE {ibge}"}), 404

    return Response(txt, mimetype="text/plain; charset=utf-8")


# ---------- MUNICÍPIO ----------

@app.route("/municipio")
def identificar_municipio_query():
    """Retorna nome e UF sem gerar relatórios (rápido)."""
    ibge = request.args.get("ibge", "").strip()
    if not ibge:
        return jsonify({"erro": "Parâmetro 'ibge' obrigatório."}), 400

    ibge, erro = _validar_ibge(ibge)
    if erro:
        return erro

    mun, uf = descobrir_municipio(ibge)
    return jsonify(municipio=mun, uf=uf, ibge=ibge)


@app.route("/municipio/<ibge>")
def identificar_municipio_path(ibge):
    ibge, erro = _validar_ibge(ibge)
    if erro:
        return erro

    mun, uf = descobrir_municipio(ibge)
    return jsonify(municipio=mun, uf=uf, ibge=ibge)


# =============================================================================
# STARTUP (dev local)
# =============================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=True)

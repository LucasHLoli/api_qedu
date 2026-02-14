# API QEdu — Gerador de Relatórios

API que recebe um **código IBGE** e retorna **5 relatórios TXT** com dados educacionais do município.

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/` | Health check |
| `GET/POST` | `/gerar/{ibge}` | Gera os 5 relatórios |
| `GET` | `/municipio/{ibge}` | Identifica nome e UF |

## Uso no n8n

No node **HTTP Request**:
```
Method: GET
URL: https://sua-url.onrender.com/gerar/2304400
```

A resposta JSON contém:
```json
{
  "municipio": "Fortaleza",
  "uf": "CE",
  "ibge": "2304400",
  "gerado_em": "2026-02-10T...",
  "arquivos": {
    "Fortaleza_aprendizado.txt": "... conteúdo ...",
    "Fortaleza_infra.txt": "...",
    "Fortaleza_censo.txt": "...",
    "Fortaleza_ideb.txt": "...",
    "Fortaleza_taxa_rendimento.txt": "..."
  }
}
```

## Deploy no Render

1. Crie repo Git com esta pasta (`api_qedu/`)
2. No Render → **New Web Service** → Conecte o repo
3. O `render.yaml` configura tudo automaticamente
4. A API fica disponível em `https://api-qedu.onrender.com`

## Executar localmente

```bash
pip install -r requirements.txt
python app.py
# API roda em http://localhost:8000
# Docs em http://localhost:8000/docs
```

## Anos Dinâmicos

O script detecta automaticamente o ano mais recente com dados:
- **SAEB/Aprendizado**: anos ímpares (2023, 2025, ...)
- **Censo/Infra/Taxa**: tenta ano atual → ano-1 → ano-2
- **IDEB**: último ano do CSV

## Estrutura

```
api_qedu/
├── app.py              # FastAPI (entry point)
├── gerador.py          # Lógica de coleta + geração
├── requirements.txt    # Dependências
├── render.yaml         # Config Render
├── dados/              # CSVs do IDEB
│   ├── ideb_saeb_municipios_28_07_final 1.csv
│   └── ideb_saeb_estados_28_07_final 1.csv
└── output/             # TXTs gerados (cache local)
```

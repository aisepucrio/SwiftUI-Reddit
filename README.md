# SwiftUI Research

Projeto de coleta e análise de dados sobre arquiteturas utilizadas com SwiftUI

## Instalação

Crie e ative um ambiente virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

Baixe o léxico usado pelo NLTK para análise de sentimento:

```bash
python -m nltk.downloader vader_lexicon
```

## Configuração das APIs

Crie um arquivo `.env` na raiz do projeto usando `.env.example` como referência:

```bash
cp .env.example .env
```

Preencha as variáveis necessárias:

```dotenv
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
REDDIT_USER_AGENT=
GITHUB_TOKEN=
STACKOVERFLOW_KEY=
```

O arquivo `.env` contém chaves e tokens reais, por isso fica fora do Git pelo `.gitignore`.

## Como Rodar

Para executar toda a pipeline de análise usando os dados já presentes em `data/raw`:

```bash
python scripts/rodar_pesquisa.py
```

O comando roda as análises de Reddit, Stack Overflow, GitHub, Forms e a análise qualitativa, depois executa as comparações entre fontes e gera as nuvens de palavras.

Os resultados são salvos em:

- `data/processed/`: arquivos CSV processados
- `outputs/`: gráficos e relatórios gerados

## Coleta de Dados

Os scripts de coleta podem ser executados individualmente quando for necessário atualizar os dados brutos:

```bash
python scripts/coleta/reddit_script.py
python scripts/coleta/github_script.py
python scripts/coleta/stackoverflow_script.py
```

Eles usam as credenciais configuradas no `.env` e gravam os dados em `data/raw`.

## Análises Individuais

Também é possível executar análises específicas:

```bash
python scripts/analise/analise_reddit.py
python scripts/analise/analise_stackoverflow.py
python scripts/analise/analise_github.py
python scripts/analise/analise_forms.py
python scripts/analise/analise_qualitativa_reddit.py
python scripts/analise/comparacao_fontes.py
python scripts/analise/gerar_nuvens_palavras.py
```

## Segurança

Não versionar arquivos com credenciais reais. Use `.env.example` apenas como modelo e mantenha as chaves no `.env` local.

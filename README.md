# Bot interno de imobiliária (MVP)

MVP em **Python** para Telegram + Google Sheets, focado em:
- caixa de ferramentas interna (links/contatos/dúvidas/padrões)
- registro operacional (visitas/captações)
- contratos gerados (logados na planilha)
- avisos/reuniões com confirmação `✅ Li` e lembretes

## Stack (MVP)
- `python-telegram-bot` (polling + job queue)
- `requests` (chama um Web App do Google Apps Script)
- `python-docx` (gera contrato `.docx` simples)

## Por que Apps Script?
Algumas organizações bloqueiam a criação de chave JSON de Service Account.
Para não depender de Google Cloud/credenciais JSON, usamos um **Apps Script anexado à própria planilha** como ponte.

## Variáveis de ambiente
Copie `.env.example` e preencha:
- `TELEGRAM_BOT_TOKEN`
- `SHEETS_WEBAPP_URL`
- `SHEETS_WEBAPP_SECRET`
- `ADMIN_TELEGRAM_IDS` (opcional, recomendado para travar módulo de gerência)

## Setup do Google Sheets (sem Google Cloud)
### 1) Criar/abrir a planilha
- Abra a planilha que o patrão vai acessar.

### 2) Criar o Apps Script (ponte)
1. Na planilha: **Extensões → Apps Script**
2. Abra o arquivo `Code.gs` e substitua o conteúdo pelo arquivo:
   `apps_script/Code.gs` (deste repositório)
3. Vá em **Configurações do projeto → Propriedades do script**
4. Crie uma propriedade:
   - `BOT_SECRET` = uma senha grande (ex: 32+ caracteres)

### 3) Publicar como Web App
1. Clique em **Implantar → Nova implantação**
2. Tipo: **Web app**
3. **Executar como:** Você
4. **Quem tem acesso:** Qualquer pessoa (ou “qualquer pessoa na organização” se for Workspace)
5. Clique em **Implantar**
6. Copie a URL que termina em `/exec` → isso é o `SHEETS_WEBAPP_URL`

> Segurança: o bot só aceita chamadas com `SHEETS_WEBAPP_SECRET` que deve ser igual ao `BOT_SECRET`.

## Rodar local
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

## Deploy (SquareCloud)
- O arquivo `squarecloud.app` já está pronto.
- Configure variáveis de ambiente no painel:
  - `TELEGRAM_BOT_TOKEN`
  - `SHEETS_WEBAPP_URL`
  - `SHEETS_WEBAPP_SECRET`
  - `ADMIN_TELEGRAM_IDS` (opcional)
- Faça redeploy/restart e verifique os logs.

/**
 * BOT Interno Imobiliária - Apps Script Web App
 *
 * COMO USAR (resumo):
 * 1) Na sua planilha: Extensões > Apps Script
 * 2) Cole este arquivo em Code.gs (substitua o conteúdo)
 * 3) Em "Configurações do projeto" > Propriedades do script, crie:
 *    BOT_SECRET = <uma_senha_grande>
 * 4) Implantar > Nova implantação > Tipo: Web app
 *    - Executar como: Você
 *    - Quem tem acesso: Qualquer pessoa (ou "Qualquer pessoa na organização" se for Workspace)
 * 5) Copie a URL /exec e coloque no SquareCloud:
 *    SHEETS_WEBAPP_URL=<url>
 *    SHEETS_WEBAPP_SECRET=<BOT_SECRET>
 */

const HEADERS = {
  "VISITAS": [
    "VISITA_ID","TELEGRAM_ID","NOME","USERNAME","PAPEL","IMOVEL","DATA","HORA",
    "CLIENTE","OBS","STATUS","RESULTADO","EXPLICACAO","CRIADO_EM","FINALIZADO_EM"
  ],
  "CAPTACOES": [
    "CAPTACAO_ID","TELEGRAM_ID","NOME","USERNAME","PAPEL","TIPO","REFERENCIA","BAIRRO",
    "STATUS","EXPLICACAO","CRIADO_EM"
  ],
  "CONTRATOS_GERADOS": [
    "CONTRATO_ID","MODELO","TELEGRAM_ID","NOME","USERNAME","PAPEL","CLIENTE","IMOVEL",
    "VALOR","CRIADO_EM","ARQUIVO_NOME","RESUMO"
  ],
  "AVISOS": [
    "AVISO_ID","TIPO","TITULO","MENSAGEM","STATUS","CRIADO_EM","CRIADO_POR_ID","CRIADO_POR_NOME",
    "REUNIAO_DATA","REUNIAO_HORA","LEMBRETE_MIN","LEMBRETE_REUNIAO_ENVIADO_EM"
  ],
  "CONFIRMACOES_AVISOS": [
    "CONF_ID","AVISO_ID","TELEGRAM_ID","NOME","USERNAME","PAPEL","STATUS",
    "ENVIADO_EM","CONFIRMADO_EM","ULTIMO_LEMBRETE_EM"
  ],
  "LINKS": ["ID","CATEGORIA","TITULO","URL","OBS","ATIVO"],
  "CONTATOS": ["ID","CATEGORIA","NOME","TELEFONE","OBS","ATIVO"],
  "DUVIDAS": ["ID","CATEGORIA","PERGUNTA","RESPOSTA","ATIVO"],
  "PADROES": ["ID","CATEGORIA","TITULO","CONTEUDO","ATIVO"],
  "USUARIOS": ["TELEGRAM_ID","USERNAME","NOME","PAPEL","ATIVO","PRIMEIRO_ACESSO_EM","ULTIMO_ACESSO_EM"],
};

function doPost(e) {
  try {
    const body = JSON.parse((e && e.postData && e.postData.contents) ? e.postData.contents : "{}");
    const okAuth = _checkSecret(body && body.secret);
    if (!okAuth) return _json({ ok: false, error: "UNAUTHORIZED" });

    const action = (body.action || "").toString();
    if (action === "ping") {
      return _json({ ok: true, ts: new Date().toISOString() });
    }

    const lock = LockService.getScriptLock();
    lock.waitLock(30000);
    try {
      if (action === "ensure_schema") {
        _ensureSchema();
        return _json({ ok: true });
      }

      if (action === "append") {
        _append(body.tab, body.data || {});
        return _json({ ok: true });
      }

      if (action === "get_all_records") {
        const rows = _getAllRecords(body.tab);
        return _json({ ok: true, rows: rows });
      }

      if (action === "update_first_match") {
        const updated = _updateFirstMatch(body.tab, body.filters || {}, body.fields || {});
        return _json({ ok: true, updated: updated });
      }

      return _json({ ok: false, error: "UNKNOWN_ACTION", action: action });
    } finally {
      lock.releaseLock();
    }
  } catch (err) {
    return _json({ ok: false, error: "EXCEPTION", message: (err && err.message) ? err.message : String(err) });
  }
}

function _checkSecret(given) {
  const expected = PropertiesService.getScriptProperties().getProperty("BOT_SECRET");
  if (!expected) return false;
  return String(given || "") === String(expected);
}

function _ss() {
  return SpreadsheetApp.getActiveSpreadsheet();
}

function _ensureSchema() {
  const ss = _ss();
  Object.keys(HEADERS).forEach(tab => {
    const headers = HEADERS[tab];
    let sh = ss.getSheetByName(tab);
    if (!sh) {
      sh = ss.insertSheet(tab);
      sh.getRange(1, 1, 1, headers.length).setValues([headers]);
      return;
    }
    const firstRow = sh.getRange(1, 1, 1, Math.max(headers.length, sh.getLastColumn())).getValues()[0];
    const current = firstRow.slice(0, headers.length).map(v => String(v || ""));
    const expected = headers.map(h => String(h));
    const matches = current.join("|") === expected.join("|");
    if (sh.getLastRow() === 0 || firstRow.every(v => String(v || "").trim() === "")) {
      sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    } else if (!matches) {
      // Não sobrescreve automaticamente — evita estragar planilha existente
      // Você pode alinhar manualmente os cabeçalhos se necessário.
    }
  });
}

function _append(tab, dataObj) {
  if (!HEADERS[tab]) throw new Error("TAB_INVALIDA: " + tab);
  const ss = _ss();
  const sh = ss.getSheetByName(tab);
  if (!sh) throw new Error("ABA_NAO_EXISTE: " + tab);
  const headers = HEADERS[tab];
  const row = headers.map(h => (dataObj && dataObj[h] !== undefined && dataObj[h] !== null) ? String(dataObj[h]) : "");
  sh.appendRow(row);
}

function _getAllRecords(tab) {
  if (!HEADERS[tab]) throw new Error("TAB_INVALIDA: " + tab);
  const ss = _ss();
  const sh = ss.getSheetByName(tab);
  if (!sh) throw new Error("ABA_NAO_EXISTE: " + tab);

  const headers = HEADERS[tab];
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];
  const values = sh.getRange(2, 1, lastRow - 1, headers.length).getValues();
  const out = [];
  for (let i = 0; i < values.length; i++) {
    const obj = {};
    for (let j = 0; j < headers.length; j++) {
      obj[headers[j]] = (values[i][j] !== null && values[i][j] !== undefined) ? String(values[i][j]) : "";
    }
    out.push(obj);
  }
  return out;
}

function _updateFirstMatch(tab, filtersObj, fieldsObj) {
  if (!HEADERS[tab]) throw new Error("TAB_INVALIDA: " + tab);
  const ss = _ss();
  const sh = ss.getSheetByName(tab);
  if (!sh) throw new Error("ABA_NAO_EXISTE: " + tab);

  const headers = HEADERS[tab];
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return false;

  // precompute filter indexes
  const filterKeys = Object.keys(filtersObj || {});
  const fieldKeys = Object.keys(fieldsObj || {});
  if (filterKeys.length === 0 || fieldKeys.length === 0) return false;

  const filterIdx = filterKeys.map(k => headers.indexOf(k));
  for (let i = 0; i < filterIdx.length; i++) {
    if (filterIdx[i] < 0) throw new Error("FILTRO_CABECALHO_INVALIDO: " + filterKeys[i]);
  }
  const fieldIdx = fieldKeys.map(k => headers.indexOf(k));
  for (let i = 0; i < fieldIdx.length; i++) {
    if (fieldIdx[i] < 0) throw new Error("CAMPO_CABECALHO_INVALIDO: " + fieldKeys[i]);
  }

  const range = sh.getRange(2, 1, lastRow - 1, headers.length);
  const values = range.getValues();

  for (let r = 0; r < values.length; r++) {
    let match = true;
    for (let f = 0; f < filterKeys.length; f++) {
      const idx = filterIdx[f];
      const expected = String(filtersObj[filterKeys[f]] || "").trim();
      const got = String(values[r][idx] || "").trim();
      if (got !== expected) { match = false; break; }
    }
    if (!match) continue;

    // update in memory
    for (let u = 0; u < fieldKeys.length; u++) {
      const idx = fieldIdx[u];
      values[r][idx] = String(fieldsObj[fieldKeys[u]] ?? "");
    }
    range.setValues(values);
    return true;
  }
  return false;
}

function _json(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

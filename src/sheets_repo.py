from __future__ import annotations
import logging
import threading
from typing import Any, Dict, List, Optional

import requests

from .utils import normalize_yes

LOGGER = logging.getLogger("bot-imobiliaria.sheets")

HEADERS: Dict[str, List[str]] = {
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
}


def _safe(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


class SheetsRepo:
    """Repositório via Google Apps Script (Web App).

    Motivo: quando a organização bloqueia chaves JSON de Service Account,
    a forma mais simples (sem plataforma terceira) é usar um Apps Script
    anexado à própria planilha como "ponte".
    """

    def __init__(self, webapp_url: str, webapp_secret: str, timeout_sec: int = 20) -> None:
        self.webapp_url = webapp_url.strip()
        self.webapp_secret = webapp_secret.strip()
        self.timeout_sec = timeout_sec
        self._schema_ok = False
        self._schema_lock = threading.Lock()

    def _ensure_schema_if_needed(self) -> None:
        """Garante abas/cabeçalhos mínimos.

        Para UX: não travamos o start do bot. Aquecemos em background e também garantimos
        antes de qualquer leitura/gravação.
        """
        if self._schema_ok:
            return
        with self._schema_lock:
            if self._schema_ok:
                return
            self._call("ensure_schema")
            self._schema_ok = True

    def _call(self, action: str, **kwargs) -> Dict[str, Any]:
        payload = {"action": action, "secret": self.webapp_secret, **kwargs}
        try:
            r = requests.post(self.webapp_url, json=payload, timeout=self.timeout_sec)
        except Exception as exc:
            raise RuntimeError(f"Falha de rede ao chamar Apps Script ({action}).") from exc

        # Apps Script normalmente responde 200 sempre, mas ainda assim:
        if r.status_code >= 400:
            raise RuntimeError(f"Apps Script HTTP {r.status_code}: {r.text[:200]}")
        try:
            data = r.json()
        except Exception as exc:
            raise RuntimeError(f"Resposta inválida do Apps Script (não é JSON): {r.text[:200]}") from exc

        if not data.get("ok"):
            raise RuntimeError(f"Apps Script erro: {data.get('error')} {data.get('message','')}")
        return data

    # SCHEMA
    def ensure_schema(self) -> None:
        self._call("ensure_schema")
        self._schema_ok = True

    # LOW-LEVEL
    def _append(self, tab: str, data: Dict[str, Any]) -> None:
        self._ensure_schema_if_needed()
        self._call("append", tab=tab, data={h: _safe(data.get(h, "")) for h in HEADERS[tab]})

    def _get_all_records(self, tab: str) -> List[Dict[str, str]]:
        self._ensure_schema_if_needed()
        res = self._call("get_all_records", tab=tab)
        rows = res.get("rows", []) or []
        out: List[Dict[str, str]] = []
        for r in rows:
            out.append({k: _safe(v) for k, v in (r or {}).items()})
        return out

    def _update_first_match(self, tab: str, filters: Dict[str, Any], fields: Dict[str, Any]) -> bool:
        self._ensure_schema_if_needed()
        res = self._call("update_first_match", tab=tab,
                         filters={k: _safe(v) for k, v in filters.items()},
                         fields={k: _safe(v) for k, v in fields.items()})
        return bool(res.get("updated"))

    # USERS
    def upsert_user(self, *, telegram_id: int, username: str, name: str, role: str | None, now_str: str) -> Dict[str, str]:
        tid = str(telegram_id)
        existing = self.get_user(telegram_id) or {}
        if not existing:
            payload = {
                "TELEGRAM_ID": tid,
                "USERNAME": username,
                "NOME": name,
                "PAPEL": role or "",
                "ATIVO": "SIM",
                "PRIMEIRO_ACESSO_EM": now_str,
                "ULTIMO_ACESSO_EM": now_str,
            }
            self._append("USUARIOS", payload)
            return payload

        updates: Dict[str, Any] = {"USERNAME": username, "NOME": name, "ULTIMO_ACESSO_EM": now_str}
        if role:
            updates["PAPEL"] = role
        if not existing.get("ATIVO"):
            updates["ATIVO"] = "SIM"
        self._update_first_match("USUARIOS", {"TELEGRAM_ID": tid}, updates)
        merged = {**existing, **{k: _safe(v) for k, v in updates.items()}, "TELEGRAM_ID": tid}
        return merged

    def get_user(self, telegram_id: int) -> Optional[Dict[str, str]]:
        tid = str(telegram_id)
        for row in self._get_all_records("USUARIOS"):
            if str(row.get("TELEGRAM_ID", "")).strip() == tid:
                return {k: _safe(v) for k, v in row.items()}
        return None

    def list_active_users(self) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        for r in self._get_all_records("USUARIOS"):
            if normalize_yes(r.get("ATIVO")):
                out.append({k: _safe(v) for k, v in r.items()})
        return out

    # CONTENT
    def get_content_categories(self, tab: str) -> List[str]:
        categories = set()
        for r in self._get_all_records(tab):
            if not normalize_yes(r.get("ATIVO", "SIM")):
                continue
            cat = str(r.get("CATEGORIA", "")).strip()
            if cat:
                categories.add(cat)
        return sorted(categories, key=lambda x: x.lower())

    def get_content_items(self, tab: str, category: str) -> List[Dict[str, str]]:
        out = []
        for r in self._get_all_records(tab):
            if not normalize_yes(r.get("ATIVO", "SIM")):
                continue
            if str(r.get("CATEGORIA", "")).strip() != category:
                continue
            out.append({k: _safe(v) for k, v in r.items()})
        return out

    # VISITAS
    def append_visit(self, data: Dict[str, Any]) -> None:
        self._append("VISITAS", data)

    def list_pending_visits_for_user(self, telegram_id: int) -> List[Dict[str, str]]:
        tid = str(telegram_id)
        out = []
        for r in self._get_all_records("VISITAS"):
            if str(r.get("TELEGRAM_ID", "")).strip() != tid:
                continue
            if str(r.get("STATUS", "")).strip().upper() != "AGENDADA":
                continue
            out.append({k: _safe(v) for k, v in r.items()})
        out.sort(key=lambda x: x.get("CRIADO_EM", ""), reverse=True)
        return out

    def finalize_visit(self, visita_id: str, resultado: str, explicacao: str, finalized_at: str) -> bool:
        return self._update_first_match("VISITAS", {"VISITA_ID": visita_id}, {
            "STATUS": "FINALIZADA",
            "RESULTADO": resultado,
            "EXPLICACAO": explicacao,
            "FINALIZADO_EM": finalized_at,
        })

    # CAPTAÇÕES
    def append_captacao(self, data: Dict[str, Any]) -> None:
        self._append("CAPTACOES", data)

    # CONTRATOS
    def append_contract_log(self, data: Dict[str, Any]) -> None:
        self._append("CONTRATOS_GERADOS", data)

    # AVISOS
    def append_notice(self, data: Dict[str, Any]) -> None:
        self._append("AVISOS", data)

    def append_notice_confirmations(self, rows: List[Dict[str, Any]]) -> None:
        for row in rows:
            self._append("CONFIRMACOES_AVISOS", row)

    def mark_notice_confirmed(self, aviso_id: str, telegram_id: int, confirmed_at: str) -> str:
        # primeiro lemos estado para conseguir devolver JA_CONFIRMADO
        for r in self._get_all_records("CONFIRMACOES_AVISOS"):
            if str(r.get("AVISO_ID", "")).strip() == str(aviso_id).strip() and str(r.get("TELEGRAM_ID", "")).strip() == str(telegram_id).strip():
                if str(r.get("STATUS", "")).strip().upper() == "CONFIRMADO":
                    return "JA_CONFIRMADO"
                ok = self._update_first_match("CONFIRMACOES_AVISOS",
                                              {"AVISO_ID": str(aviso_id).strip(), "TELEGRAM_ID": str(telegram_id).strip()},
                                              {"STATUS": "CONFIRMADO", "CONFIRMADO_EM": confirmed_at})
                return "CONFIRMADO" if ok else "NOT_FOUND"
        return "NOT_FOUND"

    def list_pending_notice_confirmations(self) -> List[Dict[str, str]]:
        confs = self._get_all_records("CONFIRMACOES_AVISOS")
        avisos = {a.get("AVISO_ID"): a for a in self._get_all_records("AVISOS")}
        out = []
        for c in confs:
            if str(c.get("STATUS", "")).strip().upper() != "PENDENTE":
                continue
            aviso_id = str(c.get("AVISO_ID", "")).strip()
            aviso = avisos.get(aviso_id)
            if not aviso:
                continue
            if str(aviso.get("STATUS", "")).strip().upper() != "ATIVO":
                continue
            merged = {**{k: _safe(v) for k, v in c.items()},
                      **{f"AVISO_{k}": _safe(v) for k, v in aviso.items()}}
            out.append(merged)
        return out

    def touch_confirmation_reminder(self, conf_id: str, ts: str) -> None:
        self._update_first_match("CONFIRMACOES_AVISOS", {"CONF_ID": conf_id}, {"ULTIMO_LEMBRETE_EM": ts})

    def list_meetings_to_remind(self) -> List[Dict[str, str]]:
        out = []
        for r in self._get_all_records("AVISOS"):
            if str(r.get("STATUS", "")).strip().upper() != "ATIVO":
                continue
            if str(r.get("TIPO", "")).strip().upper() != "REUNIAO":
                continue
            if str(r.get("LEMBRETE_REUNIAO_ENVIADO_EM", "")).strip():
                continue
            if not r.get("REUNIAO_DATA") or not r.get("REUNIAO_HORA"):
                continue
            out.append({k: _safe(v) for k, v in r.items()})
        return out

    def mark_meeting_reminder_sent(self, aviso_id: str, ts: str) -> None:
        self._update_first_match("AVISOS", {"AVISO_ID": aviso_id}, {"LEMBRETE_REUNIAO_ENVIADO_EM": ts})

from __future__ import annotations
import asyncio
import io
import logging
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile, Bot
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from .config import ConfigError, Settings, load_settings
from .contracts import ContractService
from .logging_setup import setup_logging
from .sheets_repo import SheetsRepo
from .utils import deep_copy_session_state, now_str, parse_date_br, parse_time_hhmm, slugify

LOGGER = logging.getLogger("bot-imobiliaria")
SESSIONS: Dict[int, Dict[str, Any]] = {}

CONTENT_TABS = {
    "LINKS": "Links úteis",
    "CONTATOS": "Contatos",
    "DUVIDAS": "Dúvidas frequentes",
    "PADROES": "Padrões da imobiliária",
}


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10].upper()}"


def get_session(user_id: int) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {"state": "MAIN_MENU", "data": {}, "meta": {}, "choices": [], "history": [], "menu_message_id": None}
    return SESSIONS[user_id]


def reset_flow(session: Dict[str, Any], keep_menu: bool = True) -> None:
    menu_id = session.get("menu_message_id") if keep_menu else None
    session.clear()
    session.update({"state": "MAIN_MENU", "data": {}, "meta": {}, "choices": [], "history": [], "menu_message_id": menu_id})


def push_history(session: Dict[str, Any]) -> None:
    session.setdefault("history", []).append(deep_copy_session_state(session))
    if len(session["history"]) > 30:
        session["history"] = session["history"][-30:]


def pop_history(session: Dict[str, Any]) -> bool:
    if not session.get("history"):
        return False
    snap = session["history"].pop()
    session["state"] = snap["state"]
    session["data"] = snap["data"]
    session["meta"] = snap["meta"]
    session["choices"] = snap["choices"]
    return True


def user_display_from_update(update: Update) -> Dict[str, str]:
    u = update.effective_user
    if not u:
        return {"telegram_id": "", "username": "", "name": "Desconhecido"}
    name = " ".join([p for p in [u.first_name or "", u.last_name or ""] if p]).strip() or (u.username or str(u.id))
    return {"telegram_id": str(u.id), "username": u.username or "", "name": name}


async def safe_delete_user_message(update: Update, settings: Settings) -> None:
    if not settings.delete_user_messages:
        return
    try:
        if update.effective_message:
            await update.effective_message.delete()
    except Exception:
        pass


async def render_menu(*, update: Optional[Update], context: ContextTypes.DEFAULT_TYPE, session: Dict[str, Any], text: str, keyboard: List[List[InlineKeyboardButton]], force_new: bool = False) -> None:
    if not update or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    text = text.replace("*", "").replace("`", "")
    markup = InlineKeyboardMarkup(keyboard)
    msg_id = session.get("menu_message_id")
    if force_new or not msg_id:
        sent = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=markup, disable_web_page_preview=True)
        session["menu_message_id"] = sent.message_id
        return
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=markup, disable_web_page_preview=True)
    except BadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        sent = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=markup, disable_web_page_preview=True)
        session["menu_message_id"] = sent.message_id


def back_cancel_row(include_back: bool = True, include_cancel: bool = True) -> List[InlineKeyboardButton]:
    row: List[InlineKeyboardButton] = []
    if include_back:
        row.append(InlineKeyboardButton("⬅️ Voltar", callback_data="nav:back"))
    if include_cancel:
        row.append(InlineKeyboardButton("❌ Cancelar", callback_data="nav:cancel"))
    return row


def _is_admin(telegram_id: int, settings: Settings) -> bool:
    return telegram_id in settings.admin_telegram_ids


def _infer_role(telegram_id: int, settings: Settings) -> str:
    # Regra do projeto: por padrão todo mundo é corretora. Gerência acessa via /admin.
    return "gerente" if _is_admin(telegram_id, settings) else "corretora"


async def touch_user_async(update: Update, context: ContextTypes.DEFAULT_TYPE, role_override: str | None = None) -> None:
    """Atualiza/insere o usuário na aba USUARIOS sem travar a UX."""
    if not update.effective_user:
        return
    repo: SheetsRepo = context.application.bot_data["repo"]
    settings: Settings = context.application.bot_data["settings"]
    info = user_display_from_update(update)
    role = (role_override or _infer_role(int(info["telegram_id"]), settings)).strip().lower()
    now = now_str(datetime.now(settings.tz))

    async def _runner() -> None:
        try:
            await asyncio.to_thread(
                repo.upsert_user,
                telegram_id=int(info["telegram_id"]),
                username=info["username"],
                name=info["name"],
                role=role,
                now_str=now,
            )
        except Exception as exc:
            LOGGER.warning("Falha ao registrar usuário na planilha: %s", exc)

    asyncio.create_task(_runner())


async def show_state(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    repo: SheetsRepo = context.application.bot_data["repo"]
    contracts: ContractService = context.application.bot_data["contracts"]

    user_id = update.effective_user.id
    session = get_session(user_id)
    state = session.get("state", "MAIN_MENU")
    is_admin = _is_admin(user_id, settings)
    role = "gerente" if is_admin else "corretora"

    if state == "MAIN_MENU":
        keyboard = [
            [InlineKeyboardButton("🧰 Ferramentas", callback_data="m:tools")],
            [InlineKeyboardButton("📋 Operacional", callback_data="m:ops")],
            [InlineKeyboardButton("📢 Avisos", callback_data="m:notices")],
        ]
        keyboard.append([InlineKeyboardButton("ℹ️ Ajuda", callback_data="m:help")])
        text = "🏠 Bot Interno — Imobiliária\n\nEscolha uma opção abaixo."
        if is_admin:
            text += "\n\n🛠️ Gerência: use /admin"
        await render_menu(update=update, context=context, session=session, text=text, keyboard=keyboard)
        return

    if state == "TOOLS_MENU":
        keyboard = [
            [InlineKeyboardButton("📝 Criar contrato", callback_data="tool:contract")],
            [InlineKeyboardButton("🔗 Links úteis", callback_data="tool:LINKS")],
            [InlineKeyboardButton("📞 Contatos", callback_data="tool:CONTATOS")],
            [InlineKeyboardButton("❓ Dúvidas frequentes", callback_data="tool:DUVIDAS")],
            [InlineKeyboardButton("📐 Padrões", callback_data="tool:PADROES")],
            [InlineKeyboardButton("⬅️ Voltar", callback_data="nav:back")],
        ]
        await render_menu(update=update, context=context, session=session, text="Ferramentas internas\nEscolha uma opção:", keyboard=keyboard)
        return

    if state == "OPS_MENU":
        keyboard = [
            [InlineKeyboardButton("📅 Registrar visita", callback_data="op:visit_new")],
            [InlineKeyboardButton("✅ Finalizar visita", callback_data="op:visit_finish")],
            [InlineKeyboardButton("📌 Registrar captação", callback_data="op:captacao_new")],
            [InlineKeyboardButton("⬅️ Voltar", callback_data="nav:back")],
        ]
        await render_menu(update=update, context=context, session=session, text="Controle operacional\nSelecione o fluxo:", keyboard=keyboard)
        return

    if state == "NOTICES_MENU":
        keyboard = [[InlineKeyboardButton("🔄 Atualizar avisos pendentes", callback_data="notice:pending_refresh")], [InlineKeyboardButton("⬅️ Voltar", callback_data="nav:back")]]
        try:
            pending = await asyncio.to_thread(repo.list_pending_notice_confirmations)
        except Exception as exc:
            await render_menu(update=update, context=context, session=session,
                              text=f"⚠️ Não consegui acessar a planilha agora.\n\nDetalhe: {exc}",
                              keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
            return
        mine = [p for p in pending if str(p.get("TELEGRAM_ID")) == str(update.effective_user.id)]
        if mine:
            lines = ["Avisos pendentes de confirmação:"]
            for i, row in enumerate(mine[:8], start=1):
                lines.append(f"{i}. {row.get('AVISO_TITULO','(sem título)')}")
            lines.append("\nAbra a mensagem do aviso e clique em 'Li'.")
            text = "\n".join(lines)
        else:
            text = "No momento, você não tem avisos pendentes de confirmação."
        await render_menu(update=update, context=context, session=session, text=text, keyboard=keyboard)
        return

    if state == "MANAGER_MENU":
        keyboard = [
            [InlineKeyboardButton("📣 Novo aviso/reunião", callback_data="mg:notice_new")],
            [InlineKeyboardButton("📊 Pendências de confirmação", callback_data="mg:notice_status")],
            [InlineKeyboardButton("⬅️ Voltar", callback_data="nav:back")],
        ]
        await render_menu(update=update, context=context, session=session, text="Módulo de gerência\nEscolha uma ação:", keyboard=keyboard)
        return

    if state == "CONTENT_CATEGORY":
        tab = session["meta"]["tab"]
        label = CONTENT_TABS.get(tab, tab)
        try:
            cats = await asyncio.to_thread(repo.get_content_categories, tab)
        except Exception as exc:
            await render_menu(update=update, context=context, session=session,
                              text=f"⚠️ Não consegui acessar a planilha agora.\n\nDetalhe: {exc}",
                              keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
            return
        session["choices"] = cats
        keyboard = [[InlineKeyboardButton(cat, callback_data=f"cat:{idx}")] for idx, cat in enumerate(cats)]
        keyboard.append(back_cancel_row(include_back=True, include_cancel=False))
        text = f"{label}\nEscolha uma categoria:" if cats else f"{label}\nNenhuma categoria cadastrada na planilha."
        await render_menu(update=update, context=context, session=session, text=text, keyboard=keyboard)
        return

    if state == "CONTENT_ITEMS":
        tab = session["meta"]["tab"]
        label = CONTENT_TABS.get(tab, tab)
        cat = session["meta"]["category"]
        try:
            items = await asyncio.to_thread(repo.get_content_items, tab, cat)
        except Exception as exc:
            await render_menu(update=update, context=context, session=session,
                              text=f"⚠️ Não consegui acessar a planilha agora.\n\nDetalhe: {exc}",
                              keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
            return
        parts = [f"{label} — {cat}"]
        if not items:
            parts.append("\nNada cadastrado nessa categoria.")
        else:
            for i, item in enumerate(items, start=1):
                if tab == "LINKS":
                    parts.append(f"\n{i}. {item.get('TITULO','Sem título')}\n{item.get('URL','')}")
                    if item.get("OBS"):
                        parts.append(f"↳ {item['OBS']}")
                elif tab == "CONTATOS":
                    parts.append(f"\n{i}. {item.get('NOME','Sem nome')} — {item.get('TELEFONE','')}")
                    if item.get('OBS'):
                        parts.append(f"↳ {item['OBS']}")
                elif tab == "DUVIDAS":
                    parts.append(f"\n{i}. {item.get('PERGUNTA','')}\n{item.get('RESPOSTA','')}")
                elif tab == "PADROES":
                    parts.append(f"\n{i}. {item.get('TITULO','')}\n{item.get('CONTEUDO','')}")
        keyboard = [[InlineKeyboardButton("⬅️ Voltar", callback_data="nav:back")], [InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]]
        await render_menu(update=update, context=context, session=session, text="\n".join(parts), keyboard=keyboard)
        return

    if state.startswith("VISIT_NEW_"):
        prompts = {
            "VISIT_NEW_IMOVEL": "Registrar visita\nDigite o imóvel (código/endereço curto):",
            "VISIT_NEW_DATA": "Digite a data da visita (DD/MM/AAAA):",
            "VISIT_NEW_HORA": "Digite a hora da visita (HH:MM):",
            "VISIT_NEW_CLIENTE": "Digite o nome do cliente:",
            "VISIT_NEW_OBS": "Observação (opcional). Se não tiver, digite '-'",
        }
        await render_menu(update=update, context=context, session=session, text=prompts[state], keyboard=[back_cancel_row()])
        return

    if state == "VISIT_FINISH_SELECT":
        try:
            pending = await asyncio.to_thread(repo.list_pending_visits_for_user, update.effective_user.id)
        except Exception as exc:
            await render_menu(update=update, context=context, session=session,
                              text=f"⚠️ Não consegui ler suas visitas na planilha.\n\nDetalhe: {exc}",
                              keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
            return
        session["choices"] = pending
        keyboard: List[List[InlineKeyboardButton]] = []
        if not pending:
            text = "Finalizar visita\nVocê não tem visitas pendentes (AGENDADA)."
        else:
            text = "Finalizar visita\nEscolha uma visita pendente:"
            for idx, v in enumerate(pending[:15]):
                label = f"{v.get('DATA','')} {v.get('HORA','')} | {v.get('IMOVEL','')} | {v.get('CLIENTE','')}"
                keyboard.append([InlineKeyboardButton(label[:64], callback_data=f"vsel:{idx}")])
        keyboard.append(back_cancel_row())
        await render_menu(update=update, context=context, session=session, text=text, keyboard=keyboard)
        return

    if state == "VISIT_FINISH_RESULT":
        visit = session["meta"].get("selected_visit", {})
        text = f"Finalizar visita\nImóvel: {visit.get('IMOVEL','')}\nCliente: {visit.get('CLIENTE','')}\n\nSelecione o resultado:"
        opts = ["demonstrou interesse","não teve interesse","quer pensar","não compareceu","reagendada"]
        session["choices"] = opts
        keyboard = [[InlineKeyboardButton(o.title(), callback_data=f"vres:{i}")] for i, o in enumerate(opts)]
        keyboard.append(back_cancel_row())
        await render_menu(update=update, context=context, session=session, text=text, keyboard=keyboard)
        return

    if state == "VISIT_FINISH_EXPL":
        await render_menu(update=update, context=context, session=session, text="Digite uma breve explicação (1 frase):", keyboard=[back_cancel_row()])
        return

    if state == "CAPT_NEW_TIPO":
        opts = ["imóvel", "proprietário", "cliente"]
        session["choices"] = opts
        keyboard = [[InlineKeyboardButton(o.title(), callback_data=f"capt_tipo:{i}")] for i, o in enumerate(opts)]
        keyboard.append(back_cancel_row())
        await render_menu(update=update, context=context, session=session, text="Registrar captação\nEscolha o tipo:", keyboard=keyboard)
        return

    if state == "CAPT_NEW_REFERENCIA":
        await render_menu(update=update, context=context, session=session, text="Digite referência/nome:", keyboard=[back_cancel_row()]); return
    if state == "CAPT_NEW_BAIRRO":
        await render_menu(update=update, context=context, session=session, text="Digite bairro/região:", keyboard=[back_cancel_row()]); return
    if state == "CAPT_NEW_RESULT":
        opts = ["captado", "em negociação", "não captado"]
        session["choices"] = opts
        keyboard = [[InlineKeyboardButton(o.title(), callback_data=f"capt_res:{i}")] for i, o in enumerate(opts)]
        keyboard.append(back_cancel_row())
        await render_menu(update=update, context=context, session=session, text="Selecione o resultado:", keyboard=keyboard); return
    if state == "CAPT_NEW_EXPL":
        await render_menu(update=update, context=context, session=session, text="Digite breve explicação:", keyboard=[back_cancel_row()]); return

    if state == "CONTRACT_MODEL":
        models = contracts.list_models()
        session["choices"] = models
        keyboard = [[InlineKeyboardButton(m.get("display_name","Modelo"), callback_data=f"cmod:{idx}")] for idx, m in enumerate(models)]
        keyboard.append(back_cancel_row())
        await render_menu(update=update, context=context, session=session, text="Criador de contratos\nEscolha o modelo:", keyboard=keyboard); return

    if state == "CONTRACT_FIELD":
        model = session["meta"]["contract_model"]
        fields = session["meta"]["contract_fields"]
        idx = session["meta"]["contract_idx"]
        field = fields[idx]
        await render_menu(update=update, context=context, session=session, text=f"{model['display_name']} ({idx+1}/{len(fields)})\n{field['label']}:", keyboard=[back_cancel_row()]); return

    if state == "NOTICE_NEW_TIPO":
        opts = ["AVISO", "REUNIAO"]
        session["choices"] = opts
        keyboard = [[InlineKeyboardButton(o.title(), callback_data=f"ntipo:{i}")] for i, o in enumerate(opts)]
        keyboard.append(back_cancel_row())
        await render_menu(update=update, context=context, session=session, text="Novo aviso/reunião\nSelecione o tipo:", keyboard=keyboard); return
    if state == "NOTICE_NEW_TITULO":
        await render_menu(update=update, context=context, session=session, text="Digite o título:", keyboard=[back_cancel_row()]); return
    if state == "NOTICE_NEW_MSG":
        await render_menu(update=update, context=context, session=session, text="Digite a mensagem do aviso/reunião:", keyboard=[back_cancel_row()]); return
    if state == "NOTICE_NEW_DATE":
        await render_menu(update=update, context=context, session=session, text="Digite a data da reunião (DD/MM/AAAA):", keyboard=[back_cancel_row()]); return
    if state == "NOTICE_NEW_HOUR":
        await render_menu(update=update, context=context, session=session, text="Digite a hora da reunião (HH:MM):", keyboard=[back_cancel_row()]); return
    if state == "NOTICE_NEW_REMINDER":
        opts = ["0", "30", "60", "120"]
        session["choices"] = opts
        keyboard = [[InlineKeyboardButton("Sem lembrete" if o == "0" else f"{o} min antes", callback_data=f"nrem:{i}")] for i, o in enumerate(opts)]
        keyboard.append(back_cancel_row())
        await render_menu(update=update, context=context, session=session, text="Lembrete antes da reunião:", keyboard=keyboard); return
    if state == "NOTICE_NEW_CONFIRM_SEND":
        d = session["data"]
        lines = [f"Confirmar envio\nTipo: {d.get('notice_tipo','')}\nTítulo: {d.get('notice_titulo','')}\nMensagem: {d.get('notice_msg','')}"]
        if d.get("notice_tipo") == "REUNIAO":
            lines.append(f"Data/Hora: {d.get('notice_data','')} {d.get('notice_hora','')}")
            lines.append(f"Lembrete: {d.get('notice_lembrete_min','0')} min")
        keyboard = [[InlineKeyboardButton("✅ Enviar agora", callback_data="nsend:yes")], back_cancel_row()]
        await render_menu(update=update, context=context, session=session, text="\n".join(lines), keyboard=keyboard); return

    if state == "MANAGER_NOTICE_STATUS":
        pending = await asyncio.to_thread(repo.list_pending_notice_confirmations)
        if not pending:
            text = "Pendências de confirmação\nNenhuma pendência no momento."
        else:
            grouped: Dict[str, Dict[str, Any]] = {}
            for row in pending:
                aid = row["AVISO_ID"]
                grouped.setdefault(aid, {"titulo": row.get("AVISO_TITULO",""), "count": 0, "names": []})
                grouped[aid]["count"] += 1
                grouped[aid]["names"].append(row.get("NOME") or row.get("USERNAME") or row.get("TELEGRAM_ID"))
            lines = ["Pendências de confirmação:"]
            for _, info in list(grouped.items())[:10]:
                nomes = ", ".join(info["names"][:5]) + ("..." if len(info["names"]) > 5 else "")
                lines.append(f"• {info['titulo']} — {info['count']} pendente(s)\n  {nomes}")
            text = "\n".join(lines)
        keyboard = [[InlineKeyboardButton("🔄 Atualizar", callback_data="mg:notice_status")], [InlineKeyboardButton("⬅️ Voltar", callback_data="nav:back")]]
        await render_menu(update=update, context=context, session=session, text=text, keyboard=keyboard); return

    reset_flow(session)
    await show_state(update, context)


async def render_flash(update: Update, context: ContextTypes.DEFAULT_TYPE, message: str) -> None:
    session = get_session(update.effective_user.id)
    session["meta"]["flash_return_state"] = session.get("state")
    await render_menu(update=update, context=context, session=session, text=f"⚠️ {message}\n\n(continue na mesma tela)", keyboard=[[InlineKeyboardButton("↩️ Voltar para formulário", callback_data="flash:return")]])


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    session = get_session(update.effective_user.id)
    reset_flow(session)
    session["state"] = "MAIN_MENU"
    await touch_user_async(update, context)
    await show_state(update, context)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    session = get_session(update.effective_user.id)
    await render_menu(update=update, context=context, session=session, text="Ajuda rápida:\n• Use /start\n• Sempre há botões de Voltar/Cancelar\n• O bot registra visitas, captações, contratos e avisos", keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Entrada do módulo da gerência via comando."""
    settings: Settings = context.application.bot_data["settings"]
    session = get_session(update.effective_user.id)

    if not _is_admin(update.effective_user.id, settings):
        await render_menu(update=update, context=context, session=session,
                          text="⛔ Acesso restrito.\n\nEste comando é apenas para a gerência.",
                          keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
        return

    await touch_user_async(update, context, role_override="gerente")
    reset_flow(session)
    session["state"] = "MANAGER_MENU"
    await show_state(update, context)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.effective_message or not update.effective_message.text:
        return
    settings: Settings = context.application.bot_data["settings"]
    repo: SheetsRepo = context.application.bot_data["repo"]
    contracts: ContractService = context.application.bot_data["contracts"]

    await touch_user_async(update, context)
    user_id = update.effective_user.id
    session = get_session(user_id)
    state = session.get("state")
    text = (update.effective_message.text or "").strip()

    text_states = {"VISIT_NEW_IMOVEL","VISIT_NEW_DATA","VISIT_NEW_HORA","VISIT_NEW_CLIENTE","VISIT_NEW_OBS","VISIT_FINISH_EXPL","CAPT_NEW_REFERENCIA","CAPT_NEW_BAIRRO","CAPT_NEW_EXPL","CONTRACT_FIELD","NOTICE_NEW_TITULO","NOTICE_NEW_MSG","NOTICE_NEW_DATE","NOTICE_NEW_HOUR"}
    if state not in text_states:
        await safe_delete_user_message(update, settings)
        await show_state(update, context)
        return

    await safe_delete_user_message(update, settings)

    if state == "VISIT_NEW_IMOVEL":
        if len(text) < 2:
            await render_flash(update, context, "Digite um imóvel válido (mín. 2 caracteres)."); return
        push_history(session); session["data"]["visit_imovel"] = text; session["state"] = "VISIT_NEW_DATA"; await show_state(update, context); return
    if state == "VISIT_NEW_DATA":
        p = parse_date_br(text)
        if not p:
            await render_flash(update, context, "Data inválida. Use DD/MM/AAAA."); return
        push_history(session); session["data"]["visit_data"] = p; session["state"] = "VISIT_NEW_HORA"; await show_state(update, context); return
    if state == "VISIT_NEW_HORA":
        p = parse_time_hhmm(text)
        if not p:
            await render_flash(update, context, "Hora inválida. Use HH:MM."); return
        push_history(session); session["data"]["visit_hora"] = p; session["state"] = "VISIT_NEW_CLIENTE"; await show_state(update, context); return
    if state == "VISIT_NEW_CLIENTE":
        if len(text) < 2:
            await render_flash(update, context, "Nome do cliente muito curto."); return
        push_history(session); session["data"]["visit_cliente"] = text; session["state"] = "VISIT_NEW_OBS"; await show_state(update, context); return
    if state == "VISIT_NEW_OBS":
        session["data"]["visit_obs"] = "" if text == "-" else text
        info = user_display_from_update(update)
        role = _infer_role(user_id, settings)
        payload = {
            "VISITA_ID": new_id("VIS"),
            "TELEGRAM_ID": str(user_id),
            "NOME": info.get("name",""),
            "USERNAME": info.get("username",""),
            "PAPEL": role,
            "IMOVEL": session["data"].get("visit_imovel",""),
            "DATA": session["data"].get("visit_data",""),
            "HORA": session["data"].get("visit_hora",""),
            "CLIENTE": session["data"].get("visit_cliente",""),
            "OBS": session["data"].get("visit_obs",""),
            "STATUS": "AGENDADA",
            "RESULTADO": "",
            "EXPLICACAO": "",
            "CRIADO_EM": now_str(datetime.now(settings.tz)),
            "FINALIZADO_EM": "",
        }
        try:
            await asyncio.to_thread(repo.append_visit, payload)
        except Exception as exc:
            reset_flow(session)
            await render_menu(update=update, context=context, session=session,
                              text=f"⚠️ Não consegui salvar a visita na planilha.\n\nDetalhe: {exc}",
                              keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
            return
        reset_flow(session)
        await render_menu(update=update, context=context, session=session, text=f"✅ Visita registrada com sucesso!\n\nImóvel: {payload['IMOVEL']}\nData: {payload['DATA']} {payload['HORA']}\nCliente: {payload['CLIENTE']}\nStatus: AGENDADA", keyboard=[[InlineKeyboardButton("🏠 Menu principal", callback_data="nav:cancel")]])
        return

    if state == "VISIT_FINISH_EXPL":
        selected = session["meta"].get("selected_visit", {})
        try:
            ok = await asyncio.to_thread(repo.finalize_visit, selected.get("VISITA_ID",""), session["data"].get("visit_finish_result",""), text, now_str(datetime.now(settings.tz)))
        except Exception as exc:
            ok = False
            LOGGER.warning("Falha ao finalizar visita: %s", exc)
        reset_flow(session)
        msg = f"✅ Visita finalizada. Resultado: {session['data'].get('visit_finish_result','')}" if ok else "⚠️ Não consegui atualizar essa visita na planilha."
        await render_menu(update=update, context=context, session=session, text=msg, keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
        return

    if state == "CAPT_NEW_REFERENCIA":
        if len(text) < 2:
            await render_flash(update, context, "Referência/nome muito curto."); return
        push_history(session); session["data"]["capt_ref"] = text; session["state"] = "CAPT_NEW_BAIRRO"; await show_state(update, context); return
    if state == "CAPT_NEW_BAIRRO":
        if len(text) < 2:
            await render_flash(update, context, "Bairro/região muito curto."); return
        push_history(session); session["data"]["capt_bairro"] = text; session["state"] = "CAPT_NEW_RESULT"; await show_state(update, context); return
    if state == "CAPT_NEW_EXPL":
        session["data"]["capt_expl"] = text
        info = user_display_from_update(update)
        role = _infer_role(user_id, settings)
        payload = {
            "CAPTACAO_ID": new_id("CAP"),
            "TELEGRAM_ID": str(user_id),
            "NOME": info.get("name",""),
            "USERNAME": info.get("username",""),
            "PAPEL": role,
            "TIPO": session["data"].get("capt_tipo",""),
            "REFERENCIA": session["data"].get("capt_ref",""),
            "BAIRRO": session["data"].get("capt_bairro",""),
            "STATUS": session["data"].get("capt_result",""),
            "EXPLICACAO": session["data"].get("capt_expl",""),
            "CRIADO_EM": now_str(datetime.now(settings.tz)),
        }
        try:
            await asyncio.to_thread(repo.append_captacao, payload)
        except Exception as exc:
            reset_flow(session)
            await render_menu(update=update, context=context, session=session,
                              text=f"⚠️ Não consegui salvar a captação na planilha.\n\nDetalhe: {exc}",
                              keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
            return
        reset_flow(session)
        await render_menu(update=update, context=context, session=session, text=f"✅ Captação registrada: {payload['TIPO']} / {payload['STATUS']}", keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
        return

    if state == "CONTRACT_FIELD":
        meta = session["meta"]; model = meta["contract_model"]; fields = meta["contract_fields"]; idx = meta["contract_idx"]
        field = fields[idx]
        session["data"].setdefault("contract_values", {})[field["key"]] = text
        if idx + 1 < len(fields):
            push_history(session); session["meta"]["contract_idx"] = idx + 1; session["state"] = "CONTRACT_FIELD"; await show_state(update, context); return

        values = session["data"]["contract_values"]
        file_bytes = contracts.render_docx_bytes(model["model_key"], values)
        filename = f"contrato_{model['model_key']}_{slugify(values.get('nome_locatario') or values.get('nome_locador') or 'mvp')}.docx"
        bio = io.BytesIO(file_bytes); bio.name = filename
        await context.bot.send_document(chat_id=update.effective_chat.id, document=InputFile(bio, filename=filename), caption="📝 Contrato gerado (MVP). Revise antes de usar.")
        info = user_display_from_update(update)
        role = _infer_role(user_id, settings)
        try:
            await asyncio.to_thread(repo.append_contract_log, {
            "CONTRATO_ID": new_id("CTR"),
            "MODELO": model.get("display_name", model["model_key"]),
            "TELEGRAM_ID": str(user_id),
            "NOME": info.get("name",""),
            "USERNAME": info.get("username",""),
            "PAPEL": role,
            "CLIENTE": values.get("nome_locatario",""),
            "IMOVEL": values.get("imovel",""),
            "VALOR": values.get("valor_aluguel",""),
            "CRIADO_EM": now_str(datetime.now(settings.tz)),
            "ARQUIVO_NOME": filename,
            "RESUMO": f"Modelo {model['model_key']} gerado pelo bot",
            })
        except Exception as exc:
            LOGGER.warning("Falha ao registrar contrato na planilha: %s", exc)
        reset_flow(session)
        await render_menu(update=update, context=context, session=session, text="✅ Contrato gerado e registrado na planilha (CONTRATOS_GERADOS).", keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
        return

    if state == "NOTICE_NEW_TITULO":
        if len(text) < 3:
            await render_flash(update, context, "Título muito curto."); return
        push_history(session); session["data"]["notice_titulo"] = text; session["state"] = "NOTICE_NEW_MSG"; await show_state(update, context); return
    if state == "NOTICE_NEW_MSG":
        if len(text) < 5:
            await render_flash(update, context, "Mensagem muito curta."); return
        push_history(session); session["data"]["notice_msg"] = text
        if session["data"].get("notice_tipo") == "REUNIAO":
            session["state"] = "NOTICE_NEW_DATE"
        else:
            session["data"]["notice_data"] = ""; session["data"]["notice_hora"] = ""; session["data"]["notice_lembrete_min"] = "0"; session["state"] = "NOTICE_NEW_CONFIRM_SEND"
        await show_state(update, context); return
    if state == "NOTICE_NEW_DATE":
        p = parse_date_br(text)
        if not p:
            await render_flash(update, context, "Data inválida. Use DD/MM/AAAA."); return
        push_history(session); session["data"]["notice_data"] = p; session["state"] = "NOTICE_NEW_HOUR"; await show_state(update, context); return
    if state == "NOTICE_NEW_HOUR":
        p = parse_time_hhmm(text)
        if not p:
            await render_flash(update, context, "Hora inválida. Use HH:MM."); return
        push_history(session); session["data"]["notice_hora"] = p; session["state"] = "NOTICE_NEW_REMINDER"; await show_state(update, context); return


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not update.effective_user:
        return
    await q.answer()
    settings: Settings = context.application.bot_data["settings"]
    repo: SheetsRepo = context.application.bot_data["repo"]

    await touch_user_async(update, context)
    session = get_session(update.effective_user.id)
    data = q.data or ""

    if data == "nav:cancel":
        reset_flow(session); await show_state(update, context); return
    if data == "nav:back":
        if not pop_history(session):
            reset_flow(session)
        await show_state(update, context); return
    if data == "flash:return":
        back_state = session.get("meta", {}).pop("flash_return_state", None)
        if back_state:
            session["state"] = back_state
        await show_state(update, context); return

    if data.startswith("ack:"):
        aviso_id = data.split(":",1)[1]
        try:
            result = await asyncio.to_thread(repo.mark_notice_confirmed, aviso_id, update.effective_user.id, now_str(datetime.now(settings.tz)))
        except Exception as exc:
            await q.answer(f"Falha ao confirmar: {exc}", show_alert=True)
            return
        if result == "CONFIRMADO":
            try:
                await q.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            await q.answer("Confirmação registrada ✅")
        elif result == "JA_CONFIRMADO":
            await q.answer("Você já confirmou esse aviso.")
        else:
            await q.answer("Aviso não encontrado.", show_alert=True)
        return

    # main routing
    if data == "m:tools":
        push_history(session); session["state"] = "TOOLS_MENU"; await show_state(update, context); return
    if data == "m:ops":
        push_history(session); session["state"] = "OPS_MENU"; await show_state(update, context); return
    if data == "m:notices":
        push_history(session); session["state"] = "NOTICES_MENU"; await show_state(update, context); return
    if data == "m:help":
        push_history(session)
        await render_menu(update=update, context=context, session=session,
                          text="Ajuda rápida:\n\n• /start abre o menu\n• Use os botões (evite digitar)\n• Tudo importante fica registrado na planilha\n\nGerência: /admin",
                          keyboard=[[InlineKeyboardButton("⬅️ Voltar", callback_data="nav:back")], [InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
        return

    # tools
    if data == "tool:contract":
        push_history(session); session["state"] = "CONTRACT_MODEL"; session["data"] = {}; session["meta"] = {}; await show_state(update, context); return
    if data.startswith("tool:"):
        tab = data.split(":",1)[1]
        if tab in CONTENT_TABS:
            push_history(session); session["state"] = "CONTENT_CATEGORY"; session["meta"] = {"tab": tab}; session["choices"] = []; await show_state(update, context); return

    if data.startswith("cat:"):
        try:
            cat = session["choices"][int(data.split(":",1)[1])]
        except Exception:
            await q.answer("Categoria inválida.", show_alert=True); return
        push_history(session); session["state"] = "CONTENT_ITEMS"; session["meta"]["category"] = cat; await show_state(update, context); return

    # operational
    if data == "op:visit_new":
        push_history(session); session["state"] = "VISIT_NEW_IMOVEL"; session["data"] = {}; session["meta"] = {}; await show_state(update, context); return
    if data == "op:visit_finish":
        push_history(session); session["state"] = "VISIT_FINISH_SELECT"; session["data"] = {}; session["meta"] = {}; await show_state(update, context); return
    if data == "op:captacao_new":
        push_history(session); session["state"] = "CAPT_NEW_TIPO"; session["data"] = {}; session["meta"] = {}; await show_state(update, context); return

    if data.startswith("vsel:"):
        try:
            visit = session["choices"][int(data.split(":",1)[1])]
        except Exception:
            await q.answer("Visita inválida.", show_alert=True); return
        push_history(session); session["meta"]["selected_visit"] = visit; session["state"] = "VISIT_FINISH_RESULT"; await show_state(update, context); return
    if data.startswith("vres:"):
        try:
            result = session["choices"][int(data.split(":",1)[1])]
        except Exception:
            await q.answer("Resultado inválido.", show_alert=True); return
        push_history(session); session["data"]["visit_finish_result"] = result; session["state"] = "VISIT_FINISH_EXPL"; await show_state(update, context); return

    if data.startswith("capt_tipo:"):
        try:
            val = session["choices"][int(data.split(":",1)[1])]
        except Exception:
            await q.answer("Tipo inválido.", show_alert=True); return
        push_history(session); session["data"]["capt_tipo"] = val; session["state"] = "CAPT_NEW_REFERENCIA"; await show_state(update, context); return
    if data.startswith("capt_res:"):
        try:
            val = session["choices"][int(data.split(":",1)[1])]
        except Exception:
            await q.answer("Resultado inválido.", show_alert=True); return
        push_history(session); session["data"]["capt_result"] = val; session["state"] = "CAPT_NEW_EXPL"; await show_state(update, context); return

    # contract
    if data.startswith("cmod:"):
        try:
            model = session["choices"][int(data.split(":",1)[1])]
        except Exception:
            await q.answer("Modelo inválido.", show_alert=True); return
        push_history(session)
        session["meta"]["contract_model"] = model
        session["meta"]["contract_fields"] = model.get("fields", [])
        session["meta"]["contract_idx"] = 0
        session["data"]["contract_values"] = {}
        session["state"] = "CONTRACT_FIELD"
        await show_state(update, context); return

    # notices user
    if data == "notice:pending_refresh":
        await show_state(update, context); return

    # manager
    if data == "mg:notice_new":
        if not _is_admin(update.effective_user.id, settings):
            await q.answer("Acesso restrito.", show_alert=True); return
        push_history(session); session["state"] = "NOTICE_NEW_TIPO"; session["data"] = {}; session["meta"] = {}; await show_state(update, context); return
    if data == "mg:notice_status":
        if not _is_admin(update.effective_user.id, settings):
            await q.answer("Acesso restrito.", show_alert=True); return
        push_history(session); session["state"] = "MANAGER_NOTICE_STATUS"; await show_state(update, context); return

    if data.startswith("ntipo:"):
        try:
            tipo = session["choices"][int(data.split(":",1)[1])]
        except Exception:
            await q.answer("Tipo inválido.", show_alert=True); return
        push_history(session); session["data"]["notice_tipo"] = tipo; session["state"] = "NOTICE_NEW_TITULO"; await show_state(update, context); return
    if data.startswith("nrem:"):
        try:
            mins = session["choices"][int(data.split(":",1)[1])]
        except Exception:
            await q.answer("Lembrete inválido.", show_alert=True); return
        push_history(session); session["data"]["notice_lembrete_min"] = mins; session["state"] = "NOTICE_NEW_CONFIRM_SEND"; await show_state(update, context); return

    if data == "nsend:yes":
        if not _is_admin(update.effective_user.id, settings):
            await q.answer("Acesso restrito.", show_alert=True); return
        info_sender = user_display_from_update(update)
        d = session.get("data", {})
        notice_id = new_id("AVI")
        created_at = now_str(datetime.now(settings.tz))
        notice_payload = {
            "AVISO_ID": notice_id,
            "TIPO": d.get("notice_tipo","AVISO"),
            "TITULO": d.get("notice_titulo",""),
            "MENSAGEM": d.get("notice_msg",""),
            "STATUS": "ATIVO",
            "CRIADO_EM": created_at,
            "CRIADO_POR_ID": str(update.effective_user.id),
            "CRIADO_POR_NOME": info_sender.get("name", ""),
            "REUNIAO_DATA": d.get("notice_data",""),
            "REUNIAO_HORA": d.get("notice_hora",""),
            "LEMBRETE_MIN": d.get("notice_lembrete_min","0"),
            "LEMBRETE_REUNIAO_ENVIADO_EM": "",
        }
        try:
            await asyncio.to_thread(repo.append_notice, notice_payload)
            active_users = await asyncio.to_thread(repo.list_active_users)
        except Exception as exc:
            reset_flow(session)
            await render_menu(update=update, context=context, session=session,
                              text=f"⚠️ Não consegui salvar/enviar o aviso pela planilha.\n\nDetalhe: {exc}",
                              keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
            return
        conf_rows = []
        for u in active_users:
            tid = str(u.get("TELEGRAM_ID","")).strip()
            if not tid:
                continue
            conf_rows.append({
                "CONF_ID": new_id("CNF"),
                "AVISO_ID": notice_id,
                "TELEGRAM_ID": tid,
                "NOME": u.get("NOME",""),
                "USERNAME": u.get("USERNAME",""),
                "PAPEL": u.get("PAPEL",""),
                "STATUS": "PENDENTE",
                "ENVIADO_EM": created_at,
                "CONFIRMADO_EM": "",
                "ULTIMO_LEMBRETE_EM": "",
            })
        if conf_rows:
            try:
                await asyncio.to_thread(repo.append_notice_confirmations, conf_rows)
            except Exception as exc:
                LOGGER.warning("Falha ao criar confirmações na planilha: %s", exc)

        sent_ok = 0
        sent_err = 0
        for u in active_users:
            tid = str(u.get("TELEGRAM_ID","")).strip()
            if not tid:
                continue
            lines = [("Novo aviso interno" if d.get("notice_tipo") == "AVISO" else "Nova reunião"), d.get("notice_titulo",""), d.get("notice_msg","")]
            if d.get("notice_tipo") == "REUNIAO":
                lines.append(f"Quando: {d.get('notice_data','')} {d.get('notice_hora','')}")
            try:
                await context.bot.send_message(chat_id=int(tid), text="\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Li", callback_data=f"ack:{notice_id}")]]))
                sent_ok += 1
            except Exception as exc:
                LOGGER.warning("Falha ao enviar aviso %s para %s: %s", notice_id, tid, exc); sent_err += 1

        reset_flow(session)
        await render_menu(update=update, context=context, session=session, text=f"✅ Aviso enviado.\nID: {notice_id}\nUsuários alcançados: {sent_ok}\nFalhas de envio: {sent_err}", keyboard=[[InlineKeyboardButton("🏠 Menu", callback_data="nav:cancel")]])
        return

    await show_state(update, context)


def _minutes_since(ts: str, now_dt: datetime) -> float:
    if not ts:
        return 10**9
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=now_dt.tzinfo)
        return (now_dt - dt).total_seconds() / 60.0
    except Exception:
        return 10**9


async def pending_notice_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    settings: Settings = app.bot_data["settings"]
    repo: SheetsRepo = app.bot_data["repo"]
    now = datetime.now(settings.tz)
    pending = await asyncio.to_thread(repo.list_pending_notice_confirmations)
    for row in pending:
        last = (row.get("ULTIMO_LEMBRETE_EM") or "").strip()
        created = (row.get("ENVIADO_EM") or "").strip()
        should_send = (_minutes_since(created, now) >= 15) if not last else (_minutes_since(last, now) >= settings.pending_notice_reminder_minutes)
        if not should_send:
            continue
        tid = row.get("TELEGRAM_ID"); aviso_id = row.get("AVISO_ID")
        if not tid or not aviso_id:
            continue
        try:
            await app.bot.send_message(chat_id=int(tid), text=f"⏰ Lembrete de confirmação\n{row.get('AVISO_TITULO','Aviso interno')}\n{row.get('AVISO_MENSAGEM','')}\n\nClique em 'Li' para confirmar.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Li", callback_data=f"ack:{aviso_id}")]]))
            await asyncio.to_thread(repo.touch_confirmation_reminder, row.get("CONF_ID",""), now_str(now))
        except Exception as exc:
            LOGGER.warning("Falha em lembrete de confirmação p/ %s: %s", tid, exc)


async def meeting_reminder_scan_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    settings: Settings = app.bot_data["settings"]
    repo: SheetsRepo = app.bot_data["repo"]
    now = datetime.now(settings.tz)
    meetings = await asyncio.to_thread(repo.list_meetings_to_remind)
    if not meetings:
        return
    active_users = await asyncio.to_thread(repo.list_active_users)
    for m in meetings:
        try:
            mins_i = int(str(m.get("LEMBRETE_MIN") or "0"))
        except ValueError:
            mins_i = 0
        if mins_i <= 0:
            continue
        try:
            dt = datetime.strptime(f"{m.get('REUNIAO_DATA','')} {m.get('REUNIAO_HORA','')}", "%Y-%m-%d %H:%M").replace(tzinfo=settings.tz)
        except Exception:
            continue
        if now < (dt - timedelta(minutes=mins_i)):
            continue
        for u in active_users:
            tid = str(u.get("TELEGRAM_ID","")).strip()
            if not tid:
                continue
            try:
                await app.bot.send_message(chat_id=int(tid), text=f"🔔 Lembrete de reunião\n{m.get('TITULO','')}\nHorário: {m.get('REUNIAO_DATA','')} {m.get('REUNIAO_HORA','')}\n{m.get('MENSAGEM','')}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Li", callback_data=f"ack:{m.get('AVISO_ID')}")]]))
            except Exception as exc:
                LOGGER.warning("Falha ao enviar lembrete de reunião para %s: %s", tid, exc)
        await asyncio.to_thread(repo.mark_meeting_reminder_sent, m["AVISO_ID"], now_str(now))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.error("Erro no handler: %s", context.error)
    if context.error:
        LOGGER.error("Traceback:\n%s", "".join(traceback.format_exception(None, context.error, context.error.__traceback__)))


async def validate_token(token: str) -> None:
    bot = Bot(token=token)
    me = await bot.get_me()
    LOGGER.info("token validado | bot=@%s (%s)", me.username, me.id)


async def post_init(application: Application) -> None:
    LOGGER.info("polling ativo")

    # Aquecimento: cria/valida abas em background para não atrasar /start.
    repo: SheetsRepo = application.bot_data["repo"]
    async def _warm_schema() -> None:
        try:
            await asyncio.to_thread(repo.ensure_schema)
            LOGGER.info("planilha pronta (schema ok)")
        except Exception as exc:
            LOGGER.warning("planilha ainda não está pronta (schema): %s", exc)
    asyncio.create_task(_warm_schema())
    jq = application.job_queue
    if jq is None:
        LOGGER.warning("JobQueue indisponível; lembretes automáticos desativados.")
        return
    settings: Settings = application.bot_data["settings"]
    jq.run_repeating(pending_notice_reminder_job, interval=settings.pending_notice_check_every_minutes * 60, first=60, name="pending_notice_reminders")
    jq.run_repeating(meeting_reminder_scan_job, interval=60, first=30, name="meeting_reminder_scan")


def build_application(settings: Settings, repo: SheetsRepo, contracts: ContractService) -> Application:
    app = ApplicationBuilder().token(settings.telegram_bot_token).post_init(post_init).build()
    app.bot_data["settings"] = settings
    app.bot_data["repo"] = repo
    app.bot_data["contracts"] = contracts
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(error_handler)
    return app


def run() -> None:
    logger = setup_logging()
    logger.info("iniciando")
    try:
        settings = load_settings()
    except ConfigError as exc:
        logger.error("erro de env: %s", exc)
        raise SystemExit(1)
    # Não validamos a planilha aqui para não atrasar a inicialização.
    # O aquecimento acontece em background no post_init e também antes de cada operação.
    repo = SheetsRepo(settings.sheets_webapp_url, settings.sheets_webapp_secret)

    contracts = ContractService()
    try:
        asyncio.run(validate_token(settings.telegram_bot_token))
    except Exception as exc:
        logger.error("erro ao validar token do Telegram: %s", exc)
        raise SystemExit(1)

    app = build_application(settings, repo, contracts)
    logger.info("iniciando loop de polling")
    # drop_pending_updates=True evita ficar "preso" em backlog quando o bot reinicia.
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

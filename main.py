import asyncio
import logging
import os
import random
import re
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, UserIsBlockedError
from telethon.tl.functions.messages import StartBotRequest
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl


def load_local_env(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue

        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ[key] = value


load_local_env()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "fast_check_claimer")
BOT_USERNAMES = tuple(
    item.strip().lstrip("@").lower()
    for item in os.getenv("BOT_USERNAMES", "CryptoBot,send").split(",")
    if item.strip()
)
CHECK_PREFIXES = tuple(
    item.strip().upper()
    for item in os.getenv("CHECK_PREFIXES", "CQ").split(",")
    if item.strip()
)
CLAIM_WORKERS = max(1, int(os.getenv("CLAIM_WORKERS", "3")))
CLAIM_MAX_AGE_SEC = max(0.0, float(os.getenv("CLAIM_MAX_AGE_SEC", "20")))
SEEN_TTL_SEC = max(60, int(os.getenv("SEEN_TTL_SEC", "21600")))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_CHAT = os.getenv("LOG_CHAT", "").strip()
LOG_QUEUE_EVENTS = os.getenv("LOG_QUEUE_EVENTS", "0").strip().lower() in {"1", "true", "yes", "on"}
LOG_RESULT_EVENTS = os.getenv("LOG_RESULT_EVENTS", "1").strip().lower() in {"1", "true", "yes", "on"}

TG_LINK_RE = re.compile(
    r"(?:(?:https?://)?(?:t(?:elegram)?\.me|telegram\.dog)/[^\s<>()\[\]{}]+|tg://resolve\?[^\s<>()\[\]{}]+)",
    re.IGNORECASE,
)
START_RE = re.compile(r"(?:^|\s)/start(?:@[A-Za-z0-9_]{3,})?\s+(?P<payload>[A-Za-z0-9_-]{4,})", re.IGNORECASE)
TG_HOSTS = {"t.me", "telegram.me", "telegram.dog"}
FEEDBACK_RULES = [
    ("captcha", "captcha required", ("captcha", "капч", "prove you are human", "verify you are human")),
    ("password", "password required", ("password", "парол")),
    ("join", "join/subscribe required", ("join", "subscribe", "подпиш", "вступ")),
    ("premium", "telegram premium required", ("premium", "премиум")),
    ("new_users_only", "only for new users", ("only for new users", "new users only", "новых пользователей")),
    ("recipient", "recipient restriction", ("specific user", "intended for another", "only recipient", "получатель", "предназначен")),
    ("already", "already activated", ("already activated", "already received", "already used", "уже активирован", "уже получен")),
    ("expired_or_invalid", "expired/invalid/not available", ("expired", "invalid", "not available", "not found", "не найден", "недоступ", "истек", "закрыт", "исчерпан")),
    ("success", "activated", ("deposited to your balance", "instantly deposited", "you received", "credited", "зачислен", "получили")),
]


def build_raw_payload_re(prefixes: Tuple[str, ...]) -> re.Pattern:
    alternatives = []
    for prefix in prefixes:
        suffix_min_len = max(0, 6 - len(prefix))
        alternatives.append(rf"{re.escape(prefix)}[A-Za-z0-9_-]{{{suffix_min_len},}}")

    if not alternatives:
        return re.compile(r"a^")

    return re.compile(
        rf"(?<![A-Za-z0-9_])(?P<payload>(?:{'|'.join(alternatives)}))(?![A-Za-z0-9_])",
        re.IGNORECASE,
    )


RAW_PAYLOAD_RE = build_raw_payload_re(CHECK_PREFIXES)


@dataclass
class ClaimItem:
    bot: str
    payload: str
    chat_id: int
    message_id: int
    found_at: float
    queued_at: float
    attempts: int = 1


claim_queue: "asyncio.Queue[ClaimItem]" = asyncio.Queue()
log_queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=2000)
seen: Dict[str, float] = {}
seen_lock = asyncio.Lock()
feedback_seen: Dict[str, float] = {}
feedback_seen_lock = asyncio.Lock()
recent_attempts: Dict[str, List[Tuple[str, float]]] = {}
recent_attempts_lock = asyncio.Lock()
log_peer = None

client = TelegramClient(
    SESSION_NAME,
    API_ID,
    API_HASH,
    sequential_updates=False,
)


def validate_config() -> None:
    if API_ID <= 0 or not API_HASH:
        raise SystemExit(
            "Set API_ID and API_HASH env vars before start. "
            "Example: API_ID=12345 API_HASH=abcd..."
        )
    if not BOT_USERNAMES:
        raise SystemExit("BOT_USERNAMES is empty.")
    if not CHECK_PREFIXES:
        raise SystemExit("CHECK_PREFIXES is empty.")


def mask_payload(payload: str) -> str:
    if not payload:
        return "-"
    if len(payload) <= 10:
        return payload
    return f"{payload[:4]}...{payload[-4:]}"


def compact_text(text: str, limit: int = 220) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3] + "..."


def classify_feedback(text: str) -> Optional[Tuple[str, str]]:
    normalized = (text or "").lower()
    for code, label, tokens in FEEDBACK_RULES:
        if any(token in normalized for token in tokens):
            return code, label
    return None


def extract_feedback_texts(result) -> List[str]:
    texts: List[str] = []
    seen_local: Set[str] = set()

    def add(candidate: Optional[str]) -> None:
        if not candidate:
            return
        cleaned = candidate.strip()
        if not cleaned or cleaned in seen_local:
            return
        seen_local.add(cleaned)
        texts.append(cleaned)

    updates = getattr(result, "updates", None) or []
    for update in updates:
        message = getattr(update, "message", None)
        if message is None:
            continue
        add(getattr(message, "message", None))
        add(getattr(message, "raw_text", None))

    messages = getattr(result, "messages", None) or []
    for message in messages:
        add(getattr(message, "message", None))
        add(getattr(message, "raw_text", None))

    return texts


async def enqueue_log(text: str) -> None:
    if not LOG_CHAT:
        return
    try:
        log_queue.put_nowait(text)
    except asyncio.QueueFull:
        logging.warning("Log queue is full, drop: %s", compact_text(text, 120))


async def log_worker() -> None:
    while True:
        text = await log_queue.get()
        try:
            if log_peer is not None:
                await client.send_message(log_peer, text, link_preview=False)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 0.2)
            try:
                if log_peer is not None:
                    await client.send_message(log_peer, text, link_preview=False)
            except Exception:
                logging.exception("Failed to deliver log after FloodWait retry.")
        except Exception:
            logging.exception("Failed to send log message to LOG_CHAT.")
        finally:
            log_queue.task_done()


async def remember_attempt(bot: str, payload: str) -> None:
    now = time.monotonic()
    async with recent_attempts_lock:
        items = recent_attempts.setdefault(bot, [])
        items.append((payload, now))
        cutoff = now - 120
        recent_attempts[bot] = [(p, ts) for p, ts in items if ts >= cutoff][-50:]


async def latest_attempt(bot: str, max_age_sec: int = 45) -> Optional[str]:
    now = time.monotonic()
    async with recent_attempts_lock:
        items = recent_attempts.get(bot, [])
        for payload, ts in reversed(items):
            if (now - ts) <= max_age_sec:
                return payload
    return None


async def should_log_key(key: str) -> bool:
    now = time.monotonic()
    async with feedback_seen_lock:
        ts = feedback_seen.get(key)
        if ts is not None and (now - ts) < SEEN_TTL_SEC:
            return False
        feedback_seen[key] = now
        if len(feedback_seen) > 20_000:
            cutoff = now - SEEN_TTL_SEC
            stale = [item for item, created in feedback_seen.items() if created < cutoff]
            for item in stale:
                feedback_seen.pop(item, None)
    return True


def is_check_payload(payload: str) -> bool:
    if not payload:
        return False
    normalized = payload.strip().strip("<>()[]{}.,!?:;\"'").upper()
    if len(normalized) < 4:
        return False
    return any(normalized.startswith(prefix) for prefix in CHECK_PREFIXES)


def choose_default_bot(allowed_bots: Set[str]) -> str:
    if "send" in allowed_bots:
        return "send"
    return sorted(allowed_bots)[0]


def is_supported_telegram_host(host: str) -> bool:
    if not host:
        return False
    normalized = host.lower().split(":", 1)[0]
    if normalized.startswith("www."):
        normalized = normalized[4:]
    return normalized in TG_HOSTS


def extract_from_tg_link(link: str, allowed_bots: Set[str]) -> List[Tuple[str, str]]:
    cleaned = link.strip().replace("&amp;", "&").strip("<>()[]{}\"'")
    cleaned = cleaned.rstrip(".,!?;:")
    if not cleaned:
        return []

    try:
        if cleaned.lower().startswith("tg://resolve?"):
            query = cleaned.split("?", 1)[1]
            params = parse_qs(query, keep_blank_values=True)
            bot = ((params.get("domain") or [None])[0] or "").lower().lstrip("@")
            payloads = (params.get("start") or []) + (params.get("") or [])
            if bot not in allowed_bots:
                return []
            return [(bot, value) for value in payloads if is_check_payload(value)]

        normalized = cleaned if "://" in cleaned else f"https://{cleaned}"
        parsed = urlparse(normalized)
        host = parsed.netloc or ""
        if not is_supported_telegram_host(host):
            return []

        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            return []
        bot = parts[0].lower().lstrip("@")
        if bot not in allowed_bots:
            return []

        params = parse_qs(parsed.query, keep_blank_values=True)
        payloads = (params.get("start") or []) + (params.get("") or [])
        if not payloads and len(parts) > 1:
            payloads.append(parts[1])
        return [(bot, value) for value in payloads if is_check_payload(value)]
    except Exception:
        return []


def extract_matches(fragment: str, allowed_bots: Set[str]) -> List[Tuple[str, str]]:
    if not fragment:
        return []

    found: List[Tuple[str, str]] = []
    seen_local: Set[Tuple[str, str]] = set()
    default_bot = choose_default_bot(allowed_bots)

    for match in TG_LINK_RE.finditer(fragment):
        link = match.group(0)
        for pair in extract_from_tg_link(link, allowed_bots):
            if pair not in seen_local:
                found.append(pair)
                seen_local.add(pair)

    for match in START_RE.finditer(fragment):
        payload = match.group("payload")
        pair = (default_bot, payload)
        if is_check_payload(payload) and pair not in seen_local:
            found.append(pair)
            seen_local.add(pair)

    for match in RAW_PAYLOAD_RE.finditer(fragment):
        payload = match.group("payload")
        pair = (default_bot, payload)
        if is_check_payload(payload) and pair not in seen_local:
            found.append(pair)
            seen_local.add(pair)

    return found


def iter_message_fragments(message) -> Iterable[str]:
    text = message.raw_text or ""
    if text:
        yield text

    for entity in message.entities or []:
        if isinstance(entity, MessageEntityTextUrl) and entity.url:
            yield entity.url
        elif isinstance(entity, MessageEntityUrl):
            yield text[entity.offset : entity.offset + entity.length]

    for row in message.buttons or []:
        for button in row:
            url = getattr(button, "url", None)
            if url:
                yield url


async def process_bot_feedback(message, bot_name: str) -> None:
    text = message.raw_text or ""
    if not text:
        return

    verdict = classify_feedback(text)
    if verdict is None:
        return

    code, label = verdict
    if not await should_log_key(f"msg:{message.chat_id}:{message.id}:{code}"):
        return

    payload = await latest_attempt(bot_name, max_age_sec=45)
    attempt_tag = f"{bot_name}:{mask_payload(payload)}" if payload else bot_name
    line = f"[FEEDBACK] {attempt_tag} {label} | {compact_text(text)}"
    logging.info(line)
    await enqueue_log(line)


async def mark_and_queue(bot: str, payload: str, chat_id: int, message_id: int, found_at: float) -> None:
    now = time.monotonic()
    key = f"{bot}:{payload}"
    async with seen_lock:
        ts = seen.get(key)
        if ts is not None and (now - ts) < SEEN_TTL_SEC:
            return
        seen[key] = now
        if len(seen) > 20_000:
            cutoff = now - SEEN_TTL_SEC
            stale = [item for item, created in seen.items() if created < cutoff]
            for item in stale:
                seen.pop(item, None)

    queued_at = time.perf_counter()
    await claim_queue.put(
        ClaimItem(
            bot=bot,
            payload=payload,
            chat_id=chat_id,
            message_id=message_id,
            found_at=found_at,
            queued_at=queued_at,
        )
    )
    found_ms = int((queued_at - found_at) * 1000)
    logging.info(
        "Queued %s from chat=%s msg=%s found_to_queue=%sms qsize=%s",
        key,
        chat_id,
        message_id,
        found_ms,
        claim_queue.qsize(),
    )
    if LOG_QUEUE_EVENTS:
        await enqueue_log(
            f"[QUEUE] {bot}:{mask_payload(payload)} chat={chat_id} msg={message_id} "
            f"found_to_queue={found_ms}ms qsize={claim_queue.qsize()}"
        )


async def process_message(message, allowed_bots: Set[str]) -> None:
    matches: Dict[Tuple[str, str], float] = {}
    for fragment in iter_message_fragments(message):
        for pair in extract_matches(fragment, allowed_bots):
            matches.setdefault(pair, time.perf_counter())

    if not matches:
        return

    for (bot, payload), found_at in matches.items():
        await mark_and_queue(bot, payload, message.chat_id, message.id, found_at)


async def claim_worker(worker_id: int, bot_peers) -> None:
    while True:
        item = await claim_queue.get()
        bot = item.bot
        payload = item.payload
        peer = bot_peers.get(bot)
        try:
            now = time.perf_counter()
            age_ms = int((now - item.found_at) * 1000)
            queue_ms = int((now - item.queued_at) * 1000)
            if CLAIM_MAX_AGE_SEC and (now - item.found_at) > CLAIM_MAX_AGE_SEC:
                logging.info(
                    "Worker-%s drop stale %s:%s age=%sms queue=%sms attempts=%s chat=%s msg=%s",
                    worker_id,
                    bot,
                    payload,
                    age_ms,
                    queue_ms,
                    item.attempts,
                    item.chat_id,
                    item.message_id,
                )
                if LOG_RESULT_EVENTS:
                    await enqueue_log(
                        f"[DROP] {bot}:{mask_payload(payload)} age={age_ms}ms "
                        f"queue={queue_ms}ms worker={worker_id}"
                    )
                continue

            if peer is None:
                logging.warning("Unknown bot peer: %s", bot)
                continue

            await remember_attempt(bot, payload)
            started = time.perf_counter()
            result = await client(
                StartBotRequest(
                    bot=peer,
                    peer=peer,
                    start_param=payload,
                    random_id=random.randint(1, 2**63 - 1),
                )
            )
            finished = time.perf_counter()
            elapsed_ms = int((finished - started) * 1000)
            total_ms = int((finished - item.found_at) * 1000)
            logging.info(
                "Worker-%s sent start for %s:%s request=%sms queue=%sms total=%sms attempts=%s",
                worker_id,
                bot,
                payload,
                elapsed_ms,
                queue_ms,
                total_ms,
                item.attempts,
            )
            if LOG_RESULT_EVENTS:
                await enqueue_log(
                    f"[SENT] {bot}:{mask_payload(payload)} worker={worker_id} "
                    f"request={elapsed_ms}ms queue={queue_ms}ms total={total_ms}ms"
                )

            # Some bot replies arrive in StartBotRequest updates immediately.
            for text in extract_feedback_texts(result):
                verdict = classify_feedback(text)
                if verdict is None:
                    continue
                code, label = verdict
                key = f"upd:{bot}:{payload}:{code}:{compact_text(text, 80)}"
                if not await should_log_key(key):
                    continue
                line = f"[FEEDBACK] {bot}:{mask_payload(payload)} {label} | {compact_text(text)}"
                logging.info(line)
                await enqueue_log(line)
                break
        except FloodWaitError as e:
            retry_age_ms = int((time.perf_counter() - item.found_at) * 1000)
            logging.warning(
                "Worker-%s flood wait %ss on %s:%s age=%sms attempts=%s. Requeue.",
                worker_id,
                e.seconds,
                bot,
                payload,
                retry_age_ms,
                item.attempts,
            )
            await enqueue_log(
                f"[REQUEUE] {bot}:{mask_payload(payload)} flood_wait={e.seconds}s "
                f"age={retry_age_ms}ms worker={worker_id}"
            )
            await asyncio.sleep(e.seconds + 0.2)
            requeue_at = time.perf_counter()
            if not CLAIM_MAX_AGE_SEC or (requeue_at - item.found_at) <= CLAIM_MAX_AGE_SEC:
                await claim_queue.put(replace(item, queued_at=requeue_at, attempts=item.attempts + 1))
            else:
                logging.info(
                    "Worker-%s drop stale after flood wait %s:%s age=%sms attempts=%s",
                    worker_id,
                    bot,
                    payload,
                    int((requeue_at - item.found_at) * 1000),
                    item.attempts,
                )
        except UserIsBlockedError:
            logging.error("Unblock @%s and restart script.", bot)
            await enqueue_log(f"[ERROR] @{bot} is blocked. Unblock bot to continue.")
        except Exception:
            logging.exception("Worker-%s failed on %s:%s", worker_id, bot, payload)
            await enqueue_log(f"[ERROR] worker={worker_id} failed for {bot}:{mask_payload(payload)}")
        finally:
            claim_queue.task_done()


async def run() -> None:
    global log_peer

    validate_config()
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    await client.start()
    me = await client.get_me()
    logging.info("Authorized as id=%s username=@%s", me.id, me.username)

    allowed_bots = set(BOT_USERNAMES)
    bot_peers = {}
    bot_names_by_id: Dict[int, str] = {}
    for bot in sorted(allowed_bots):
        try:
            entity = await client.get_entity(bot)
            bot_peers[bot] = await client.get_input_entity(entity)
            bot_names_by_id[int(entity.id)] = bot
        except Exception:
            logging.exception("Failed to resolve bot @%s. Skip.", bot)
    if not bot_peers:
        raise SystemExit("No bot peers resolved. Check BOT_USERNAMES and access to bot.")

    allowed_bots = set(bot_peers)

    if LOG_CHAT:
        try:
            log_peer = await client.get_input_entity(LOG_CHAT)
            logging.info("Telegram log chat enabled: %s", LOG_CHAT)
        except Exception as e:
            raise SystemExit(f"Cannot resolve LOG_CHAT={LOG_CHAT!r}: {e}") from e

    logging.info(
        "Watching all incoming chats. Targets: %s | check prefixes: %s | workers=%s | max_age=%ss",
        ", ".join(sorted(allowed_bots)),
        ", ".join(CHECK_PREFIXES),
        CLAIM_WORKERS,
        CLAIM_MAX_AGE_SEC,
    )
    if LOG_CHAT:
        await enqueue_log(
            "[START] fast_check_claimer online | "
            f"targets={','.join(sorted(allowed_bots))} prefixes={','.join(CHECK_PREFIXES)} "
            f"workers={CLAIM_WORKERS} max_age={CLAIM_MAX_AGE_SEC}s"
        )

    @client.on(events.NewMessage(incoming=True))
    async def handle_new(event):
        sender_id = getattr(event.message, "sender_id", None)
        bot_name = bot_names_by_id.get(int(sender_id)) if sender_id is not None else None
        if bot_name is not None:
            await process_bot_feedback(event.message, bot_name)
        await process_message(event.message, allowed_bots)

    @client.on(events.MessageEdited(incoming=True))
    async def handle_edited(event):
        sender_id = getattr(event.message, "sender_id", None)
        bot_name = bot_names_by_id.get(int(sender_id)) if sender_id is not None else None
        if bot_name is not None:
            await process_bot_feedback(event.message, bot_name)
        await process_message(event.message, allowed_bots)

    workers = [
        asyncio.create_task(claim_worker(worker_id=i + 1, bot_peers=bot_peers))
        for i in range(CLAIM_WORKERS)
    ]
    log_task = asyncio.create_task(log_worker()) if LOG_CHAT else None

    try:
        await client.run_until_disconnected()
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        if log_task is not None:
            log_task.cancel()
            await asyncio.gather(log_task, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass

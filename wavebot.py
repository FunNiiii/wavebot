import redis
import redis
import os
import json
import random
import math
import asyncio
import re
import datetime
import glob

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Set, Tuple, Any
from collections import deque

import discord

# =========================================================
# Redis 설정 및 자동 마이그레이션 (Railway 대응)
# =========================================================
REDIS_URL = os.getenv('REDIS_URL')
if REDIS_URL:
    db = redis.from_url(REDIS_URL, decode_responses=True)
    print('Connected to Redis')
else:
    db = None
    print('Redis URL not found. Using local file system')

# =========================================================
# Redis 설정 (Railway 대응)
# =========================================================
REDIS_URL = os.getenv("REDIS_URL")
if REDIS_URL:
    db = redis.from_url(REDIS_URL, decode_responses=True)
    print("Connected to Redis")
else:
    db = None
    print("Redis URL not found. Using local file system")
from discord import app_commands
from discord.ext import commands

# =========================================================
# 설정(필요 시 수정)
# =========================================================
BOT_NAME = "WAVE BOT"
TEAM_VOICE_CATEGORY_NAME = "TEAM VOICE"
TEAM_VOICE_PREFIX = ""          # 예: "🔊 "
MAX_TEAMS = 50

GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0") or "0")

# 점수 파일: 스크립트 폴더 기준으로 통일 (서버 cwd와 무관하게 동일 경로 사용)
BOT_DIR = os.path.dirname(os.path.abspath(__file__))
SCORES_DAILY_FILE = "scores_daily.json"
SCORES_WEEKLY_FILE = "scores_weekly.json"
SCORES_EVENT_FILE = "scores_event.json"
# 실제 읽기/쓰기 경로 (스크립트와 같은 폴더)
SCORES_DAILY_PATH = os.path.join(BOT_DIR, SCORES_DAILY_FILE)
SCORES_WEEKLY_PATH = os.path.join(BOT_DIR, SCORES_WEEKLY_FILE)
SCORES_EVENT_PATH = os.path.join(BOT_DIR, SCORES_EVENT_FILE)
MATCH_STATS_FILE = "match_stats.json"

def _score_file_path(filename: str) -> str:
    """점수 파일이면 스크립트 폴더 경로, 아니면 cwd/스크립트 폴더에서 찾기"""
    if filename == SCORES_DAILY_FILE:
        return SCORES_DAILY_PATH
    if filename == SCORES_WEEKLY_FILE:
        return SCORES_WEEKLY_PATH
    if filename == SCORES_EVENT_FILE:
        return SCORES_EVENT_PATH
    if os.path.exists(filename):
        return filename
    alt = os.path.join(BOT_DIR, filename)
    return alt if os.path.exists(alt) else filename
QUEUE_MODE_FILE = "queue_mode.json"
TIERS_FILE = "tiers.json"
BANSAL_FILE = "bansal.json"
MATCH_STATE_FILE = "last_match.json"
QUEUE_STATE_FILE = "queue_state.json"
EXEMPTION_FILE = "exemptions.json"
EXEMPTION_PASS_LOGS_FILE = "exemption_pass_logs.json"
ROLLBACK_FILE = "last_result.json"

DRAFT_STATE_FILE = "draft_state.json"
PANEL_STATE_FILE = "panel_state.json"
SCOREBOARD_STATE_FILE = "scoreboard_state.json"

DICE_MIN = 1
DICE_MAX = 99

SELECT_MAX_OPTIONS = 25
VOICE_AUTO_DELETE_GRACE_SEC = 8

WIN_SCORE_MULTIPLIER = 1  # 필요하면 조절

EVENT_SCOREBOARD_MESSAGE_ID = None
EVENT_SCOREBOARD_CHANNEL_ID = None

# Tier helper (used for sorting)
def get_member_tier(x):
    """Return numeric tier (1~4). Supports discord.Member or user_id(int).
    Fallback: 999 (unknown) so it sorts last.
    """
    try:
        if isinstance(x, int):
            try:
                t = int(tiers.get(str(x), 0))
            except Exception:
                t = 0
            return t if t > 0 else 999

        uid = getattr(x, "id", None)
        if uid is not None:
            try:
                t = int(tiers.get(str(int(uid)), 0))
            except Exception:
                t = 0
            return t if t > 0 else 999
    except Exception:
        pass
    return 999

# =========================================================
# JSON 저장/로드
# =========================================================


def load_json(path: str) -> Any:
    key = os.path.basename(path)
    if db:
        data = db.get(key)
        if data: 
            try: return json.loads(data)
            except: pass
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = json.load(f)
                if db: db.set(key, json.dumps(content, ensure_ascii=False))
                return content
        except: return {}
    return {}

def load_json_lenient(path: str) -> dict:
    """JSON 로드(복구용 강화 버전).
    - 정상 JSON이면 그대로 로드
    - JSON 파싱 실패 시, 다음과 같은 '느슨한' 포맷도 최대한 복구합니다.
      예) { 닉네임: 12, 다른닉: 3 }  (키 따옴표 누락, 줄 단위, 끝 콤마 등)
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        pass

    # Fallback: line-based "key: value" parser
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception:
        return {}

    # Remove outer braces if present
    raw2 = raw.strip()
    if raw2.startswith("{") and raw2.endswith("}"):
        raw2 = raw2[1:-1]

    out: dict = {}
    for line in raw2.splitlines():
        s = line.strip()
        if not s:
            continue
        # remove trailing commas
        if s.endswith(","):
            s = s[:-1].rstrip()
        # ignore braces
        if s in ("{", "}"):
            continue

        # Match: "key": 123  OR  key: 123  OR  key : "123"
        m = re.match(r'^"?\s*(.*?)\s*"?\s*:\s*"?(-?\d+)"?\s*$', s)
        if not m:
            continue
        k = m.group(1).strip()
        v = int(m.group(2))
        if k:
            out[k] = v
    return out

def save_json(path: str, data: Any):
    key = os.path.basename(path)
    if db: db.set(key, json.dumps(data, ensure_ascii=False))
    else:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

def _load_json(path):
    return load_json(path)

def _save_json(path, data):
    save_json(path, data)

# ---------- queue mode state ----------
QUEUE_MODE = {}  # guild_id -> "normal" | "event"
# =========================================================
# 상태 영속화(재부팅 복구)
# =========================================================
queue_state_data: Dict[str, dict] = load_json(QUEUE_STATE_FILE)
draft_state_data: Dict[str, dict] = load_json(DRAFT_STATE_FILE)
panel_state_data: Dict[str, list] = load_json(PANEL_STATE_FILE)
queue_mode_data: Dict[str, str] = load_json(QUEUE_MODE_FILE)


def _save_queue_state_file():
    save_json(QUEUE_STATE_FILE, queue_state_data)

def _save_draft_state_file():
    save_json(DRAFT_STATE_FILE, draft_state_data)

def _save_panel_state_file():
    save_json(PANEL_STATE_FILE, panel_state_data)

def _save_queue_mode_file():
    save_json(QUEUE_MODE_FILE, queue_mode_data)

def get_queue_mode(guild_id: int) -> str:
    try:
        return str(queue_mode_data.get(str(int(guild_id)), "normal") or "normal")
    except Exception:
        return "normal"

def set_queue_mode(guild_id: int, mode: str) -> None:
    mode = (mode or "normal").strip().lower()
    if mode not in ("normal", "event"):
        mode = "normal"
    queue_mode_data[str(int(guild_id))] = mode
    _save_queue_mode_file()

def _serialize_queue_state(st: "GuildQueueState") -> dict:
    return {
        "member_ids": sorted([int(x) for x in (st.member_ids or [])]),
        "message_id": st.message_id,
        "channel_id": st.channel_id,
    }

def _save_queue_state_for_guild(guild_id: int):
    st = guild_queues.get(guild_id)
    if not st:
        queue_state_data.pop(str(guild_id), None)
    else:
        queue_state_data[str(guild_id)] = _serialize_queue_state(st)
    _save_queue_state_file()

def _load_queue_state_into_memory():
    for gid_str, d in (queue_state_data or {}).items():
        try:
            gid = int(gid_str)
        except Exception:
            continue
        st = GuildQueueState()
        mids = d.get("member_ids", []) or []
        st.member_ids = set(int(x) for x in mids if str(x).isdigit() or isinstance(x, int))
        st.message_id = d.get("message_id", None)
        st.channel_id = d.get("channel_id", None)
        guild_queues[gid] = st

def _serialize_draft_state(ds: "DraftSession") -> dict:
    return {
        "guild_id": int(ds.guild_id),
        "channel_id": int(ds.channel_id),
        "message_id": int(ds.message_id),
        "team_count": int(ds.team_count),
        "team_size": int(ds.team_size),
        "captain_ids": [int(x) for x in (ds.captain_ids or [])],
        "pool_ids": [int(x) for x in (ds.pool_ids or [])],
        "teams": [[int(x) for x in (tm or [])] for tm in (ds.teams or [])],
        "draft_mode": str(getattr(ds, "draft_mode", "snake")),
        "round_index": int(getattr(ds, "round_index", 1) or 1),
        "total_rounds": int(getattr(ds, "total_rounds", 1) or 1),
        "rolls": {str(int(k)): int(v) for k, v in (ds.rolls or {}).items()},
        "roll_order": [int(x) for x in (ds.roll_order or [])],
        "pick_sequence": [int(x) for x in (ds.pick_sequence or [])],
        "pick_pos": int(getattr(ds, "pick_pos", 0) or 0),
        "phase": str(getattr(ds, "phase", "picking")),
    }

def _save_draft_state_for_guild(guild_id: int):
    ds = guild_draft.get(guild_id)
    if not ds:
        draft_state_data.pop(str(guild_id), None)
    else:
        draft_state_data[str(guild_id)] = _serialize_draft_state(ds)
    _save_draft_state_file()

def _load_draft_state_into_memory():
    for gid_str, d in (draft_state_data or {}).items():
        try:
            gid = int(gid_str)
        except Exception:
            continue
        try:
            ds = DraftSession(
                guild_id=int(d.get("guild_id", gid)),
                channel_id=int(d.get("channel_id", 0)),
                message_id=int(d.get("message_id", 0)),
                team_count=int(d.get("team_count", 0)),
                team_size=int(d.get("team_size", 0)),
                captain_ids=[int(x) for x in (d.get("captain_ids", []) or [])],
                pool_ids=[int(x) for x in (d.get("pool_ids", []) or [])])
            ds.teams = [[int(x) for x in (tm or [])] for tm in (d.get("teams", []) or [])]
            ds.draft_mode = str(d.get("draft_mode", "snake") or "snake")
            ds.round_index = int(d.get("round_index", 1) or 1)
            ds.total_rounds = int(d.get("total_rounds", 1) or 1)
            ds.rolls = {int(k): int(v) for k, v in (d.get("rolls", {}) or {}).items()}
            ds.roll_order = [int(x) for x in (d.get("roll_order", []) or [])]
            ds.pick_sequence = [int(x) for x in (d.get("pick_sequence", []) or [])]
            ds.pick_pos = int(d.get("pick_pos", 0) or 0)
            ds.phase = str(d.get("phase", "picking") or "picking")
            guild_draft[gid] = ds
        except Exception:
            continue
def register_exemption_panel_message(*, guild_id: int, channel_id: int, message_id: int):
    gid = str(int(guild_id))
    arr = panel_state_data.setdefault(gid, [])
    if not isinstance(arr, list):
        arr = []
        panel_state_data[gid] = arr
    for it in arr:
        if int(it.get("channel_id", 0)) == int(channel_id) and int(it.get("message_id", 0)) == int(message_id):
            _save_panel_state_file()
            return
    arr.append({"channel_id": int(channel_id), "message_id": int(message_id)})
    if len(arr) > 20:
        panel_state_data[gid] = arr[-20:]
    _save_panel_state_file()


# =========================================================
# 점수 백업/복구 유틸 (실수로 초기화했을 때 대비)
# =========================================================
BACKUP_DIR = "backups"

def _ensure_backup_dir() -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    return BACKUP_DIR

def _backup_scores(kind: str, data: Dict[str, int], src_file: str) -> Optional[str]:
    """현재 점수 데이터를 백업 파일로 저장합니다.
    kind: 'daily' | 'weekly'
    반환: 생성된 백업 파일 경로 (실패 시 None)
    """
    try:
        _ensure_backup_dir()
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # 메모리 데이터가 비어있어도, 파일에 남아있을 수 있으므로 파일도 함께 고려
        file_data = load_json(src_file) if os.path.exists(src_file) else {}
        merged = {}
        # file_data 우선, 메모리 최신값으로 덮어쓰기
        if isinstance(file_data, dict):
            merged.update(file_data)
        if isinstance(data, dict):
            merged.update(data)

        backup_path = os.path.join(BACKUP_DIR, f"scores_{kind}_backup_{ts}.json")
        save_json(backup_path, merged)
        return backup_path
    except Exception:
        return None

def _find_latest_backup(kind: str) -> Optional[str]:
    try:
        pattern = os.path.join(BACKUP_DIR, f"scores_{kind}_backup_*.json")
        files = glob.glob(pattern)
        if not files:
            return None
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return files[0]
    except Exception:
        return None

def _coerce_scores_dict(d: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        if k is None:
            continue
        name = str(k)
        try:
            out[name] = int(v)
        except Exception:
            continue
    return out

# 시작 시 점수 파일은 스크립트와 같은 폴더(서버 cwd와 무관)에서 로드
daily_scores: Dict[str, int] = load_json(SCORES_DAILY_PATH)
weekly_scores: Dict[str, int] = load_json(SCORES_WEEKLY_PATH)
event_scores: Dict[str, int] = load_json(SCORES_EVENT_PATH)
tiers: Dict[str, int] = load_json(TIERS_FILE)
bansal_data: Dict[str, dict] = load_json(BANSAL_FILE)
exemptions_data: Dict[str, dict] = load_json(EXEMPTION_FILE)

# ---------------------------------------------------------
# 벤살 동시 실행 방지(상호작용/중복 클릭 대비) - 길드별 Lock
# ---------------------------------------------------------
_bansal_locks: Dict[int, asyncio.Lock] = {}

def get_bansal_lock(guild_id: int) -> asyncio.Lock:
    """길드별 벤살 동시 실행을 직렬화하기 위한 Lock."""
    lock = _bansal_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        _bansal_locks[guild_id] = lock
    return lock


# ---------------------------------------------------------
# 반성문 면제권 동시 실행 방지 - 길드별 Lock
# ---------------------------------------------------------
_exemption_locks: Dict[int, asyncio.Lock] = {}

def get_exemption_lock(guild_id: int) -> asyncio.Lock:
    lock = _exemption_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        _exemption_locks[guild_id] = lock
    return lock


match_state_data: Dict[str, dict] = load_json(MATCH_STATE_FILE)
queue_state_data: Dict[str, dict] = load_json(QUEUE_STATE_FILE)



# =========================================================
# LastMatchState 영속화 (메시지/채널 삭제되어도 매치 정보 유지)
# =========================================================
def _serialize_last_match(st: "LastMatchState") -> dict:
    return {
        "teams": st.teams,
        "team_count": st.team_count,
        "team_size": st.team_size,
        "voice_category_id": st.voice_category_id,
        "voice_channel_ids": st.voice_channel_ids,
        "active": st.active,
        "pending": st.pending,  # ✅ 추가
        "match_mode": getattr(st, "match_mode", "normal"),
    }

def _load_last_match_into_memory():
    """프로세스 시작 시 last_match.json -> guild_last_match로 로드"""
    global match_state_data
    try:
        for gid_str, d in (match_state_data or {}).items():
            try:
                gid = int(gid_str)
            except Exception:
                continue
            st = LastMatchState()
            st.teams = d.get("teams", []) or []
            st.team_count = int(d.get("team_count", 0) or 0)
            st.team_size = int(d.get("team_size", 0) or 0)
            st.voice_category_id = d.get("voice_category_id", None)
            st.voice_channel_ids = d.get("voice_channel_ids", []) or []
            st.active = bool(d.get("active", False))
            st.pending = bool(d.get("pending", d.get("active", False)))  # ✅ 추가(구버전 호환)
            st.match_mode = str(d.get("match_mode", "normal") or "normal")
            # guild_id 변수가 아닌, 현재 루프에서 파싱한 gid를 사용해야 합니다.
            guild_last_match[gid] = st  # ✅ 반드시 줄 분리(문법오류 방지)
    except Exception:
        pass

def _save_last_match_for_guild(guild_id: int):
    global match_state_data
    st = guild_last_match.get(guild_id)
    if not st:
        match_state_data.pop(str(guild_id), None)
    else:
        match_state_data[str(guild_id)] = _serialize_last_match(st)
    save_json(MATCH_STATE_FILE, match_state_data)

def _ensure_last_match_loaded(guild_id: int):
    """메모리에 없으면 파일에서 다시 로드(재시작/핫리로드 대응)"""
    if guild_id in guild_last_match:
        return
    d = (match_state_data or {}).get(str(guild_id))
    if not d:
        return
    st = LastMatchState()
    st.teams = d.get("teams", []) or []
    st.team_count = int(d.get("team_count", 0) or 0)
    st.team_size = int(d.get("team_size", 0) or 0)
    st.voice_category_id = d.get("voice_category_id", None)
    st.voice_channel_ids = d.get("voice_channel_ids", []) or []
    st.active = bool(d.get("active", False))
    st.pending = bool(d.get("pending", d.get("active", False)))  # ✅ 추가(구버전 호환)
    guild_last_match[guild_id] = st  # ✅ guild_id로 저장
    
# =========================================================
# 타임아웃 방지
# =========================================================
async def safe_defer(interaction: discord.Interaction, thinking: bool = False, ephemeral: bool = False):
    try:
        if interaction.response.is_done():
            return
        try:
            await interaction.response.defer(thinking=thinking, ephemeral=ephemeral)
        except TypeError:
            await interaction.response.defer()
    except Exception:
        pass


async def safe_send(
    interaction: discord.Interaction,
    *,
    content: Optional[str] = None,
    embed: Optional[discord.Embed] = None,
    view: Optional[discord.ui.View] = None,
    ephemeral: bool = False,
):

    kwargs = {}
    if content is not None:
        kwargs["content"] = content
    if embed is not None:
        kwargs["embed"] = embed
    kwargs["ephemeral"] = ephemeral
    if view is not None:
        kwargs["view"] = view

    try:
        if interaction.response.is_done():
            return await interaction.followup.send(**kwargs)
        return await interaction.response.send_message(**kwargs)
    except discord.errors.NotFound:
        return None
    except Exception:
        # 마지막 시도: followup
        try:
            return await interaction.followup.send(**kwargs)
        except Exception:
            return None


async def safe_edit_message(message: Optional[discord.Message], *, content=None, embed=None, view=None):
    if not message:
        return
    try:
        await message.edit(content=content, embed=embed, view=view)
    except Exception:
        pass


# =========================================================
# 점수판 자동갱신 (점수 변동 시 등록된 점수판 메시지 자동 갱신)
# =========================================================
def _load_scoreboard_state() -> dict:
    if not os.path.exists(SCOREBOARD_STATE_FILE):
        return {}
    try:
        data = load_json(SCOREBOARD_STATE_FILE)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save_scoreboard_state(data: dict):
    try:
        save_json(SCOREBOARD_STATE_FILE, data)
    except Exception:
        pass

def _register_scoreboard_message(guild_id: int, channel_id: int, message_id: int, scope: str = "weekly"):
    """점수판 메시지를 등록하여 점수 변동 시 자동 갱신되도록 합니다."""
    data = _load_scoreboard_state()
    gid = str(int(guild_id))
    arr = data.get(gid, [])
    if not isinstance(arr, list):
        arr = []
    for it in arr:
        if int(it.get("channel_id", 0)) == int(channel_id) and int(it.get("message_id", 0)) == int(message_id):
            it["scope"] = str(scope)
            _save_scoreboard_state(data)
            return
    arr.append({"channel_id": int(channel_id), "message_id": int(message_id), "scope": str(scope)})
    if len(arr) > 20:
        arr = arr[-20:]
    data[gid] = arr
    _save_scoreboard_state(data)

def _build_scoreboard_embed_auto(scope: str):
    """자동갱신용 점수판 embed (scope: daily | weekly | event)"""
    if scope == "daily":
        scores = daily_scores
        title = "📊 점수판 (일간)"
    elif scope == "weekly":
        scores = weekly_scores
        title = "📊 점수판 (주간)"
    else:
        scores = event_scores
        title = "📊 점수판 (이벤트)"
    embed = build_scoreboard_embed(title, scores)
    embed.set_footer(text="CLAN WAVE · 점수 변동 시 자동 갱신")
    return embed

DASHBOARD_STATE_FILE = "dashboard_state.json"
DASHBOARD_STATE_PATH = os.path.join(BOT_DIR, DASHBOARD_STATE_FILE)

def _load_dashboard_state() -> dict:
    if not os.path.exists(DASHBOARD_STATE_PATH):
        return {}
    try:
        data = load_json(DASHBOARD_STATE_PATH)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save_dashboard_state(data: dict):
    try:
        save_json(DASHBOARD_STATE_PATH, data)
    except Exception:
        pass

def _register_dashboard_message(guild_id: int, channel_id: int, message_id: int):
    data = _load_dashboard_state()
    gid = str(int(guild_id))
    data[gid] = {"channel_id": int(channel_id), "message_id": int(message_id)}
    _save_dashboard_state(data)

async def refresh_dashboard(bot, gid):
    try:
        guild = bot.get_guild(gid)
        if not guild: return
        q_state = guild_queues.get(gid)
        if not q_state or not q_state.message_id: return
        channel = bot.get_channel(q_state.channel_id)
        if not channel: return
        try:
            msg = await channel.fetch_message(q_state.message_id)
        except: return
        members = []
        for uid in q_state.member_ids:
            m = guild.get_member(uid)
            if m:
                t = get_tier(uid)
                sort_t = t if t > 0 else 999
                members.append((sort_t, m.display_name.lower(), f"- {display_with_tier(m)}"))
        members.sort(key=lambda x: (x[0], x[1]))
        body = "\n".join([line for _, __, line in members]).strip() if members else "(비어있음)"
        embed = discord.Embed(
            title="📌 대기열",
            description=f"현재 인원: **{len(q_state.member_ids)}명**\n\n{body}",
            color=discord.Color.blue()
        )
        await msg.edit(embed=embed)
    except Exception as e:
        print(f"Dashboard refresh error: {e}")

async def build_dashboard_embed(guild: discord.Guild) -> discord.Embed:
    gid = guild.id
    embed = discord.Embed(
        title="🖥️ WAVE 실시간 대시보드",
        color=discord.Color.blue(),
        timestamp=discord.utils.utcnow()
    )
    q_state = guild_queues.get(gid)
    body = "(비어있음)"
    if q_state and q_state.member_ids:
        members = []
        for uid in q_state.member_ids:
            m = guild.get_member(uid)
            if m:
                t = get_tier(uid)
                sort_t = t if t > 0 else 999
                members.append((sort_t, m.display_name.lower(), f"- {display_with_tier(m)}"))
        members.sort(key=lambda x: (x[0], x[1]))
        body = "\n".join([line for _, __, line in members]).strip() if members else "(비어있음)"
    embed.add_field(name="📌 대기열", value=f"현재 인원: **{len(q_state.member_ids) if q_state else 0}명**\n\n{body}", inline=False)
    return embed

async def refresh_scoreboard_messages(bot_instance, guild_id=None):
    """등록된 점수판 메시지들을 현재 점수로 갱신합니다. guild_id가 있으면 해당 길드만."""
    if guild_id:
        asyncio.create_task(refresh_dashboard(bot_instance, guild_id))
    try:
        data = _load_scoreboard_state()
        for gid_str, arr in list(data.items()):
            try:
                gid = int(gid_str)
                if guild_id is not None and gid != guild_id:
                    continue
            except Exception:
                continue
            for it in (arr or []):
                try:
                    ch_id = int(it.get("channel_id", 0))
                    msg_id = int(it.get("message_id", 0))
                    scope = str(it.get("scope", "weekly"))
                    if not ch_id or not msg_id:
                        continue
                    ch = bot_instance.get_channel(ch_id)
                    if ch is None:
                        try:
                            ch = await bot_instance.fetch_channel(ch_id)
                        except Exception:
                            continue
                    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                        continue
                    try:
                        msg = await ch.fetch_message(msg_id)
                    except Exception:
                        continue
                    embed = _build_scoreboard_embed_auto(scope)
                    await msg.edit(embed=embed)
                except Exception:
                    continue
    except Exception:
        pass


def _load_exemption_pass_logs() -> list:
    """exemption_pass_logs.json을 list 형태로 관리. 없으면 자동 생성."""
    try:
        if not os.path.exists(EXEMPTION_PASS_LOGS_FILE):
            save_json(EXEMPTION_PASS_LOGS_FILE, [])
            return []
        data = load_json(EXEMPTION_PASS_LOGS_FILE)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_exemption_pass_logs(items: list) -> None:
    try:
        save_json(EXEMPTION_PASS_LOGS_FILE, items)
    except Exception:
        pass


try:
    _load_exemption_pass_logs()
except Exception:
    pass

def append_exemption_log(
    *,
    guild_id: int,
    action: str,
    target_user_id: int,
    amount: int,
    actor_member: Optional[discord.abc.User] = None,
    target_member: Optional[discord.abc.User] = None,
    note: Optional[str] = None,
) -> None:
    """면제권 추가/제거/사용 로그를 exemption_pass_logs.json에 기록."""
    items = _load_exemption_pass_logs()

    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    entry = {
        "ts": int(now.timestamp()),
        "iso": now.isoformat(),
        "guild_id": int(guild_id),
        "action": str(action),
        "target_user_id": int(target_user_id),
        "target_name": str(target_member) if target_member else None,
        "amount": int(amount),
        "actor_user_id": int(actor_member.id) if actor_member else None,
        "actor_name": str(actor_member) if actor_member else None,
        "note": note,
    }
    items.append(entry)

    # 너무 커지면 최근 5000개만 유지
    if len(items) > 5000:
        items = items[-5000:]

    _save_exemption_pass_logs(items)


async def log_exemption_event(guild: discord.Guild, text: str):
    """
    (호환용) 예전엔 지정 채널로 로그를 보냈지만,
    현재는 exemption_pass_logs.json에만 기록합니다.
    """
    append_exemption_log(
        guild_id=guild.id,
        action="text",
        target_user_id=0,
        amount=0,
        actor_member=None,
        target_member=None,
        note=text
    )


# =========================================================
# 권한 체크
# =========================================================
def is_admin():
    async def predicate(interaction: discord.Interaction):
        if interaction.user.guild_permissions.administrator:
            return True
        await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
        return False
    return app_commands.check(predicate)


def user_is_admin(interaction: discord.Interaction) -> bool:
    return bool(interaction.user.guild_permissions.administrator)


# =========================================================
# 길드별 상태
# =========================================================
@dataclass
class GuildQueueState:
    member_ids: Set[int] = field(default_factory=set)
    message_id: Optional[int] = None
    channel_id: Optional[int] = None


@dataclass
class LastMatchState:
    teams: List[List[int]] = field(default_factory=list)
    team_count: int = 0
    team_size: int = 0
    voice_category_id: Optional[int] = None
    voice_channel_ids: List[int] = field(default_factory=list)
    active: bool = False
    pending: bool = False  # ✅ 추가
    match_mode: str = "normal"  # "normal" | "event"


@dataclass
class DraftSession:
    guild_id: int
    channel_id: int
    message_id: int

    team_count: int
    team_size: int
    captain_ids: List[int]
    pool_ids: List[int]

    teams: List[List[int]] = field(default_factory=list)

    # 드래프트 모드
    # - "snake": 기존 스네이크(팀장 다이스 1회 → 전체 픽 진행)
    # - "dice": (요청) 라운드마다 다이스를 다시 굴려 픽 순서를 재결정(라운드=팀당 추가 인원 수)
    draft_mode: str = "snake"
    round_index: int = 1
    total_rounds: int = 1

    # 자동 다이스(동점 없게 보정) 결과
    rolls: Dict[int, int] = field(default_factory=dict)          # captain_id -> dice
    roll_order: List[int] = field(default_factory=list)          # 높은값 순 (팀장 순위)

    # 스네이크 픽 시퀀스: 1등->...->꼴등->...->1등 반복
    pick_sequence: List[int] = field(default_factory=list)       # captain_id list
    pick_pos: int = 0                                            # pick_sequence index (0-based)

    phase: str = "picking"  # "picking" only (자동 다이스 후 바로 픽)

guild_queues: Dict[int, GuildQueueState] = {}



# =========================================================
# 대기열 상태 영속화 (봇 재부팅 후에도 기존 패널/버튼이 동작하도록)
# - member_ids / message_id / channel_id 를 저장/복구합니다.
# =========================================================
def _serialize_queue_state(st: "GuildQueueState") -> dict:
    return {
        "member_ids": sorted([int(x) for x in (st.member_ids or set())]),
        "message_id": st.message_id,
        "channel_id": st.channel_id,
    }

def _load_queue_state_into_memory():
    """프로세스 시작/재부팅 시 queue_state.json -> guild_queues 로 로드"""
    global queue_state_data
    try:
        for gid_str, d in (queue_state_data or {}).items():
            try:
                gid = int(gid_str)
            except Exception:
                continue
            st = GuildQueueState()
            st.member_ids = set(int(x) for x in (d.get("member_ids") or []) if str(x).isdigit())
            st.message_id = d.get("message_id", None)
            st.channel_id = d.get("channel_id", None)
            guild_queues[gid] = st
    except Exception:
        pass

def _save_queue_state_for_guild(guild_id: int):
    global queue_state_data
    st = guild_queues.get(guild_id)
    if not st:
        queue_state_data.pop(str(guild_id), None)
    else:
        queue_state_data[str(guild_id)] = _serialize_queue_state(st)
    save_json(QUEUE_STATE_FILE, queue_state_data)
guild_last_match: Dict[int, LastMatchState] = {}
guild_draft: Dict[int, DraftSession] = {}

# 부팅 시 저장된 상태 복구
_load_queue_state_into_memory()
_load_draft_state_into_memory()

# ---------- score helpers ----------
def _add_score(path, name, pts):
    global daily_scores, weekly_scores, event_scores
    path_use = _score_file_path(path)
    d = _load_json(path_use)
    d[name] = int(d.get(name, 0)) + int(pts)
    _save_json(path_use, d)
    if path == SCORES_DAILY_FILE:
        daily_scores = load_json(SCORES_DAILY_PATH)
    elif path == SCORES_WEEKLY_FILE:
        weekly_scores = load_json(SCORES_WEEKLY_PATH)
    elif path == SCORES_EVENT_FILE:
        event_scores = load_json(SCORES_EVENT_PATH)

def _remove_score(path, name, pts):
    global daily_scores, weekly_scores, event_scores
    path_use = _score_file_path(path)
    d = _load_json(path_use)
    d[name] = int(d.get(name, 0)) - int(pts)
    d[name] = max(0, d[name])
    _save_json(path_use, d)
    if path == SCORES_DAILY_FILE:
        daily_scores = load_json(SCORES_DAILY_PATH)
    elif path == SCORES_WEEKLY_FILE:
        weekly_scores = load_json(SCORES_WEEKLY_PATH)
    elif path == SCORES_EVENT_FILE:
        event_scores = load_json(SCORES_EVENT_PATH)

def _set_score(path, name, pts):
    global daily_scores, weekly_scores, event_scores
    path_use = _score_file_path(path)
    d = _load_json(path_use)
    d[name] = int(pts)
    _save_json(path_use, d)
    if path == SCORES_DAILY_FILE:
        daily_scores = load_json(SCORES_DAILY_PATH)
    elif path == SCORES_WEEKLY_FILE:
        weekly_scores = load_json(SCORES_WEEKLY_PATH)
    elif path == SCORES_EVENT_FILE:
        event_scores = load_json(SCORES_EVENT_PATH)

# =========================================================
# 점수 유틸
# =========================================================
def add_points(name: str, pts: int, *, daily: bool = True, weekly: bool = True):
    global daily_scores, weekly_scores
    if daily:
        daily_scores[name] = int(daily_scores.get(name, 0)) + int(pts)
        save_json(SCORES_DAILY_PATH, daily_scores)
    if weekly:
        weekly_scores[name] = int(weekly_scores.get(name, 0)) + int(pts)
        save_json(SCORES_WEEKLY_PATH, weekly_scores)

def add_event_points(name: str, pts: int):
    """이벤트 점수 추가"""
    global event_scores
    event_scores[name] = int(event_scores.get(name, 0)) + int(pts)
    save_json(SCORES_EVENT_PATH, event_scores)

def clamp_nonnegative(scores: Dict[str, int], name: str):
    scores[name] = max(0, int(scores.get(name, 0)))


def sorted_top(scores: Dict[str, int], top_n: int) -> List[Tuple[str, int]]:
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_n]


# =========================================================
# 티어 유틸
# =========================================================
def get_tier(user_id: int) -> int:
    return int(tiers.get(str(user_id), 0))


def tier_badge(t: int) -> str:
    n = int(t)
    # 티어별 색(원) 매핑: 필요 시 여기만 수정
    color = {
        1: "1️⃣",
        2: "2️⃣",
        3: "3️⃣",
        4: "4️⃣",
    }.get(n, "⚪" if n <= 0 else "🔵")

    return f"{color}"


def tier_emoji_text(t: int) -> str:
    """(티어목록 등에서만) 텍스트 라벨이 필요한 경우."""
    if t == 4:
        return "🟣 4티어"
    if t == 3:
        return "🟢 3티어"
    if t == 2:
        return "🟠 2티어"
    if t == 1:
        return "🔴 1티어"
    if t <= 0:
        return "⚪ 미등록"
    return f"🏷️ {t}티어"


def display_with_tier(member: discord.Member) -> str:
    t = get_tier(member.id)
    # 예: 닉네임 🔴①
    return f"{member.display_name} {tier_badge(t)}"


# =========================================================
# 티어 균형 분배(팀장 없음)
# =========================================================
def tier_balanced_assign_no_captain(member_ids: List[int], team_count: int, team_size: int) -> List[List[int]]:
    teams: List[List[int]] = [[] for _ in range(team_count)]
    sums: List[int] = [0 for _ in range(team_count)]

    sorted_ids = sorted(member_ids, key=lambda uid: get_tier(uid), reverse=True)

    for uid in sorted_ids:
        candidates = [i for i in range(team_count) if len(teams[i]) < team_size]
        min_sum = min(sums[i] for i in candidates)
        min_teams = [i for i in candidates if sums[i] == min_sum]
        i = random.choice(min_teams)
        teams[i].append(uid)
        sums[i] += get_tier(uid)

    return teams


# =========================================================
# 다음판 팀원 중복 방지(직전 매치 기준)
# - 같은 두 사람이 연속으로 같은 팀에 배정되는 것을 최대한 방지
# =========================================================
def _pairs_from_teams(teams: List[List[int]]) -> Set[Tuple[int, int]]:
    pairs: Set[Tuple[int, int]] = set()
    for team in teams or []:
        t = [int(x) for x in (team or [])]
        for i in range(len(t)):
            for j in range(i + 1, len(t)):
                a, b = t[i], t[j]
                if a == b:
                    continue
                pairs.add((a, b) if a < b else (b, a))
    return pairs

def _count_repeated_pairs(new_teams: List[List[int]], prev_pairs: Set[Tuple[int, int]]) -> int:
    if not prev_pairs:
        return 0
    new_pairs = _pairs_from_teams(new_teams)
    return sum(1 for p in new_pairs if p in prev_pairs)

def make_teams_avoid_repeat(
    member_ids: List[int],
    team_count: int,
    team_size: int,
    *,
    prev_teams: Optional[List[List[int]]] = None,
    mode: str = "tier_balanced",   # "tier_balanced" | "random"
    max_tries: int = 250,
) -> Tuple[List[List[int]], int]:
    prev_pairs = _pairs_from_teams(prev_teams or [])
    ids = [int(x) for x in member_ids]
    best_teams: List[List[int]] = []
    best_repeat = 10**9

    for _ in range(max_tries):
        trial_ids = ids[:]
        random.shuffle(trial_ids)

        # 팀 만들기
        if mode == "tier_balanced":
            teams = tier_balanced_assign_no_captain(trial_ids, team_count, team_size)
        else:
            teams = [trial_ids[i*team_size:(i+1)*team_size] for i in range(team_count)]

        rep = _count_repeated_pairs(teams, prev_pairs)

        if rep < best_repeat:
            best_repeat = rep
            best_teams = teams

        if rep == 0:
            break

    return best_teams, (0 if best_repeat == 10**9 else best_repeat)

def _unique_list(items):
    """Return list with duplicates removed, preserving original order."""
    seen = set()
    out = []
    for x in items or []:
        if x is None:
            continue
        s = str(x).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out

def _get_bansal_bucket(guild_id: int) -> dict:
    """길드별 벤살 상태(dict)를 가져오고, 스키마를 보정합니다.

    저장 구조(길드별):
      - md/pd: 영구 목록(관리자/명령어로 추가/제거)
      - cur_md/cur_pd: 현재 판에서 뽑힌 결과(종료 전까지 유지)
      - exclude_md/exclude_pd: 이번 판에서만 제외(직전 판 결과 1회 제외 등)
      - next_md/next_pd: 벤살 종료 시 저장되는 '다음 판 1회 제외' 대상
      - finalized: 이번 판 '벤살 종료' 여부(종료 후에는 추가 뽑기 불가)
    """
    gid = str(guild_id)

    # ❗ 재귀 호출 금지: 반드시 전역 저장소(bansal_data)에서 버킷을 꺼내거나 생성합니다.
    b = bansal_data.setdefault(gid, {})

    # 영구 목록
    b.setdefault("md", [])
    b.setdefault("pd", [])

    # 현재 결과
    b.setdefault("cur_md", [])
    b.setdefault("cur_pd", [])

    # 이번 판 제외 / 다음 판 1회 제외
    b.setdefault("exclude_md", [])
    b.setdefault("exclude_pd", [])
    b.setdefault("next_md", [])
    b.setdefault("next_pd", [])

    # 과거 버전(영구 중복방지 used_*)가 남아 있으면 무시(마이그레이션)
    if "used_md" in b:
        b.pop("used_md", None)
    if "used_pd" in b:
        b.pop("used_pd", None)
    if "last_md" in b:
        # 예전 last_*는 next_*로 흡수
        if not b.get("next_md"):
            b["next_md"] = list(b.get("last_md") or [])
        b.pop("last_md", None)
    if "last_pd" in b:
        if not b.get("next_pd"):
            b["next_pd"] = list(b.get("last_pd") or [])
        b.pop("last_pd", None)

    # 중복 제거/정리
    b["md"] = [str(x).strip() for x in b.get("md", []) if str(x).strip()]
    b["pd"] = [str(x).strip() for x in b.get("pd", []) if str(x).strip()]
    b["cur_md"] = [str(x).strip() for x in b.get("cur_md", []) if str(x).strip()]
    b["cur_pd"] = [str(x).strip() for x in b.get("cur_pd", []) if str(x).strip()]
    b["exclude_md"] = [str(x).strip() for x in b.get("exclude_md", []) if str(x).strip()]
    b["exclude_pd"] = [str(x).strip() for x in b.get("exclude_pd", []) if str(x).strip()]
    b["next_md"] = [str(x).strip() for x in b.get("next_md", []) if str(x).strip()]
    b["next_pd"] = [str(x).strip() for x in b.get("next_pd", []) if str(x).strip()]

    return b

def save_bansal():
    save_json(BANSAL_FILE, bansal_data)


# =========================================================
# 반성문 면제권(길드별) 유틸 / 저장
# =========================================================
# exemptions_data 구조(길드별):
# {
#   "<guild_id>": {
#       "<user_id>": 3,
#       ...
#   }
# }

def save_exemptions():
    save_json(EXEMPTION_FILE, exemptions_data)

def _get_exemption_bucket(guild_id: int) -> Dict[str, int]:
    gid = str(guild_id)
    b = exemptions_data.setdefault(gid, {})

    if not isinstance(b, dict):
        b = {}
        exemptions_data[gid] = b
    # 값 타입 보정(int)
    for k, v in list(b.items()):
        try:
            b[str(k)] = int(v)
        except Exception:
            b.pop(k, None)
    return b


def format_recent_exemption_logs(guild: discord.Guild, guild_id: int, limit: int = 12) -> str:
    """면제권(추가/제거/사용) 최근 로그를 패널에 표시용으로 포맷."""
    logs = _load_exemption_pass_logs()
    items = [x for x in logs if int(x.get("guild_id", 0)) == int(guild_id)]
    if not items:
        return "(로그 없음)"

    # 최신순
    items = items[-limit:][::-1]

    def _name(uid: int) -> str:
        m = guild.get_member(uid)
        return m.display_name if m else f"{uid}"
    lines: List[str] = []
    for it in items:
        action = str(it.get("action", "")).lower()
        amt = int(it.get("amount", 0) or 0)
        actor = _name(int(it.get("actor_user_id", 0) or 0))
        target = _name(int(it.get("target_user_id", 0) or 0))

        if action == "use":
            msg = f"{target} 사용 (-{abs(amt) or 1})"
        elif action == "add":
            msg = f"{actor} → {target} 추가 (+{abs(amt)})"
        elif action == "remove":
            msg = f"{actor} → {target} 제거 (-{abs(amt)})"
        elif action == "reset":
            # target_user_id가 0일 수 있음
            msg = f"{actor} 면제권 전체 초기화"
        else:
            msg = f"{actor} → {target} {action} ({amt})"
        lines.append(msg)

    text = "\n".join(lines)
    # embed field limit safety
    return text[:1020] + "…" if len(text) > 1024 else text


def get_exemption_count(guild_id: int, user_id: int) -> int:
    b = _get_exemption_bucket(guild_id)
    return max(0, int(b.get(str(user_id), 0)))

def add_exemptions(guild_id: int, user_id: int, amount: int) -> int:
    b = _get_exemption_bucket(guild_id)
    amt = max(0, int(amount))
    b[str(user_id)] = max(0, int(b.get(str(user_id), 0)) + amt)
    save_exemptions()
    return int(b[str(user_id)])

def use_one_exemption(guild_id: int, user_id: int) -> bool:
    b = _get_exemption_bucket(guild_id)
    cur = int(b.get(str(user_id), 0))
    if cur <= 0:
        return False
    b[str(user_id)] = cur - 1
    if b[str(user_id)] <= 0:
        b.pop(str(user_id), None)
    save_exemptions()
    return True



def remove_exemptions(guild_id: int, user_id: int, amount: int) -> Tuple[int, int]:
    """면제권 차감(0 밑으로 내려가지 않음). 반환: (실제 차감, 남은 수량)"""
    b = _get_exemption_bucket(guild_id)
    key = str(int(user_id))
    cur = int(b.get(key, 0) or 0)
    amt = max(0, int(amount))
    removed = min(cur, amt)
    left = cur - removed
    if left <= 0:
        b.pop(key, None)
        left = 0
    else:
        b[key] = left
    save_exemptions()
    return removed, left

def reset_exemptions(guild_id: int) -> int:
    """해당 길드의 면제권 보유 데이터를 전부 초기화합니다.

    Returns:
        int: 초기화(삭제)된 보유자(키) 수
    """
    # NOTE:
    # 기존 구현은 `_load_exemptions()` 를 호출했지만, 본 파일에는 해당 함수가 없어서
    # `/면제권초기화` 실행 시 NameError 가 발생했습니다.
    # 면제권 데이터는 이미 전역 `exemptions_data` 로 로딩/관리되고 있으므로,
    # 그 버킷을 직접 비우고 `save_exemptions()` 로 저장합니다.

    bucket = _get_exemption_bucket(guild_id)
    cleared = len(bucket)
    bucket.clear()
    save_exemptions()
    return cleared

def bansal_add(guild_id: int, kind: str, items: List[str]) -> Tuple[int, int]:
    b = _get_bansal_bucket(guild_id)
    key = "md" if kind == "md" else "pd"
    existed = 0
    added = 0
    for it in items:
        if it in b[key]:
            existed += 1
            continue
        b[key].append(it)
        added += 1
    save_bansal()
    return added, existed

def bansal_remove(guild_id: int, kind: str, item: str) -> bool:
    b = _get_bansal_bucket(guild_id)
    key = "md" if kind == "md" else "pd"
    if item in b[key]:
        b[key].remove(item)
        # 진행 중/사용중에서도 제거
        for k in ("cur_md","cur_pd","last_md","last_pd"):
            if item in b.get(k, []):
                b[k].remove(item)
        save_bansal()
        return True
    return False

def bansal_list_text(guild_id: int, kind: str = "all") -> str:
    b = _get_bansal_bucket(guild_id)

    md = b.get("md", [])
    pd = b.get("pd", [])

    kind = (kind or "all").lower()

    lines: List[str] = []
    lines.append("📄 **벤살 유닛 목록**")

    if kind in ("md", "마뎀"):
        lines.append("")
        lines.append(f"**마뎀 유닛 ({len(md)}개)**")
        lines.append( "\n".join([f"- {x}" for x in md]) if md else "(없음)")
    elif kind in ("pd", "물뎀"):
        lines.append("")
        lines.append(f"**물뎀 유닛 ({len(pd)}개)**")
        lines.append( "\n".join([f"- {x}" for x in pd]) if pd else "(없음)")
    else:
        lines.append("")
        lines.append(f"**마뎀 유닛 ({len(md)}개)**")
        lines.append( "\n".join([f"- {x}" for x in md]) if md else "(없음)")
        lines.append("")
        lines.append(f"**물뎀 유닛 ({len(pd)}개)**")
        lines.append( "\n".join([f"- {x}" for x in pd]) if pd else "(없음)")

    return "\n".join(lines)

def bansal_total_current(guild_id: int) -> int:
    b = _get_bansal_bucket(guild_id)
    return len(b.get("cur_md", [])) + len(b.get("cur_pd", []))


def bansal_begin_round_if_needed(guild_id: int) -> None:
    # 벤살 중복 방지
    b = _get_bansal_bucket(guild_id)

    # 진행 중인 벤살이 있으면 라운드 승계를 하지 않습니다.
    if b.get("cur_md") or b.get("cur_pd"):
        return

    # 직전 판 결과를 이번 판 제외로 반영(1회 제외)
    b["exclude_md"] = _unique_list(b.get("next_md", []))
    b["exclude_pd"] = _unique_list(b.get("next_pd", []))

    # next_*는 로그/다음판 검사용으로 유지합니다.
    save_bansal()


def bansal_available(guild_id: int, category: str) -> list[str]:
    """현재 길드 벤살에서 뽑을 수 있는 후보를 반환합니다.

    중복 방지(요구사항 반영):
    - exclude_* : 이번 판에서 제외(직전 판 1회 제외 포함)
    - cur_*     : 이번 판 진행 중 결과
    - next_*    : '벤살 종료'로 확정된 직전 판 결과(다음 판 1회 제외용 레코드)
    위 6개 리스트를 모두 합쳐 'used'로 보고,
    **마딜/물딜 구분 없이 동일 유닛은 다시 나오지 않도록(교차 중복 방지)** 처리합니다.
    """
    b = _get_bansal_bucket(guild_id)

    # 풀은 길드별 저장 목록(md/pd)에서 가져옵니다.
    pool = list(b.get("md" if category == "md" else "pd", []))

    used = set(
        (b.get("exclude_md", []) or [])
        + (b.get("exclude_pd", []) or [])
        + (b.get("cur_md", []) or [])
        + (b.get("cur_pd", []) or [])
        + (b.get("next_md", []) or [])
        + (b.get("next_pd", []) or [])
    )

    return [x for x in pool if x not in used]
def bansal_draw_one(guild_id: int, category: str) -> str | None:
    bansal_begin_round_if_needed(guild_id)

    b = _get_bansal_bucket(guild_id)
    available = bansal_available(guild_id, category)
    if not available:
        return None

    pick = random.choice(available)
    if category == "md":
        b["cur_md"].append(pick)
    else:
        b["cur_pd"].append(pick)

    save_bansal()
    return pick


def bansal_finalize_round(guild_id: int) -> None:
    """종료 버튼 시(또는 경기 종료 시):
    - 이번 판 결과(cur_*)를 다음 판 1회 제외(next_*)로 저장
    - 단, 이미 cur_*가 비어있으면(이미 종료된 상태 등) next_*를 덮어쓰지 않는다.
      => '벤살 종료' 후 '승리팀 반영'에서 finalize를 다시 호출해도 next_*가 날아가지 않도록 보호
    - 이번 판 결과/제외 상태는 초기화
    """
    b = _get_bansal_bucket(guild_id)

    cur_md = list(b.get("cur_md", []))
    cur_pd = list(b.get("cur_pd", []))

    # ✅ 이미 종료된 상태(현재 결과가 비어있음)라면 next_*를 덮어쓰지 않음
    if not cur_md and not cur_pd:
        return

    b["next_md"] = cur_md
    b["next_pd"] = cur_pd

    b["cur_md"] = []
    b["cur_pd"] = []
    b["exclude_md"] = []
    b["exclude_pd"] = []

    save_bansal()

def bansal_reset_current(guild_id: int) -> None:
    """이번 판 결과만 초기화(중복방지에는 영향 없음).

    기존: cur_md/cur_pd만 비워서, 리롤 시 직전 결과가 그대로 다시 등장할 수 있음.
    변경: 이번 판에 이미 등장했던 유닛(cur_md+cur_pd)을 exclude_md/exclude_pd 양쪽에
    모두 누적한 뒤 초기화(교차 제외)하여, 리롤/연속 뽑기에서도 중복이 나오지 않게 합니다.
    """
    b = _get_bansal_bucket(guild_id)

    appeared = _unique_list((b.get("cur_md", []) or []) + (b.get("cur_pd", []) or []))
    if appeared:
        b["exclude_md"] = _unique_list((b.get("exclude_md", []) or []) + appeared)
        b["exclude_pd"] = _unique_list((b.get("exclude_pd", []) or []) + appeared)

    b["cur_md"] = []
    b["cur_pd"] = []
    b["finalized"] = False
    save_bansal()
# =========================================================
# 팀/음성 유틸
# =========================================================
async def get_or_create_team_voice_category(guild: discord.Guild) -> discord.CategoryChannel:
    for c in guild.categories:
        if c.name == TEAM_VOICE_CATEGORY_NAME:
            return c
    return await guild.create_category(name=TEAM_VOICE_CATEGORY_NAME, reason="WAVE BOT team voice category")


async def create_team_voice_channels(
    guild: discord.Guild,
    category: discord.CategoryChannel,
    team_count: int,
    teams_member_ids: List[List[int]],
    *,
    user_limit: int = 0,
) -> List[discord.VoiceChannel]:
    """팀 음성 채널 생성
    - 공개 채널(누구나 볼 수 있음)로 생성
    - user_limit(팀당 인원 제한)이 0보다 크면 해당 값으로 제한
    """
    voice_channels: List[discord.VoiceChannel] = []
    for idx in range(team_count):
        team_name = f"{TEAM_VOICE_PREFIX}{idx+1}팀"
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=True, connect=True, speak=True),
        }

        ch = await guild.create_voice_channel(
            name=team_name,
            category=category,
            overwrites=overwrites,
            user_limit=int(user_limit) if int(user_limit) > 0 else 0,
            reason="WAVE BOT team voice channel"
        )
        voice_channels.append(ch)

    return voice_channels


async def delete_team_voice_channels(guild: discord.Guild, state: LastMatchState):
    for ch_id in list(state.voice_channel_ids):
        ch = guild.get_channel(ch_id)
        if isinstance(ch, discord.VoiceChannel):
            try:
                await ch.delete(reason="WAVE BOT cleanup")
            except Exception:
                pass

    # 2) 카테고리 삭제(비었으면)
    if state.voice_category_id:
        cat = guild.get_channel(state.voice_category_id)
        if isinstance(cat, discord.CategoryChannel):
            try:
                # 캐시가 늦게 갱신될 수 있어 fetch 후 channels 확인
                if len(cat.channels) == 0:
                    await cat.delete(reason="WAVE BOT cleanup category")
            except Exception:
                pass

    # 3) 상태 정리
    state.voice_channel_ids = []
    state.voice_category_id = None


async def move_members_to_voice(
    guild: discord.Guild,
    teams_member_ids: List[List[int]],
    voice_channels: List[discord.VoiceChannel],
) -> Tuple[int, int, int]:
    """
    모든 팀원을 각자의 음성 채널로 즉시 이동시킵니다.
    asyncio.gather를 사용하여 모든 이동 요청을 동시에(병렬로) 처리합니다.
    """
    tasks = []
    
    async def safe_move(member, target):
        try:
            # 멤버가 음성 채널에 접속해 있는 경우에만 이동 실행
            if member.voice and member.voice.channel:
                # 이미 목표 채널에 있는 경우는 제외
                if member.voice.channel.id == target.id:
                    return "moved"
                await member.move_to(target, reason="WAVE BOT Team Match")
                return "moved"
            return "not_in_voice"
        except Exception as e:
            print(f"Move failed for {member.display_name}: {e}")
            return "failed"

    # 모든 팀의 모든 멤버에 대해 이동 태스크 생성
    for i, members in enumerate(teams_member_ids):
        if i >= len(voice_channels):
            break
        target = voice_channels[i]
        for uid in members:
            m = guild.get_member(int(uid))
            if m:
                tasks.append(safe_move(m, target))

    if not tasks:
        return 0, 0, 0

    # 모든 이동 태스크를 동시에 실행 (병렬 처리 극대화)
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 결과 집계
    moved = 0
    not_in_voice = 0
    failed = 0
    
    for res in results:
        if res == "moved":
            moved += 1
        elif res == "not_in_voice":
            not_in_voice += 1
        else:
            failed += 1

    return moved, not_in_voice, failed

# /내정보 관련
def get_rank(scores: dict, name: str) -> tuple[int | None, int]:
    """
    반환:
      (rank, points)
      - rank: 없으면 None
      - points: 없으면 0
    """
    if not scores:
        return None, 0

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    for idx, (n, pts) in enumerate(sorted_scores, 1):
        if n == name:
            return idx, int(pts)

    return None, 0

def team_embed_from_ids(guild: discord.Guild, teams_member_ids: List[List[int]], title: str) -> discord.Embed:
    """팀 결과 Embed: 팀장 라벨 + 멤버 목록( / 구분)로 가독성 강화."""
    embed = discord.Embed(title=title)
    for i, team in enumerate(teams_member_ids, start=1):
        if not team:
            embed.add_field(name=f"{i}팀(팀장:없음)", value="(없음)", inline=False)
            continue
        cap_id = team[0]
        cap_m = guild.get_member(cap_id)
        cap_name = cap_m.display_name if cap_m else f"Unknown({cap_id})"
        member_texts: List[str] = []
        for uid in team[1:]:
            m = guild.get_member(uid)
            member_texts.append(display_with_tier(m) if m else f"Unknown({uid})")
        embed.add_field(
            name=f"{i}팀(팀장:{cap_name})",
            value=(" / ".join(member_texts) if member_texts else "(팀장 단독)"),
            inline=False
        )
    return embed

# =========================================================
# 음성 자동 삭제: 인원 0명 되면 삭제 예약
# =========================================================
async def maybe_cleanup_empty_team_voice(guild: discord.Guild, channel: discord.VoiceChannel):
    await asyncio.sleep(VOICE_AUTO_DELETE_GRACE_SEC)

    if channel.members:
        return

    st = guild_last_match.get(guild.id)
    if not st or not st.active:
        return

    if channel.id not in st.voice_channel_ids:
        return

    try:
        await channel.delete(reason="WAVE BOT auto delete empty team voice")
    except Exception:
        return

    st.voice_channel_ids = [cid for cid in st.voice_channel_ids if cid != channel.id]

    if not st.voice_channel_ids:
        await delete_team_voice_channels(guild, st)
        st.active = False
        st.pending = True   # ✅ 유지(승리 반영은 가능해야 함)
    _save_last_match_for_guild(guild.id)


# =========================================================
# 매치 확정 공통 (단일 함수로 통일)
# =========================================================
async def finalize_match_and_move(
    interaction: discord.Interaction,
    teams_member_ids: List[List[int]],
    team_count: int,
    team_size: int,
    mode_title: str,
    match_mode: str = "normal",
):
    await safe_defer(interaction, thinking=False)

    guild = interaction.guild
    if not guild:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return

    st = guild_last_match.setdefault(guild.id, LastMatchState())
    st.match_mode = (match_mode or "normal").strip().lower()
    if st.match_mode not in ("normal", "event"):
        st.match_mode = "normal"
    if st.active:
        await delete_team_voice_channels(guild, st)

    category = await get_or_create_team_voice_category(guild)
    
    # 1. 먼저 인원 제한 없이 채널을 생성 (이동 시 걸림돌 제거)
    voice_channels = await create_team_voice_channels(guild, category, team_count, teams_member_ids, user_limit=0)

    # 2. 모든 팀원 이동 실행 (병렬 처리로 즉시 이동 시도)
    moved, not_in_voice, failed = await move_members_to_voice(guild, teams_member_ids, voice_channels)

    # 3. 이동 명령 전달 후, 각 채널에 원래 설정하려던 인원 제한(team_size)을 적용
    # (이동 처리가 백그라운드에서 진행되는 동안 채널 설정을 업데이트합니다)
    async def set_limit(ch, limit):
        try: await ch.edit(user_limit=int(limit))
        except: pass
    
    if voice_channels:
        await asyncio.gather(*[set_limit(ch, team_size) for ch in voice_channels], return_exceptions=True)

    st.teams = teams_member_ids
    st.team_count = team_count
    st.team_size = team_size
    st.voice_category_id = category.id
    st.voice_channel_ids = [ch.id for ch in voice_channels]
    st.active = True
    st.pending = True
    asyncio.create_task(refresh_dashboard(interaction.client, guild.id))

    embed = team_embed_from_ids(guild, teams_member_ids, title=f"✅ 매칭 완료 - {mode_title}")
    await safe_send(
        interaction,
        content="✅ **매칭 완료!** 아래에서 승리팀을 선택하세요.",
        embed=embed,
        view=MatchResultView(team_count=team_count),
        ephemeral=False
    )


# =========================================================
# 대기열 메시지 갱신
# =========================================================
async def refresh_queue_message(bot: commands.Bot, guild_id: int):
    state = guild_queues.get(guild_id)
    if not state or not state.message_id or not state.channel_id:
        return

    channel = bot.get_channel(state.channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(state.channel_id)
        except Exception:
            return
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return

    try:
        msg = await channel.fetch_message(state.message_id)
    except Exception:
        return

    guild = msg.guild

    # 티어별(1티어→...→상위)로 정렬해서 표시 (요청: 1티어부터 순서대로)
    members: List[Tuple[int, str, str]] = []
    for uid in state.member_ids:
        m = guild.get_member(uid)
        if not m:
            continue
        t = get_tier(uid)
        # 1티어부터 정렬(미등록=0은 맨 아래)
        sort_t = t if t > 0 else 999
        members.append((sort_t, m.display_name.lower(), f"- {display_with_tier(m)}"))

    members.sort(key=lambda x: (x[0], x[1]))
    body = "\n".join([line for _, __, line in members]).strip() if members else "(비어있음)"
    embed = discord.Embed(
        title="📌 대기열",
        description=f"현재 인원: **{len(state.member_ids)}명**\n\n{body}"
    )
    await msg.edit(embed=embed, view=QueueFullView())
    _save_queue_state_for_guild(guild_id)
    asyncio.create_task(refresh_dashboard(bot, guild_id))



# =========================================================
# 대기열 정리(다음 시퀀스로 넘어갈 때 메시지 삭제/초기화)
# =========================================================
async def clear_queue_state(bot: commands.Bot, guild_id: int, *, delete_message: bool = True, clear_members: bool = True):
    state = guild_queues.get(guild_id)
    if not state:
        return

    if clear_members:
        state.member_ids.clear()

    if delete_message and state.message_id and state.channel_id:
        ch = bot.get_channel(state.channel_id)
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            try:
                msg = await ch.fetch_message(state.message_id)
                await msg.delete()
            except Exception:
                pass

    state.message_id = None
    state.channel_id = None
    _save_queue_state_for_guild(guild_id)

    _save_queue_state_for_guild(guild_id)


# =========================================================
# 대기열 관리자(운영진) 수동 추가/제거 유틸
# =========================================================
def _parse_user_id(text: str) -> Optional[int]:
    if not text:
        return None
    s = str(text).strip()
    m = re.search(r"(\d{5,})", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def load_scores_daily():
    return load_json_lenient(SCORES_DAILY_PATH)

def load_scores_weekly():
    return load_json_lenient(SCORES_WEEKLY_PATH)

def load_scores_event():
    return load_json_lenient(SCORES_EVENT_PATH)

def build_scoreboard_embed(title: str, scores: dict) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        color=discord.Color.gold()
    )

    if not scores:
        embed.description = "기록된 점수가 없습니다."
        return embed

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    lines = []
    for i, (name, pts) in enumerate(sorted_scores, 1):
        lines.append(f"**{i}.** {name} — `{pts}점`")

    embed.description = "\n".join(lines[:30])
    embed.set_footer(text="CLAN WAVE 자동 포인트 시스템")
    return embed


def _parse_user_ids_multi(text: str) -> List[int]:
    """멘션/ID를 , 또는 줄바꿈으로 여러 개 입력받아 user_id 리스트로 파싱."""
    if not text:
        return []
    s = str(text)
    ids = []
    seen = set()
    for m in re.findall(r"(\d{5,})", s):
        try:
            uid = int(m)
        except Exception:
            continue
        if uid in seen:
            continue
        seen.add(uid)
        ids.append(uid)
    return ids


class QueueAdminBulkModal(discord.ui.Modal):
    """대기열 멤버 추가/제거(운영진) - 텍스트로 여러명 입력"""

    def __init__(self, *, mode: str):
        super().__init__(title=("대기열 멤버 추가(운영진)" if (mode or '').lower() == 'add' else "대기열 멤버 제거(운영진)"))
        self.mode = (mode or 'add').strip().lower()
        self.member_input = discord.ui.TextInput(
            label="멘션 또는 ID (여러명 가능)",
            placeholder="예: @유저1, @유저2 또는 123,456 (쉼표/줄바꿈 구분)",
            required=True,
            max_length=1200
        )
        self.add_item(self.member_input)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        if not interaction.guild or interaction.guild_id is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        ids = _parse_user_ids_multi(self.member_input.value)
        if not ids:
            await safe_send(interaction, content="❗ 멘션 또는 ID를 올바르게 입력해주세요.", ephemeral=True)
            return

        guild = interaction.guild
        q = guild_queues.setdefault(interaction.guild_id, GuildQueueState())

        ok_names = []
        skipped = 0
        changed = 0

        for uid in ids:
            m = guild.get_member(uid)
            if not m:
                skipped += 1
                continue
            if self.mode == 'remove':
                if uid in q.member_ids:
                    q.member_ids.discard(uid)
                    changed += 1
                    ok_names.append(m.display_name)
            else:
                if uid not in q.member_ids:
                    q.member_ids.add(uid)
                    changed += 1
                    ok_names.append(m.display_name)

        _save_queue_state_for_guild(interaction.guild_id)
        await refresh_queue_message(interaction.client, interaction.guild_id)

        action = "제거" if self.mode == 'remove' else "추가"
        msg = f"✅ 대기열 {action} 완료: {changed}명"
        if ok_names:
            msg += "\n" + ", ".join(ok_names[:30]) + ("..." if len(ok_names) > 30 else "")
        if skipped:
            msg += f"\n(서버에 없는/해석 불가: {skipped}개)"
        await safe_send(interaction, content=msg, ephemeral=True)

class QueueAdminMemberSelectView(discord.ui.View):
    """대기열 멤버 추가/제거(운영진) - UserSelect로 여러명 선택"""

    def __init__(self, *, mode: str, guild_id: int):
        super().__init__(timeout=120)
        self.mode = (mode or 'add').strip().lower()
        self.guild_id = int(guild_id)
        self.selected_user_ids: List[int] = []

        self.user_select = discord.ui.UserSelect(
            placeholder=("추가할 유저 선택 (여러명 가능)" if self.mode == 'add' else "제거할 유저 선택 (여러명 가능)"),
            min_values=1,
            max_values=25
        )
        self.user_select.callback = self._on_select  # type: ignore
        self.add_item(self.user_select)

        apply_btn = discord.ui.Button(label=("추가" if self.mode == 'add' else "제거"), style=discord.ButtonStyle.success)
        apply_btn.callback = self._on_apply  # type: ignore
        self.add_item(apply_btn)

        bulk_btn = discord.ui.Button(label="텍스트로 입력", style=discord.ButtonStyle.secondary)
        bulk_btn.callback = self._on_bulk  # type: ignore
        self.add_item(bulk_btn)

        close_btn = discord.ui.Button(label="닫기", style=discord.ButtonStyle.secondary)
        close_btn.callback = self._on_close  # type: ignore
        self.add_item(close_btn)

    async def _on_select(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        members = list(self.user_select.values)
        self.selected_user_ids = [int(m.id) for m in members]
        mention_list = ", ".join([m.mention for m in members]) if members else "(없음)"
        await safe_send(interaction, content=f"선택됨: {mention_list}", ephemeral=True)

    async def _on_apply(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        if not interaction.guild or interaction.guild_id is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        if not self.selected_user_ids:
            members = list(getattr(self.user_select, 'values', []))
            self.selected_user_ids = [int(m.id) for m in members]
        if not self.selected_user_ids:
            await safe_send(interaction, content="대상 유저를 1명 이상 선택해주세요.", ephemeral=True)
            return

        q = guild_queues.setdefault(self.guild_id, GuildQueueState())
        changed = 0
        ok_names = []

        for uid in self.selected_user_ids:
            m = interaction.guild.get_member(uid)
            if not m:
                continue
            if self.mode == 'remove':
                if uid in q.member_ids:
                    q.member_ids.discard(uid)
                    changed += 1
                    ok_names.append(m.display_name)
            else:
                if uid not in q.member_ids:
                    q.member_ids.add(uid)
                    changed += 1
                    ok_names.append(m.display_name)

        await refresh_queue_message(interaction.client, self.guild_id)

        action = "제거" if self.mode == 'remove' else "추가"
        msg = f"✅ 대기열 {action} 완료: {changed}명"
        if ok_names:
            msg += "\n" + ", ".join(ok_names[:30]) + ("..." if len(ok_names) > 30 else "")
        await safe_send(interaction, content=msg, ephemeral=True)

    async def _on_bulk(self, interaction: discord.Interaction):
        if not interaction.guild or interaction.guild_id is None:
            try:
                await interaction.response.send_message("서버에서만 사용 가능합니다.", ephemeral=True)
            except Exception:
                pass
            return
        if not user_is_admin(interaction):
            try:
                await interaction.response.send_message("❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            except Exception:
                pass
            return
        await interaction.response.send_modal(QueueAdminBulkModal(mode=self.mode))

    async def _on_close(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        try:
            if interaction.message:
                await interaction.message.delete()
        except Exception:
            pass

class QueueAdminAddModal(discord.ui.Modal, title="대기열 멤버 추가(운영진)"):
    member_input = discord.ui.TextInput(label="멘션 또는 ID", placeholder="@유저 또는 123...", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        if not interaction.guild or interaction.guild_id is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        uid = _parse_user_id(self.member_input.value)
        if not uid:
            await safe_send(interaction, content="❗ 멘션 또는 ID를 올바르게 입력해주세요.", ephemeral=True)
            return

        m = interaction.guild.get_member(uid)
        if not m:
            await safe_send(interaction, content="❗ 서버에 없는 유저입니다.", ephemeral=True)
            return

        q = guild_queues.setdefault(interaction.guild_id, GuildQueueState())
        q.member_ids.add(uid)
        await safe_send(interaction, content=f"✅ 대기열에 추가했습니다: {m.display_name}", ephemeral=True)
        _save_queue_state_for_guild(interaction.guild_id)
        await refresh_queue_message(interaction.client, interaction.guild_id)


class QueueAdminRemoveSelect(discord.ui.Select):
    def __init__(self, guild: discord.Guild, guild_id: int):
        q = guild_queues.setdefault(guild_id, GuildQueueState())

        items: List[Tuple[int, str]] = []
        for uid in q.member_ids:
            m = guild.get_member(uid)
            if m:
                items.append((uid, m.display_name))

        items.sort(key=lambda x: (get_member_tier(x[0]), x[1].lower()))

        options: List[discord.SelectOption] = []
        for uid, name in items[:25]:
            options.append(discord.SelectOption(label=name, value=str(uid)))

        super().__init__(placeholder="제거할 멤버 선택", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        if not interaction.guild or interaction.guild_id is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        uid = int(self.values[0])
        q = guild_queues.setdefault(interaction.guild_id, GuildQueueState())
        q.member_ids.discard(uid)
        m = interaction.guild.get_member(uid)
        nm = m.display_name if m else str(uid)

        await safe_send(interaction, content=f"✅ 대기열에서 제거했습니다: {nm}", ephemeral=True)
        _save_queue_state_for_guild(interaction.guild_id)
        await refresh_queue_message(interaction.client, interaction.guild_id)

# =========================================================
# Views / Modals
# =========================================================
class QueueFullView(discord.ui.View):
    """대기열 참여/나가기/명단(일반) + 관리자 패널(관리자만 실제 버튼 노출)"""
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="참여", style=discord.ButtonStyle.success, custom_id="wave_queue_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        gid = interaction.guild_id
        if gid is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        state = guild_queues.setdefault(gid, GuildQueueState())
        state.member_ids.add(interaction.user.id)

        # 재부팅 후에도 기존 패널 메시지를 갱신할 수 있도록 message_id/channel_id 복구
        if not state.message_id and getattr(interaction, "message", None):
            state.message_id = interaction.message.id
        if not state.channel_id:
            state.channel_id = interaction.channel_id
        _save_queue_state_for_guild(gid)

        await safe_send(interaction, content="✅ 대기열 참여 완료", ephemeral=True)
        await refresh_queue_message(interaction.client, gid)

    @discord.ui.button(label="나가기", style=discord.ButtonStyle.danger, custom_id="wave_queue_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        gid = interaction.guild_id
        if gid is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        state = guild_queues.setdefault(gid, GuildQueueState())
        state.member_ids.discard(interaction.user.id)

        if not state.message_id and getattr(interaction, "message", None):
            state.message_id = interaction.message.id
        if not state.channel_id:
            state.channel_id = interaction.channel_id
        _save_queue_state_for_guild(gid)

        await safe_send(interaction, content="🚪 대기열 나가기 완료", ephemeral=True)
        await refresh_queue_message(interaction.client, gid)

    @discord.ui.button(label="명단", style=discord.ButtonStyle.secondary, custom_id="wave_queue_list")
    async def list_members(self, interaction: discord.Interaction, button: discord.ui.Button):
        gid = interaction.guild_id
        if gid is None or not interaction.guild:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        state = guild_queues.setdefault(gid, GuildQueueState())
        guild = interaction.guild

        members: List[Tuple[int, str, str]] = []
        for uid in state.member_ids:
            m = guild.get_member(uid)
            if not m:
                continue
            t = get_tier(uid)
            sort_t = t if t > 0 else 999
            members.append((sort_t, m.display_name.lower(), f"- {display_with_tier(m)}"))

        members.sort(key=lambda x: (x[0], x[1]))
        body = "\n".join([line for _, __, line in members]).strip() if members else "(비어있음)"
        await safe_send(interaction, content="📋 현재 대기열:\n" + body, ephemeral=True)

    @discord.ui.button(label="⚙️ 관리자 패널", style=discord.ButtonStyle.primary, custom_id="wave_admin_panel")
    async def admin_panel(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 관리자만 '관리자 버튼'이 보이도록: 패널은 ephemeral로 별도 노출
        await safe_defer(interaction, thinking=False, ephemeral=True)

        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        if not interaction.guild or interaction.guild_id is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        view = QueueAdminPanelView(guild=interaction.guild, guild_id=interaction.guild_id)
        await safe_send(interaction, content="관리자 기능을 선택하세요.", view=view, ephemeral=True)


class QueueAdminPanelView(discord.ui.View):
    """관리자 전용 버튼 묶음(항상 ephemeral로만 노출)"""

    def __init__(self, *, guild: discord.Guild, guild_id: int):
        super().__init__(timeout=180)
        self.guild = guild
        self.guild_id = int(guild_id)

    @discord.ui.button(label="✍ 수동 드래프트", style=discord.ButtonStyle.secondary, custom_id="wave_admin_draft_panel")
    async def admin_draft(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return
        await interaction.response.send_modal(DraftSetupModal())

    @discord.ui.button(label="⚖ 티어균형 랜덤", style=discord.ButtonStyle.primary, custom_id="wave_admin_balance_panel")
    async def admin_balance(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return
        await interaction.response.send_modal(BalancedTeamModal())

    @discord.ui.button(label="🎲 랜덤 팀배정(티어무시)", style=discord.ButtonStyle.primary, custom_id="wave_admin_random_panel")
    async def admin_random(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return
        await interaction.response.send_modal(RandomTeamModal())

    @discord.ui.button(label="➕ 멤버추가", style=discord.ButtonStyle.success, custom_id="wave_admin_queue_add_panel")
    async def admin_queue_add(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return
        view = QueueAdminMemberSelectView(mode="add", guild_id=interaction.guild_id)
        await safe_send(interaction, content="추가할 유저를 선택하세요.", view=view, ephemeral=True)

    @discord.ui.button(label="➖ 멤버제거", style=discord.ButtonStyle.danger, custom_id="wave_admin_queue_remove_panel")
    async def admin_queue_remove(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        if not interaction.guild or interaction.guild_id is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        state = guild_queues.setdefault(interaction.guild_id, GuildQueueState())
        if not state.member_ids:
            await safe_send(interaction, content="대기열이 비어있습니다.", ephemeral=True)
            return

        view = QueueAdminMemberSelectView(mode="remove", guild_id=interaction.guild_id)
        await safe_send(interaction, content="제거할 유저를 선택하세요.", view=view, ephemeral=True)

class QueueAdminRemoveView(discord.ui.View):
    def __init__(self, guild: discord.Guild, guild_id: int):
        super().__init__(timeout=120)
        self.add_item(QueueAdminRemoveSelect(guild, guild_id))

        #==========점수삭제, 선택뷰===============
class ScoreAddModeView(discord.ui.View):
    def __init__(self, nickname: str, points: int):
        super().__init__(timeout=60)
        self.nickname = nickname
        self.points = points

    @discord.ui.button(label="일간", style=discord.ButtonStyle.secondary, custom_id="wave_score_unified_daily")
    async def daily(self, interaction: discord.Interaction, _):
        _add_score(SCORES_DAILY_FILE, self.nickname, self.points)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("일간 점수 반영 완료", ephemeral=True)

    @discord.ui.button(label="주간(내전)", style=discord.ButtonStyle.primary, custom_id="wave_score_unified_weekly")
    async def weekly(self, interaction: discord.Interaction, _):
        _add_score(SCORES_WEEKLY_FILE, self.nickname, self.points)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("주간 점수 반영 완료", ephemeral=True)

    @discord.ui.button(label="이벤트", style=discord.ButtonStyle.danger, custom_id="wave_score_unified_event")
    async def event(self, interaction: discord.Interaction, _):
        _add_score(SCORES_EVENT_FILE, self.nickname, self.points)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("이벤트 점수 반영 완료", ephemeral=True)

class ScoreRemoveSetView(discord.ui.View):
    def __init__(self, nickname: str, points: int, mode: str):
        super().__init__(timeout=60)
        self.nickname = nickname
        self.points = points
        self.mode = mode  # "remove" or "set"

    def _apply(self, path):
        if self.mode == "remove":
            _remove_score(path, self.nickname, self.points)
        else:
            _set_score(path, self.nickname, self.points)

    @discord.ui.button(label="일간", style=discord.ButtonStyle.secondary, custom_id="wave_score_unified_daily")
    async def daily(self, interaction: discord.Interaction, _):
        self._apply(SCORES_DAILY_FILE)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("일간 처리 완료", ephemeral=True)

    @discord.ui.button(label="주간(내전)", style=discord.ButtonStyle.primary, custom_id="wave_score_unified_weekly")
    async def weekly(self, interaction: discord.Interaction, _):
        self._apply(SCORES_WEEKLY_FILE)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("주간 처리 완료", ephemeral=True)

    @discord.ui.button(label="이벤트", style=discord.ButtonStyle.danger, custom_id="wave_score_unified_event")
    async def event(self, interaction: discord.Interaction, _):
        self._apply(SCORES_EVENT_FILE)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("이벤트 처리 완료", ephemeral=True)

def _scores_from_file(scope: str) -> dict:
    """파일에서 직접 읽어 점수 dict 반환 (스크립트 폴더 기준)"""
    if scope == "daily":
        return load_json_lenient(SCORES_DAILY_PATH) or {}
    if scope == "weekly":
        return load_json_lenient(SCORES_WEEKLY_PATH) or {}
    return load_json_lenient(SCORES_EVENT_PATH) or {}

class ScoreboardPinScopeView(discord.ui.View):
    """채널에 고정할 점수판 구분 선택 (일간/주간/이벤트)"""
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="일간", style=discord.ButtonStyle.secondary, custom_id="wave_pin_daily")
    async def daily(self, interaction: discord.Interaction, _):
        await self._pin(interaction, "daily", "일간")

    @discord.ui.button(label="주간(내전)", style=discord.ButtonStyle.primary, custom_id="wave_score_unified_weekly")
    async def weekly(self, interaction: discord.Interaction, _):
        await self._pin(interaction, "weekly", "주간")

    @discord.ui.button(label="이벤트", style=discord.ButtonStyle.danger, custom_id="wave_pin_event")
    async def event(self, interaction: discord.Interaction, _):
        await self._pin(interaction, "event", "이벤트")

    async def _pin(self, interaction: discord.Interaction, scope: str, label: str):
        await safe_defer(interaction, thinking=False)
        scores = _scores_from_file(scope)
        title = f"📊 점수판 ({label})"
        embed = build_scoreboard_embed(title, scores)
        embed.set_footer(text="CLAN WAVE · 점수 변동 시 자동 갱신")
        try:
            msg = await interaction.channel.send(embed=embed)
            if interaction.guild_id and msg.id and msg.channel.id:
                _register_scoreboard_message(interaction.guild_id, msg.channel.id, msg.id, scope)
            await safe_send(interaction, content="✅ 점수판을 채널에 고정했습니다. 점수 변동 시 자동으로 갱신됩니다.", ephemeral=True)
        except Exception as e:
            await safe_send(interaction, content=f"❗ 전송 실패: {e}", ephemeral=True)

class ScoreboardUnifiedView(discord.ui.View):
    """점수판 View: 일간/주간/이벤트 (표시 시 자동갱신 등록 → 점수 변동 시 해당 메시지 자동 갱신)"""
    def __init__(self):
        super().__init__(timeout=None)

    async def _send_and_register(self, interaction: discord.Interaction, scope: str, label: str):
        scores = _scores_from_file(scope)
        embed = build_scoreboard_embed(f"📊 점수판 ({label})", scores)
        embed.set_footer(text="CLAN WAVE · 점수 변동 시 자동 갱신")
        await interaction.response.send_message(embed=embed, ephemeral=False)
        try:
            msg = await interaction.original_response()
            if msg and interaction.guild_id and getattr(msg, "id", None) and getattr(msg, "channel", None):
                _register_scoreboard_message(interaction.guild_id, msg.channel.id, msg.id, scope)
        except Exception:
            pass

    @discord.ui.button(label="일간", style=discord.ButtonStyle.secondary, custom_id="wave_score_unified_daily")
    async def daily(self, interaction: discord.Interaction, _):
        await self._send_and_register(interaction, "daily", "일간")

    @discord.ui.button(label="주간(내전)", style=discord.ButtonStyle.primary, custom_id="wave_score_unified_weekly")
    async def weekly(self, interaction: discord.Interaction, _):
        await self._send_and_register(interaction, "weekly", "주간")

    @discord.ui.button(label="이벤트", style=discord.ButtonStyle.danger, custom_id="wave_score_unified_event")
    async def event(self, interaction: discord.Interaction, _):
        await self._send_and_register(interaction, "event", "이벤트")

class RankingModeSelectView(discord.ui.View):
    """랭킹 모드 선택 View (파일 + 메모리 이중 확인으로 항상 표시)"""
    def __init__(self, top: int = 40):
        super().__init__(timeout=None)
        self.top = top

    def _scores_for_scope(self, scope: str) -> dict:
        """스크립트 폴더의 점수 파일에서 읽고, 비어 있으면 메모리 fallback"""
        if scope == "daily":
            data = load_json_lenient(SCORES_DAILY_PATH)
            return data if isinstance(data, dict) and data else (daily_scores or {})
        if scope == "weekly":
            data = load_json_lenient(SCORES_WEEKLY_PATH)
            return data if isinstance(data, dict) and data else (weekly_scores or {})
        data = load_json_lenient(SCORES_EVENT_PATH)
        return data if isinstance(data, dict) and data else (event_scores or {})

    async def _send_ranking(self, interaction: discord.Interaction, scope: str, label: str):
        scores = self._scores_for_scope(scope)
        if not scores:
            await interaction.response.send_message("아직 기록된 점수가 없습니다.", ephemeral=True)
            return
        top_list = sorted_top(scores, self.top)
        lines = [f"**{i}위** — {name} : **{pts}점**" for i, (name, pts) in enumerate(top_list, start=1)]
        embed = discord.Embed(
            title=f"🏆 포인트 랭킹 ({label})",
            description="\n".join(lines)
        )
        embed.set_footer(text="CLAN WAVE 자동 포인트 시스템")
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @discord.ui.button(label="일간", style=discord.ButtonStyle.secondary, custom_id="wave_ranking_daily")
    async def daily(self, interaction: discord.Interaction, _):
        await self._send_ranking(interaction, "daily", "일간")

    @discord.ui.button(label="주간(내전)", style=discord.ButtonStyle.primary, custom_id="wave_score_unified_weekly")
    async def weekly(self, interaction: discord.Interaction, _):
        await self._send_ranking(interaction, "weekly", "주간")

    @discord.ui.button(label="이벤트", style=discord.ButtonStyle.danger, custom_id="wave_ranking_event")
    async def event(self, interaction: discord.Interaction, _):
        await self._send_ranking(interaction, "event", "이벤트")

class ResetModeSelectView(discord.ui.View):
    """초기화 모드 선택 View"""
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="일간", style=discord.ButtonStyle.secondary, custom_id="wave_reset_daily")
    async def daily(self, interaction: discord.Interaction, _):
        global daily_scores
        backup_files = []
        bp = _backup_scores("daily", daily_scores, SCORES_DAILY_PATH)
        if bp:
            backup_files.append(bp)
        
        daily_scores = {}
        save_json(SCORES_DAILY_PATH, daily_scores)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("✅ 초기화 완료: 일간", ephemeral=False)

    @discord.ui.button(label="주간(내전)", style=discord.ButtonStyle.primary, custom_id="wave_score_unified_weekly")
    async def weekly(self, interaction: discord.Interaction, _):
        global weekly_scores
        backup_files = []
        bp = _backup_scores("weekly", weekly_scores, SCORES_WEEKLY_PATH)
        if bp:
            backup_files.append(bp)
        
        weekly_scores = {}
        save_json(SCORES_WEEKLY_PATH, weekly_scores)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("✅ 초기화 완료: 주간", ephemeral=False)

    @discord.ui.button(label="이벤트", style=discord.ButtonStyle.danger, custom_id="wave_reset_event")
    async def event(self, interaction: discord.Interaction, _):
        global event_scores
        backup_files = []
        bp = _backup_scores("event", event_scores, SCORES_EVENT_PATH)
        if bp:
            backup_files.append(bp)
        
        event_scores = {}
        save_json(SCORES_EVENT_PATH, event_scores)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("✅ 초기화 완료: 이벤트", ephemeral=False)

    @discord.ui.button(label="일간+주간", style=discord.ButtonStyle.success, custom_id="wave_reset_both")
    async def both(self, interaction: discord.Interaction, _):
        global daily_scores, weekly_scores
        backup_files = []
        
        bp = _backup_scores("daily", daily_scores, SCORES_DAILY_PATH)
        if bp:
            backup_files.append(bp)
        bp = _backup_scores("weekly", weekly_scores, SCORES_WEEKLY_PATH)
        if bp:
            backup_files.append(bp)
        
        daily_scores = {}
        weekly_scores = {}
        save_json(SCORES_DAILY_PATH, daily_scores)
        save_json(SCORES_WEEKLY_PATH, weekly_scores)
        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message("✅ 초기화 완료: 일간+주간", ephemeral=False)

class BalancedTeamModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="랜덤 팀배정")
        self.team_count = discord.ui.TextInput(
            label="팀수",
            placeholder="예: 3 ",
            required=False
        )
        self.team_size = discord.ui.TextInput(label="인원(팀당)", placeholder="예: 2 or 3", required=True)
        self.add_item(self.team_count)
        self.add_item(self.team_size)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False)

        guild = interaction.guild
        gid = interaction.guild_id
        if not guild or gid is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        q = guild_queues.setdefault(gid, GuildQueueState())
        pool_ids = [uid for uid in q.member_ids if guild.get_member(uid)]

        try:
            team_size = int(self.team_size.value.strip())
        except ValueError:
            await safe_send(interaction, content="예) 3", ephemeral=True)
            return

        if team_size not in (2, 3):
            await safe_send(interaction, content="예) 3", ephemeral=True)
            return

        # 드래프트 방식
        draft_mode = (getattr(self, "draft_mode", None).value if getattr(self, "draft_mode", None) else "snake")
        draft_mode = (draft_mode or "snake").strip().lower()
        if draft_mode not in ("snake", "dice"):
            await safe_send(interaction, content="❗ 드래프트 방식은 snake 또는 dice 만 가능합니다.", ephemeral=True)
            return

        # 팀수: 비우면 자동(올림)
        team_count_raw = (self.team_count.value or "").strip()
        if team_count_raw:
            try:
                team_count = int(team_count_raw)
            except ValueError:
                await safe_send(interaction, content="❗ 팀수는 숫자로 입력해주세요. (또는 비우면 자동)", ephemeral=True)
                return
            if team_count < 2 or team_count > MAX_TEAMS:
                await safe_send(interaction, content=f"❗ 팀수는 2~{MAX_TEAMS} 사이만 가능합니다.", ephemeral=True)
                return

            required = team_count * team_size
            if len(pool_ids) < required:
                await safe_send(interaction, content=f"❗ 인원 부족: 필요 {required} / 현재 {len(pool_ids)}", ephemeral=True)
                return
            ids = pool_ids[:]
            random.shuffle(ids)
            ids = ids[:required]
        else:
            if len(pool_ids) < team_size * 2:
                await safe_send(interaction, content=f"❗ 최소 2팀을 위해 인원이 부족합니다. (현재 {len(pool_ids)}명)", ephemeral=True)
                return
            team_count = int(math.ceil(len(pool_ids) / team_size))
            team_count = max(2, min(MAX_TEAMS, team_count))
            ids = pool_ids[:]
            random.shuffle(ids)
            # 자동 모드에서는 인원 전원 사용

        teams = tier_balanced_assign_no_captain(ids, team_count, team_size)

        await clear_queue_state(interaction.client, gid, delete_message=True, clear_members=True)

        await finalize_match_and_move(
            interaction,
            teams,
            team_count,
            team_size,
            mode_title=f"⚖ 랜덤 팀배정",
            match_mode=get_queue_mode(gid)
        )

class RandomTeamModal(discord.ui.Modal):
    """팀장 없이: 티어 무시 랜덤 분배
    - 팀수(team_count)를 비우면: 현재 대기열 인원 / 팀당 인원(team_size) 기준으로 자동 계산합니다.
    """
    def __init__(self):
        super().__init__(title="랜덤 팀배정(티어무시)")
        self.team_count = discord.ui.TextInput(
            label="팀수",
            placeholder="예: 3 (비우면 자동)",
            required=False
        )
        self.team_size = discord.ui.TextInput(label="인원(팀당)", placeholder="예: 2 or 3", required=True)
        self.add_item(self.team_count)
        self.add_item(self.team_size)

    async def on_submit(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False)

        guild = interaction.guild
        gid = interaction.guild_id
        if not guild or gid is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        q = guild_queues.setdefault(gid, GuildQueueState())
        pool_ids = [uid for uid in q.member_ids if guild.get_member(uid)]

        try:
            team_size = int(self.team_size.value.strip())
        except ValueError:
            await safe_send(interaction, content="예) 3", ephemeral=True)
            return

        if team_size not in (2, 3):
            await safe_send(interaction, content="예) 2 또는 3", ephemeral=True)
            return

        team_count_raw = (self.team_count.value or "").strip()
        if team_count_raw:
            try:
                team_count = int(team_count_raw)
            except ValueError:
                await safe_send(interaction, content="❗ 팀수는 숫자로 입력해주세요. (또는 비우면 자동)", ephemeral=True)
                return
            if team_count < 2 or team_count > MAX_TEAMS:
                await safe_send(interaction, content=f"❗ 팀수는 2~{MAX_TEAMS} 사이만 가능합니다.", ephemeral=True)
                return

            required = team_count * team_size
            if len(pool_ids) < required:
                await safe_send(interaction, content=f"❗ 인원 부족: 필요 {required} / 현재 {len(pool_ids)}", ephemeral=True)
                return

            ids = pool_ids[:]
            random.shuffle(ids)
            ids = ids[:required]
        else:
            if len(pool_ids) < team_size * 2:
                await safe_send(interaction, content=f"❗ 최소 2팀을 위해 인원이 부족합니다. (현재 {len(pool_ids)}명)", ephemeral=True)
                return
            team_count = int(math.ceil(len(pool_ids) / team_size))
            team_count = max(2, min(MAX_TEAMS, team_count))
            ids = pool_ids[:]
            random.shuffle(ids)

        teams: List[List[int]] = [[] for _ in range(team_count)]
        for idx, uid in enumerate(ids):
            teams[idx % team_count].append(uid)

        await clear_queue_state(interaction.client, gid, delete_message=True, clear_members=True)

        await finalize_match_and_move(
            interaction,
            teams,
            team_count,
            team_size,
            mode_title="🎲 랜덤 팀배정(티어무시)",
            match_mode=get_queue_mode(gid)
        )


class DraftSetupModal(discord.ui.Modal, title="드래프트 시작 설정"):
    # 팀수는 선택 사항: 비우면 자동 계산(대기열 인원 / 팀당 인원)
    team_count = discord.ui.TextInput(label="팀수", placeholder="예: 3 ", required=False)
    team_size = discord.ui.TextInput(label="인원(팀당)", placeholder="예: 2 or 3", required=True)
    draft_mode = discord.ui.TextInput(
        label="드래프트 방식",
        placeholder="snake 또는 dice (기본: snake)",
        required=False,
        default="snake",
        max_length=30
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        gid = interaction.guild_id
        if not guild or gid is None:
            await interaction.response.send_message("서버에서만 사용 가능합니다.", ephemeral=True)
            return

        if gid in guild_draft:
            await interaction.response.send_message("❗ 이미 진행 중인 드래프트가 있습니다.", ephemeral=True)
            return

        q = guild_queues.setdefault(gid, GuildQueueState())
        pool_ids = [uid for uid in q.member_ids if guild.get_member(uid)]

        try:
            team_size = int((self.team_size.value or "").strip())
        except ValueError:
            await interaction.response.send_message("❗ 팀수/인원은 숫자로 입력해주세요.", ephemeral=True)
            return

        if team_size not in (2, 3):
            await interaction.response.send_message("❗ 팀당 인원은 2 또는 3만 입력해주세요.", ephemeral=True)
            return

        # 팀수: 비우면 자동(올림)
        team_count_raw = (self.team_count.value or "").strip()
        if team_count_raw:
            try:
                team_count = int(team_count_raw)
            except ValueError:
                await interaction.response.send_message("❗ 팀수는 숫자로 입력해주세요.", ephemeral=True)
                return

            if team_count < 2 or team_count > MAX_TEAMS:
                await interaction.response.send_message(f"❗ 팀수는 2~{MAX_TEAMS} 사이만 가능합니다.", ephemeral=True)
                return

            required = team_count * team_size
            if len(pool_ids) < required:
                await interaction.response.send_message(
                    f"❗ 인원이 부족합니다. 필요: {required}명 / 현재: {len(pool_ids)}명",
                    ephemeral=True
                )
                return

            # required명만 사용(많으면 랜덤)
            ids = pool_ids[:]
            random.shuffle(ids)
            ids = ids[:required]
        else:
            if len(pool_ids) < team_size * 2:
                await interaction.response.send_message(
                    f"❗ 최소 2팀을 위해 인원이 부족합니다. (현재 {len(pool_ids)}명)",
                    ephemeral=True
                )
                return
            team_count = int(math.ceil(len(pool_ids) / team_size))
            team_count = max(2, min(MAX_TEAMS, team_count))
            ids = pool_ids[:]  # 전원 사용(드래프트 풀)
            random.shuffle(ids)
        # 팀장 선택 Select는 컴포넌트당 최대 25개 제한이 있습니다.
        # (해결) 1~4티어로 Select를 분리하므로, '티어별 인원'이 25를 넘으면 UI가 불가합니다.
        tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
        for uid in pool_ids:
            t = get_tier(int(uid))
            t = t if t in (1, 2, 3, 4) else 4
            tier_counts[t] += 1

        over = [t for t, c in tier_counts.items() if c > SELECT_MAX_OPTIONS]
        if over:
            over_txt = ", ".join([f"{t}티어({tier_counts[t]}명)" for t in over])
            await interaction.response.send_message(
                "❗ 팀장 선택 UI 제한(Select 당 25명)으로 인해 진행할 수 없습니다."
                f"해당 티어 인원이 25명을 초과했습니다: {over_txt}"
                "해당 티어 인원을 줄인 뒤 다시 시도해주세요.",
                ephemeral=True
            )
            return
        draft_mode = (self.draft_mode.value or "snake").strip().lower()

        view = CaptainSelectView(guild=guild, guild_id=gid, team_count=team_count, team_size=team_size, queue_ids=pool_ids, draft_mode=draft_mode)
        
        # 명단 텍스트 생성
        members = []
        for uid in pool_ids:
            m = guild.get_member(uid)
            if m:
                members.append((get_tier(uid), m.display_name, display_with_tier(m)))
        members.sort(key=lambda x: x[0])
        body = "\n".join([line for _, __, line in members]).strip() if members else "(비어있음)"

        embed = discord.Embed(
            title="팀장 선택",
            description=f"현재 인원: **{len(pool_ids)}명**\n\n{body}"
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=False)




class CaptainTierSelect(discord.ui.Select):
    """티어별 팀장 선택 Select (티어별 최대 25명 제한 대응)"""

    def __init__(self, *, guild: discord.Guild, tier: int, member_ids: List[int]):
        self._tier = int(tier)
        self._key = f"tier{self._tier}"

        # 옵션(최대 25개)
        options: List[discord.SelectOption] = []
        for uid in member_ids[:SELECT_MAX_OPTIONS]:
            m = guild.get_member(int(uid))
            if not m:
                continue
            options.append(discord.SelectOption(label=display_with_tier(m), value=str(uid)))

        super().__init__(
            placeholder=f"{self._tier}티어 팀장 선택 (0개 이상 선택)",
            min_values=0,
            max_values=len(options),
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        view: "CaptainSelectView" = self.view  # type: ignore

        # 이 Select(티어)의 현재 선택값을 반영(교체)
        view._selected_map[self._key] = {int(v) for v in self.values}

        # 전체 선택 합산
        union: Set[int] = set()
        for s in view._selected_map.values():
            union.update(s)

        view.selected_captain_ids = sorted(list(union))

        await interaction.response.send_message(
            f"현재 선택된 팀장 수: {len(view.selected_captain_ids)} / {view.team_count}명",
            ephemeral=True
        )


class CaptainSelectView(discord.ui.View):
    """팀장 선택 View: 1~4티어 Select를 동시에 열어두고, 합산해서 정확히 team_count명을 선택"""

    def __init__(
        self,
        *,
        guild: discord.Guild,
        guild_id: int,
        team_count: int,
        team_size: int,
        queue_ids: List[int],
        draft_mode: str,
        timeout: int = 300
    ):
        super().__init__(timeout=timeout)

        self.guild = guild
        self.guild_id = int(guild_id)
        self.team_count = int(team_count)
        self.team_size = int(team_size)
        self.queue_ids = [int(x) for x in (queue_ids or [])]
        self.draft_mode = (draft_mode or "snake").strip().lower()

        self.selected_captain_ids: List[int] = []
        self._selected_map: Dict[str, Set[int]] = {}

        # 티어별 분리(1~4). 미등록/기타는 4티어로 편입
        tier_map: Dict[int, List[int]] = {1: [], 2: [], 3: [], 4: []}
        for uid in self.queue_ids:
            t = get_tier(int(uid))
            t = t if t in (1, 2, 3, 4) else 4
            tier_map[t].append(int(uid))

        # 정렬(표시 안정성)
        for t in (1, 2, 3, 4):
            tier_map[t].sort(key=lambda uid: (self.guild.get_member(uid).display_name.lower()
                                             if self.guild.get_member(uid) else str(uid)))

        # 1~4티어 Select를 동시에 노출(티어별 25명 제한)
        for t in (1, 2, 3, 4):
            ids = tier_map.get(t) or []
            if not ids:
                continue
            # DraftSetupModal에서 티어별 25명 초과를 이미 차단했지만, 안전상 다시 한번 제한
            ids = ids[:SELECT_MAX_OPTIONS]
            self.add_item(CaptainTierSelect(guild=self.guild, tier=t, member_ids=ids))

        self.add_item(self.ConfirmButton())
        self.add_item(self.CancelButton())

    class ConfirmButton(discord.ui.Button):
        def __init__(self):
            super().__init__(label="✅ 확정", style=discord.ButtonStyle.success, custom_id="wave_captain_confirm")

        async def callback(self, interaction: discord.Interaction):
            view: "CaptainSelectView" = self.view  # type: ignore

            # 중복 제거(안전)
            unique_ids = []
            seen = set()
            for uid in view.selected_captain_ids:
                if uid in seen:
                    continue
                seen.add(uid)
                unique_ids.append(uid)
            view.selected_captain_ids = unique_ids

            if len(view.selected_captain_ids) != view.team_count:
                await safe_send(
                    interaction,
                    content=f"팀장 {view.team_count}명을 정확히 선택해야 합니다. (현재 {len(view.selected_captain_ids)}명)",
                    ephemeral=True
                )
                return

            await safe_defer(interaction, thinking=False, ephemeral=True)

            await on_captains_confirmed(
                interaction,
                guild_id=view.guild_id,
                team_count=view.team_count,
                team_size=view.team_size,
                queue_ids=view.queue_ids,
                captain_ids=view.selected_captain_ids,
                draft_mode=view.draft_mode
            )

    class CancelButton(discord.ui.Button):
        def __init__(self):
            super().__init__(label="취소", style=discord.ButtonStyle.danger, custom_id="wave_captain_cancel")

        async def callback(self, interaction: discord.Interaction):
            try:
                if interaction.message:
                    await interaction.message.delete()
            except Exception:
                await safe_send(interaction, content="팀장 선택이 취소되었습니다.", ephemeral=True)
            self.view.stop()

def _chunked(items: List[int], size: int) -> List[List[int]]:
    out: List[List[int]] = []
    cur: List[int] = []
    for x in items or []:
        cur.append(int(x))
        if len(cur) >= int(size):
            out.append(cur)
            cur = []
    if cur:
        out.append(cur)
    return out


class DraftPickTierSelect(discord.ui.Select):
    """픽 Select는 컴포넌트당 옵션이 25개로 제한됩니다.
    - 풀(pool_ids)이 25명을 넘으면 discord.py에서 ValueError/HTTPException이 날 수 있습니다.
    - 해결: 티어(1~4)별로 분리하고, 한 티어가 25명을 초과하면 25개 단위로 페이지를 쪼갭니다.
    """

    def __init__(
        self,
        guild: discord.Guild,
        ds: DraftSession,
        *,
        tier: int,
        page_index: int,
        page_total: int,
        member_ids: List[int]
    ):
        self._tier = int(tier)
        self._page_index = int(page_index)
        self._page_total = int(page_total)

        options: List[discord.SelectOption] = []
        for uid in member_ids[:SELECT_MAX_OPTIONS]:
            m = guild.get_member(int(uid))
            if not m:
                continue
            options.append(discord.SelectOption(label=display_with_tier(m), value=str(int(uid))))

        if self._page_total > 1:
            placeholder = f"{self._tier}티어 픽 ({self._page_index}/{self._page_total})"
        else:
            placeholder = f"{self._tier}티어 픽"

        super().__init__(
            placeholder=placeholder,
            min_values=1,
            max_values=1,
            options=options
        )

    async def callback(self, interaction: discord.Interaction):
        # 3초 내 ACK 필요
        await safe_defer(interaction, thinking=False, ephemeral=True)

        if not interaction.guild or not interaction.guild_id:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        try:
            picked = int(self.values[0])
        except Exception:
            await safe_send(interaction, content="선택값을 처리할 수 없습니다.", ephemeral=True)
            return

        await handle_draft_pick(interaction, picked_id=picked)


def _add_draft_pick_selects(v: "DraftPickView", guild: discord.Guild, ds: DraftSession) -> None:
    """드래프트 픽 Select를 티어/페이지로 분할하여 View에 추가합니다."""
    if not ds.pool_ids:
        return

    tier_map: Dict[int, List[int]] = {1: [], 2: [], 3: [], 4: []}
    for uid in list(ds.pool_ids):
        t = get_tier(int(uid))
        t = t if t in (1, 2, 3, 4) else 4
        tier_map[t].append(int(uid))

    # 안정적인 표시(닉네임 정렬)
    for t in (1, 2, 3, 4):
        tier_map[t].sort(
            key=lambda uid: (
                guild.get_member(uid).display_name.lower()
                if guild.get_member(uid) else str(uid)
            )
        )

    # 티어 1 → 4 순서로 Select 생성
    for t in (1, 2, 3, 4):
        ids = tier_map.get(t) or []
        if not ids:
            continue
        pages = _chunked(ids, SELECT_MAX_OPTIONS)
        for idx, chunk in enumerate(pages, start=1):
            v.add_item(
                DraftPickTierSelect(
                    guild,
                    ds,
                    tier=t,
                    page_index=idx,
                    page_total=len(pages),
                    member_ids=chunk
                )
            )


class DraftPickView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id

    @staticmethod
    def build_with_select(guild: discord.Guild, ds: DraftSession, guild_id: int) -> "DraftPickView":
        v = DraftPickView(guild_id)
        if ds.pool_ids:
            _add_draft_pick_selects(v, guild, ds)
        # 드래프트 진행 중, 대기열 화면(참여/나가기)로 복귀
        v.add_item(BackToQueueButton(guild_id=guild_id))
        v.add_item(DraftCancelButton(guild_id=guild_id))
        return v

class BackToQueueButton(discord.ui.Button):
    def __init__(self, guild_id: int):
        super().__init__(
            label="↩️ 대기열이동",
            style=discord.ButtonStyle.secondary,
            custom_id=f"wave_draft_back_to_queue_{guild_id}"
        )
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False)

        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        gid = self.guild_id
        guild = interaction.guild
        if not guild or interaction.guild_id != gid:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        ds = guild_draft.get(gid)
        if not ds:
            await safe_send(interaction, content="진행 중인 드래프트가 없습니다.", ephemeral=True)
            return

        # 1) 참가자 목록 복구(선택: 드래프트 참가자들을 대기열로 되돌림)
        all_ids = set(ds.captain_ids or [])
        all_ids.update(ds.pool_ids or [])
        for tm in (ds.teams or []):
            for uid in (tm or []):
                all_ids.add(int(uid))

        # 2) 드래프트 종료
        guild_draft.pop(gid, None)

        # 3) 드래프트 메시지 정리(삭제 또는 비활성화)
        try:
            if interaction.message:
                await interaction.message.delete()
        except Exception:
            # 삭제 실패 시, 최소한 버튼/셀렉트는 제거
            try:
                if interaction.message:
                    await interaction.message.edit(content="드래프트가 종료되었습니다.", embed=None, view=None)
            except Exception:
                pass

        # 4) 대기열 메시지 새로 생성
        state = guild_queues.setdefault(gid, GuildQueueState())
        state.member_ids = set(all_ids)   # ✅ 복구를 원치 않으면 이 줄을 지우고 clear()로 대체
        state.channel_id = interaction.channel_id
        _save_queue_state_for_guild(gid)

        embed = discord.Embed(
            title="📌 대기열",
            description=f"현재 인원: **{len(state.member_ids)}명**\n(참여/나가기 버튼을 사용하세요)"
        )

        msg = await interaction.channel.send(embed=embed, view=QueueFullView())
        state.message_id = msg.id
        _save_queue_state_for_guild(gid)
        _save_queue_state_for_guild(gid)

        # 5) 명단 갱신(티어 배지 포함 출력)
        await refresh_queue_message(interaction.client, gid)

        await safe_send(interaction, content="✅ 대기열 화면", ephemeral=True)



class DraftCancelButton(discord.ui.Button):
    def __init__(self, guild_id: int):
        super().__init__(label="⛔ 드래프트 종료(관리자)", style=discord.ButtonStyle.danger,
                         custom_id=f"wave_draft_cancel_{guild_id}")
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction):
        if not user_is_admin(interaction):
            await interaction.response.send_message("관리자만 종료할 수 있습니다.", ephemeral=True)
            return
        gid = self.guild_id
        guild_draft.pop(gid, None)
        try:
            await interaction.response.edit_message(content="드래프트가 종료되었습니다.", embed=None, view=None)
            asyncio.create_task(_delete_message_later(interaction.message, 2))
        except Exception:
            pass

async def update_event_scoreboard(bot: discord.Client):
    global EVENT_SCOREBOARD_MESSAGE_ID, EVENT_SCOREBOARD_CHANNEL_ID

    if not EVENT_SCOREBOARD_MESSAGE_ID or not EVENT_SCOREBOARD_CHANNEL_ID:
        return

    channel = bot.get_channel(EVENT_SCOREBOARD_CHANNEL_ID)
    if not channel:
        return

    try:
        msg = await channel.fetch_message(EVENT_SCOREBOARD_MESSAGE_ID)
    except Exception:
        return

    scores = load_json_lenient(SCORES_EVENT_PATH)
    if not isinstance(scores, dict):
        scores = {}
    embed = build_scoreboard_embed("📊 이벤트 점수판", scores)
    await msg.edit(embed=embed)

async def handle_draft_pick(interaction: discord.Interaction, picked_id: int):
    gid = interaction.guild_id
    guild = interaction.guild
    if not gid or not guild:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return

    ds = guild_draft.get(gid)
    if not ds or ds.phase != "picking":
        await safe_send(interaction, content="픽 단계가 아닙니다.", ephemeral=True)
        return

    if not ds.pick_sequence:
        await safe_send(interaction, content="픽 시퀀스가 없습니다.", ephemeral=True)
        return

    cur_captain_id = ds.pick_sequence[ds.pick_pos]

    # 현재 픽 차례 팀장(또는 관리자)만 픽 가능
    if interaction.user.id != cur_captain_id and not user_is_admin(interaction):
        cur_m = guild.get_member(cur_captain_id)
        cur_name = cur_m.display_name if cur_m else "Unknown"
        await safe_send(interaction, content=f"지금은 **{cur_name}** 픽 차례입니다.", ephemeral=True)
        return

    if picked_id not in ds.pool_ids:
        await safe_send(interaction, content="이미 선택되었거나 풀에 없습니다.", ephemeral=True)
        return

    # 팀장 팀 찾기(팀장=팀의 0번)
    team_index = None
    for i, tm in enumerate(ds.teams):
        if tm and tm[0] == cur_captain_id:
            team_index = i
            break
    if team_index is None:
        await safe_send(interaction, content="팀 정보를 찾지 못했습니다.", ephemeral=True)
        return

    if len(ds.teams[team_index]) >= ds.team_size:
        await safe_send(interaction, content="이미 팀 인원이 꽉 찼습니다.", ephemeral=True)
        return

    # 픽 적용
    ds.teams[team_index].append(picked_id)
    ds.pool_ids.remove(picked_id)
    _save_draft_state_for_guild(gid)

    # 다음 픽
    ds.pick_pos += 1

    # =========================================================
    # (dice/dice2) 라운드 드래프트:
    # - 라운드(=팀장 제외 인원 수)마다 다이스를 다시 굴려 픽 순서를 재결정
    # - 각 라운드에서 팀장당 1명씩 픽(총 team_count회)
    # =========================================================
    if (
        getattr(ds, "draft_mode", "snake") in ("dice", "dice2")
        and ds.pick_pos >= len(ds.pick_sequence)
        and ds.pool_ids
        and ds.round_index < ds.total_rounds
        and (not all(len(t) >= ds.team_size for t in ds.teams))
    ):
        ds.round_index += 1

        # ---- 라운드 시작: 팀장 다이스 재굴림(동점 방지) ----
        while True:
            rolls = {cid: random.randint(DICE_MIN, DICE_MAX) for cid in ds.captain_ids}
            if len(set(rolls.values())) == len(rolls.values()):
                break

        items = sorted(rolls.items(), key=lambda x: x[1], reverse=True)
        roll_order = [cid for cid, _ in items]

        ds.rolls = rolls
        ds.roll_order = roll_order

        # ✅ 요청: 두 번째 다이스(다음 라운드)부터 팀/표시를 다이스 순서대로 재정렬
        # 팀 멤버는 유지하고, 팀 번호/표시만 roll_order 기준으로 다시 나열합니다.
        try:
            by_cap = {int(tm[0]): tm for tm in (ds.teams or []) if tm}
            ds.teams = [by_cap.get(int(cid), [int(cid)]) for cid in roll_order]
        except Exception:
            pass
        # 다음 라운드는 팀장당 1픽
        picks_this_round = min(ds.team_count, len(ds.pool_ids))
        ds.pick_sequence = roll_order[:picks_this_round]
        ds.pick_pos = 0
        _save_draft_state_for_guild(gid)

        # 다이스 로그(선택): 채널에 남김
        try:
            parts = []
            for i, cid in enumerate(roll_order, start=1):
                m = guild.get_member(cid)
                nm = m.display_name if m else str(cid)
                parts.append(f"{i}등 {rolls.get(cid)} / {nm}")
            #await interaction.channel.send(
        except Exception:
            pass

        # 화면 업데이트(다음 라운드 픽 시작)
        embed = build_draft_pick_embed(guild, ds)
        view = DraftPickView.build_with_select(guild, ds, gid)
        try:
            if interaction.message:
                await interaction.message.edit(embed=embed, view=view)
        except Exception:
            pass
        return

    # ---------------------------------------------------------
    # 종료 조건:
    # - 모든 팀 완성 또는 풀 고갈이면 즉시 종료
    # - snake: pick_sequence 소진 시 종료
    # - dice/dice2: 마지막 라운드에서 pick_sequence 소진 시 종료
    # ---------------------------------------------------------
    finished = all(len(t) >= ds.team_size for t in ds.teams) or (not ds.pool_ids)

    if not finished:
        if getattr(ds, "draft_mode", "snake") in ("dice", "dice2"):
            finished = (ds.round_index >= ds.total_rounds) and (ds.pick_pos >= len(ds.pick_sequence))
        else:
            finished = ds.pick_pos >= len(ds.pick_sequence)

    if finished:
        # ✅ 마지막 픽이 반영된 상태를 "드래프트 진행" 메시지에도 먼저 반영(빈칸/아직없음 방지)
        try:
            if interaction.message:
                final_embed = build_draft_pick_embed(guild, ds)
                final_embed.title = "✅ 드래프트 완료"
                await interaction.message.edit(embed=final_embed, view=None)
        except Exception:
            pass

        teams = ds.teams
        msg_to_delete = interaction.message  # 드래프트 진행 메시지(자동 삭제 대상)
        guild_draft.pop(gid, None)
        _save_draft_state_for_guild(gid)

        await finalize_match_and_move(
            interaction,
            teams,
            ds.team_count,
            ds.team_size,
            mode_title=("🎲 다이스 드래프트" if getattr(ds, "draft_mode", "snake") in ("dice","dice2") else "수동 드래프트"),
            match_mode=get_queue_mode(gid)
        )

        # ✅ 드래프트 진행/완료 화면은 잠깐 보여준 뒤 자동 삭제 (매칭 완료 메시지만 남김)
        if msg_to_delete:
            asyncio.create_task(_delete_message_later(msg_to_delete, delay_sec=6))
        return
    # 픽 진행 중 화면 업데이트
    embed = build_draft_pick_embed(guild, ds)
    view = DraftPickView.build_with_select(guild, ds, gid)

    try:
        if interaction.message:
            await interaction.message.edit(embed=embed, view=view)
    except Exception:
        pass

# =========================================================
# Draft helpers (added)
# =========================================================
def _split_items(raw: str) -> List[str]:
    """Split comma/newline separated items and normalize."""
    s = (raw or "").strip()
    if not s:
        return []
    parts = [x.strip() for x in re.split(r"[\n,\s]+", s) if x.strip()]
    seen = set()
    out: List[str] = []
    for p in parts:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out

async def _delete_message_later(message: Optional[discord.Message], delay_sec: int = 3):
    try:
        await asyncio.sleep(max(0, int(delay_sec)))
        if message:
            await message.delete()
    except Exception:
        pass

def build_draft_pick_embed(guild: discord.Guild, ds: DraftSession) -> discord.Embed:
    mode = getattr(ds, "draft_mode", "snake")
    title = "수동 드래프트 진행" if mode == "snake" else "🎲 다이스 드래프트 진행"

    # 라운드 표기(dice 계열)
    round_line = ""
    if mode in ("dice", "dice2"):
        round_line = f"라운드: **{ds.round_index}/{ds.total_rounds}**\n"

    # 현재 픽 차례
    cur_picker = None
    if ds.pick_sequence and ds.pick_pos < len(ds.pick_sequence):
        cid = ds.pick_sequence[ds.pick_pos]
        m = guild.get_member(cid)
        cur_picker = m.display_name if m else str(cid)

    desc_parts = [f"팀수: **{ds.team_count}팀**, 팀당 인원: **{ds.team_size}명**"]
    if round_line:
        desc_parts.append(round_line.rstrip())
    if cur_picker:
        desc_parts.append(f"현재 픽 차례: **{cur_picker}**")
    desc_parts.append(f"남은 풀: **{len(ds.pool_ids)}명**")

    embed = discord.Embed(title=title, description="\n".join(desc_parts))

    # 다이스 순서(라운드별)
    if mode in ("dice", "dice2") and getattr(ds, "roll_order", None):
        lines = []
        for i, cid in enumerate(ds.roll_order, start=1):
            m = guild.get_member(cid)
            nm = m.display_name if m else str(cid)
            rv = ds.rolls.get(cid)
            lines.append(f"{i}. {rv} / {nm}" if rv is not None else f"{i}. {nm}")
        embed.add_field(
            name=f"🎲 다이스 순서 (라운드 {ds.round_index})",
            value=("\n".join(lines)[:1024] if lines else "(없음)"),
            inline=False
        )

    # ------------------------------
    # 팀 목록(남은 멤버 표기 없음)
    # - 팀장/팀 목록은 세로(아래로) 정렬
    # ------------------------------
    for i, team in enumerate(ds.teams, start=1):
        if not team:
            embed.add_field(name=f"{i}팀", value="(비어있음)", inline=False)
            continue

        cap_id = team[0]
        cap_m = guild.get_member(cap_id)
        cap_name = cap_m.display_name if cap_m else str(cap_id)

        member_texts: List[str] = []
        for uid in team[1:]:
            m = guild.get_member(uid)
            member_texts.append(display_with_tier(m) if m else f"Unknown({uid})")

        embed.add_field(
            name=f"{i}팀 (팀장:{cap_name})",
            value=(" / ".join(member_texts) if member_texts else "(팀장 단독)"),
            inline=False
        )

    if mode in ("dice", "dice2"): 
        embed.set_footer(text="라운드가 끝날 때마다 다이스를 다시 굴려 순서를 재정렬합니다.")
    else:
        embed.set_footer(text="스네이크 순서로 픽이 진행됩니다.")

    return embed

async def on_captains_confirmed(
    interaction: discord.Interaction,
    *,
    guild_id: int,
    team_count: int,
    team_size: int,
    queue_ids: List[int],
    captain_ids: List[int],
    draft_mode: str = "snake",
):
    guild = interaction.guild
    if not guild:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return

    gid = int(guild_id)
    if gid in guild_draft:
        await safe_send(interaction, content="❗ 이미 진행 중인 드래프트가 있습니다.", ephemeral=True)
        return

    team_count = int(team_count)
    team_size = int(team_size)
    draft_mode = (draft_mode or "snake").strip().lower()
    if draft_mode not in ("snake", "dice", "dice2"):
        draft_mode = "snake"

    cap_set = {int(x) for x in (captain_ids or [])}
    pool_ids = [int(x) for x in (queue_ids or []) if int(x) not in cap_set]

    ds = DraftSession(
        guild_id=gid,
        channel_id=int(interaction.channel_id),
        message_id=int(interaction.message.id) if interaction.message else 0,
        team_count=team_count,
        team_size=team_size,
        captain_ids=[int(x) for x in captain_ids],
        pool_ids=pool_ids
    )
    ds.draft_mode = draft_mode
    ds.total_rounds = max(1, team_size - 1)
    ds.round_index = 1
    ds.teams = [[cid] for cid in ds.captain_ids]

    # ---- initial dice (tie-free) ----
    while True:
        rolls = {cid: random.randint(DICE_MIN, DICE_MAX) for cid in ds.captain_ids}
        if len(set(rolls.values())) == len(rolls.values()):
            break
    items = sorted(rolls.items(), key=lambda x: x[1], reverse=True)
    roll_order = [cid for cid, _ in items]
    ds.rolls = rolls
    ds.roll_order = roll_order

    # ✅ 초기 팀(팀장) 표시 순서를 다이스 순서로 정렬
    ds.teams = [[cid] for cid in roll_order]


    picks_needed = team_count * max(0, team_size - 1)

    if draft_mode in ("dice", "dice2"):
        picks_this_round = min(team_count, len(ds.pool_ids))
        ds.pick_sequence = roll_order[:picks_this_round]
        ds.pick_pos = 0
    else:
        seq: List[int] = []
        forward = roll_order[:]
        backward = list(reversed(forward))
        while len(seq) < picks_needed and ds.pool_ids:
            for cid in forward:
                if len(seq) >= picks_needed:
                    break
                seq.append(cid)
            if len(seq) >= picks_needed:
                break
            for cid in backward:
                if len(seq) >= picks_needed:
                    break
                seq.append(cid)

        seq = seq[:min(len(seq), len(ds.pool_ids))]
        ds.pick_sequence = seq
        ds.pick_pos = 0

    ds.phase = "picking"
    guild_draft[gid] = ds
    _save_draft_state_for_guild(gid)

    # 대기열 메시지/상태 정리(다음 게임 대비)
    try:
        await clear_queue_state(interaction.client, gid, delete_message=True, clear_members=True)
    except Exception:
        pass

    embed = build_draft_pick_embed(guild, ds)
    view = DraftPickView.build_with_select(guild, ds, gid)

    try:
        if interaction.message:
            await interaction.message.edit(embed=embed, view=view)
        else:
            await safe_send(interaction, embed=embed, view=view, ephemeral=False)
    except Exception:
        await safe_send(interaction, embed=embed, view=view, ephemeral=False)
# =========================================================
# 벤살 뽑기 View
# =========================================================
class BansalView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.ended = False
        self.guild_id = guild_id

    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        b = _get_bansal_bucket(self.guild_id)
        md_cur = b.get("cur_md", [])
        pd_cur = b.get("cur_pd", [])
        total = len(md_cur) + len(pd_cur)

        md_left = len(bansal_available(self.guild_id, "md"))
        pd_left = len(bansal_available(self.guild_id, "pd"))

        desc = []
        desc.append(f"총 벤살: **{total}** ")
        desc.append("")
        desc.append("**마뎀**")
        desc.append(" / ".join(md_cur) if md_cur else "(없음)")
        desc.append("")
        desc.append("**물뎀**")
        desc.append(" / ".join(pd_cur) if pd_cur else "(없음)")
        desc.append("")
        
        embed = discord.Embed(title="🚫 벤살 뽑기", description="\n".join(desc))
        #embed.set_footer(text="관리자 전용")
        return embed

    def _sync_buttons(self):
        # 버튼 비활성화는 '종료'를 눌렀을 때만 적용
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id in ("bansal_add_md", "bansal_add_pd", "bansal_stop"):
                child.disabled = self.ended


    @discord.ui.button(label="마뎀 추가", style=discord.ButtonStyle.primary, custom_id="bansal_add_md")
    async def add_md(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not (interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        if self.ended:
            await interaction.response.send_message("이미 종료되었습니다.", ephemeral=True)
            return

        await interaction.response.defer()

        pick = bansal_draw_one(self.guild_id, "md")
        if not pick:
            await interaction.followup.send("❗ 마뎀 유닛 (목록 추가 필요)", ephemeral=True)
            return

        self._sync_buttons()
        embed = self.build_embed(interaction.guild)
        try:
            await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass

    @discord.ui.button(label="물뎀 추가", style=discord.ButtonStyle.primary, custom_id="bansal_add_pd")
    async def add_pd(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not (interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        if self.ended:
            await interaction.response.send_message("이미 종료되었습니다.", ephemeral=True)
            return

        await interaction.response.defer()

        pick = bansal_draw_one(self.guild_id, "pd")
        if not pick:
            await interaction.followup.send("❗ 물뎀 유닛 (목록 추가 필요)", ephemeral=True)
            return

        self._sync_buttons()
        embed = self.build_embed(interaction.guild)
        try:
            await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass

    @discord.ui.button(label="벤살 종료", style=discord.ButtonStyle.success, custom_id="bansal_finish")
    async def finish(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not (interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return
        await interaction.response.defer()

        gid = interaction.guild_id
        b = _get_bansal_bucket(gid)

        md_picked = list(b.get("cur_md", []))
        pd_picked = list(b.get("cur_pd", []))


        bansal_finalize_round(self.guild_id)

        # 버튼 비활성화 + 메시지 정리
        for child in self.children:
            child.disabled = True

        md_text = " / ".join(md_picked) if md_picked else "(없음)"
        pd_text = " / ".join(pd_picked) if pd_picked else "(없음)"

        embed = discord.Embed(
            title="✅ 벤살 종료",
            description=f"**마뎀**\n {md_text}\n\n **물뎀**\n {pd_text}\n\n 다음판 중복으로 나오지 않습니다."
        )
        try:
            await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass

    @discord.ui.button(label="현재 결과 초기화(이번판만)", style=discord.ButtonStyle.secondary, custom_id="bansal_reset")
    async def reset_current(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not user_is_admin(interaction):
            await interaction.response.send_message("❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return
        await interaction.response.defer()

        bansal_reset_current(self.guild_id)
        self._sync_buttons()
        embed = self.build_embed(interaction.guild)
        try:
            await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass



# =========================================================
# 반성문 면제권 패널 View
# =========================================================

# =========================================================
# 반성문 면제권 패널: 관리자용 추가/제거 모달
# =========================================================
class ExemptionAmountModal(discord.ui.Modal):
    """면제권 장수만 입력받는 모달.
    대상 유저는 UserSelect에서 선택한 값을 사용합니다.
    """
    def __init__(
        self,
        *,
        mode: str,  # "add" | "remove"
        target_user_ids: List[int],
        panel_channel_id: int,
        panel_message_id: int
    ):
        super().__init__(title=("면제권 추가(패널)" if mode == "add" else "면제권 제거(패널)"))
        self.mode = (mode or "add").strip().lower()
        self.target_user_ids = [int(x) for x in (target_user_ids or [])]
        self.panel_channel_id = int(panel_channel_id)
        self.panel_message_id = int(panel_message_id)

        self.amount_input = discord.ui.TextInput(
            label="장수",
            placeholder="예: 1",
            required=True,
            max_length=8
        )
        self.add_item(self.amount_input)

    async def on_submit(self, interaction: discord.Interaction):
            await safe_defer(interaction, thinking=False, ephemeral=True)

            if not interaction.guild or interaction.guild_id is None:
                await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
                return

            if not user_is_admin(interaction):
                await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
                return

            try:
                amount = int((self.amount_input.value or "").strip())
            except Exception:
                await safe_send(interaction, content="❗ 장수는 숫자로 입력해주세요. (예: 1)", ephemeral=True)
                return

            if amount <= 0:
                await safe_send(interaction, content="❗ 장수는 1 이상이어야 합니다.", ephemeral=True)
                return

            gid = int(interaction.guild_id)
            target_ids = [int(x) for x in (self.target_user_ids or [])]
            if not target_ids:
                await safe_send(interaction, content="❗ 대상 유저를 찾을 수 없습니다.", ephemeral=True)
                return

            # 결과 요약
            summary_lines: List[str] = []

            if self.mode == "remove":
                for uid in target_ids:
                    target_member = interaction.guild.get_member(uid)
                    removed, left = remove_exemptions(gid, uid, amount)
                    name = (target_member.display_name if target_member else str(uid))
                    summary_lines.append(f"• {name} : -{removed}장 (남은 {left}장)")
                    try:
                        append_exemption_log(
                            guild_id=gid,
                            action="remove",
                            target_user_id=uid,
                            amount=removed,
                            actor_member=interaction.user,
                            target_member=target_member
                        )
                    except Exception:
                        pass
            else:
                for uid in target_ids:
                    target_member = interaction.guild.get_member(uid)
                    new_cnt = add_exemptions(gid, uid, amount)
                    name = (target_member.display_name if target_member else str(uid))
                    summary_lines.append(f"• {name} : +{amount}장 (총 {new_cnt}장)")
                    try:
                        append_exemption_log(
                            guild_id=gid,
                            action="add",
                            target_user_id=uid,
                            amount=amount,
                            actor_member=interaction.user,
                            target_member=target_member
                        )
                    except Exception:
                        pass

            # 패널 갱신(가능하면)
            try:
                ch = interaction.client.get_channel(self.panel_channel_id)
                if isinstance(ch, (discord.TextChannel, discord.Thread)):
                    msg = await ch.fetch_message(self.panel_message_id)
                    if msg and msg.embeds:
                        v = ExemptionPanelView()
                        embed = v.build_embed(interaction.guild)
                        await msg.edit(embed=embed, view=v)
            except Exception:
                pass

            header = "✅ 면제권 제거 완료" if self.mode == "remove" else "✅ 면제권 추가 완료"
            text = header + "\n" + "\n".join(summary_lines)
            if len(text) > 1800:
                text = text[:1790] + "…"
            await safe_send(interaction, content=text, ephemeral=True)



class ExemptionTargetSelectView(discord.ui.View):
    """패널용 대상 선택 View (여러명 선택 가능)"""

    def __init__(self, *, mode: str, panel_channel_id: int, panel_message_id: int):
        super().__init__(timeout=120)
        self.mode = (mode or "add").strip().lower()
        self.panel_channel_id = int(panel_channel_id)
        self.panel_message_id = int(panel_message_id)

        self.selected_user_ids: List[int] = []

        self.user_select = discord.ui.UserSelect(
            placeholder="대상 유저 선택 (여러명 가능)",
            min_values=1,
            max_values=25
        )
        self.user_select.callback = self._on_select  # type: ignore
        self.add_item(self.user_select)

        self.next_button = discord.ui.Button(label="다음", style=discord.ButtonStyle.success)
        self.next_button.callback = self._on_next  # type: ignore
        self.add_item(self.next_button)

        self.close_button = discord.ui.Button(label="닫기", style=discord.ButtonStyle.secondary)
        self.close_button.callback = self._on_close  # type: ignore
        self.add_item(self.close_button)

    async def _on_select(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        members = list(self.user_select.values)
        self.selected_user_ids = [int(m.id) for m in members]
        mention_list = ", ".join([m.mention for m in members]) if members else "(없음)"
        await safe_send(interaction, content=f"선택됨: {mention_list}", ephemeral=True)

    
    async def _on_next(self, interaction: discord.Interaction):
        # ⚠️ Modal을 열 때는 interaction.response를 한 번만 사용해야 합니다.
        # 따라서 여기서는 defer/send_message 등을 먼저 호출하지 않습니다.
        if not interaction.guild or interaction.guild_id is None:
            try:
                await interaction.response.send_message("서버에서만 사용 가능합니다.", ephemeral=True)
            except Exception:
                pass
            return

        if not user_is_admin(interaction):
            try:
                await interaction.response.send_message("❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            except Exception:
                pass
            return

        # 선택값이 아직 반영되지 않았으면 현재 values로 보정
        if not self.selected_user_ids:
            members = list(getattr(self.user_select, "values", []))
            self.selected_user_ids = [int(m.id) for m in members]

        if not self.selected_user_ids:
            try:
                await interaction.response.send_message("대상 유저를 1명 이상 선택해주세요.", ephemeral=True)
            except Exception:
                pass
            return

        await interaction.response.send_modal(
            ExemptionAmountModal(
                mode=self.mode,
                target_user_ids=self.selected_user_ids,
                panel_channel_id=self.panel_channel_id,
                panel_message_id=self.panel_message_id
                )
            )

    async def _on_close(self, interaction: discord.Interaction):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        try:
            if interaction.message:
                await interaction.message.delete()
        except Exception:
            pass
class ExemptionResetConfirmView(discord.ui.View):
    """면제권 전체 초기화 확인용(예/아니요)."""

    def __init__(self, *, guild_id: int, panel_channel_id: int, panel_message_id: int):
        super().__init__(timeout=45)
        self.guild_id = int(guild_id)
        self.panel_channel_id = int(panel_channel_id)
        self.panel_message_id = int(panel_message_id)

    async def _update_panel(self, guild: discord.Guild):
        try:
            ch = guild.get_channel(self.panel_channel_id)
            if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                return
            msg = await ch.fetch_message(self.panel_message_id)
        except Exception:
            return

        try:
            v = ExemptionPanelView()
            embed = v.build_embed(guild)
            await msg.edit(embed=embed, view=v)
        except Exception:
            pass

    @discord.ui.button(label="예", style=discord.ButtonStyle.danger, custom_id="wave_exempt_reset_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        if not interaction.guild or interaction.guild_id is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        gid = interaction.guild_id
        cleared = reset_exemptions(gid)
        try:
            append_exemption_log(guild_id=gid, action="reset", target_user_id=0, amount=0, actor_member=interaction.user)
        except Exception:
            pass

        await self._update_panel(interaction.guild)

        for c in self.children:
            if hasattr(c, "disabled"):
                c.disabled = True
        try:
            await interaction.message.edit(content=f"✅ 면제권 초기화 완료: {cleared}명 데이터 삭제", view=self)
        except Exception:
            pass

    @discord.ui.button(label="아니요", style=discord.ButtonStyle.secondary, custom_id="wave_exempt_reset_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        for c in self.children:
            if hasattr(c, "disabled"):
                c.disabled = True
        try:
            await interaction.message.edit(content="취소했습니다.", view=self)
        except Exception:
            pass




class ExemptionPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    def build_embed(self, guild: discord.Guild) -> discord.Embed:
        gid = guild.id
        bucket = _get_exemption_bucket(gid)

        # 보유자 목록(보유 장수 desc)
        items: List[Tuple[int, int]] = []
        for uid_str, cnt in (bucket or {}).items():
            try:
                uid = int(uid_str)
            except Exception:
                continue
            items.append((uid, int(cnt)))
        items.sort(key=lambda x: x[1], reverse=True)

        holder_lines: List[str] = []
        for uid, cnt in items:
            m = guild.get_member(uid)
            label = m.mention if m else f"`{uid}`"
            holder_lines.append(f"• {label} : **{cnt}장**")

        holder_text = "\n".join(holder_lines) if holder_lines else "(없음)"
        if len(holder_text) > 1024:
            holder_text = holder_text[:1020] + "…"

        # 최근 로그(추가/제거/사용) - 패널 옆(인라인 필드)로 표시
        log_text = format_recent_exemption_logs(guild, gid, limit=12)

        embed = discord.Embed(
            title="🧾 반성문 면제권",
            description=f"면제권 시스템 패널입니다."
        )
        embed.add_field(name="보유 목록", value=holder_text, inline=True)
        embed.add_field(name="최근 로그", value=log_text, inline=True)

        #embed.set_footer(text="운영진: /면제권추가 로 지급 | 사용 버튼으로 1장 차감")
        return embed

    @discord.ui.button(label="사용(1장 차감)", style=discord.ButtonStyle.success, custom_id="wave_exempt_use")
    async def use(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        if not interaction.guild:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        gid = interaction.guild_id
        if gid is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return

        lock = get_exemption_lock(gid)
        async with lock:
            ok = use_one_exemption(gid, interaction.user.id)

        if not ok:
            await safe_send(interaction, content="❗ 사용할 면제권이 없습니다.", ephemeral=True)
            return

        # 본인 안내
        left = get_exemption_count(gid, interaction.user.id)
        await safe_send(interaction, content=f"✅ 면제권 1장 사용 완료. 남은 면제권: **{left}장**", ephemeral=True)

        # 사용/차감 로그(JSON)
        if interaction.guild:
            try:
                append_exemption_log(
                    guild_id=interaction.guild.id,
                    action="use",
                    target_user_id=interaction.user.id,
                    amount=1,
                    actor_member=interaction.user,
                    target_member=interaction.user
                    )
            except Exception:
                pass

        # 패널 메시지 갱신(가능하면)
        try:
            if interaction.message and interaction.guild:
                embed = self.build_embed(interaction.guild)
                await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass

    @discord.ui.button(label="새로고침", style=discord.ButtonStyle.secondary, custom_id="wave_exempt_refresh")
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        if not interaction.guild:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        try:
            if interaction.message:
                embed = self.build_embed(interaction.guild)
                await interaction.message.edit(embed=embed, view=self)
            await safe_send(interaction, content="✅ 새로고침 완료", ephemeral=True)
        except Exception:
            await safe_send(interaction, content="❗ 새로고침 실패", ephemeral=True)



    @discord.ui.button(label="➕ 면제권 추가(관리자)", style=discord.ButtonStyle.primary, custom_id="wave_exempt_admin_add")
    async def admin_add(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return
        view = ExemptionTargetSelectView(mode="add", panel_channel_id=interaction.channel_id, panel_message_id=(interaction.message.id if interaction.message else 0))
        await safe_send(interaction, content="대상 유저를 선택하세요.", view=view, ephemeral=True)

    @discord.ui.button(label="➖ 면제권 제거(관리자)", style=discord.ButtonStyle.danger, custom_id="wave_exempt_admin_remove")
    async def admin_remove(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return
        view = ExemptionTargetSelectView(mode="remove", panel_channel_id=interaction.channel_id, panel_message_id=(interaction.message.id if interaction.message else 0))
        await safe_send(interaction, content="대상 유저를 선택하세요.", view=view, ephemeral=True)


    @discord.ui.button(label="🧹 면제권 초기화(관리자)", style=discord.ButtonStyle.danger, custom_id="wave_exempt_admin_reset")
    async def admin_reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, thinking=False, ephemeral=True)
        if not interaction.guild or interaction.guild_id is None:
            await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
            return
        if not user_is_admin(interaction):
            await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        if not interaction.message:
            await safe_send(interaction, content="❗ 패널 메시지를 찾지 못했습니다.", ephemeral=True)
            return

        confirm_view = ExemptionResetConfirmView(
            guild_id=interaction.guild_id,
            panel_channel_id=int(interaction.channel_id),
            panel_message_id=int(interaction.message.id)
        )
        await safe_send(
            interaction,
            content="⚠️ **면제권을 전부 초기화**합니다. 정말 진행할까요?\n(예 / 아니요)",
            view=confirm_view,
            ephemeral=True
            )

# =========================================================
# 승리팀 버튼 View
# - 중요: 봇 재시작 후에도 버튼이 동작하려면 "persistent view" 로 등록되어 있어야 합니다.
#   그래서 custom_id는 길드/메시지에 따라 바뀌지 않는 "고정 값"을 사용합니다.
# =========================================================

MATCH_MAX_TEAMS = 20  # 버튼 UI는 최대 10팀까지 표시(필요시 늘리세요)



class BansalOpenButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="🚫 벤살 뽑기",
            style=discord.ButtonStyle.secondary,
            custom_id="wave_match_bansal_open"
        )

    async def callback(self, interaction: discord.Interaction):
        if not user_is_admin(interaction):
            return await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            
        gid = interaction.guild_id
        
        # 벤살 초기화 (새로운 라운드 시작 보장)
        bansal_begin_round_if_needed(gid)
        
        # 마뎀 4개, 물뎀 2개 자동 뽑기
        for _ in range(4):
            bansal_draw_one(gid, "md")
        for _ in range(2):
            bansal_draw_one(gid, "pd")
            
        view = BansalView(gid)
        embed = view.build_embed(interaction.guild)
        await interaction.response.send_message(embed=embed, view=view)



class CancelMatchButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="❌ 매치 취소",
            style=discord.ButtonStyle.danger,
            custom_id="wave_match_cancel_all"
        )

    async def callback(self, interaction: discord.Interaction):
        if not user_is_admin(interaction):
            return await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)
            
        gid = interaction.guild_id
        _ensure_last_match_loaded(gid)
        state = guild_last_match.get(gid)
        
        if not state or not state.pending:
            return await safe_send(interaction, content="❌ 취소할 진행 중인 매치가 없습니다.", ephemeral=True)

        # 상태 초기화
        state.active = False
        state.pending = False
        _save_last_match_for_guild(gid)

        # UI 업데이트
        try:
            for item in self.view.children:
                if isinstance(item, (discord.ui.Button, discord.ui.Select)):
                    item.disabled = True
            await interaction.message.delete()
        except:
            pass

        await safe_send(interaction, content="✅ 진행 중인 매치가 취소되었습니다. 모든 데이터가 초기화되었습니다.")


class MatchResultView(discord.ui.View):
    """매칭 완료 메시지에 붙는 View(메시지별로 생성).

    - custom_id는 고정 규칙을 사용합니다.
    - 팀 수(team_count)에 맞춰 필요한 버튼만 추가합니다.
    """

    def __init__(self, team_count: int):
        super().__init__(timeout=None)
        self.team_count = int(team_count)

        for i in range(min(self.team_count, MATCH_MAX_TEAMS)):
            self.add_item(WinButton(team_index=i))

        self.add_item(BansalOpenButton())
        self.add_item(CancelMatchButton())


class PersistentMatchResultRegistry(discord.ui.View):
    """봇 재시작 후에도 기존 메시지의 버튼을 처리하기 위한 '등록용' View.

    - on_ready에서 bot.add_view(...)로 1회 등록하면,
      과거 메시지에 남아있는 custom_id들도 정상적으로 라우팅됩니다.
    - 실제로 메시지에 표시되는 버튼 개수는 MatchResultView가 결정합니다.
    """

    def __init__(self):
        super().__init__(timeout=None)
        for i in range(MATCH_MAX_TEAMS):
            self.add_item(WinButton(team_index=i))
        self.add_item(BansalOpenButton())
        self.add_item(CancelMatchButton())


class WinButton(discord.ui.Button):
    def __init__(self, team_index: int):
        super().__init__(
            label=f"🏆 {team_index + 1}팀 승리",
            style=discord.ButtonStyle.success,
            custom_id=f"wave_match_win_{team_index}"
        )
        self.team_index = int(team_index)

    async def callback(self, interaction: discord.Interaction):
        # 1. 관리자 권한 확인
        if not user_is_admin(interaction):
            return await safe_send(interaction, content="❌ 관리자만 사용할 수 있습니다.", ephemeral=True)

        gid = interaction.guild_id
        _ensure_last_match_loaded(gid)
        state = guild_last_match.get(gid)

        # 2. 상태 확인 (이미 처리되었거나 없는 경우)
        if not state or not state.pending or not state.teams:
            return await safe_send(interaction, content="❌ 이미 처리되었거나 진행 중인 매치가 없습니다.", ephemeral=True)

        # 3. 즉시 버튼 비활성화 (UI 차단)
        try:
            for item in self.view.children:
                if isinstance(item, (discord.ui.Button, discord.ui.Select)):
                    item.disabled = True
            await interaction.response.edit_message(view=self.view)
        except:
            pass

        # 4. 데이터 처리 시작
        guild = interaction.guild
        if not guild:
            return

        # 승점 계산
        per_team = 2 if int(getattr(state, 'team_size', 0) or 0) == 3 else 1
        pts = int(state.team_count) * int(per_team)
        
        if self.team_index >= len(state.teams):
            return await safe_send(interaction, content="❌ 팀 정보가 올바르지 않습니다.", ephemeral=True)
            
        winners = state.teams[self.team_index]
        winner_names = []
        for uid in winners:
            m = guild.get_member(uid)
            if m: winner_names.append(m.display_name)

        # 점수 반영
        for nm in winner_names:
            if str(getattr(state, "match_mode", "normal")).lower() == "event":
                add_event_points(nm, pts)
            else:
                add_points(nm, pts, daily=True, weekly=True)

        try:
            bansal_finalize_round(gid)
        except:
            pass

        # 5. 로비 이동 및 채널 정리 (병렬 처리)
        try:
            target_lobby = None
            for vc in guild.voice_channels:
                if vc.name == "✨ㅣ출항대기(게임 대기)":
                    target_lobby = vc
                    break
            
            if target_lobby and guild.me.guild_permissions.move_members:
                tasks = []
                for team in state.teams:
                    for uid in team:
                        m = guild.get_member(uid)
                        if m and m.voice and m.voice.channel:
                            tasks.append(m.move_to(target_lobby))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            print(f"Move error: {e}")

        await delete_team_voice_channels(guild, state)
        
        # 6. 모든 처리가 완료된 후 상태 변경 (중복 방지 마침표)
        state.active = False
        state.pending = False
        _save_last_match_for_guild(gid)

        # 7. 결과 표시
        winner_list_str = ", ".join(winner_names) if winner_names else "(없음)"
        embed = discord.Embed(
            title="✅ 경기 결과 반영 완료",
            description=(
                f"**승리 팀:** {self.team_index + 1}팀\n"
                f"**승리 인원:** {winner_list_str}\n"
                f"**부여 점수(1인):** **{pts}점**"
            ),
            color=discord.Color.green()
        )
        await interaction.followup.send(embed=embed)
# =========================================================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.voice_states = True

intents.message_content = True  # 다이스 커맨드 !dice
bot = commands.Bot(command_prefix="!", intents=intents, case_insensitive=True)

# =========================================================
# 채팅 커맨드: !dice (주사위)
# - 주의: discord 개발자 포털에서 "MESSAGE CONTENT INTENT"도 켜야 합니다.
# =========================================================
@bot.command(name="dice")
async def dice_cmd(ctx: commands.Context, max_value: int = DICE_MAX):
    # 사용법:
    # !dice        -> 1 ~ 99
    # !dice 100    -> 1 ~ 100

    try:
        max_v = int(max_value)
    except Exception:
        max_v = DICE_MAX

    if max_v < 1:
        max_v = DICE_MAX

    roll = random.randint(DICE_MIN, max_v)
    await ctx.send(
        f"🎲 **{ctx.author.display_name}** 주사위 결과: **{roll}** "
        )

# (선택) 슬래시 명령어도 같이 제공하면, message content intent가 꺼져 있어도 사용 가능
@bot.tree.command(name="주사위", description="주사위를 굴립니다. (1~최대값)")
@app_commands.describe(max_value="최대값(기본 99)")
async def dice_slash(interaction: discord.Interaction, max_value: app_commands.Range[int, 1, 100000] = DICE_MAX):
    roll = random.randint(DICE_MIN, int(max_value))
    await safe_send(interaction, content=f"🎲 **{interaction.user.display_name}** 주사위: **{roll}** ", ephemeral=False)

@bot.event
async def on_ready():
    bot.add_view(QueueFullView())
    bot.add_view(ScoreboardUnifiedView())
    bot.add_view(RankingModeSelectView())
    bot.add_view(ResetModeSelectView())
    bot.add_view(ScoreboardPinScopeView())
    bot.add_view(RestoreModeSelectView())
    bot.add_view(ExemptionPanelView())
    bot.add_view(PersistentMatchResultRegistry())  # ✅ 재시작 후 승리팀 버튼 복구용
    # ------------------------------
    # 재부팅 복구: 파일에서 대기열/드래프트/매치 상태 재로드 후 메시지 재활성화
    # ------------------------------
    try:
        global queue_state_data, draft_state_data, bansal_data, match_state_data
        
        # 매치 상태 복구 (승리팀 버튼 작동 보장)
        match_state_data = load_json(MATCH_STATE_FILE)
        guild_last_match.clear()
        _load_last_match_into_memory()

        # 진행 중인 드래프트 및 벤살 뷰 복구
        for gid_str in draft_state_data.keys():
            try:
                gid = int(gid_str)
                bot.add_view(DraftPickView(gid))
            except: pass
        
        for gid_str in bansal_data.keys():
            try:
                gid = int(gid_str)
                bot.add_view(BansalView(gid))
            except: pass
        queue_state_data = load_json(QUEUE_STATE_FILE)
        draft_state_data = load_json(DRAFT_STATE_FILE)
        guild_queues.clear()
        guild_draft.clear()
        _load_queue_state_into_memory()
        _load_draft_state_into_memory()

        # 1) 대기열 메시지 복구(버튼 다시 붙이기 + embed 갱신)
        for gid, st in list(guild_queues.items()):
            if not st or not st.message_id or not st.channel_id:
                continue
            try:
                await refresh_queue_message(bot, int(gid))
            except Exception:
                continue

        # 2) 드래프트 진행 메시지 복구
        for gid, ds in list(guild_draft.items()):
            try:
                gid = int(gid)
                guild = bot.get_guild(gid)
                if not guild:
                    continue
                ch = bot.get_channel(int(ds.channel_id))
                if ch is None:
                    try:
                        ch = await bot.fetch_channel(int(ds.channel_id))
                    except Exception:
                        continue
                if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                    continue
                try:
                    msg = await ch.fetch_message(int(ds.message_id))
                except Exception:
                    continue

                embed = build_draft_pick_embed(guild, ds)
                view = DraftPickView.build_with_select(guild, ds, gid)
                try:
                    await msg.edit(embed=embed, view=view)
                except Exception:
                    pass
            except Exception:
                continue

        # 3) 면제권 패널 메시지 복구(여러 개 가능)
        for gid_str, arr in list((panel_state_data or {}).items()):
            try:
                gid = int(gid_str)
            except Exception:
                continue
            guild = bot.get_guild(gid)
            if not guild:
                continue

            keep: list = []
            for it in (arr or []):
                try:
                    ch_id = int(it.get("channel_id", 0))
                    msg_id = int(it.get("message_id", 0))
                    if not ch_id or not msg_id:
                        continue

                    ch = bot.get_channel(ch_id)
                    if ch is None:
                        try:
                            ch = await bot.fetch_channel(ch_id)
                        except Exception:
                            continue
                    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                        continue

                    try:
                        msg = await ch.fetch_message(msg_id)
                    except Exception:
                        continue

                    v = ExemptionPanelView()
                    embed = v.build_embed(guild)
                    try:
                        await msg.edit(embed=embed, view=v)
                        keep.append({"channel_id": ch_id, "message_id": msg_id})
                    except Exception:
                        continue
                except Exception:
                    continue

            if keep:
                panel_state_data[str(gid)] = keep
            else:
                panel_state_data.pop(str(gid), None)
        _save_panel_state_file()

        # 4) 점수판 메시지 갱신(재부팅 후 현재 점수로 갱신)
        await refresh_scoreboard_messages(bot, guild_id=None)

        # 5) 대시보드 패널 복구
        dash_data = _load_dashboard_state()
        for gid_str, it in list(dash_data.items()):
            try:
                gid = int(gid_str)
                guild = bot.get_guild(gid)
                if not guild: continue
                ch_id = int(it.get("channel_id", 0))
                msg_id = int(it.get("message_id", 0))
                ch = bot.get_channel(ch_id) or await bot.fetch_channel(ch_id)
                msg = await ch.fetch_message(msg_id)
                embed = await build_dashboard_embed(guild)
                await msg.edit(embed=embed)
            except: pass
    except Exception:
        pass



@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    guild = member.guild
    st = guild_last_match.get(guild.id)
    if not st or not st.active:
        return

    if before and before.channel and isinstance(before.channel, discord.VoiceChannel):
        ch = before.channel
        if ch.id in st.voice_channel_ids and len(ch.members) == 0:
            asyncio.create_task(maybe_cleanup_empty_team_voice(guild, ch))


# =========================================================
# 슬래시 명령어: 대기열 생성
# =========================================================

async def queue_create(interaction: discord.Interaction):
    """대기열 메시지를 생성하는 헬퍼 함수"""
    gid = interaction.guild_id
    if gid is None:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return

    state = guild_queues.setdefault(gid, GuildQueueState())
    embed = discord.Embed(
        title="📌 대기열",
        description=f"현재 인원: **{len(state.member_ids)}명**\n(비어있음)"
        )
    msg = await interaction.channel.send(embed=embed, view=QueueFullView())
    state.message_id = msg.id
    state.channel_id = msg.channel.id
    _save_queue_state_for_guild(gid)
    await refresh_queue_message(interaction.client, gid)

class QueueModeSelectView(discord.ui.View):
    def __init__(self, interaction: discord.Interaction):
        super().__init__(timeout=60)
        self.interaction = interaction

    @discord.ui.button(label="내전", style=discord.ButtonStyle.primary)
    async def normal(self, interaction: discord.Interaction, _):
        set_queue_mode(interaction.guild_id, "normal")
        await interaction.response.send_message("내전 모드로 대기열을 생성합니다.", ephemeral=True)
        await queue_create(interaction)

    @discord.ui.button(label="이벤트", style=discord.ButtonStyle.danger, custom_id="wave_score_unified_event")
    async def event(self, interaction: discord.Interaction, _):
        set_queue_mode(interaction.guild_id, "event")
        await interaction.response.send_message("이벤트 모드로 대기열을 생성합니다.", ephemeral=True)
        await queue_create(interaction)

@bot.tree.command(name="점수판", description="일간/주간/이벤트 점수판을 확인합니다.")
async def scoreboard(interaction: discord.Interaction):
    await interaction.response.send_message(
        "점수판을 선택하세요.",
        view=ScoreboardUnifiedView(),
        ephemeral=True
        )

@bot.tree.command(name="대시보드", description="실시간 대기열, 매치 현황, 랭킹을 보여주는 패널을 생성합니다. (관리자)")
@is_admin()
async def dashboard_create(interaction: discord.Interaction):
    await safe_defer(interaction, thinking=False)
    
    embed = await build_dashboard_embed(interaction.guild)
    await safe_send(interaction, embed=embed)

    try:
        msg = await interaction.original_response()
        if msg:
            _register_dashboard_message(interaction.guild_id, interaction.channel_id, msg.id)
    except Exception:
        pass

@bot.tree.command(name="대기열생성", description="대기열 참여/나가기 버튼 메시지를 생성합니다. (관리자)")
@is_admin()
async def queue_create_select(interaction: discord.Interaction):
    await interaction.response.send_message("모드를 선택하세요.", 
    view=QueueModeSelectView(interaction), ephemeral=True)

@bot.tree.command(name="정보")
async def my_info(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    member = interaction.user
    name = member.display_name

    daily_rank, daily_pts = get_rank(load_scores_daily(), name)
    weekly_rank, weekly_pts = get_rank(load_scores_weekly(), name)
    event_rank, event_pts = get_rank(load_scores_event(), name)

    # 승률 계산
    stats = load_json(MATCH_STATS_FILE) if os.path.exists(MATCH_STATS_FILE) else {}
    user_stats = stats.get(str(member.id), {"wins": 0, "losses": 0})
    wins = user_stats.get("wins", 0)
    losses = user_stats.get("losses", 0)
    total = wins + losses
    win_rate = (wins / total * 100) if total > 0 else 0

    def fmt(rank, pts):
        if rank is None:
            return f"{pts}점 (기록 없음)"
        medal = " 👑" if rank == 1 else ""
        return f"{pts}점 ({rank}위{medal})"

    embed = discord.Embed(
        title=f"📊 {name}님의 정보",
        color=discord.Color.blurple()
        )

    embed.add_field(name="📅 일간", value=fmt(daily_rank, daily_pts), inline=False)
    embed.add_field(name="📆 주간", value=fmt(weekly_rank, weekly_pts), inline=False)
    embed.add_field(name="🎉 이벤트", value=fmt(event_rank, event_pts), inline=False)
    embed.add_field(name="📈 전적", value=f"{wins}승 {losses}패 (승률 {win_rate:.1f}%)", inline=False)

    embed.set_footer(text="CLAN WAVE 자동 점수 시스템")

    await interaction.followup.send(embed=embed, ephemeral=True)
@bot.tree.command(name="대기열리셋", description="현재 서버의 대기열 및 드래프트 상태를 완전히 초기화합니다. (관리자)")
@app_commands.checks.has_permissions(administrator=True)
async def queue_reset_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    gid = interaction.guild_id
    
    # 메모리에서 제거
    if gid in guild_queues:
        del guild_queues[gid]
    if gid in guild_draft:
        del guild_draft[gid]
        
    # 파일 데이터에서 제거
    queue_state_data.pop(str(gid), None)
    draft_state_data.pop(str(gid), None)
    
    # 파일 저장
    _save_queue_state_file()
    _save_draft_state_file()
    
    embed = discord.Embed(
        title="🧹 대기열/드래프트 초기화 완료",
        description="현재 서버의 모든 대기열 및 드래프트 데이터가 삭제되었습니다.\n이제 새로운 대기열을 생성하여 이용하실 수 있습니다.",
        color=discord.Color.blue()
    )
    await interaction.followup.send(embed=embed, ephemeral=True)

@queue_reset_cmd.error
async def queue_reset_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.errors.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 관리자 권한이 필요합니다.", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ 오류 발생: {error}", ephemeral=True)


# =========================================================
# 슬래시 명령어: 동기화(즉시 반영)
# =========================================================
@bot.tree.command(name="동기화", description="슬래시 명령어를 즉시 동기화하고 중복을 제거합니다. (관리자)")
@is_admin()
async def sync_commands(interaction: discord.Interaction):
    """중복된 명령어를 정리하고 새로운 명령어 구조를 서버에 반영합니다."""
    if not interaction.guild_id:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return
    
    await interaction.response.defer(ephemeral=True)
    try:
        guild_obj = discord.Object(id=interaction.guild_id)
        
        # 1. 전역 명령어 목록을 현재 서버로 복사
        bot.tree.copy_global_to(guild=guild_obj)
        
        # 2. 현재 서버의 명령어 목록을 디스코드 API에 동기화
        # 이 과정에서 이전 구조의 명령어들이 새로운 구조로 덮어씌워집니다.
        synced = await bot.tree.sync(guild=guild_obj)
        
        await interaction.followup.send(
            content=f"✅ **동기화 및 중복 정리 완료!**\n- 총 {len(synced)}개의 명령어가 새로 등록되었습니다.\n- 여전히 중복이 보인다면 **디스코드 재시작(Ctrl+R)**을 해주세요.",
            ephemeral=True
            )
    except Exception as e:
        await interaction.followup.send(content=f"❗ 동기화 실패: {type(e).__name__}: {e}", ephemeral=True)

# =========================================================
# 슬래시 명령어: 랭킹/점수
# =========================================================
@bot.tree.command(name="랭킹", description="점수 랭킹을 확인합니다.")
async def ranking(interaction: discord.Interaction, top: app_commands.Range[int, 1, 50] = 40):
    await interaction.response.send_message(
        "확인할 랭킹을 선택하세요.",
        view=RankingModeSelectView(top=top),
        ephemeral=True
        )

@bot.tree.command(name="점수추가", description="점수 추가 (모드 선택)")
@is_admin()
@app_commands.describe(닉네임="닉네임", 점수="점수")
async def score_add_v3(interaction: discord.Interaction, 닉네임: str, 점수: int):
    await interaction.response.send_message(
        "어디에 점수를 반영할까요?",
        view=ScoreAddModeView(닉네임, 점수),
        ephemeral=True
        )


@bot.tree.command(name="점수설정", description="점수 설정 (주간/이벤트)")
@is_admin()
@app_commands.describe(닉네임="닉네임", 점수="점수")
async def score_set_v3(interaction: discord.Interaction, 닉네임: str, 점수: int):
    await interaction.response.send_message(
        "어디에 설정할까요?",
        view=ScoreRemoveSetView(닉네임, 점수, "set"),
        ephemeral=True
        )

@bot.tree.command(name="점수삭제", description="특정 유저 점수를 차감합니다. (관리자)")
@is_admin()
@app_commands.describe(닉네임="닉네임", 점수="점수")
async def score_remove(interaction: discord.Interaction, 닉네임: str, 점수: int):
    await interaction.response.send_message(
        "어디에서 제거할까요?",
        view=ScoreRemoveSetView(닉네임, 점수, "remove"),
        ephemeral=True
        )


@bot.tree.command(name="점수제거", description="점수 제거 (주간/이벤트)")
@is_admin()
@app_commands.describe(닉네임="닉네임", 점수="점수")
async def score_remove_v3(interaction: discord.Interaction, 닉네임: str, 점수: int):
    await interaction.response.send_message(
        "어디에서 제거할까요?",
        view=ScoreRemoveSetView(닉네임, 점수, "remove"),
        ephemeral=True
        )

@bot.tree.command(name="경기승리", description="승리 멤버들에게 (팀수 * 배수) 점수를 일간/주간에 모두 추가합니다. (관리자)")
@is_admin()
async def match_win(
    interaction: discord.Interaction,
    팀수: app_commands.Range[int, 2, 10],
    멤버들: str,
):
    await safe_defer(interaction, thinking=False)

    points_per_player = int(팀수) * int(WIN_SCORE_MULTIPLIER)

    raw = (멤버들 or "").strip()
    names = [x.strip() for x in re.split(r"[\n,\s]+", raw) if x.strip()]
    if not names:
        await safe_send(interaction, content="❗ 승리 멤버를 1명 이상 입력하세요. 예) `라시, 드뚜, 손수건`", ephemeral=True)
        return

    result_lines = []
    for name in names:
        add_points(name, points_per_player, daily=True, weekly=True)
        result_lines.append(f"- **{name}** : +{points_per_player}점 (일간/주간)")

    embed = discord.Embed(
        title="🏆 /경기승리 점수 반영 완료",
        description="승리 팀 멤버들에게 점수가 부여되었습니다."
    )
    embed.set_footer(text="CLAN WAVE 자동 포인트 시스템")
    asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
    await safe_send(interaction, embed=embed, ephemeral=False)


@bot.tree.command(name="초기화", description="점수 초기화 (관리자 전용)")
@is_admin()
async def reset_scores(interaction: discord.Interaction):
    """모든 점수 초기화 명령어를 버튼 방식으로 실행합니다."""
    await interaction.response.send_message(
        "초기화할 점수 카테고리를 선택해주세요.",
        view=ResetModeSelectView(),
        ephemeral=True
        )

class RestoreModeSelectView(discord.ui.View):
    """복구 모드 선택 View"""
    def __init__(self):
        super().__init__(timeout=None)

    async def _restore(self, interaction: discord.Interaction, kind: str, label: str):
        global daily_scores, weekly_scores, event_scores
        path = _find_latest_backup(kind)
        if not path:
            await interaction.response.send_message(f"❗ {label} 복구할 백업 파일이 없습니다.", ephemeral=True)
            return

        data = load_json_lenient(path)
        scores = _coerce_scores_dict(data)

        if kind == "daily":
            daily_scores = scores
            save_json(SCORES_DAILY_PATH, daily_scores)
        elif kind == "weekly":
            weekly_scores = scores
            save_json(SCORES_WEEKLY_PATH, weekly_scores)
        elif kind == "event":
            event_scores = scores
            save_json(SCORES_EVENT_PATH, event_scores)

        if interaction.guild_id:
            asyncio.create_task(refresh_scoreboard_messages(interaction.client, interaction.guild_id))
        await interaction.response.send_message(f"✅ 점수 복구 완료: {label} / {len(scores)}명", ephemeral=False)

    @discord.ui.button(label="일간", style=discord.ButtonStyle.secondary, custom_id="wave_score_unified_daily")
    async def daily(self, interaction: discord.Interaction, _):
        await self._restore(interaction, "daily", "일간")

    @discord.ui.button(label="주간(내전)", style=discord.ButtonStyle.primary, custom_id="wave_score_unified_weekly")
    async def weekly(self, interaction: discord.Interaction, _):
        await self._restore(interaction, "weekly", "주간")

    @discord.ui.button(label="이벤트", style=discord.ButtonStyle.danger, custom_id="wave_score_unified_event")
    async def event(self, interaction: discord.Interaction, _):
        await self._restore(interaction, "event", "이벤트")

@bot.tree.command(name="점수복구", description="마지막 백업에서 점수를 복구합니다. (관리자)")
@is_admin()
async def restore_scores(interaction: discord.Interaction):
    """가장 최근 백업 파일로 점수 데이터를 복구합니다."""
    await interaction.response.send_message(
        "복구할 점수를 선택하세요.",
        view=RestoreModeSelectView(),
        ephemeral=True
        )

# =========================================================
# 슬래시 명령어: 티어
# =========================================================
@bot.tree.command(name="티어설정", description="유저 티어를 설정합니다. (관리자)")
@is_admin()
async def tier_set(interaction: discord.Interaction, 멤버: discord.Member, 티어: app_commands.Range[int, 0, 10]):
    tiers[str(멤버.id)] = int(티어)
    save_json(TIERS_FILE, tiers)
    await safe_send(interaction, content=f"✅ {멤버.mention} 티어를 **{티어}** 로 설정했습니다.", ephemeral=False)


@bot.tree.command(name="티어조회", description="유저 티어를 조회합니다.")
async def tier_get(interaction: discord.Interaction, 멤버: discord.Member):
    t = tiers.get(str(멤버.id), 0)
    await safe_send(interaction, content=f"ℹ️ {멤버.mention} 티어: **{t}**", ephemeral=False)


@bot.tree.command(name="티어목록", description="티어 목록을 출력합니다.")
async def tier_list(interaction: discord.Interaction):
    guild = interaction.guild
    if not guild:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return

    bucket: Dict[int, List[str]] = {}
    for uid_str, t in tiers.items():
        try:
            uid = int(uid_str)
        except Exception:
            continue
        m = guild.get_member(uid)
        if not m:
            continue
        bucket.setdefault(int(t), []).append(m.display_name)

    if not bucket:
        await safe_send(interaction, content="등록된 티어가 없습니다.", ephemeral=True)
        return

    lines = []
    for t in sorted(bucket.keys(), reverse=True):
        names = ", ".join(bucket[t])
        lines.append(f"**{tier_emoji_text(t)}**: {names}")

    embed = discord.Embed(title="🏷️ 티어 목록", description="\n".join(lines))
    await safe_send(interaction, embed=embed, ephemeral=False)


# =========================================================
# 슬래시 명령어: 벤살(밴) 관리/뽑기
# =========================================================
@bot.tree.command(name="벤살추가", description="벤살유닛 추가. (관리자)")
@is_admin()
@app_commands.choices(
    타입=[
        app_commands.Choice(name="마뎀", value="md"),
        app_commands.Choice(name="물뎀", value="pd"),
    ]
)
async def bansal_add_cmd(interaction: discord.Interaction, 타입: app_commands.Choice[str], 항목: str):
    gid = interaction.guild_id
    if gid is None:
        await interaction.response.send_message("서버에서만 사용 가능합니다.", ephemeral=True)
        return
    items = _split_items(항목)
    if not items:
        await interaction.response.send_message("❗ 추가할 항목이 없습니다. 예) 루피, 조로, 상디", ephemeral=True)
        return
    added, existed = bansal_add(gid, 타입.value, items)
    await interaction.response.send_message(f"✅ 추가 완료: {added}개 (중복 {existed}개)", ephemeral=True)

@bot.tree.command(name="벤살제거", description="벤살유닛 삭제. (관리자)")
@is_admin()
@app_commands.choices(
    타입=[
        app_commands.Choice(name="마뎀", value="md"),
        app_commands.Choice(name="물뎀", value="pd"),
    ]
)
async def bansal_remove_cmd(interaction: discord.Interaction, 타입: app_commands.Choice[str], 항목: str):
    gid = interaction.guild_id
    if gid is None:
        await interaction.response.send_message("서버에서만 사용 가능합니다.", ephemeral=True)
        return
    ok = bansal_remove(gid, 타입.value, 항목.strip())
    if ok:
        await interaction.response.send_message(f"🗑️ 제거 완료: {항목}", ephemeral=True)
    else:
        await interaction.response.send_message("❗ 목록에 존재하지 않습니다.", ephemeral=True)

@bot.tree.command(name="벤살목록", description="벤살유닛 목록 (관리자)")
@app_commands.describe(kind="목록 분류(마뎀/물뎀/전체)")
@app_commands.choices(kind=[
    app_commands.Choice(name="전체", value="all"),
    app_commands.Choice(name="마뎀", value="md"),
    app_commands.Choice(name="물뎀", value="pd"),
])
async def bansal_list_cmd(interaction: discord.Interaction, kind: Optional[app_commands.Choice[str]] = None):
    gid = interaction.guild_id
    if gid is None:
        await interaction.response.send_message("서버에서만 사용 가능합니다.", ephemeral=True)
        return
    text_list = bansal_list_text(gid, (kind.value if kind else 'all'))
    embed = discord.Embed(description=text_list)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="벤살뽑기", description="벤살뽑기")
#@is_admin()
async def bansal_draw_cmd(interaction: discord.Interaction):
    gid = interaction.guild_id
    if gid is None or not interaction.guild:
        await interaction.response.send_message("서버에서만 사용 가능합니다.", ephemeral=True)
        return

    # 새 판 시작: 직전 판 결과(next_*)를 이번 판 제외(exclude_*)로 1회만 적용
    bansal_begin_round_if_needed(gid)

    # 처음 열었고 결과가 비어있다면: 마뎀 6개 자동 소환
    b = _get_bansal_bucket(gid)
    if not b.get("cur_md") and not b.get("cur_pd"):
        for _ in range(4):
            if bansal_draw_one(gid, "md") is None:
                break          
        for _ in range(2):
            if bansal_draw_one(gid, "pd") is None:
                break
    view = BansalView(gid)
    embed = view.build_embed(interaction.guild)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=False)



# =========================================================
# 슬래시 명령어: 반성문 면제권
# =========================================================
@bot.tree.command(name="면제권추가", description="반성문 면제권을 추가합니다. (운영진/관리자)")
@is_admin()
async def exempt_add_cmd(interaction: discord.Interaction, 멤버: discord.Member, 장수: app_commands.Range[int, 1, 999] = 1):
    gid = interaction.guild_id
    if gid is None:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return
    new_cnt = add_exemptions(gid, 멤버.id, int(장수))
    await safe_send(interaction, content=f"✅ {멤버.mention} 면제권 **+{장수}장** (총 {new_cnt}장)", ephemeral=True)

    # 로그
    if interaction.guild:
        try:
            import datetime as _dt
            ts = int(_dt.datetime.now(_dt.timezone.utc).timestamp())
            await log_exemption_event(
                interaction.guild,
                f"➕ [면제권 추가] {interaction.user.mention} -> {멤버.mention} : +{장수}장 / 총 {new_cnt}장 / 시간: <t:{ts}:F>"
                )
        except Exception:
            pass



@bot.tree.command(name="면제권제거", description="반성문 면제권 차감(운영진)")
@is_admin()
async def exempt_remove_cmd(interaction: discord.Interaction, 멤버: discord.Member, 몇장: app_commands.Range[int, 1, 1000]):
    gid = interaction.guild_id
    if gid is None or not interaction.guild:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return

    removed, left = remove_exemptions(gid, 멤버.id, int(몇장))
    await safe_send(interaction, content=f"✅ 면제권 제거 완료: {멤버.display_name} -{removed}장 (남은 {left}장)", ephemeral=True)

    # 로그
    try:
        ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        await log_exemption_event(
            interaction.guild,
            f"➖ [면제권 제거] {interaction.user.mention} -> {멤버.mention} : -{removed}장 / 남은 {left}장 / 시간: <t:{ts}:F>"
                )
    except Exception:
        pass


@bot.tree.command(name="면제권초기화", description="반성문 면제권을 전부 초기화합니다. (운영진/관리자)")
@is_admin()
async def exempt_reset_cmd(interaction: discord.Interaction):
    gid = interaction.guild_id
    if gid is None or not interaction.guild:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return

    cleared = reset_exemptions(gid)
    try:
        append_exemption_log(guild_id=gid, action="reset", target_user_id=0, amount=0, actor_member=interaction.user)
    except Exception:
        pass

    await safe_send(interaction, content=f"✅ 면제권 초기화 완료: {cleared}명 데이터 삭제", ephemeral=True)

    # 채널 로그(선택): 기존 로거를 유지(패널 옆 로그는 JSON 기반이라 자동 반영)
    try:
        ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        await log_exemption_event(
            interaction.guild,
            f"🧹 [면제권 초기화] {interaction.user.mention} : 전체 초기화 / 시간: <t:{ts}:F>"
                )
    except Exception:
        pass


@bot.tree.command(name="면제권로그채널", description="(사용안함) 면제권 로그는 JSON 파일(exemption_pass_logs.json)에 자동 저장됩니다.")
@is_admin()
async def exempt_log_channel_cmd(interaction: discord.Interaction):
    await safe_send(interaction, content="✅ 면제권 로그는 **exemption_pass_logs.json** 파일에 자동 저장됩니다. (채널 설정 불필요)", ephemeral=True)

@bot.tree.command(name="면제권", description="내 반성문 면제권 보유량을 확인합니다.")
async def exempt_me_cmd(interaction: discord.Interaction):
    gid = interaction.guild_id
    if gid is None:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return
    cnt = get_exemption_count(gid, interaction.user.id)
    await safe_send(interaction, content=f"🧾 현재 면제권: **{cnt}장**", ephemeral=True)

@bot.tree.command(name="면제권패널", description="반성문 면제권 패널(사용 버튼 포함)을 생성합니다. (운영진/관리자)")
@is_admin()
async def exempt_panel_cmd(interaction: discord.Interaction):
    if not interaction.guild:
        await safe_send(interaction, content="서버에서만 사용 가능합니다.", ephemeral=True)
        return
    v = ExemptionPanelView()
    embed = v.build_embed(interaction.guild)
    await safe_send(interaction, content="✅ 면제권 패널을 생성했습니다. (원하면 이 메시지를 고정하세요)", ephemeral=True)
    panel_msg = await interaction.channel.send(embed=embed, view=v)
    try:
        register_exemption_panel_message(guild_id=interaction.guild.id, channel_id=int(panel_msg.channel.id), message_id=int(panel_msg.id))
    except Exception:
        pass

# =========================================================
# 실행
# =========================================================
if __name__ == "__main__":
    TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN 환경변수가 없습니다. 토큰을 환경변수로 설정하세요.")
    bot.run(TOKEN)

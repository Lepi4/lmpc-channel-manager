#!/usr/bin/env python3
"""
Channel Manager + TS Buffer — единый IPTV сервис.
Управление каналами, источниками, EPG и встроенный HLS/TS стриминг с буфером.

Запуск:
  python channel_manager.py                              # сервис
  python channel_manager.py --migrate /opt/tvlink/data  # импорт из TVLINK + выход
"""

import asyncio
import aiohttp
import logging
import sqlite3
import os
import socket
import subprocess
import re
import json
import datetime
import hashlib
import argparse
import gzip
import io
import time
from collections import Counter as _Counter, deque
from urllib.parse import quote, unquote, urljoin, urlsplit
from xml.etree import ElementTree
from aiohttp import web
from contextlib import contextmanager

__version__ = "2026.11.07.1"

# ─── CONFIG ──────────────────────────────────────────────────────────────────

CONFIG = {
    "port":            3002,
    "db_path":         "/opt/channel-manager/channels.db",
    "epg_url":         "https://epg.team/tvteam.xml.1.7",
    "auto_sync_secs":  21600,
    "log_level":       "INFO",
    # ── TSB стриминг ───────────────────────────────────────────────────────
    "buffer_seconds":       12,
    "buffer_seconds_ts":    3,
    "chunk_size":           65424,
    "bitrate_mbps":         12.0,
    "hls_connect_timeout":  5,
    "hls_read_timeout":     12,
    "hls_seg_timeout":      30,
    "hls_fallback_secs":    15,
    "ts_connect_timeout":   5,
    "ts_read_timeout":      15,
    "ts_tei_threshold":     0.02,
    "ts_cc_threshold":      0.01,
    "ts_quality_window":    5.0,
    "fallback_check_secs":  900,
    "session_idle_timeout": 30,
    "tvh_quality_poll_secs":  10,
    "tvh_quality_hard_delta": 30,
    "audio_gain_fix_enabled": False,  # Optional per-channel gain/downmix; disabled by default.
    "force_video_unify":      False,
    "max_transcodes":         3,   # GPU общий с Frigate (камеры/детекция) на том же /dev/dri/renderD128 —
                                    # запас нужен постоянно, не только пока ТВ не смотрят
}

_CFG_FILE = os.environ.get("CHANNEL_MANAGER_CONFIG", "/opt/channel-manager/config.json")

def _load_cfg():
    try:
        with open(_CFG_FILE) as f:
            CONFIG.update(json.load(f))
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Config load error: {e}")

def _save_cfg():
    try:
        os.makedirs(os.path.dirname(_CFG_FILE), exist_ok=True)
        with open(_CFG_FILE, "w") as f:
            json.dump(CONFIG, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Config save error: {e}")

_load_cfg()

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("cm")

# ─── ПОПРАВКА ГРОМКОСТИ ПО КАНАЛАМ (обычная, не-unify ссылка) ────────────────
# Таблица из /opt/channel-manager/measure_gain.py (еженедельный автозамер, см. README рядом с файлом).
# Перечитываем по mtime — замер обновляет файл без перезапуска сервиса.

_GAIN_FILE = "/opt/channel-manager/channel_gain.json"
_GAIN: dict = {}
_gain_mtime = 0.0

def _load_gain():
    global _GAIN, _gain_mtime
    try:
        st = os.stat(_GAIN_FILE)
        if st.st_mtime == _gain_mtime:
            return
        with open(_GAIN_FILE, encoding="utf-8") as f:
            data = json.load(f)
        _GAIN = data.get("channels", {})
        _gain_mtime = st.st_mtime
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning(f"channel_gain.json: {e}")

def _audio_fix_filter(ch_id: str) -> tuple[str | None, float]:
    """Поправка звука для ОБЫЧНОЙ (не-unify) ссылки. Форсируем AAC-перекодирование звука только если:
    - есть реальная поправка громкости (|gain_db|>0 — таблица уже отбросила шум <1.5дБ), или
    - канал многоканальный (5.1) — для него всегда свой даунмикс в стерео, независимо от поправки громкости:
      центр ГРОМЧЕ стандартного (ITU-R BS.775 даёт центру 0.707≈-3дБ, здесь 1.0≈0дБ — речь разборчивее на ТВ),
      LFE (саб) подмешан слегка (0.25≈-12дБ вместо полного исключения) — на маленьких ТВ-динамиках бас гудит,
      а не басит, поэтому его лучше приглушить, чем честно отдать."""
    if not CONFIG.get("audio_gain_fix_enabled", False):
        return None, 0.0
    _load_gain()
    g = _GAIN.get(ch_id.split("~", 1)[0])
    if not g:
        return None, 0.0
    gain_db = g.get("gain_db") or 0.0
    layout  = g.get("layout") or ""
    if not gain_db and not layout.startswith("5.1"):
        return None, 0.0
    if layout.startswith("5.1"):
        back = "(back)" in layout or layout.strip() == "5.1"
        sl, sr = ("BL", "BR") if back else ("SL", "SR")
        af = f"pan=stereo|FL=1.0*FL+1.0*FC+0.707*{sl}+0.25*LFE|FR=1.0*FR+1.0*FC+0.707*{sr}+0.25*LFE"
        if gain_db:
            af += f",volume={gain_db:+.1f}dB"
    else:
        af = f"volume={gain_db:+.1f}dB"
    return af, gain_db

class _MemLog(logging.Handler):
    def __init__(self, n: int = 500):
        super().__init__()
        self.records: list[str] = []
        self._n = n
    def emit(self, record: logging.LogRecord):
        self.records.append(self.format(record))
        if len(self.records) > self._n:
            self.records = self.records[-self._n:]

_memlog = _MemLog()
_memlog.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s",
                                        datefmt="%H:%M:%S"))
log.addHandler(_memlog)

# ─── SCHEMA ──────────────────────────────────────────────────────────────────

_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT    UNIQUE NOT NULL,
    src_type     TEXT    NOT NULL DEFAULT 'm3u',    -- 'm3u' | 'enigma2'
    stream_type  TEXT    NOT NULL DEFAULT 'hls',    -- 'hls' | 'ts'
    url          TEXT,
    auth_user    TEXT    NOT NULL DEFAULT '',
    auth_pass    TEXT    NOT NULL DEFAULT '',
    priority     INTEGER NOT NULL DEFAULT 50,        -- меньше = главнее
    enabled      INTEGER NOT NULL DEFAULT 1,
    refresh_secs INTEGER NOT NULL DEFAULT 21600,
    last_sync    TEXT
);

CREATE TABLE IF NOT EXISTS channels (
    id         TEXT    PRIMARY KEY,
    name       TEXT    NOT NULL,
    name_norm  TEXT    NOT NULL DEFAULT '',
    group_name TEXT    NOT NULL DEFAULT '',
    icon_url   TEXT    NOT NULL DEFAULT '',
    epg_id     TEXT    NOT NULL DEFAULT '',  -- ID канала в XMLTV источнике
    enabled    INTEGER NOT NULL DEFAULT 1,
    sort_order INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tracks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  TEXT    NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    source_id   INTEGER NOT NULL REFERENCES sources(id)  ON DELETE CASCADE,
    priority    INTEGER NOT NULL DEFAULT 500,  -- source_prio*100 + variant_prio
    stream_type TEXT    NOT NULL DEFAULT 'hls',
    url         TEXT    NOT NULL,
    group_name  TEXT    NOT NULL DEFAULT '',   -- букет/группа из источника
    enabled     INTEGER NOT NULL DEFAULT 1,    -- per-source включение трека
    UNIQUE(channel_id, source_id)
);

CREATE TABLE IF NOT EXISTS epg_channels (
    epg_id       TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    display_norm TEXT NOT NULL DEFAULT '',
    icon_url     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS source_group_filters (
    source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    group_name TEXT    NOT NULL,
    enabled    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (source_id, group_name)
);

CREATE TABLE IF NOT EXISTS source_channel_filters (
    source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    ch_norm    TEXT    NOT NULL,   -- normalize_name(channel_title) из плейлиста
    ch_title   TEXT    NOT NULL DEFAULT '',
    enabled    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (source_id, ch_norm)
);

CREATE INDEX IF NOT EXISTS idx_tracks_ch  ON tracks(channel_id);
CREATE INDEX IF NOT EXISTS idx_ch_norm    ON channels(name_norm);
CREATE INDEX IF NOT EXISTS idx_ch_epg     ON channels(epg_id);
CREATE INDEX IF NOT EXISTS idx_epgch_norm ON epg_channels(display_norm);
"""

# ─── BUILT-IN SOURCES (добавляются при первом запуске) ───────────────────────

_DEFAULT_SOURCES = []  # Configure sources in the UI; never distribute account URLs.

# ─── VARIANT PRIORITY ────────────────────────────────────────────────────────

_VARIANT_RE = re.compile(
    r'\b(4k|uhd|hdr|fhd|hd\s*50\s*fps|hd50fps|hd50|hd|sd|double|iptv)\b',
    re.IGNORECASE,
)
# суффиксы-мусор: IPTV_1, _2 — только с подчёркиванием, чтобы "Матч! Футбол 1" не терял "1"
_SUFFIX_RE = re.compile(r'_+\d+\s*$')

# Замена латинских lookalike символов на кириллические — только в словах со смешанными символами
_LAT_TO_CYR = str.maketrans(
    'ABCEHKMOPTXabcehkmoptx',
    'АВСЕНКМОРТХавсенкмортх',
)
_CYR_RE = re.compile(r'[а-яёА-ЯЁ]')
_LAT_RE = re.compile(r'[a-zA-Z]')

def _fix_homoglyphs(text: str) -> str:
    """В словах с mixed кирилл+латин заменяет lookalike латиницу на кириллицу.
    Разбивает по разделителям чтобы не трогать чисто-латинские аббревиатуры (KBC, VHS)."""
    parts = re.split(r'(\W+)', text)
    result = []
    for part in parts:
        if part and _CYR_RE.search(part) and _LAT_RE.search(part):
            part = part.translate(_LAT_TO_CYR)
        result.append(part)
    return ''.join(result)
_VARIANT_PRIO: dict[str, int] = {
    "4k": 1, "uhd": 1,
    "hdr": 2,
    "fhd": 3,
    "hd50fps": 4, "hd 50fps": 4, "hd50": 4,
    "hd": 5,
    # 6 = без суффикса (default)
    "sd": 7,
    "double": 8,
}

def variant_priority(name: str) -> int:
    m = _VARIANT_RE.search(name)
    if not m:
        return 6
    key = re.sub(r'\s+', ' ', m.group(1).lower())
    return _VARIANT_PRIO.get(key, 6)

def normalize_name(name: str) -> str:
    """Убирает суффиксы качества, сохраняет +N временные сдвиги."""
    n = name.lower().strip()
    n = _fix_homoglyphs(n)   # исправляем смешанную раскладку (HТB → нтв)
    # сначала числовой хвост, потом quality-слова (иначе iptv_1 -> iptv остаётся)
    n = _SUFFIX_RE.sub('', n)
    n = _VARIANT_RE.sub('', n)
    # пробел между буквой и цифрой: тв3 → тв 3, тв2 → тв 2
    n = re.sub(r'([а-яёa-z])(\d)', r'\1 \2', n)
    n = re.sub(r'(\d)([а-яёa-z])', r'\1 \2', n)
    n = re.sub(r'[^\w\s+]', ' ', n)
    n = re.sub(r'\s+', ' ', n).strip()
    # убираем хвостовые служебные слова (часто различаются у провайдеров)
    n = re.sub(r'\s+(канал|channel)$', '', n).strip()
    return n

def track_priority(source_priority: int, channel_name: str) -> int:
    """Источник — первичный критерий, вариант качества — вторичный."""
    return source_priority * 100 + variant_priority(channel_name)

def _stable_id(seed: str) -> str:
    return hashlib.sha1(seed.encode()).hexdigest()[:16].upper()

def _s(v) -> str:
    return v.decode("utf-8", errors="replace") if isinstance(v, bytes) else (str(v) if v else "")

# ─── DB ──────────────────────────────────────────────────────────────────────

def open_db(path: str | None = None) -> sqlite3.Connection:
    p = path or CONFIG["db_path"]
    os.makedirs(os.path.dirname(p), exist_ok=True)
    con = sqlite3.connect(p, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.executescript(_SCHEMA)
    return con

def _first_run_setup(db_path: str | None = None):
    """Создаёт источники по умолчанию если DB пустая. Применяет колонки-миграции."""
    db = open_db(db_path)
    # Миграция: добавляем колонки если их нет
    # profile: '' = отдавать как есть; 'gpu_h264' = видео перекодировать на GPU (H.264 progressive)
    for col, dflt in [("auth_user","''"), ("auth_pass","''"), ("profile","''")]:
        try:
            db.execute(f"ALTER TABLE sources ADD COLUMN {col} TEXT NOT NULL DEFAULT {dflt}")
            db.commit()
        except Exception:
            pass
    try:
        db.execute("ALTER TABLE tracks ADD COLUMN group_name TEXT NOT NULL DEFAULT ''")
        db.commit()
    except Exception:
        pass
    try:
        # исторический маркер уровня канала — заменён профилем дорожки (ниже); колонка остаётся
        # для обратной совместимости БД, но в логике перекодирования больше не участвует
        db.execute("ALTER TABLE channels ADD COLUMN profile TEXT NOT NULL DEFAULT ''")
        db.commit()
    except Exception:
        pass
    try:
        # маркер перекодирования КОНКРЕТНОЙ дорожки (канал+источник): '' = как у источника,
        # 'gpu_h264' = перекодировать. Точнее channels.profile — канал может иметь несколько
        # источников, и перекодирование обычно нужно не для всех сразу (напр. только для tvteam,
        # если поток именно этого источника помечен чересстрочным).
        db.execute("ALTER TABLE tracks ADD COLUMN profile TEXT NOT NULL DEFAULT ''")
        db.commit()
        # одноразовая миграция: перенести уже выставленные channels.profile на их tvteam-дорожку (source_id=2),
        # для которой этот маркер и ставился в веб-интерфейсе до появления профиля дорожки
        rows = db.execute("SELECT id FROM channels WHERE profile='gpu_h264'").fetchall()
        moved = 0
        for r in rows:
            cur = db.execute("UPDATE tracks SET profile='gpu_h264' WHERE channel_id=? AND source_id=2",
                             (r["id"],))
            if cur.rowcount:
                moved += 1
        if moved:
            db.execute("UPDATE channels SET profile=''")
            db.commit()
            log.info(f"миграция: профиль {moved} каналов перенесён с уровня канала на дорожку tvteam")
    except Exception:
        pass
    try:
        # 1 = URL правили вручную (h_track_patch) — синк источника его не перезаписывает
        db.execute("ALTER TABLE tracks ADD COLUMN url_manual INTEGER NOT NULL DEFAULT 0")
        db.commit()
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE tracks ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
        db.commit()
        # Синхронизируем tracks.enabled со значениями source_group_filters (один раз при миграции)
        filters = db.execute("SELECT source_id, group_name, enabled FROM source_group_filters").fetchall()
        for f in filters:
            db.execute(
                "UPDATE tracks SET enabled=? WHERE source_id=? AND group_name=?",
                (f["enabled"], f["source_id"], f["group_name"]),
            )
        db.commit()
    except Exception:
        pass
    count = db.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
    if count == 0:
        for name, src_type, stream_type, url, priority in _DEFAULT_SOURCES:
            db.execute(
                "INSERT OR IGNORE INTO sources (name,src_type,stream_type,url,priority) "
                "VALUES (?,?,?,?,?)",
                (name, src_type, stream_type, url, priority),
            )
        db.commit()
        log.info("Первый запуск: созданы источники по умолчанию")
    db.close()

# ─── EPG CACHE ───────────────────────────────────────────────────────────────

def _parse_xmltv_dt(s: str) -> float:
    """'20240101120000 +0300' → UTC timestamp."""
    s = s.strip()
    tz_offset = 0
    if " " in s:
        dt_part, tz_part = s.rsplit(" ", 1)
        sign = 1 if tz_part.startswith("+") else -1
        tz_part = tz_part.lstrip("+-")
        if len(tz_part) >= 4:
            tz_offset = sign * (int(tz_part[:2]) * 3600 + int(tz_part[2:4]) * 60)
    else:
        dt_part = s
    import calendar
    dt = datetime.datetime.strptime(dt_part[:14], "%Y%m%d%H%M%S")
    return calendar.timegm(dt.timetuple()) - tz_offset

class EpgCache:
    """
    Скачивает XMLTV с epg_url, парсит потоково (iterparse),
    кэширует отфильтрованный XML и компактный prog_cache.
    _xml никогда не хранится в памяти дольше одного обновления.
    """
    def __init__(self):
        self._xml_filtered: bytes | None = None
        # {epg_id: [(start_ts, stop_ts, title), ...]} — только ±48ч
        self._prog_cache:   dict[str, list[tuple[float, float, str]]] = {}
        self._fetched_at:   float = 0.0
        self._fail_at:      float = 0.0
        self.last_error:    str = ""
        self._lock = asyncio.Lock()
        self._refreshing = False
        self._refresh_done = asyncio.Event()
        self._refresh_done.set()

    def invalidate_filtered(self):
        self._xml_filtered = None

    async def _ensure_fresh(self, db_path: str | None, force: bool = False):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            age = now - self._fetched_at
            if not force and self._xml_filtered is not None and age < 3600:
                return
            # Сбойный источник не опрашиваем с каждым открытием плейлиста: прежний кэш остаётся.
            if not force and self._fail_at and now - self._fail_at < 120:
                return
            if self._refreshing:
                wait_ev = self._refresh_done
                need_wait = True
            else:
                self._refreshing = True
                self._refresh_done.clear()
                need_wait = False
        if need_wait:
            await wait_ev.wait()
            return
        try:
            raw = await self._fetch(db_path)
            if not raw or b"<channel" not in raw:
                self.last_error = self.last_error or "пустой ответ EPG"
                self._fail_at = asyncio.get_event_loop().time()
                log.warning("EPG: пустой ответ, прежний кэш и база сохранены")
                return
            loop = asyncio.get_event_loop()
            filtered, prog_cache, ch_count = await loop.run_in_executor(
                None, self._parse_and_filter, raw, db_path
            )
            del raw  # освобождаем сразу
            if ch_count <= 0 or filtered is None:
                self.last_error = self.last_error or "в EPG нет каналов"
                self._fail_at = asyncio.get_event_loop().time()
                log.warning("EPG: разбор не дал каналов, прежний кэш сохранён")
                return
            log.info(f"EPG channels: {ch_count} записей, prog_cache: {sum(len(v) for v in prog_cache.values())} программ")
            async with self._lock:
                self._xml_filtered = filtered
                self._prog_cache   = prog_cache
                self._fetched_at   = asyncio.get_event_loop().time()
                self._fail_at = 0.0
                self.last_error = ""
        except Exception as e:
            self.last_error = type(e).__name__
            self._fail_at = asyncio.get_event_loop().time()
            log.warning(f"EPG refresh error: {e}")
        finally:
            async with self._lock:
                self._refreshing = False
            self._refresh_done.set()

    async def refresh_epg_channels(self, db_path: str | None = None) -> int:
        await self._ensure_fresh(db_path, force=True)
        return sum(len(v) for v in self._prog_cache.values())

    async def get_filtered(self, db_path: str) -> bytes:
        await self._ensure_fresh(db_path)
        return self._xml_filtered or b"<tv></tv>"

    @staticmethod
    def _parse_and_filter(raw: bytes, db_path: str) -> tuple[bytes, dict, int]:
        """Потоковый парсинг XML: заполняет epg_channels DB, строит prog_cache и filtered XML."""
        import io
        db = open_db(db_path)
        our_ids = {r[0] for r in db.execute(
            "SELECT DISTINCT epg_id FROM channels WHERE epg_id != '' AND enabled=1"
        ).fetchall()}

        now = time.time()
        prog_cutoff_start = now - 7200       # 2 часа назад
        prog_cutoff_end   = now + 48 * 3600  # 48 часов вперёд

        ch_rows:   list[tuple] = []
        prog_cache: dict[str, list[tuple[float, float, str]]] = {}
        filtered_parts = ['<?xml version="1.0" encoding="utf-8"?>\n<tv>']

        try:
            context = ElementTree.iterparse(io.BytesIO(raw), events=("start", "end"))
            cur_ch   = None
            cur_prog = None
            in_prog_title = False
            prog_title_buf = []
            ch_count = 0

            for event, elem in context:
                tag = elem.tag

                if event == "start":
                    if tag == "channel":
                        cur_ch = {"id": elem.get("id", ""), "dn": "", "ico": ""}
                    elif tag == "programme" and cur_prog is None:
                        cur_prog = {"ch": elem.get("channel",""), "start": elem.get("start",""), "stop": elem.get("stop","")}
                        in_prog_title = False
                        prog_title_buf = []
                    elif tag == "title" and cur_prog is not None:
                        in_prog_title = True
                        prog_title_buf = []
                    elif tag == "display-name" and cur_ch is not None:
                        pass
                    elif tag == "icon" and cur_ch is not None:
                        cur_ch["ico"] = elem.get("src", "")

                elif event == "end":
                    if tag == "display-name" and cur_ch is not None and not cur_ch["dn"]:
                        cur_ch["dn"] = (elem.text or "").strip()
                    elif tag == "title" and in_prog_title:
                        prog_title_buf.append(elem.text or "")
                        in_prog_title = False
                    elif tag == "channel" and cur_ch is not None:
                        eid = cur_ch["id"]
                        dn  = cur_ch["dn"]
                        ico = cur_ch["ico"]
                        if eid and dn:
                            ch_rows.append((eid, dn, normalize_name(dn), ico))
                            ch_count += 1
                        if eid in our_ids and dn:
                            filtered_parts.append(ElementTree.tostring(elem, encoding="unicode"))
                        cur_ch = None
                        elem.clear()
                    elif tag == "programme" and cur_prog is not None:
                        ch_id = cur_prog["ch"]
                        if ch_id in our_ids:
                            filtered_parts.append(ElementTree.tostring(elem, encoding="unicode"))
                            try:
                                ts = _parse_xmltv_dt(cur_prog["start"])
                                te = _parse_xmltv_dt(cur_prog["stop"])
                                if te > prog_cutoff_start and ts < prog_cutoff_end:
                                    title = "".join(prog_title_buf).strip()
                                    prog_cache.setdefault(ch_id, []).append((ts, te, title))
                            except Exception:
                                pass
                        cur_prog = None
                        elem.clear()

        except Exception as e:
            log.warning(f"EPG iterparse error: {e}")

        # Пустой разбор не стирает уже скачанную программу: обрыв или битый XML иначе
        # оставляли базу пустой до следующего успешного часа.
        if not ch_rows:
            log.warning("EPG: разбор без каналов, база программы не затронута")
            db.close()
            return None, {}, 0
        try:
            db.execute("DELETE FROM epg_channels")
            db.executemany(
                "INSERT OR REPLACE INTO epg_channels (epg_id,display_name,display_norm,icon_url) VALUES (?,?,?,?)",
                ch_rows
            )
            db.execute(
                """UPDATE channels SET icon_url = (
                       SELECT e.icon_url FROM epg_channels e
                       WHERE e.epg_id = channels.epg_id AND e.icon_url != ''
                   )
                   WHERE icon_url = '' AND epg_id != ''
                     AND EXISTS (
                       SELECT 1 FROM epg_channels e
                       WHERE e.epg_id = channels.epg_id AND e.icon_url != ''
                     )"""
            )
            db.commit()
        except Exception as e:
            log.warning(f"EPG db write error: {e}")
        db.close()

        filtered_parts.append("</tv>")
        filtered = "".join(filtered_parts).encode("utf-8")
        log.info(f"EPG filtered: {len(filtered)//1024} КБ (из {len(raw)//1024} КБ raw)")
        return filtered, prog_cache, ch_count

    async def _fetch(self, db_path: str | None) -> bytes:
        epg_url = CONFIG.get("epg_url", "https://epg.team/1.1.xml")
        log.info(f"EPG: загружаем {epg_url}")
        try:
            tc = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(timeout=tc) as sess:
                async with sess.get(epg_url) as r:
                    raw = await r.read()
            if raw[:2] == b'\x1f\x8b':
                raw = gzip.decompress(raw)
            log.info(f"EPG: получено {len(raw)//1024} КБ")
            return raw
        except Exception as e:
            log.warning(f"EPG fetch error: {e}")
            self.last_error = type(e).__name__
            return None

    def get_current_program(self, epg_id: str) -> dict | None:
        """Возвращает {title, start, stop} текущей программы из кэша."""
        if not epg_id:
            return None
        progs = self._prog_cache.get(epg_id)
        if not progs:
            return None
        now_ts = time.time()
        for ts, te, title in progs:
            if ts <= now_ts < te:
                fmt = lambda t: datetime.datetime.fromtimestamp(t).strftime("%H:%M")
                return {"title": title, "start": fmt(ts), "stop": fmt(te)}
        return None

_epg_cache = EpgCache()

# ─── MIGRATION FROM TVLINK ───────────────────────────────────────────────────

_TVLINK_TABLES: dict[str, tuple[str, int]] = {
    "NTVHD":        ("ts",  1),
    "tvteamonline": ("hls", 2),
    "NTVSD":        ("ts",  10),
}


def migrate_from_tvlink(tvlink_path: str, db_path: str | None = None) -> tuple[int, int]:
    """
    Вариант Б: каналы с одинаковым normalize_name() сливаются в один.
    Canonical ID = chID версии с наилучшим variant_priority.
    Также мигрирует epg_id если он есть в channels.db.
    """
    ch_db  = os.path.join(tvlink_path, "channels.db")
    set_db = os.path.join(tvlink_path, "setup.db")
    dst = open_db(db_path)

    # 1. Создаём источники
    src_ids: dict[str, int] = {}
    for tbl, (stream_type, prio) in _TVLINK_TABLES.items():
        src_type = "enigma2" if stream_type == "ts" else "m3u"
        dst.execute(
            "INSERT OR IGNORE INTO sources (name,src_type,stream_type,priority) VALUES (?,?,?,?)",
            (tbl, src_type, stream_type, prio),
        )
        dst.commit()
        row = dst.execute("SELECT id FROM sources WHERE name=?", (tbl,)).fetchone()
        if row:
            src_ids[tbl] = row["id"]

    # 2. URL из source tables
    by_bind: dict[str, list[tuple]] = {}
    try:
        with sqlite3.connect(set_db) as src:
            for tbl, (stream_type, prio) in _TVLINK_TABLES.items():
                try:
                    for row in src.execute(f"SELECT chID, chUrl FROM {tbl}"):
                        bid, url = _s(row[0]), _s(row[1])
                        if bid and url:
                            by_bind.setdefault(bid, []).append((tbl, stream_type, prio, url))
                except Exception:
                    pass
    except Exception as e:
        log.warning(f"migrate: source tables: {e}")

    # 3. Каналы из channels.db — проверяем есть ли epg-колонки
    epg_col = False
    try:
        with sqlite3.connect(ch_db) as src:
            cols = [r[1] for r in src.execute("PRAGMA table_info(channels)").fetchall()]
            epg_col = any(c.lower() in ("chepg", "epgid", "epg_id", "tvgid") for c in cols)
    except Exception:
        pass

    by_norm: dict[str, list[tuple]] = {}
    try:
        with sqlite3.connect(ch_db) as src:
            for row in src.execute("SELECT chID, chTitle, chBinds FROM channels"):
                ch_id, title, binds_raw = _s(row[0]), _s(row[1]), _s(row[2])
                if not ch_id or not binds_raw:
                    continue
                norm = normalize_name(title)
                if not norm:
                    continue
                tracks = []
                for bid in [b.strip() for b in binds_raw.split(",") if b.strip()]:
                    tracks.extend(by_bind.get(bid, []))
                by_norm.setdefault(norm, []).append(
                    (ch_id, title, variant_priority(title), tracks)
                )
    except Exception as e:
        log.warning(f"migrate: channels.db: {e}")

    # 4. Слипание → canonical
    ch_count = tr_count = 0
    for norm, group in by_norm.items():
        group.sort(key=lambda x: x[2])
        canonical_id, canonical_title = group[0][0], group[0][1]

        dst.execute(
            "INSERT OR IGNORE INTO channels (id,name,name_norm) VALUES (?,?,?)",
            (canonical_id, canonical_title, norm),
        )
        ch_count += 1

        for _cid, ch_title, _vp, tracks in group:
            for (tbl, stream_type, src_prio, url) in tracks:
                sid = src_ids.get(tbl)
                if not sid:
                    continue
                prio = track_priority(src_prio, ch_title)
                dst.execute(
                    "INSERT OR IGNORE INTO tracks "
                    "(channel_id,source_id,priority,stream_type,url) VALUES (?,?,?,?,?)",
                    (canonical_id, sid, prio, stream_type, url),
                )
                tr_count += 1

    dst.commit()
    dst.close()
    log.info(f"Миграция TVLINK: {ch_count} каналов, {tr_count} треков")
    return ch_count, tr_count

# ─── ENIGMA2 SYNC ────────────────────────────────────────────────────────────

def _is_playable_ref(sref: str) -> bool:
    if not sref or "FROM BOUQUET" in sref or "FROM FILE" in sref:
        return False
    parts = sref.split(":")
    return len(parts) >= 2 and parts[1] == "0"

async def _e2_fetch(sess: aiohttp.ClientSession, url: str) -> ElementTree.Element | None:
    for attempt in range(3):
        try:
            async with sess.get(url) as r:
                return ElementTree.fromstring(await r.text())
        except Exception as e:
            if attempt == 2:
                log.warning(f"E2 {url}: {e}")
            await asyncio.sleep(0.3 * (attempt + 1))
    return None

async def sync_enigma2(source_id: int, host: str, source_priority: int,
                        db_path: str | None = None,
                        auth_user: str = "", auth_pass: str = "") -> tuple[int, int]:
    if not host:
        log.warning(f"Enigma2 source#{source_id}: хост не задан — пропускаем")
        return 0, 0
    base = f"http://{host}"
    tc   = aiohttp.ClientTimeout(total=20)
    entries: list[tuple[str, str, str]] = []

    auth = aiohttp.BasicAuth(auth_user, auth_pass) if auth_user else None
    async with aiohttp.ClientSession(timeout=tc, auth=auth) as sess:
        # iptv-aggregator approach: bouquets.tv master list → fallback to no-sRef
        _bq_sref = '1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "bouquets.tv" ORDER BY bouquet'
        root = await _e2_fetch(sess, f"{base}/web/getservices?sRef={quote(_bq_sref, safe='')}")
        if root is None:
            root = await _e2_fetch(sess, f"{base}/web/getservices")
        if root is None:
            return 0, 0
        bouquets = [
            (svc.findtext("e2servicereference", "").strip(),
             svc.findtext("e2servicename", "").strip())
            for svc in root.findall(".//e2service")
            if not _is_playable_ref(svc.findtext("e2servicereference", "").strip())
               and svc.findtext("e2servicereference", "").strip()
        ]
        log.info(f"Enigma2 {host}: {len(bouquets)} букетов")
        # entries_with_group: [(epg_id, name, url, group_name)]
        entries_with_group: list[tuple[str, str, str, str]] = []
        for b_ref, b_name in bouquets:
            if not b_ref:
                continue
            await asyncio.sleep(0.05)
            tree = await _e2_fetch(sess, f"{base}/web/getservices?sRef={quote(b_ref, safe='')}")
            if tree is None:
                continue
            for svc in tree.findall(".//e2service"):
                sref = svc.findtext("e2servicereference", "").strip()
                name = svc.findtext("e2servicename", "").strip()
                if sref and name and _is_playable_ref(sref):
                    entries_with_group.append(
                        (sref, name, f"http://{host}:8001/{quote(sref, safe='')}", b_name)
                    )

    log.info(f"Enigma2 {host}: {len(entries_with_group)} каналов")

    # Сортируем: меньший букет = специфичнее → побеждает при дедупликации.
    # Это гарантирует что НТВ+hd (41) обгоняет Last Scanned (300) и Триколор (213).
    _bq_size = _Counter(e[3] for e in entries_with_group)
    entries_with_group.sort(key=lambda e: (_bq_size[e[3]], e[3]))

    seen_sref: set[str] = set()
    deduped: list[tuple[str, str, str, str]] = []
    for sref, name, url, b_name in entries_with_group:
        if sref in seen_sref:
            continue
        seen_sref.add(sref)
        deduped.append((sref, name, url, b_name))
    log.info(f"Enigma2 {host}: {len(deduped)} уникальных каналов (было {len(entries_with_group)})")

    added_ch, upd_tr = await _apply_sync(source_id, source_priority, "ts", deduped, db_path)
    return added_ch, upd_tr

# ─── TVHEADEND SYNC ──────────────────────────────────────────────────────────

async def sync_tvheadend(source_id: int, host: str, source_priority: int,
                          db_path: str | None = None,
                          auth_user: str = "", auth_pass: str = "",
                          mode: str = "hd") -> tuple[int, int]:
    """host — адрес с портом, напр. 'tvheadend.local:9981'. Раздаёт сырой MPEG-TS по HTTP
    (как Энигма), но требует авторизацию — встраиваем прямо в URL трека (digest на сервере
    отключён, basic-через-URL работает). Деление HD/SD — по РЕАЛЬНЫМ тегам TVHeadend
    (HDTV/SDTV, расставляются автоматически при сканировании по флагу из вещания, надёжнее
    эвристики по имени) — аналог того, как у Энигмы HD/SD были разными букетами на приёмнике.
    Искать тег по имени, а не хардкодить uuid — переживает пересоздание тегов."""
    if not host:
        log.warning(f"TVHeadend source#{source_id}: хост не задан — пропускаем")
        return 0, 0
    base = f"http://{host}"
    tc = aiohttp.ClientTimeout(total=30)
    auth = aiohttp.BasicAuth(auth_user, auth_pass) if auth_user else None
    tag_name = "HDTV" if mode == "hd" else "SDTV"
    try:
        async with aiohttp.ClientSession(timeout=tc, auth=auth) as sess:
            async with sess.get(f"{base}/api/channeltag/grid?limit=999") as r:
                if r.status != 200:
                    log.warning(f"TVHeadend {host}: HTTP {r.status} (channeltag)")
                    return 0, 0
                tags_data = await r.json(content_type=None)
            tag_uuid = next((t["uuid"] for t in tags_data.get("entries", [])
                              if t.get("name") == tag_name), None)
            if not tag_uuid:
                log.warning(f"TVHeadend {host}: тег {tag_name} не найден")
                return 0, 0
            async with sess.get(f"{base}/api/channel/grid?limit=9999") as r:
                if r.status != 200:
                    log.warning(f"TVHeadend {host}: HTTP {r.status}")
                    return 0, 0
                data = await r.json(content_type=None)
    except Exception as e:
        log.warning(f"TVHeadend {host}: {e}")
        return 0, 0

    cred = f"{auth_user}:{quote(auth_pass, safe='')}@" if auth_user else ""
    grp = f"TVHeadend {mode.upper()}"
    entries: list[tuple[str, str, str, str]] = []
    for ch in data.get("entries", []):
        name = (ch.get("name") or "").strip()
        uid = ch.get("uuid")
        if not name or not uid or not ch.get("enabled", True):
            continue
        if tag_uuid not in (ch.get("tags") or []):
            continue
        url = f"http://{cred}{host}/stream/channel/{uid}"
        entries.append(("", name, url, grp))

    log.info(f"TVHeadend {host} [{mode}]: {len(entries)} каналов")
    added_ch, upd_tr = await _apply_sync(source_id, source_priority, "ts", entries, db_path, update_equal=True)
    return added_ch, upd_tr

# ─── M3U SYNC ────────────────────────────────────────────────────────────────

def _parse_m3u(text: str) -> list[dict]:
    result: list[dict] = []
    meta: dict = {}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            meta = {}
            for k, v in re.findall(r'(tvg-id|tvg-name|group-title|tvg-logo)="([^"]*)"', line):
                meta[k] = v
            if "," in line:
                meta["_d"] = line.split(",", 1)[1].strip()
        elif line.startswith("#EXTGRP:") and meta is not None:
            # #EXTGRP:groupname — альтернативный формат группы (tvteamonline)
            if not meta.get("group-title"):
                meta["group-title"] = line[8:].strip()
        elif line and not line.startswith("#") and meta:
            result.append({
                "epg_id": meta.get("tvg-id", ""),
                "name":   meta.get("tvg-name") or meta.get("_d", ""),
                "group":  meta.get("group-title", ""),
                "icon":   meta.get("tvg-logo", ""),
                "url":    line,
            })
            meta = {}
    return result

async def sync_m3u(source_id: int, url: str, stream_type: str,
                   source_priority: int, db_path: str | None = None) -> tuple[int, int]:
    if not url:
        log.warning(f"M3U source#{source_id}: URL не задан — пропускаем")
        return 0, 0
    tc = aiohttp.ClientTimeout(total=60)
    try:
        async with aiohttp.ClientSession(timeout=tc) as sess:
            async with sess.get(url) as r:
                if r.status != 200:
                    log.warning(f"M3U source#{source_id}: HTTP {r.status}")
                    return 0, 0
                text = await r.text()
    except Exception as e:
        log.warning(f"M3U source#{source_id}: {e}")
        return 0, 0

    parsed = _parse_m3u(text)
    if not parsed:
        log.warning(f"M3U source#{source_id}: пустой плейлист")
        return 0, 0

    # source_group_filters здесь не отсекают записи: каналы выключенных групп тоже попадают в БД
    # (чтобы были видны в настройках), а tracks.enabled=0 выставляет _apply_sync в конце.
    entries = [(e["epg_id"], e["name"], e["url"], e.get("group","")) for e in parsed if e["name"] and e["url"]]
    added_ch, upd_tr = await _apply_sync(source_id, source_priority, stream_type, entries, db_path)

    # Обновляем icon_url на channels (только если ещё не заполнено)
    db = open_db(db_path)
    by_epg  = {e["epg_id"]: e for e in parsed if e["epg_id"]}
    by_norm = {normalize_name(e["name"]): e for e in parsed if e["name"]}
    for row in db.execute("SELECT id, epg_id, name_norm FROM channels"):
        meta = by_epg.get(row["epg_id"]) or by_norm.get(row["name_norm"])
        if not meta:
            continue
        db.execute(
            "UPDATE channels SET "
            "icon_url = CASE WHEN icon_url='' THEN ? ELSE icon_url END "
            "WHERE id=?",
            (meta.get("icon",""), row["id"]),
        )
    db.commit()
    db.close()
    return added_ch, upd_tr

# ─── APPLY SYNC ──────────────────────────────────────────────────────────────

async def _apply_sync(source_id: int, source_priority: int, stream_type: str,
                       entries: list[tuple[str, str, str, str]],
                       db_path: str | None, update_equal: bool = False) -> tuple[int, int]:
    """entries = [(epg_id, name, url, group_name), ...]"""
    db = open_db(db_path)

    by_epg:  dict[str, str] = {}
    by_norm: dict[str, str] = {}
    _epg_dup: set[str] = set()
    for row in db.execute("SELECT id, epg_id, name_norm FROM channels"):
        if row["epg_id"]:
            if row["epg_id"] in by_epg:
                _epg_dup.add(row["epg_id"])
            by_epg[row["epg_id"]] = row["id"]
        if row["name_norm"]:
            by_norm[row["name_norm"]] = row["id"]
    # epg_id, общий у нескольких каналов (напр. Velilla World Cup/EuroCup), однозначно канал не задаёт:
    # иначе запись плейлиста попадает на случайный из них и трек нужного канала не обновляется
    for _e in _epg_dup:
        by_epg.pop(_e, None)

    added_ch = upd_tr = 0
    now = datetime.datetime.now().isoformat(timespec="seconds")
    # Треки, которые уже были у источника: их enabled не трогаем (ручные галочки каналов)
    existing_tr = {r["channel_id"] for r in
                   db.execute("SELECT channel_id FROM tracks WHERE source_id=?", (source_id,))}
    new_tr: set[str] = set()

    for epg_id, name, url, grp in entries:
        if not name or not url:
            continue
        norm = normalize_name(name)
        ch_id = (by_epg.get(epg_id) if epg_id else None) or by_norm.get(norm)
        if not ch_id:
            ch_id = _stable_id(f"src{source_id}:{name}")
            db.execute(
                "INSERT OR IGNORE INTO channels (id,name,name_norm,epg_id,enabled) VALUES (?,?,?,?,0)",
                (ch_id, name, norm, epg_id),
            )
            by_norm[norm] = ch_id
            if epg_id:
                by_epg[epg_id] = ch_id
            added_ch += 1
        else:
            if epg_id:
                db.execute("UPDATE channels SET epg_id=? WHERE id=? AND epg_id=''",
                           (epg_id, ch_id))
            # Обновляем имя если текущее хуже: имеет IPTV_N суффикс, а новое — нет
            cur_name_row = db.execute("SELECT name FROM channels WHERE id=?", (ch_id,)).fetchone()
            if cur_name_row:
                cur = cur_name_row["name"]
                cur_is_bad = bool(re.search(r'\bIPTV_\d+\b', cur, re.IGNORECASE))
                new_is_clean = not re.search(r'\bIPTV_\d+\b', name, re.IGNORECASE)
                if cur_is_bad and new_is_clean:
                    db.execute("UPDATE channels SET name=?, name_norm=? WHERE id=?",
                               (name, norm, ch_id))

        if ch_id not in existing_tr:
            new_tr.add(ch_id)
        prio = track_priority(source_priority, name)
        # group_name и url меняются вместе — нельзя чтобы group_name была из одного бука, а url из другого
        # При равном приоритете URL обновляем только у M3U-источников (сменили сервер/токен в плейлисте).
        # У Энигмы нельзя: несколько сервисов с одним именем (Триколор/НТВ+) дали бы «победу последнего»
        # и подмену правильного (в т.ч. исправленного вручную) адреса на другой — как было с «Первым каналом».
        eq = 1 if stream_type != "ts" or update_equal else 0
        win = (f"(excluded.priority < tracks.priority OR "
               f"(excluded.priority = tracks.priority AND tracks.url_manual=0 AND {eq}=1))")
        db.execute(f"""
            INSERT INTO tracks (channel_id,source_id,priority,stream_type,url,group_name)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(channel_id,source_id) DO UPDATE SET
                priority   = MIN(tracks.priority, excluded.priority),
                stream_type= CASE WHEN {win} THEN excluded.stream_type ELSE tracks.stream_type END,
                url        = CASE WHEN {win} THEN excluded.url ELSE tracks.url END,
                group_name = CASE WHEN {win} THEN excluded.group_name
                                  WHEN tracks.group_name='' THEN excluded.group_name
                                  ELSE tracks.group_name END
        """, (ch_id, source_id, prio, stream_type, url, grp))
        upd_tr += 1

    db.execute("UPDATE sources SET last_sync=? WHERE id=?", (now, source_id))
    # source_group_filters применяем только к НОВЫМ трекам. Существующие не перезаписываем:
    # иначе синк сбрасывает галочки отдельных каналов, выставленные вручную внутри группы.
    # Групповые переключатели применяются сразу в h_source_groups_patch.
    flt = {r["group_name"]: r["enabled"] for r in db.execute(
        "SELECT group_name, enabled FROM source_group_filters WHERE source_id=?", (source_id,)
    )}
    for ch_id in new_tr:
        row = db.execute("SELECT group_name FROM tracks WHERE source_id=? AND channel_id=?",
                         (source_id, ch_id)).fetchone()
        if row and row["group_name"] in flt:
            db.execute("UPDATE tracks SET enabled=? WHERE source_id=? AND channel_id=?",
                       (flt[row["group_name"]], source_id, ch_id))
    db.commit()
    db.close()
    log.info(f"Sync source#{source_id}: +{added_ch} каналов, {upd_tr} треков")
    return added_ch, upd_tr

# ─── SYNC ALL ────────────────────────────────────────────────────────────────

async def sync_all_sources(db_path: str | None = None) -> dict:
    """Пересинхронизирует все включённые источники."""
    db = open_db(db_path)
    sources = db.execute(
        "SELECT * FROM sources WHERE enabled=1 ORDER BY priority"
    ).fetchall()
    db.close()

    total_ch = total_tr = 0
    results = []
    for src in sources:
        src = dict(src)
        try:
            if src["src_type"] == "enigma2":
                ch, tr = await sync_enigma2(src["id"], src["url"] or "",
                                             src["priority"], db_path,
                                             src.get("auth_user",""), src.get("auth_pass",""))
            elif src["src_type"] == "tvheadend":
                _prof = (src.get("profile") or "").lower()
                _mode = _prof if _prof in ("hd", "sd") else ("sd" if re.search(r"(^|[^A-Za-z])SD([^A-Za-z]|$)", src["name"]) else "hd")
                ch, tr = await sync_tvheadend(src["id"], src["url"] or "",
                                               src["priority"], db_path,
                                               src.get("auth_user",""), src.get("auth_pass",""),
                                               _mode)
            else:
                ch, tr = await sync_m3u(src["id"], src["url"] or "",
                                        src["stream_type"], src["priority"], db_path)
            total_ch += ch
            total_tr += tr
            results.append({"source": src["name"], "ok": True, "channels": ch, "tracks": tr})
        except Exception as e:
            log.exception(f"sync_all source {src['name']}")
            results.append({"source": src["name"], "ok": False, "error": str(e)})

    # Обновляем EPG: скачиваем и перепарсиваем epg_channels
    await _epg_cache.refresh_epg_channels(db_path)

    log.info(f"sync_all: +{total_ch} каналов, {total_tr} треков")
    await _notify_sync_done()
    return {"total_channels": total_ch, "total_tracks": total_tr, "sources": results}

# TS-буфер держит кеш источников (CmChannelDB); после синка URL могли смениться (сервер/токен) —
# без refresh потоки продолжают ходить по старым адресам до часового обновления.
_on_sync_done = None

async def _notify_sync_done():
    if _on_sync_done:
        try:
            await _on_sync_done()
        except Exception:
            log.exception("on_sync_done")

async def _auto_sync_loop(db_path: str | None = None):
    interval = int(CONFIG.get("auto_sync_secs", 21600))
    if not interval:
        return
    log.info(f"Авто-синхр: каждые {interval//3600}ч")
    while True:
        await asyncio.sleep(interval)
        log.info("Авто-синхр: запуск...")
        await sync_all_sources(db_path)

async def _url_watch_once(db_path: str | None = None):
    """Если плейлист M3U-источника отдаёт хост, которого нет в его треках (сменили сервер) — синк сразу."""
    from urllib.parse import urlsplit
    db = open_db(db_path)
    srcs = [dict(r) for r in db.execute(
        "SELECT id,name,url,stream_type,priority FROM sources "
        "WHERE enabled=1 AND src_type NOT IN ('enigma2','tvheadend') AND url!=''")]
    db.close()
    for s in srcs:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as sess:
                async with sess.get(s["url"]) as r:
                    if r.status != 200:
                        continue
                    text = await r.text()
        except Exception as e:
            log.debug(f"url_watch {s['name']}: {e}")
            continue
        pl_hosts = {urlsplit(e["url"]).netloc for e in _parse_m3u(text)}
        db = open_db(db_path)
        db_hosts = {urlsplit(u).netloc for (u,) in db.execute(
            "SELECT url FROM tracks WHERE source_id=?", (s["id"],))}
        db.close()
        new_hosts = pl_hosts - db_hosts
        if new_hosts:
            log.info(f"url_watch: у источника {s['name']} сменился сервер → {sorted(new_hosts)}, синхронизирую")
            await sync_m3u(s["id"], s["url"], s["stream_type"], s["priority"], db_path)
            await _notify_sync_done()

async def _url_watch_loop(db_path: str | None = None):
    interval = int(CONFIG.get("url_watch_secs", 300))
    if interval <= 0:
        return
    log.info(f"url_watch: проверка смены сервера в плейлистах каждые {interval} сек")
    while True:
        await asyncio.sleep(interval)
        try:
            await _url_watch_once(db_path)
        except Exception:
            log.exception("url_watch")

# ─── PLAYLIST ────────────────────────────────────────────────────────────────

def _m3u_attr(value: str) -> str:
    return (value or "").replace('"', "").replace("\r", " ").replace("\n", " ")


def generate_m3u(db_path: str | None = None,
                 base_url: str | None = None, unify: bool = False) -> str:
    base = (base_url or f"http://localhost:{CONFIG['port']}").rstrip("/")
    db   = open_db(db_path)
    epg_url = f"{base}/xmltv"
    # x-tvg-url — де-факто стандарт (используется наравне со старым url-tvg) для автоопределения EPG плеерами
    lines = [f'#EXTM3U url-tvg="{epg_url}" x-tvg-url="{epg_url}"']

    lines += [
        f'#EXTINF:-1 tvg-id="" tvg-name="⟳ Обновить все" group-title="⚙ Сервис",⟳ Обновить все',
        f"{base}/sync-all",
    ]

    rows = db.execute("""
        SELECT c.id, c.name, c.epg_id, c.group_name,
               CASE WHEN c.icon_url != '' THEN c.icon_url ELSE COALESCE(e.icon_url, '') END AS icon_url,
               c.sort_order
        FROM channels c
        LEFT JOIN epg_channels e ON e.epg_id = c.epg_id
        WHERE c.enabled=1
          AND c.sort_order > 0
          AND EXISTS (
              SELECT 1 FROM tracks t JOIN sources s ON s.id=t.source_id
              WHERE t.channel_id=c.id AND s.enabled=1 AND t.enabled=1)
        ORDER BY c.sort_order, c.group_name, c.name
    """).fetchall()

    for ch in rows:
        attrs = f'tvg-id="{_m3u_attr(ch["epg_id"])}" tvg-name="{_m3u_attr(ch["name"])}" tvg-chno="{ch["sort_order"]}" group-title="Каналы"'
        if ch["icon_url"]:
            attrs += f' tvg-logo="{_m3u_attr(ch["icon_url"])}"'
        lines.append(f"#EXTINF:-1 {attrs},{_m3u_attr(ch['name'])}")
        lines.append(f"{base}/stream/{ch['id']}" + ("?unify=1" if unify else ""))

    db.close()
    return "\n".join(lines)

# ─── NULL-TS ─────────────────────────────────────────────────────────────────

_NULL_PKT  = bytes([0x47, 0x1F, 0xFF, 0x10]) + bytes(184)
_NULL_CHUNK = _NULL_PKT * 348  # ~65 KB

async def _stream_null_ts(resp: web.StreamResponse, stop: asyncio.Event):
    try:
        while not stop.is_set():
            await resp.write(_NULL_CHUNK)
            await asyncio.sleep(0.5)
    except (ConnectionResetError, asyncio.CancelledError):
        pass

# ─── WEB UI ──────────────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Channel Manager</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d0d1a;color:#e0e0e0;font-family:'Courier New',monospace;font-size:13px;padding:16px;min-height:100vh}
h1{font-size:20px;color:#4488ff;letter-spacing:2px}
header{display:flex;align-items:baseline;gap:16px;border-bottom:1px solid #1e1e3a;padding-bottom:10px;margin-bottom:14px;flex-wrap:wrap}
a.hl{color:#4488ff;font-size:12px;text-decoration:none}a.hl:hover{text-decoration:underline}
.sub{color:#555;font-size:11px}
.tabs{display:flex;gap:4px;margin-bottom:14px}
.tab{padding:5px 18px;border:1px solid #2a2a4a;border-radius:3px;cursor:pointer;color:#888;background:#0d0d1a}
.tab.active{background:#1a1a3a;color:#4488ff;border-color:#4488ff}
.pane{display:none}.pane.active{display:block}
table{width:100%;border-collapse:collapse}
th{text-align:left;color:#555;padding:4px 8px;border-bottom:1px solid #1e1e3a;font-weight:normal;white-space:nowrap}
td{padding:5px 8px;border-bottom:1px solid #111;vertical-align:middle}
tr.cl{cursor:pointer}tr.cl:hover td{background:#111128}tr.cl.sel td{background:#0e1a2e}
input,select{background:#1a1a2e;color:#e0e0e0;border:1px solid #2a2a4a;padding:4px 8px;border-radius:3px;font-family:inherit;font-size:13px}
input:focus,select:focus{outline:none;border-color:#4488ff}
.btn{padding:4px 12px;border:1px solid;border-radius:3px;cursor:pointer;font-family:inherit;font-size:12px;background:transparent}
.b-bl{border-color:#4488ff;color:#4488ff}.b-gr{border-color:#00cc66;color:#00cc66}
.b-rd{border-color:#cc4444;color:#cc4444}.b-or{border-color:#f0a500;color:#f0a500}
.b-gy{border-color:#555;color:#888}
.btn:hover{background:#1a1a2e}
.badge{display:inline-block;padding:1px 6px;border-radius:3px;font-size:11px;line-height:1.6}
.ts{background:#1a2a3a;color:#4488ff}.hls{background:#1a3a1e;color:#00cc66}
.e2{background:#2a1a3a;color:#cc88ff}.m3u{background:#1a2a1a;color:#88cc88}
.on{color:#00cc66}.off{color:#cc4444}.dim{color:#555}
.bar{display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
.panel{background:#0f0f22;border:1px solid #1e1e3a;border-radius:4px;padding:14px;margin-bottom:14px}
.pt{color:#666;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px}
.trk{display:flex;gap:7px;align-items:center;padding:5px 0;border-bottom:1px solid #111}
.trk-u{flex:1;color:#555;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px}
.fg{display:grid;grid-template-columns:140px 1fr;gap:6px 12px;align-items:center;margin-bottom:10px}
.fg label{color:#888;text-align:right}
.logbox{background:#060610;border:1px solid #1e1e3a;border-radius:3px;padding:10px;height:380px;overflow-y:auto;font-size:12px;color:#888}
.logbox .err{color:#cc4444}.logbox .wrn{color:#f0a500}
.msg{font-size:12px;min-height:18px;margin-top:4px}
.ov{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:100;align-items:center;justify-content:center}
.ov.open{display:flex}
.modal{background:#111128;border:1px solid #2a2a4a;border-radius:6px;padding:20px;width:500px;max-width:96vw}
.mt{color:#4488ff;font-size:15px;margin-bottom:14px}
.layout{display:flex;gap:14px}
.ch-list{flex:1;min-width:0;overflow-x:auto}
.det{display:none;width:460px;flex-shrink:0}
</style>
</head>
<body>
<header>
  <h1>Channel Manager</h1>
  <span class="sub">порт <b id="h-port">3002</b></span>
  <a class="hl" href="/playlist.m3u8">⬇ M3U</a>
  <a class="hl" href="/xmltv" style="color:#888">📺 EPG</a>
  <span style="flex:1"></span>
  <button class="btn b-or" onclick="syncAll()">⟳ Синхр. все + EPG</button>
  <div class="msg" id="hm"></div>
</header>
<div class="tabs">
  <div class="tab active" onclick="tab('channels')">Каналы</div>
  <div class="tab" onclick="tab('sources')">Источники</div>
  <div class="tab" onclick="tab('epg')">EPG</div>
  <div class="tab" onclick="tab('tsbuf')">TS Buffer</div>
  <div class="tab" onclick="tab('links')">Ссылки</div>
  <div class="tab" onclick="tab('logs')">Лог</div>
</div>

<!-- ═ CHANNELS ═ -->
<div class="pane active" id="pane-channels">
  <div class="bar">
    <input id="q" type="text" placeholder="Поиск…" style="width:220px" oninput="_page=0;renderCh()">
    <select id="fg" onchange="_page=0;renderCh()"><option value="">Все группы</option></select>
    <select id="fs" onchange="_page=0;renderCh()" title="Фильтр по источнику"><option value="">Все источники</option></select>
    <span class="dim" id="cnt"></span>
    <button class="btn b-gr" onclick="ov('m-ac')">+ Канал</button>
    <button class="btn b-gy" onclick="renumber()" title="Переназначить номера по порядку без дублей"># Перенумеровать</button>
    <div class="msg" id="chm"></div>
  </div>
  <div class="layout">
    <div class="ch-list">
      <table>
        <thead><tr><th style="width:36px">№</th><th>Название</th><th>Группа</th><th>Источники</th><th>EPG id</th><th>Тр</th><th>Вкл</th><th></th></tr></thead>
        <tbody id="chb"></tbody>
      </table>
      <div id="pgbar" style="display:flex;gap:8px;align-items:center;margin-top:10px;padding:4px 0"></div>
    </div>
    <div class="det" id="det">
      <div class="panel">
        <div class="pt" id="dett">Треки</div>
        <div id="detr"></div>
        <div style="color:#555;font-size:11px;margin-top:10px">Добавить трек вручную:</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px">
          <select id="ts" style="width:160px"></select>
          <select id="tt"><option value="hls">HLS</option><option value="ts">TS</option></select>
          <input id="tu" placeholder="URL" style="width:100%;margin-top:4px">
          <input id="tp" type="number" placeholder="Приоритет" style="width:90px" value="50">
          <button class="btn b-gr" onclick="addTrk()">+ Добавить</button>
          <div class="msg" id="tm"></div>
        </div>
        <div style="color:#555;font-size:11px;margin-top:14px">Номер канала (порядок в плейлисте):</div>
        <div style="display:flex;gap:6px;margin-top:6px;align-items:center">
          <input id="dn" type="number" min="1" placeholder="без номера" style="width:120px">
          <button class="btn b-bl" onclick="saveNum()">Сохранить</button>
          <div class="msg" id="nm"></div>
        </div>
        <div style="color:#555;font-size:11px;margin-top:14px">EPG ID канала (tvg-id в XMLTV):</div>
        <div style="position:relative;margin-top:6px">
          <div style="display:flex;gap:6px">
            <input id="ei" placeholder="ntv.ru / ch076" style="flex:1" oninput="epgSearch(this.value)" autocomplete="off">
            <button class="btn b-bl" onclick="saveEpg()">Сохранить</button>
          </div>
          <div id="epg-drop" style="display:none;position:absolute;top:100%;left:0;right:0;z-index:50;background:#111128;border:1px solid #2a2a4a;border-radius:4px;max-height:200px;overflow-y:auto;box-shadow:0 4px 16px rgba(0,0,0,.5)"></div>
        </div>
        <style>.epg-opt{padding:6px 10px;cursor:pointer;border-bottom:1px solid #1a1a3a}.epg-opt:hover{background:#1a1a3a}</style>
      </div>
    </div>
  </div>
</div>

<!-- ═ SOURCES ═ -->
<div class="pane" id="pane-sources">
  <div class="panel">
    <div class="pt">Добавить источник</div>
    <div class="fg">
      <label>Имя</label><input id="ns-n" placeholder="tvteamonline" style="max-width:240px">
      <label>Тип</label>
      <div style="display:flex;gap:8px">
        <select id="ns-t" onchange="onST()"><option value="m3u">M3U</option><option value="enigma2">Enigma2</option></select>
        <select id="ns-s"><option value="hls">HLS</option><option value="ts">TS</option></select>
      </div>
      <label id="ns-ul">URL плейлиста</label>
      <input id="ns-u" placeholder="http://…" style="max-width:420px">
      <label>Приоритет</label><input id="ns-p" type="number" value="50" style="max-width:90px">
    </div>
    <div style="display:flex;gap:8px;align-items:center">
      <button class="btn b-gr" onclick="addSrc()">+ Добавить</button>
      <div class="msg" id="sam"></div>
    </div>
  </div>
  <table>
    <thead><tr><th>Имя</th><th>Тип</th><th>Приоритет</th><th>URL / Хост</th><th>Синхр.</th><th>Треков</th><th>Вкл</th><th title="Перекодировать на GPU (H.264, деинтерлейс), если сырой поток не смотрибелен на приставке">ffmpeg</th><th></th></tr></thead>
    <tbody id="srb"></tbody>
  </table>
  <div class="msg" id="srm" style="margin-top:8px"></div>
</div>

<!-- ═ EPG ═ -->
<div class="pane" id="pane-epg">
  <div class="panel">
    <div class="pt">Настройки EPG</div>
    <div class="fg">
      <label>URL источника</label>
      <div style="display:flex;gap:8px;flex:1">
        <input id="epg-url" style="flex:1" placeholder="https://epg.team/tvteam.xml.1.7">
        <button class="btn b-bl" onclick="saveEpgUrl()">Сохранить</button>
      </div>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;align-items:center">
      <button class="btn b-or" onclick="epgRefresh()">↻ Скачать EPG и обновить список каналов</button>
      <button class="btn b-gr" onclick="epgAutomap()">⚡ Авто-маппинг по имени</button>
      <div class="msg" id="epgm"></div>
    </div>
  </div>
  <div class="panel">
    <div class="pt">Каналы в EPG источнике (<span id="epg-cnt">0</span>)</div>
    <input id="epg-q" placeholder="Поиск по имени или ID…" style="width:280px;margin-bottom:10px" oninput="searchEpgList(this.value)">
    <div id="epg-list" style="max-height:400px;overflow-y:auto"></div>
  </div>
</div>

<!-- ═ TS BUFFER ═ -->
<div class="pane" id="pane-tsbuf">
  <div style="display:flex;gap:24px;background:#13132a;border-radius:8px;padding:12px 20px;margin-bottom:12px;border:1px solid #1e1e3a;flex-wrap:wrap">
    <div><div style="color:#555;font-size:10px;text-transform:uppercase">Каналов</div><div id="tsb-sess-cnt" style="color:#fff;font-size:22px;font-weight:bold">0</div></div>
    <div><div style="color:#555;font-size:10px;text-transform:uppercase">Клиентов</div><div id="tsb-cli-cnt" style="color:#fff;font-size:22px;font-weight:bold">0</div></div>
    <div style="margin-left:auto;display:flex;align-items:center;gap:8px">
      <button onclick="tsbRefreshDb()" style="background:#1a3a1e;border:1px solid #00ff88;color:#00ff88;padding:5px 14px;border-radius:4px;cursor:pointer;font-family:inherit;font-size:13px">↻ Обновить каналы</button>
      <button onclick="tsbRestart()" style="background:#3a1e1e;border:1px solid #ff4444;color:#ff8888;padding:5px 14px;border-radius:4px;cursor:pointer;font-family:inherit;font-size:13px">⟳ Перезапуск</button>
      <span id="tsb-act-msg" style="font-size:12px;color:#555"></span>
    </div>
  </div>
  <div id="tsb-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:12px;margin-bottom:12px"></div>
  <details style="background:#13132a;border-radius:8px;border:1px solid #1e1e3a;margin-bottom:6px">
    <summary style="padding:10px 16px;cursor:pointer;color:#555;font-size:12px;text-transform:uppercase;letter-spacing:1px;list-style:none;user-select:none">⚙  Буфер и переключение</summary>
    <div style="padding:12px 16px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">Буфер HLS (сек)</label><input id="tsb-buf" type="number" min="3" max="60" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase" title="Целевой буфер для прямого TS (спутник/TVHeadend) — живой поток, меньше буфер = быстрее старт канала, но меньше запас на случай кратковременного сбоя сигнала">Буфер TS (сек)</label><input id="tsb-buf-ts" type="number" min="1" max="30" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">Скорость (Мбит)</label><input id="tsb-mbps" type="number" min="1" max="50" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">Переключение (сек,0=нет)</label><input id="tsb-fallback" type="number" min="0" max="60" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">Проверка осн. (сек)</label><input id="tsb-check" type="number" min="0" max="7200" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">Idle сессии (сек)</label><input id="tsb-idle" type="number" min="5" max="300" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <button onclick="saveTsbConfig(1)" style="background:#1e3a6a;border:1px solid #4488ff;color:#4488ff;padding:5px 16px;border-radius:4px;cursor:pointer;font-family:inherit">Применить</button>
      <span id="tsb-cfg-msg" style="font-size:12px;color:#555"></span>
    </div>
  </details>
  <details style="background:#13132a;border-radius:8px;border:1px solid #1e1e3a;margin-bottom:6px">
    <summary style="padding:10px 16px;cursor:pointer;color:#555;font-size:12px;text-transform:uppercase;letter-spacing:1px;list-style:none;user-select:none">⚙  HLS таймауты</summary>
    <div style="padding:12px 16px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">HLS: подключение (сек)</label><input id="tsb-hls-ct" type="number" min="1" max="60" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">HLS: плейлист (сек)</label><input id="tsb-hls-rt" type="number" min="1" max="60" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">HLS: сегмент (сек)</label><input id="tsb-hls-st" type="number" min="1" max="120" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">TS: подключение (сек)</label><input id="tsb-ts-ct" type="number" min="1" max="60" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">TS: без данных (сек)</label><input id="tsb-ts-rt" type="number" min="1" max="120" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <button onclick="saveTsbConfig(2)" style="background:#1e3a6a;border:1px solid #4488ff;color:#4488ff;padding:5px 16px;border-radius:4px;cursor:pointer;font-family:inherit">Применить</button>
      <span id="tsb-cfg-msg2" style="font-size:12px;color:#555"></span>
    </div>
  </details>
  <details style="background:#13132a;border-radius:8px;border:1px solid #1e1e3a;margin-bottom:8px">
    <summary style="padding:10px 16px;cursor:pointer;color:#555;font-size:12px;text-transform:uppercase;letter-spacing:1px;list-style:none;user-select:none">⚙  TS качество</summary>
    <div style="padding:12px 16px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">TS: окно качества (сек)</label><input id="tsb-ts-qw" type="number" min="1" max="60" step="0.5" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">TS: порог TEI % (0=нет)</label><input id="tsb-ts-tei" type="number" min="0" max="100" step="0.01" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase">TS: порог CC % (0=нет)</label><input id="tsb-ts-cc" type="number" min="0" max="100" step="0.01" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <button onclick="saveTsbConfig(3)" style="background:#1e3a6a;border:1px solid #4488ff;color:#4488ff;padding:5px 16px;border-radius:4px;cursor:pointer;font-family:inherit">Применить</button>
      <span id="tsb-cfg-msg3" style="font-size:12px;color:#555"></span>
    </div>
  </details>
  <details style="background:#13132a;border-radius:8px;border:1px solid #1e1e3a;margin-bottom:8px">
    <summary style="padding:10px 16px;cursor:pointer;color:#555;font-size:12px;text-transform:uppercase;letter-spacing:1px;list-style:none;user-select:none">⚙  TVHeadend качество</summary>
    <div style="padding:12px 16px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase" title="Как часто спрашивать у TVHeadend счётчик ошибок демультиплексора по своей подписке">Опрос (сек)</label><input id="tsb-tvh-poll" type="number" min="3" max="120" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase" title="Разовый скачок счётчика ошибок за один опрос — переключает немедленно, без хронического накопления">Резкий скачок ошибок</label><input id="tsb-tvh-hard" type="number" min="1" max="1000" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <button onclick="saveTsbConfig(5)" style="background:#1e3a6a;border:1px solid #4488ff;color:#4488ff;padding:5px 16px;border-radius:4px;cursor:pointer;font-family:inherit">Применить</button>
      <span id="tsb-cfg-msg5" style="font-size:12px;color:#555"></span>
    </div>
  </details>
  <details style="background:#13132a;border-radius:8px;border:1px solid #1e1e3a;margin-bottom:8px">
    <summary style="padding:10px 16px;cursor:pointer;color:#555;font-size:12px;text-transform:uppercase;letter-spacing:1px;list-style:none;user-select:none">⚙  Видео</summary>
    <div style="padding:12px 16px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
      <div style="display:flex;flex-direction:column;gap:4px"><label style="color:#555;font-size:10px;text-transform:uppercase" title="GPU общий с Frigate (детекция по камерам) — держите запас, не только под текущую нагрузку ТВ">Лимит GPU-перекодирований</label><input id="tsb-vid-max" type="number" min="1" max="9" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit"></div>
      <label style="display:flex;align-items:center;gap:6px;color:#ccc;font-size:12px;padding-bottom:7px" title="Принудительно перекодирует АБСОЛЮТНО все каналы в единый кодек/развёртку (gpu_h264), игнорируя даже ручной флажок OFF у отдельных дорожек. Лимит GPU (слева) остаётся — каналы сверх лимита временно идут сырым потоком.">
        <input id="tsb-vid-unify" type="checkbox" style="width:16px;height:16px">
        Единый поток (принудительный транскод всех каналов, без исключений)
      </label>
      <button onclick="saveTsbConfig(6)" style="background:#1e3a6a;border:1px solid #4488ff;color:#4488ff;padding:5px 16px;border-radius:4px;cursor:pointer;font-family:inherit">Применить</button>
      <span id="tsb-cfg-msg6" style="font-size:12px;color:#555"></span>
    </div>
  </details>
  <details style="background:#13132a;border-radius:8px;border:1px solid #1e1e3a;margin-bottom:8px">
    <summary style="padding:10px 16px;cursor:pointer;color:#555;font-size:12px;text-transform:uppercase;letter-spacing:1px;list-style:none;user-select:none">⚙  Аудио</summary>
    <div style="padding:12px 16px;display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
      <div style="display:flex;flex-direction:column;gap:4px">
        <label style="color:#555;font-size:10px;text-transform:uppercase" title="Язык дорожки, которую брать у HLS-каналов с отдельными аудиопотоками (напр. TEAM Квартирник)">HLS: язык дорожки</label>
        <input id="tsb-aud-lang" type="text" maxlength="3" style="width:60px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit">
      </div>
      <div style="display:flex;flex-direction:column;gap:4px">
        <label style="color:#555;font-size:10px;text-transform:uppercase">AAC битрейт, кбит/с</label>
        <input id="tsb-aud-br" type="number" min="64" max="320" step="32" style="width:80px;background:#0d0d1a;border:1px solid #2a2a4a;color:#ddd;padding:5px 7px;border-radius:4px;font-family:inherit">
      </div>
      <label style="display:flex;align-items:center;gap:6px;color:#ccc;font-size:12px;padding-bottom:7px" title="Действует на ВСЕ каналы (и HLS/tvteam, и спутник/Энигма), независимо от видео-перекодирования: видео всегда идёт копией, звук принудительно сводится в AAC стерео заданным битрейтом (многоканальный/раздельные аудиодорожки → один стерео-поток с видео).">
        <input id="tsb-aud-force" type="checkbox" style="width:16px;height:16px">
        Принудительно AAC стерео (все каналы, видео копией)
      </label>
      <button onclick="saveTsbConfig(4)" style="background:#1e3a6a;border:1px solid #4488ff;color:#4488ff;padding:5px 16px;border-radius:4px;cursor:pointer;font-family:inherit">Применить</button>
      <span id="tsb-cfg-msg4" style="font-size:12px;color:#555"></span>
    </div>
  </details>
</div>

<!-- ═ LINKS ═ -->
<div class="pane" id="pane-links">
  <div style="max-width:640px;margin:0 auto">
    <div class="panel" style="margin-bottom:16px">
      <div class="pt">Плейлист M3U</div>
      <div style="color:#aaa;font-size:12px;margin-bottom:8px">Добавьте эту ссылку в Kodi, VLC или другой IPTV-плеер</div>
      <div style="display:flex;gap:8px;align-items:center">
        <input id="lnk-m3u" readonly style="flex:1;font-size:13px;color:#ccc;background:#0a0a18;border:1px solid #1e1e3a;border-radius:4px;padding:7px 10px">
        <button class="btn b-bl" onclick="cpLnk('lnk-m3u','lnk-m3u-msg')">⎘ Копировать</button>
        <a id="lnk-m3u-dl" class="btn b-gr" style="text-decoration:none" download="playlist.m3u8">⬇ Скачать</a>
      </div>
      <div id="lnk-m3u-msg" class="msg" style="margin-top:4px"></div>
    </div>
    <div class="panel" style="margin-bottom:16px">
      <div class="pt">Плейлист M3U (единый поток)</div>
      <div style="color:#aaa;font-size:12px;margin-bottom:8px">Все каналы принудительно перекодируются в единый кодек/звук/деинтерлейс (H.264+AAC 256, стиль tvteam) — дороже по GPU/CPU, используйте только если нужна единообразная картинка</div>
      <div style="display:flex;gap:8px;align-items:center">
        <input id="lnk-m3u-unify" readonly style="flex:1;font-size:13px;color:#ccc;background:#0a0a18;border:1px solid #1e1e3a;border-radius:4px;padding:7px 10px">
        <button class="btn b-bl" onclick="cpLnk('lnk-m3u-unify','lnk-m3u-unify-msg')">⎘ Копировать</button>
        <a id="lnk-m3u-unify-dl" class="btn b-gr" style="text-decoration:none" download="playlist-unify.m3u8">⬇ Скачать</a>
      </div>
      <div id="lnk-m3u-unify-msg" class="msg" style="margin-top:4px"></div>
    </div>
    <div class="panel" style="margin-bottom:16px">
      <div class="pt">EPG (XMLTV)</div>
      <div style="color:#aaa;font-size:12px;margin-bottom:8px">Ссылка на телепрограмму в формате XMLTV</div>
      <div style="display:flex;gap:8px;align-items:center">
        <input id="lnk-epg" readonly style="flex:1;font-size:13px;color:#ccc;background:#0a0a18;border:1px solid #1e1e3a;border-radius:4px;padding:7px 10px">
        <button class="btn b-bl" onclick="cpLnk('lnk-epg','lnk-epg-msg')">⎘ Копировать</button>
      </div>
      <div id="lnk-epg-msg" class="msg" style="margin-top:4px"></div>
    </div>
    <div class="panel">
      <div class="pt">Прямой стрим (пример)</div>
      <div style="color:#aaa;font-size:12px;margin-bottom:8px">Шаблон URL для конкретного канала — hex ID из таблицы каналов</div>
      <div style="background:#0a0a18;border:1px solid #1e1e3a;border-radius:4px;padding:8px 12px;font-size:13px;color:#888;font-family:monospace" id="lnk-stream-tmpl"></div>
    </div>
  </div>
</div>

<!-- ═ LOGS ═ -->
<div class="pane" id="pane-logs">
  <div style="margin-bottom:8px"><button class="btn b-bl" onclick="loadLogs()">↻ Обновить</button></div>
  <div class="logbox" id="lb"></div>
</div>

<!-- MODAL: add channel -->
<div class="ov" id="m-ac" onclick="if(bg(event))co('m-ac')">
  <div class="modal">
    <div class="mt">+ Новый канал</div>
    <div class="fg">
      <label>Название *</label><input id="ac-n" placeholder="НТВ">
      <label>Группа</label><input id="ac-g" placeholder="Новости">
      <label>EPG tvg-id</label><input id="ac-e" placeholder="ntv.ru">
      <label>Иконка</label><input id="ac-i" placeholder="https://…">
      <label>URL потока</label><input id="ac-u" placeholder="(необязательно)">
      <label>Тип / Источник</label>
      <div style="display:flex;gap:8px">
        <select id="ac-t"><option value="hls">HLS</option><option value="ts">TS</option></select>
        <select id="ac-s" style="flex:1"></select>
      </div>
    </div>
    <div style="display:flex;gap:8px;margin-top:10px">
      <button class="btn b-gr" onclick="doAC()">Создать</button>
      <button class="btn b-gy" onclick="co('m-ac')">Отмена</button>
      <div class="msg" id="acm"></div>
    </div>
  </div>
</div>

<!-- MODAL: groups filter -->
<div class="ov" id="m-grp" onclick="if(bg(event))co('m-grp')">
  <div class="modal" style="width:700px;max-height:85vh;display:flex;flex-direction:column">
    <div class="mt" id="grp-title">Группы источника</div>
    <div style="display:flex;gap:8px;margin-bottom:8px;align-items:center">
      <button class="btn b-gr" onclick="grpAll(1)" style="padding:2px 10px;font-size:12px">✓ Все</button>
      <button class="btn b-rd" onclick="grpAll(0)" style="padding:2px 10px;font-size:12px">✗ Ни одного</button>
      <button class="btn b-bl" onclick="saveGroups()" style="margin-left:auto">Сохранить</button>
    </div>
    <div style="overflow-y:auto;flex:1" id="grp-body"></div>
    <div class="msg" id="grpm" style="margin-top:8px"></div>
  </div>
</div>

<!-- MODAL: migrate -->
<div class="ov" id="m-mg" onclick="if(bg(event))co('m-mg')">
  <div class="modal">
    <div class="mt">⇑ Миграция из TVLINK</div>
    <div class="fg">
      <label>Путь к данным</label><input id="mg-p" value="/opt/tvlink/data">
    </div>
    <div style="color:#888;font-size:12px;margin:8px 0">
      Каналы с одинаковым именем (без HD/SD/Double) сливаются в один.<br>
      Canonical ID = версия с наилучшим качеством (HD &gt; без суф. &gt; SD).
    </div>
    <div style="display:flex;gap:8px;margin-top:10px">
      <button class="btn b-or" onclick="doMG()">⇑ Мигрировать</button>
      <button class="btn b-gy" onclick="co('m-mg')">Отмена</button>
      <div class="msg" id="mgm"></div>
    </div>
  </div>
</div>

<script>
'use strict';
let _ch=[], _src=[], _cur=null, _page=0;
const _PER=100;

function tab(n){
  ['channels','sources','epg','tsbuf','links','logs'].forEach((x,i)=>{
    document.querySelectorAll('.tab')[i].classList.toggle('active',x===n);
    document.getElementById('pane-'+x).classList.toggle('active',x===n);
  });
  if(n==='sources') loadSrc();
  if(n==='epg') loadEpg();
  if(n==='logs') loadLogs();
  if(n==='tsbuf') initTsb();
  if(n==='links') initLinks();
}
function initLinks(){
  const base=location.protocol+'//'+location.host;
  const m3u=base+'/playlist.m3u8';
  const m3uUnify=base+'/playlist-unify.m3u8';
  const epg=base+'/xmltv';
  _('lnk-m3u').value=m3u;
  _('lnk-m3u-dl').href=m3u;
  _('lnk-m3u-unify').value=m3uUnify;
  _('lnk-m3u-unify-dl').href=m3uUnify;
  _('lnk-epg').value=epg;
  _('lnk-stream-tmpl').textContent=base+'/stream/{hex-id}  (например: '+base+'/stream/621B6E30)';
}
function cpLnk(inputId,msgId){
  navigator.clipboard.writeText(_(inputId).value).then(()=>msg(msgId,true,'✓ Скопировано')).catch(()=>{_(inputId).select();document.execCommand('copy');msg(msgId,true,'✓ Скопировано');});
}

async function loadCh(){
  const d=await api('/api/channels'); _ch=d.channels||[];
  const grps=[...new Set(_ch.map(c=>c.group_name).filter(Boolean))].sort();
  const g=_('fg'); const pv=g.value;
  g.innerHTML='<option value="">Все группы</option>'+grps.map(x=>`<option>${x}</option>`).join('');
  g.value=pv;
  // источники для фильтра
  const srcs=[...new Set(_ch.flatMap(c=>(c.sources||'').split(',').filter(Boolean)))].sort();
  const fs=_('fs'); const psrc=fs.value;
  fs.innerHTML='<option value="">Все источники</option>'+
    srcs.map(x=>`<option>${x}</option>`).join('')+
    '<option value="__none__">— Без источника</option>';
  fs.value=psrc;
  _page=0; renderCh();
}
function numLabel(s){return (s>0&&s<9999)?s:'—';}
function renderCh(){
  const q=_('q').value.toLowerCase(), g=_('fg').value, fs=_('fs').value;
  const fsVal=fs||'';
  let rows=_ch.filter(c=>
    (!q||c.name.toLowerCase().includes(q))&&
    (!g||c.group_name===g)&&
    (fsVal==='__none__'
      ? !c.track_count
      : (c.track_count>0 && (fsVal===''||((c.sources||'').split(',').includes(fsVal))))
    )
  );
  rows.sort((a,b)=>{
    const an=a.sort_order>0&&a.sort_order<9999?0:1, bn=b.sort_order>0&&b.sort_order<9999?0:1;
    if(an!==bn) return an-bn;
    if(an===0) return a.sort_order-b.sort_order;
    return (a.group_name||'').localeCompare(b.group_name||'')||a.name.localeCompare(b.name);
  });
  const pages=Math.max(1,Math.ceil(rows.length/_PER));
  if(_page>=pages) _page=pages-1;
  const slice=rows.slice(_page*_PER, (_page+1)*_PER);
  _('cnt').textContent=`${rows.length} / ${_ch.length}   стр.${_page+1}/${pages}`;
  _('chb').innerHTML=slice.map(c=>`
    <tr class="cl${_cur===c.id?' sel':''}" onclick="selCh('${c.id}')">
      <td class="dim" style="text-align:right;padding-right:8px">${numLabel(c.sort_order)}</td>
      <td>${z(c.name)}</td><td class="dim">${z(c.group_name||'—')}</td>
      <td style="font-size:11px">${(c.sources||'').split(',').filter(Boolean).map(s=>`<span class="badge ${s==='NTVHD'||s==='NTVSD'?'e2':'m3u'}" style="margin:1px">${z(s)}</span>`).join('')||'<span class="dim" style="color:#cc4444;font-size:10px">нет</span>'}</td>
      <td class="dim" style="font-size:11px">${z(c.epg_id||'—')}</td>
      <td>${c.track_count}</td>
      <td class="${c.enabled?'on':'off'}">${c.enabled?'✓':'✗'}</td>
      <td><button class="btn b-${c.enabled?'rd':'gr'}" style="padding:2px 8px;font-size:11px"
        onclick="event.stopPropagation();togCh('${c.id}',${c.enabled})">${c.enabled?'Выкл':'Вкл'}</button></td>
    </tr>`).join('');
  // Пагинация
  _('pgbar').innerHTML=
    `<button class="btn b-gy" onclick="goPage(-1)" ${_page===0?'disabled':''}>◀</button>`+
    `<span class="dim" style="padding:0 10px">стр. ${_page+1} из ${pages}</span>`+
    `<button class="btn b-gy" onclick="goPage(1)" ${_page>=pages-1?'disabled':''}>▶</button>`;
}
function goPage(d){_page=Math.max(0,_page+d);renderCh();}
async function selCh(id){
  _cur=id;
  const c=_ch.find(x=>x.id===id)||{};
  _('det').style.display='block';
  _('dett').textContent='Треки: '+(c.name||id);
  _('ei').value=c.epg_id||'';
  const so=c.sort_order;
  _('dn').value=(so>0&&so<9999)?so:'';
  hideEpgDrop();
  renderCh(); await loadTrks();
}
async function loadTrks(){
  if(!_cur) return;
  const d=await api('/api/channels/'+_cur);
  _('detr').innerHTML=(d.tracks||[]).map(t=>{
    // эффективное состояние = явный override дорожки, а если его нет — берём из источника
    const eff = t.profile==='off' ? false : t.profile==='gpu_h264' ? true : t.source_profile==='gpu_h264';
    const overridden = t.profile==='gpu_h264'||t.profile==='off';
    return `
    <div class="trk">
      <span class="dim" style="min-width:36px">#${t.priority}</span>
      <span class="badge ${t.stream_type}">${t.stream_type.toUpperCase()}</span>
      <span style="color:#888;min-width:100px;font-size:12px">${z(t.source_name)}</span>
      <span class="trk-u" title="${z(t.url)}">${z(t.url)}</span>
      <input type="number" value="${t.priority}" style="width:66px;padding:2px 4px" id="tp-${t.id}">
      <label style="display:flex;align-items:center;gap:4px;font-size:11px;color:#888;white-space:nowrap"
        title="Перекодировать эту дорожку на GPU. ${overridden?'Переопределено для этого канала вручную.':'Сейчас по умолчанию от источника «'+z(t.source_name)+'» ('+(eff?'вкл':'выкл')+'); снимите/поставьте галочку, чтобы задать явно для этого канала.'}">
        <input type="checkbox" ${eff?'checked':''} onchange="togTrkProfile(${t.id},this.checked)">ffmpeg${overridden?' <span class="dim">(вручную)</span>':''}
      </label>
      ${overridden?`<button class="btn b-gy" style="padding:2px 6px;font-size:10px" title="Вернуть к значению по умолчанию (как у источника)" onclick="togTrkProfile(${t.id},null)">⟲</button>`:''}
      <button class="btn b-bl" style="padding:2px 6px" onclick="updTrk(${t.id})">✓</button>
      <button class="btn b-rd" style="padding:2px 6px" onclick="delTrk(${t.id})">✕</button>
    </div>`;
  }).join('')||'<span class="dim">Нет треков</span>';
}
async function saveNum(){
  if(!_cur) return;
  const n=parseInt(_('dn').value)||0;
  if(n>0){
    const dup=_ch.find(c=>c.id!==_cur&&c.sort_order===n);
    if(dup) return msg('nm',false,`Номер ${n} занят: «${dup.name}»`);
  }
  const d=await post('/api/channels/'+_cur,{sort_order:n},'PATCH');
  msg('nm',d.ok,d.ok?'✓ Сохранено':'✗ '+d.error);
  if(d.ok) loadCh();
}
async function renumber(){
  if(!confirm('Переназначить номера по порядку (без дублей), сохранив относительный порядок?')) return;
  const d=await post('/api/channels/renumber',{});
  msg('chm',d.ok,d.ok?`✓ Перенумеровано ${d.count} каналов`:'✗ '+d.error);
  if(d.ok) loadCh();
}
async function saveEpg(){
  if(!_cur) return;
  const epg=_('ei').value.trim();
  hideEpgDrop();
  const d=await post('/api/channels/'+_cur,{epg_id:epg},'PATCH');
  msg('tm',d.ok,d.ok?'✓ EPG ID сохранён':'✗ '+d.error);
  if(d.ok) loadCh();
}
let _epgTimer=null;
function epgSearch(v){
  hideEpgDrop();
  clearTimeout(_epgTimer);
  if(!v.trim()){return;}
  _epgTimer=setTimeout(async()=>{
    const d=await api('/api/epg/channels?q='+encodeURIComponent(v));
    const rows=d.channels||[];
    const drop=_('epg-drop');
    if(!rows.length){drop.style.display='none';return;}
    drop.innerHTML=rows.map(r=>`<div class="epg-opt" onclick="pickEpg('${z(r.epg_id)}')">
      <span style="font-size:12px">${z(r.display_name)}</span>
      <span class="dim" style="font-size:10px;margin-left:8px">${z(r.epg_id)}</span></div>`).join('');
    drop.style.display='block';
  },280);
}
function pickEpg(id){_('ei').value=id; hideEpgDrop();}
function hideEpgDrop(){const d=_('epg-drop');if(d)d.style.display='none';}
async function togCh(id,cur){
  await post('/api/channels/'+id,{enabled:cur?0:1},'PATCH'); loadCh();
}
async function addTrk(){
  if(!_cur) return;
  const d=await post('/api/tracks',{channel_id:_cur,source_id:parseInt(_('ts').value),
    stream_type:_('tt').value,url:_('tu').value.trim(),priority:parseInt(_('tp').value)||50});
  msg('tm',d.ok,d.ok?'Добавлено':d.error);
  if(d.ok){_('tu').value=''; loadTrks(); loadCh();}
}
async function updTrk(id){
  const p=parseInt(document.getElementById('tp-'+id).value);
  await post('/api/tracks/'+id,{priority:p},'PATCH'); loadTrks();
}
async function togTrkProfile(id,on){
  // on===true -> принудительно включить, on===false -> принудительно выключить, on===null -> сброс (как у источника)
  const profile=on===null?'':on?'gpu_h264':'off';
  const d=await post('/api/tracks/'+id,{profile},'PATCH');
  if(!d.ok){alert('Ошибка: '+(d.error||'?'));} loadTrks();
}
async function delTrk(id){
  if(!confirm('Удалить трек?')) return;
  await fetch('/api/tracks/'+id,{method:'DELETE'}); loadTrks(); loadCh();
}

// ── EPG tab ──
let _epgAll=[];
async function loadEpg(){
  const cfg=await api('/api/config');
  _('epg-url').value=cfg.epg_url||'';
  const d=await api('/api/epg/channels');
  _epgAll=d.channels||[];
  _('epg-cnt').textContent=_epgAll.length;
  renderEpgList(_epgAll.slice(0,200));
}
function renderEpgList(rows){
  _('epg-list').innerHTML=rows.map(r=>`
    <div style="display:flex;gap:10px;padding:4px 0;border-bottom:1px solid #1a1a3a">
      ${r.icon_url?`<img src="${z(r.icon_url)}" style="width:24px;height:24px;object-fit:contain" onerror="this.style.display='none'">`:'<span style="width:24px"></span>'}
      <span style="flex:1">${z(r.display_name)}</span>
      <span class="dim" style="font-size:11px">${z(r.epg_id)}</span>
    </div>`).join('')||'<span class="dim">Список пуст — нажмите «Скачать EPG»</span>';
}
function searchEpgList(v){
  const q=v.toLowerCase();
  const rows=(q?_epgAll.filter(r=>r.display_name.toLowerCase().includes(q)||r.epg_id.toLowerCase().includes(q)):_epgAll).slice(0,200);
  renderEpgList(rows);
}
async function saveEpgUrl(){
  const url=_('epg-url').value.trim();
  if(!url) return msg('epgm',false,'Укажите URL');
  const d=await post('/api/config',{epg_url:url},'PATCH');
  msg('epgm',d.ok,d.ok?'✓ Сохранено':'✗ '+d.error);
}
async function epgRefresh(){
  msg('epgm',true,'⟳ Скачиваю EPG…');
  const d=await post('/api/epg/refresh',{});
  msg('epgm',d.ok,d.ok?`✓ Загружено ${d.count} каналов EPG`:'✗ '+d.error);
  if(d.ok) loadEpg();
}
async function epgAutomap(){
  msg('epgm',true,'⚡ Авто-маппинг…');
  const d=await post('/api/epg/auto-map',{});
  msg('epgm',d.ok,d.ok?`✓ Замаппировано ${d.mapped} из ${d.total}`:'✗ '+d.error);
  if(d.ok) loadCh();
}

async function syncAll(){
  msg('hm',true,'⟳ Синхронизирую все…');
  const d=await post('/api/sync-all',{});
  msg('hm',d.ok,d.ok?`✓ +${d.total_channels} кан, ${d.total_tracks} треков`:'✗ '+d.error);
  if(d.ok){loadCh(); loadSrc();}
}

// ── add channel ──
function ov(id){_('ac-s').innerHTML=_src.map(s=>`<option value="${s.id}">${z(s.name)}</option>`).join('');document.getElementById(id).classList.add('open')}
function co(id){document.getElementById(id).classList.remove('open')}
function bg(e){return e.target===e.currentTarget}
async function doAC(){
  const name=_('ac-n').value.trim();
  if(!name) return msg('acm',false,'Укажите название');
  const d=await post('/api/channels',{name,group_name:_('ac-g').value.trim(),epg_id:_('ac-e').value.trim(),icon_url:_('ac-i').value.trim()});
  if(!d.ok) return msg('acm',false,d.error);
  const url=_('ac-u').value.trim();
  if(url&&d.id) await post('/api/tracks',{channel_id:d.id,source_id:parseInt(_('ac-s').value),stream_type:_('ac-t').value,url,priority:50});
  co('m-ac'); loadCh();
}
async function doMG(){
  msg('mgm',true,'⟳ Мигрирую…');
  const d=await post('/api/migrate',{tvlink_path:_('mg-p').value.trim()});
  msg('mgm',d.ok,d.ok?`✓ ${d.channels} каналов, ${d.tracks} треков`:'✗ '+d.error);
  if(d.ok){setTimeout(()=>co('m-mg'),1500); loadCh();}
}

// ── sources ──
async function loadSrc(){
  const d=await api('/api/sources'); _src=d.sources||[];
  ['ts','ac-s'].forEach(id=>{const el=document.getElementById(id);if(el)el.innerHTML=_src.map(s=>`<option value="${s.id}">${z(s.name)}</option>`).join('')});
  _('srb').innerHTML=_src.map(s=>`
    <tr id="sr-${s.id}">
      <td><b>${z(s.name)}</b></td>
      <td><span class="badge ${s.src_type==='enigma2'?'e2':'m3u'}">${s.src_type}</span>
          <span class="badge ${s.stream_type}">${s.stream_type.toUpperCase()}</span></td>
      <td><input type="number" id="sp-${s.id}" value="${s.priority}" style="width:58px;padding:2px 4px"></td>
      <td style="max-width:260px">
        <input id="su-${s.id}" value="${z(s.url||'')}" style="width:100%;max-width:240px;padding:2px 4px;font-size:12px"
          placeholder="${s.src_type==='enigma2'?'IP хост':'URL плейлиста'}">
        ${s.src_type==='enigma2'?`<div style="display:flex;gap:4px;margin-top:2px">
          <input id="sau-${s.id}" value="${z(s.auth_user||'')}" style="width:80px;padding:2px 4px;font-size:11px" placeholder="login">
          <input id="sap-${s.id}" value="${z(s.auth_pass||'')}" type="password" style="width:80px;padding:2px 4px;font-size:11px" placeholder="pass">
        </div>`:''}
      </td>
      <td class="dim" style="white-space:nowrap">${s.last_sync?s.last_sync.replace('T',' ').slice(0,16):'—'}</td>
      <td>${s.track_count}</td>
      <td class="${s.enabled?'on':'off'}">${s.enabled?'✓':'✗'}</td>
      <td style="text-align:center">
        <input type="checkbox" ${s.profile==='gpu_h264'?'checked':''} onchange="togSrcProfile(${s.id},this.checked)"
          title="Перекодировать этот источник на GPU (нужно, если сырой поток не смотрибелен на приставке)">
      </td>
      <td style="display:flex;gap:4px;flex-wrap:wrap">
        <button class="btn b-bl" onclick="saveSrc(${s.id})" title="Сохранить изменения">✓</button>
        <button class="btn b-or" onclick="syncSrc(${s.id})">↻</button>
        <button class="btn b-gy" onclick="openGroups(${s.id},'${z(s.name)}')" title="Группы каналов">☰ Группы</button>
        <button class="btn b-${s.enabled?'rd':'gr'}" onclick="togSrc(${s.id},${s.enabled})">${s.enabled?'Выкл':'Вкл'}</button>
        <button class="btn b-rd" onclick="delSrc(${s.id})">✕</button>
      </td>
    </tr>`).join('');
}
function onST(){const t=_('ns-t').value;_('ns-ul').textContent=t==='enigma2'?'Хост (IP)':'URL плейлиста';_('ns-s').value=t==='enigma2'?'ts':'hls'}
async function addSrc(){
  const name=_('ns-n').value.trim();
  if(!name) return msg('sam',false,'Укажите имя');
  const d=await post('/api/sources',{name,src_type:_('ns-t').value,stream_type:_('ns-s').value,url:_('ns-u').value.trim(),priority:parseInt(_('ns-p').value)||50});
  msg('sam',d.ok,d.ok?'✓ Добавлен':'✗ '+d.error); if(d.ok) loadSrc();
}
async function syncSrc(id){
  msg('srm',true,`⟳ Синхр. #${id}…`);
  const d=await post(`/api/sources/${id}/sync`,{});
  msg('srm',d.ok,d.ok?`✓ +${d.added_channels} кан, ${d.updated_tracks} треков`:'✗ '+d.error);
  if(d.ok){loadSrc(); loadCh();}
}
async function saveSrc(id){
  const p=parseInt(document.getElementById('sp-'+id).value);
  const u=document.getElementById('su-'+id).value.trim();
  const au_el=document.getElementById('sau-'+id);
  const ap_el=document.getElementById('sap-'+id);
  const body={priority:p,url:u};
  if(au_el) body.auth_user=au_el.value;
  if(ap_el) body.auth_pass=ap_el.value;
  const d=await post('/api/sources/'+id,body,'PATCH');
  msg('srm',d.ok,d.ok?'✓ Сохранено':'✗ '+d.error);
  if(d.ok) loadSrc();
}
async function togSrc(id,cur){await post('/api/sources/'+id,{enabled:cur?0:1},'PATCH');loadSrc()}
async function togSrcProfile(id,on){
  const d=await post('/api/sources/'+id,{profile:on?'gpu_h264':''},'PATCH');
  if(!d.ok){alert('Ошибка: '+(d.error||'?'));} loadSrc();
}
async function delSrc(id){if(!confirm('Удалить источник и все его треки?'))return;await fetch('/api/sources/'+id,{method:'DELETE'});loadSrc();loadCh()}

// ── Groups modal ──
let _grpSrcId=null, _grpData=[];
async function openGroups(id, name){
  _grpSrcId=id; _grpData=[];
  _('grp-title').textContent='Группы: '+name;
  _('grp-body').innerHTML='<span class="dim">Загрузка…</span>';
  ov('m-grp');
  const d=await api(`/api/sources/${id}/groups`);
  _grpData=d.groups||[];
  renderGroups();
}
function renderGroups(){
  _('grp-body').innerHTML=_grpData.map((g,i)=>`
    <div style="border-bottom:1px solid #1a1a3a;padding:6px 0">
      <div style="display:flex;align-items:center;gap:8px;cursor:pointer" onclick="toggleGrpExpand(${i})">
        <input type="checkbox" id="gc-${i}" ${g.filter_enabled?'checked':''} onclick="event.stopPropagation();grpCheck(${i},this.checked)">
        <b style="flex:1">${z(g.group)}</b>
        <span class="dim" style="font-size:11px">${g.enabled_cnt}/${g.total} включено</span>
        <span id="ge-${i}" style="font-size:11px;color:#4488ff">▶</span>
      </div>
      <div id="gch-${i}" style="display:none;margin-left:24px;margin-top:4px;max-height:200px;overflow-y:auto"></div>
    </div>`).join('')||'<span class="dim">Нет групп. Сначала синхронизируйте источник.</span>';
}
async function toggleGrpExpand(i){
  const el=_(`gch-${i}`);
  if(el.style.display==='none'){
    const icon=_(`ge-${i}`);
    icon.textContent='▼';
    el.style.display='block';
    if(!el.innerHTML){
      el.innerHTML='<span class="dim">Загрузка…</span>';
      const grp=_grpData[i].group;
      const d=await api(`/api/sources/${_grpSrcId}/groups/${encodeURIComponent(grp)}/channels`);
      el.innerHTML=(d.channels||[]).map(c=>`
        <div style="display:flex;align-items:center;gap:6px;padding:2px 0">
          <input type="checkbox" data-ch="${c.id}" ${c.enabled?'checked':''}>
          <span style="font-size:12px">${z(c.name)}</span>
        </div>`).join('')||'<span class="dim">Нет каналов</span>';
    }
  } else {
    el.style.display='none'; _(`ge-${i}`).textContent='▶';
  }
}
function grpCheck(i, v){
  _grpData[i].filter_enabled=v?1:0;
  // обновляем чекбоксы каналов в развёрнутой группе
  const el=_(`gch-${i}`);
  if(el) el.querySelectorAll('input[data-ch]').forEach(cb=>cb.checked=v);
}
function grpAll(v){
  _grpData.forEach((g,i)=>{g.filter_enabled=v;const cb=_(`gc-${i}`);if(cb)cb.checked=v;});
}
async function saveGroups(){
  const groups=_grpData.map(g=>({group:g.group,enabled:g.filter_enabled}));
  // Собираем индивидуальные изменения каналов
  const channels=[];
  document.querySelectorAll('#grp-body input[data-ch]').forEach(cb=>{
    channels.push({id:cb.dataset.ch,enabled:cb.checked?1:0});
  });
  const d=await post(`/api/sources/${_grpSrcId}/groups/filters`,{groups,channels});
  msg('grpm',d.ok,d.ok?'✓ Сохранено':'✗ '+d.error);
  if(d.ok){setTimeout(()=>co('m-grp'),800); loadCh(); loadSrc();}
}

async function loadLogs(){
  const d=await api('/api/logs');
  const b=_('lb');
  b.innerHTML=(d.logs||[]).map(l=>{const c=l.includes('ERROR')?'err':l.includes('WARNING')?'wrn':'';return`<div class="${c}">${z(l)}</div>`}).join('');
  b.scrollTop=b.scrollHeight;
}

function _(id){return document.getElementById(id)}
function z(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
function msg(id,ok,txt){const el=_(id);if(!el)return;el.textContent=txt;el.style.color=ok?'#00cc66':'#cc4444';setTimeout(()=>{if(el.textContent===txt)el.textContent=''},6000)}
async function api(u){return(await fetch(u)).json()}
async function post(u,b,m='POST'){return(await fetch(u,{method:m,headers:{'Content-Type':'application/json'},body:JSON.stringify(b)})).json()}

// ── TSB Dashboard ─────────────────────────────────────────────────────────────
let _tsbEs=null;
const _tsbColors={rising:'↑',falling:'↓',stable:'→'};
function _tsbBufColor(pct,state){
  if(state==='danger')return'#ff4444';if(state==='warn')return'#ffaa00';
  if(pct<30)return'#4488ff';if(pct<80)return'#00aaff';return'#00ff88';
}
function _tsbCard(s){
  const pct=Math.min(100,s.target_sec>0?Math.round(s.buffer_sec/s.target_sec*100):0);
  const state=s.state||'startup';
  const bc=_tsbBufColor(pct,state);
  const srcLabel=s.source_name||s.source_type;
  const srcColor=s.source_type==='ts'?'#00cc88':'#4488ff';
  const typeTag=`<span style="color:${srcColor}">${z(srcLabel)}</span>`;
  const fbTag=s.source_idx>0?' <span style="color:#ffaa00">▲резерв</span>':'';
  const iconHtml=s.icon_url?`<img src="${z(s.icon_url)}" style="height:28px;width:28px;object-fit:contain;border-radius:4px;margin-right:8px;background:#0a0a18" onerror="this.style.display='none'">`:'';
  const epgHtml=s.epg?`<div style="color:#999;font-size:11px;margin-top:3px">${z(s.epg.start)}–${z(s.epg.stop)}&nbsp;<span style="color:#ccc">${z(s.epg.title)}</span></div>`:'';
  const urlHtml=s.stream_url?`<div style="font-size:10px;margin-top:4px;display:flex;align-items:center;gap:6px"><span class="dim" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:260px" title="${z(s.stream_url)}">${z(s.stream_url)}</span><button onclick="navigator.clipboard.writeText('${z(s.stream_url)}')" style="background:none;border:1px solid #333;color:#666;border-radius:3px;padding:1px 6px;cursor:pointer;font-size:10px">⎘</button></div>`:'';
  return`<div style="background:#13132a;border-radius:8px;border:1px solid #1e1e3a;padding:14px">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px">
      <div style="display:flex;align-items:center;flex:1;min-width:0">${iconHtml}<div style="min-width:0"><div style="color:#e0e0e0;font-size:14px;font-weight:bold">${z(s.name)}</div>${epgHtml}${urlHtml}</div></div>
      <span style="padding:2px 10px;border-radius:4px;font-size:11px;font-weight:bold;text-transform:uppercase;flex-shrink:0;margin-left:8px;background:${state==='normal'?'#003322':state==='startup'?'#222':'#330000'};color:${state==='normal'?'#00ff88':state==='startup'?'#666':'#ff4444'}">${state}</span>
    </div>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px">
      <div><div style="color:#555;font-size:10px">Клиенты</div><div style="color:#ddd">${s.clients} 📺</div>${(s.client_ips&&s.client_ips.length)?`<div style="color:#7a9fd6;font-size:10px;word-break:break-all" title="IP клиентов">${s.client_ips.map(z).join(', ')}</div>`:''}${s.drops>0?`<div style="color:#ff6666;font-size:10px" title="кусков потока потеряно из-за переполнения очереди клиента">потери: ${s.drops}</div>`:''}</div>
      <div><div style="color:#555;font-size:10px">Скорость</div><div style="color:#ddd">${s.speed_mbps} М ${_tsbColors[s.trend]||'→'}</div></div>
      <div><div style="color:#555;font-size:10px">Источник</div><div>${typeTag}${fbTag}</div></div>
      <div><div style="color:#555;font-size:10px">Reconnects</div><div style="color:#ddd">${s.reconnects_60s}/мин</div></div>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:11px;color:#555;margin-bottom:4px"><span>Буфер</span><span>${s.buffer_sec}с&nbsp;/&nbsp;${s.target_sec}с <b style="color:${bc}">${pct}%</b></span></div>
    <div style="background:#0a0a18;border-radius:4px;height:10px;overflow:hidden;border:1px solid #1e1e3a"><div style="height:100%;border-radius:4px;width:${pct}%;background:${bc};transition:width .6s"></div></div>
  </div>`;
}
function _tsbRender(sessions){
  _('tsb-sess-cnt').textContent=sessions.length;
  _('tsb-cli-cnt').textContent=sessions.reduce((a,s)=>a+s.clients,0);
  const g=_('tsb-grid');
  g.innerHTML=sessions.length?sessions.map(_tsbCard).join(''):'<div style="text-align:center;padding:40px;color:#333">Нет активных каналов</div>';
}
function initTsb(){
  if(_tsbEs)return;
  fetch('/tsb/config').then(r=>r.json()).then(c=>{
    _('tsb-buf').value=c.buffer_seconds??12;
    _('tsb-buf-ts').value=c.buffer_seconds_ts??3;
    _('tsb-mbps').value=c.bitrate_mbps??12;
    _('tsb-fallback').value=c.hls_fallback_secs??10;
    _('tsb-check').value=c.fallback_check_secs??900;
    _('tsb-idle').value=c.session_idle_timeout??30;
    _('tsb-hls-ct').value=c.hls_connect_timeout??5;
    _('tsb-hls-rt').value=c.hls_read_timeout??12;
    _('tsb-hls-st').value=c.hls_seg_timeout??30;
    _('tsb-ts-ct').value=c.ts_connect_timeout??5;
    _('tsb-ts-rt').value=c.ts_read_timeout??15;
    _('tsb-ts-qw').value=c.ts_quality_window??5;
    _('tsb-ts-tei').value=c.ts_tei_threshold??0.02;
    _('tsb-ts-cc').value=c.ts_cc_threshold??0.01;
    _('tsb-aud-lang').value=c.hls_audio_lang??'rus';
    _('tsb-aud-br').value=c.force_audio_bitrate??256;
    _('tsb-aud-force').checked=!!c.force_audio_aac;
    _('tsb-tvh-poll').value=c.tvh_quality_poll_secs??10;
    _('tsb-tvh-hard').value=c.tvh_quality_hard_delta??30;
    _('tsb-vid-unify').checked=!!c.force_video_unify;
    _('tsb-vid-max').value=c.max_transcodes??3;
  });
  _tsbEs=new EventSource('/tsb/events');
  _tsbEs.onmessage=e=>{const d=JSON.parse(e.data);_tsbRender(d.sessions||[]);};
  _tsbEs.onerror=()=>{_tsbEs.close();_tsbEs=null;setTimeout(()=>{if(document.getElementById('pane-tsbuf').classList.contains('active'))initTsb();},5000);};
}
function saveTsbConfig(sec){
  let body={};
  const msgEl=sec===2?'tsb-cfg-msg2':sec===3?'tsb-cfg-msg3':sec===4?'tsb-cfg-msg4':sec===5?'tsb-cfg-msg5':sec===6?'tsb-cfg-msg6':'tsb-cfg-msg';
  if(sec===1||sec===undefined){
    body.buffer_seconds=parseInt(_('tsb-buf').value);
    body.buffer_seconds_ts=parseInt(_('tsb-buf-ts').value);
    body.bitrate_mbps=parseFloat(_('tsb-mbps').value);
    body.hls_fallback_secs=parseInt(_('tsb-fallback').value);
    body.fallback_check_secs=parseInt(_('tsb-check').value);
    body.session_idle_timeout=parseInt(_('tsb-idle').value);
  } else if(sec===2){
    body.hls_connect_timeout=parseInt(_('tsb-hls-ct').value);
    body.hls_read_timeout=parseInt(_('tsb-hls-rt').value);
    body.hls_seg_timeout=parseInt(_('tsb-hls-st').value);
    body.ts_connect_timeout=parseInt(_('tsb-ts-ct').value);
    body.ts_read_timeout=parseInt(_('tsb-ts-rt').value);
  } else if(sec===3){
    body.ts_quality_window=parseFloat(_('tsb-ts-qw').value);
    body.ts_tei_threshold=parseFloat(_('tsb-ts-tei').value);
    body.ts_cc_threshold=parseFloat(_('tsb-ts-cc').value);
  } else if(sec===4){
    body.hls_audio_lang=_('tsb-aud-lang').value.trim()||'rus';
    body.force_audio_bitrate=parseInt(_('tsb-aud-br').value)||256;
    body.force_audio_aac=_('tsb-aud-force').checked;
  } else if(sec===5){
    body.tvh_quality_poll_secs=parseInt(_('tsb-tvh-poll').value)||10;
    body.tvh_quality_hard_delta=parseInt(_('tsb-tvh-hard').value)||30;
  } else if(sec===6){
    body.force_video_unify=_('tsb-vid-unify').checked;
    body.max_transcodes=parseInt(_('tsb-vid-max').value)||3;
  }
  post('/tsb/config',body).then(d=>msg(msgEl,d.ok,'✓ Применено'));
}
function tsbRefreshDb(){
  _('tsb-act-msg').textContent='↻ Обновляю...';_('tsb-act-msg').style.color='#ffaa00';
  post('/tsb/refresh-db',{}).then(d=>{msg('tsb-act-msg',d.ok,d.ok?`✓ ${d.channels} каналов`:'✗ Ошибка');});
}
function tsbRestart(){
  if(!confirm('Перезапустить сервис?\nВсе стримы прервутся на 2-3 сек.'))return;
  _('tsb-act-msg').textContent='⟳ Перезапуск...';_('tsb-act-msg').style.color='#ffaa00';
  post('/tsb/restart',{}).then(()=>{setTimeout(()=>location.reload(),4000);});
}

(async()=>{
  const d=await api('/api/config');
  _('h-port').textContent=d.port||3002;
  await Promise.all([loadCh(),loadSrc()]);
})();
</script>
</body>
</html>"""

# ─── TSB STREAMING ENGINE ────────────────────────────────────────────────────

_TSB_NULL_PKT   = bytes([0x47, 0x1F, 0xFF, 0x10]) + bytes(184)
_TSB_NULL_CHUNK = _TSB_NULL_PKT * 188
_TSB_T0         = __import__("time").monotonic()


def _detect_src_type(url: str) -> str:
    return "ts" if (":8001/" in url or "/stream/channel/" in url) else "hls"


def _parse_m3u8(content: str, base_url: str) -> tuple[float, list[tuple[int, str]]]:

    target_dur = 8.0
    segs: list[tuple[int, str]] = []
    seq = 0
    want_url = False
    for line in content.strip().splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try: seq = int(line.split(":", 1)[1])
            except Exception: pass
        elif line.startswith("#EXT-X-TARGETDURATION:"):
            try: target_dur = float(line.split(":", 1)[1])
            except Exception: pass
        elif line.startswith("#EXTINF:"):
            want_url = True
        elif want_url and line and not line.startswith("#"):
            segs.append((seq, line if line.startswith("http") else urljoin(base_url, line)))
            seq += 1; want_url = False
        elif line and not line.startswith("#"):
            segs.append((seq, line if line.startswith("http") else urljoin(base_url, line)))
            seq += 1
    return target_dur, segs


def _parse_master_m3u8(content: str, base_url: str) -> list[tuple[int, str]]:

    variants: list[tuple[int, str]] = []
    bw = 0
    for line in content.strip().splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-STREAM-INF:"):
            m = re.search(r'BANDWIDTH=(\d+)', line)
            bw = int(m.group(1)) if m else 0
        elif line and not line.startswith("#"):
            variants.append((bw, line if line.startswith("http") else urljoin(base_url, line)))
            bw = 0
    variants.sort(key=lambda x: -x[0])
    return variants


_TS_NULL = b"\x47\x1f\xff\x10" + b"\xff" * 184   # пустой TS-пакет для keepalive


def _chunk_pcr(chunk: bytes):
    """Первый PCR (сек) в куске TS или None: adaptation field + флаг PCR (0x10), 33-битная база 90 кГц."""
    for i in range(0, len(chunk) - 187, 188):
        if chunk[i + 3] & 0x20 and chunk[i + 4] >= 7 and chunk[i + 5] & 0x10:
            return ((chunk[i + 6] << 25) | (chunk[i + 7] << 17) | (chunk[i + 8] << 9)
                    | (chunk[i + 9] << 1) | (chunk[i + 10] >> 7)) / 90000.0
    return None


# Запас HLS по каналу: с какого сегмента от «живого края» начинаем (3 сегмента = ~18 с). После каждого опустошения
# очереди (обрыв сети/источника) для этого канала растёт до 5 (~30 с), через 6 ч без обрывов забывается.
_HLS_BACK_FILE = "/opt/channel-manager/hls_back.json"

def _hls_back_load() -> dict:
    try:
        with open(_HLS_BACK_FILE) as f:
            return {k: (int(v[0]), float(v[1])) for k, v in json.load(f).items()}
    except Exception:
        return {}

_HLS_BACK: dict = _hls_back_load()   # ch_id -> (сегментов назад, время последнего роста); переживает перезапуск сервиса

def _hls_back(ch_id: str) -> int:
    v = _HLS_BACK.get(ch_id)
    if v and time.time() - v[1] < 6 * 3600:
        return v[0]
    return int(CONFIG.get("hls_start_back", 3))

def _hls_back_grow(ch_id: str):
    _HLS_BACK[ch_id] = (min(5, _hls_back(ch_id) + 1), time.time())
    try:
        with open(_HLS_BACK_FILE, "w") as f:
            json.dump({k: list(v) for k, v in _HLS_BACK.items()}, f)
    except Exception:
        pass


async def _drain_stderr(proc, tail: deque, tag: str = ""):
    """Копит последние строки stderr ffmpeg (иначе причина молчания потока неизвестна и pipe может забиться).
    Если задан tag — предупреждения о повреждённых кадрах/ошибках декодера пишутся в журнал (не чаще раза в 5 с)."""
    last_log = 0.0
    try:
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            txt = line.decode("utf-8", "replace").strip()[:150]
            tail.append(txt)
            if tag and re.search(r"rror|orrupt|nvalid|missing|concealing|discard|Non-monotonic|too large", txt):
                now = time.monotonic()
                if now - last_log > 5:
                    last_log = now
                    log.warning(f"{tag} ffmpeg: {txt}")
    except Exception:
        pass


class _SegFail(Exception):
    """Сегмент не докачан за все попытки; .got — сколько байт уже отдано (докачаем с этого места, а не пропустим)."""
    def __init__(self, msg: str, got: int):
        super().__init__(msg)
        self.got = got


class _Restart(Exception):
    """Перезапуск чтения того же источника с уточнёнными параметрами (не сбой источника)."""

# Результаты ffprobe/idet по потокам держим в файле: после перезапуска первый старт канала не ждёт пробу
_PROBE_FILE = "/opt/channel-manager/probe_cache.json"
_PROBE_TTL = 24 * 3600

def _probe_load() -> dict:
    try:
        with open(_PROBE_FILE) as f:
            return {k: (v[0], v[1]) for k, v in json.load(f).items()}
    except Exception:
        return {}

def _probe_save(cache: dict):
    try:
        with open(_PROBE_FILE, "w") as f:
            json.dump({k: list(v) for k, v in cache.items()}, f)
    except Exception:
        pass

class TsErrorTracker:
    """Отслеживает TEI и CC-ошибки в TS-потоке для детектирования деградации сигнала."""

    def __init__(self, window_secs: float = 5.0,
                 tei_threshold: float = 0.02,
                 cc_threshold:  float = 0.01):
        self._window        = window_secs
        self._tei_threshold = tei_threshold
        self._cc_threshold  = cc_threshold
        self._events: deque = deque()
        self._cc_state: dict[int, int] = {}

    def scan(self, data: bytes) -> tuple[int, int, int]:
        tei_err = cc_err = total = 0
        pos = 0
        while pos + 188 <= len(data):
            if data[pos] != 0x47:
                pos += 1
                continue
            b1 = data[pos + 1]; b2 = data[pos + 2]; b3 = data[pos + 3]
            pid = ((b1 & 0x1F) << 8) | b2
            if b1 & 0x80:
                tei_err += 1
            if pid != 0x1FFF:
                afc = (b3 >> 4) & 0x03
                if afc in (0x01, 0x03):
                    curr_cc = b3 & 0x0F
                    disc = False
                    if afc == 0x03 and pos + 5 <= len(data):
                        afl = data[pos + 4]
                        if afl > 0 and pos + 5 < len(data):
                            disc = bool(data[pos + 5] & 0x80)
                    if pid in self._cc_state and not disc:
                        expected = (self._cc_state[pid] + 1) & 0x0F
                        if curr_cc != expected:
                            cc_err += 1
                    self._cc_state[pid] = curr_cc
            total += 1
            pos += 188
        return tei_err, cc_err, total

    def push(self, tei_err: int, cc_err: int, total: int):
        now = time.monotonic()
        self._events.append((now, tei_err, cc_err, total))
        while self._events and self._events[0][0] < now - self._window:
            self._events.popleft()

    def rates(self) -> tuple[float, float]:
        tei_sum = cc_sum = total_sum = 0
        for _, t, c, n in self._events:
            tei_sum += t; cc_sum += c; total_sum += n
        if total_sum == 0:
            return 0.0, 0.0
        return tei_sum / total_sum, cc_sum / total_sum

    def is_degraded(self) -> tuple[bool, float, float]:
        tei_rate, cc_rate = self.rates()
        tei_bad = self._tei_threshold > 0 and tei_rate > self._tei_threshold
        cc_bad  = self._cc_threshold  > 0 and cc_rate  > self._cc_threshold
        degraded = tei_bad or (cc_bad and tei_rate >= 0.005)
        return degraded, tei_rate, cc_rate


class _TsbMetrics:
    def __init__(self, window: float = 10.0):
        self._w = window
        self._s: deque = deque()
    def record(self, n: int):
        now = time.monotonic()
        self._s.append((now, n))
        cutoff = now - self._w
        while self._s and self._s[0][0] < cutoff:
            self._s.popleft()
    def speed(self) -> float:
        if len(self._s) < 2: return 0.0
        t = self._s[-1][0] - self._s[0][0]
        return sum(x[1] for x in self._s) / t if t > 0 else 0.0
    def trend(self) -> str:
        if len(self._s) < 4: return "stable"
        mid = len(self._s) // 2
        sl = list(self._s)
        def sp(p):
            t = p[-1][0] - p[0][0]; return sum(x[1] for x in p) / t if t > 0 else 0
        s1, s2 = sp(sl[:mid]), sp(sl[mid:])
        return "falling" if s2 < s1 * 0.8 else ("rising" if s2 > s1 * 1.2 else "stable")


class StreamSession:
    """Один буферизованный канал: управляет источниками и клиентами."""

    def __init__(self, ch_id: str, sources: list[tuple[str, str]], name: str):
        self.ch_id    = ch_id
        self.name     = name or ch_id
        self._tag     = f"[{self.name}]"
        self._sources = sources
        self._buf: deque = deque()
        self._clients: list[tuple[str, asyncio.Queue]] = []
        self.metrics  = _TsbMetrics()
        self._src_idx  = 0
        self._src_type = sources[0][0] if sources else "hls"
        self._src_name = sources[0][2] if sources and len(sources[0]) > 2 else ""
        self.running  = True
        self._rc      = 0
        self._rc_times: deque = deque()
        self._last_data = time.monotonic()
        self._state   = "startup"
        self._switch_requested = False
        self._next_check_at: float = 0.0
        self._tail = b""
        self._drops = 0
        self._drop_log_at = 0.0
        self._paced = False        # выравнивание выдачи по PCR (HLS)
        self._pq: deque = deque()   # (кусок, PCR)
        self._pq_bytes = 0
        self._pq_task = None
        self._pq_dropped = 0
        self._pq_drop_log = 0.0
        self._fail: dict = {}  # idx источника -> (число ошибок подряд, время последней)
        self._starve_hist: dict = {}  # idx источника -> deque(monotonic-время эпизодов просадки) за последние 15 мин
        self._last_push = time.monotonic()   # когда последний раз данные реально вошли в конвейер (для сторожа)
        self._reader_task = None
        self._hls_seq: dict = {}   # ключ плейлиста -> (последний выданный номер сегмента, когда): отсев дублей при перезапуске читателя
        self._sink = None     # приёмник вместо _push (перекодирование HLS: читатель кормит ffmpeg через stdin)
        self._host_override: dict = {}   # idx источника -> хост HLS-сервера, на который переключились
        self._rot_hist: dict = {}        # idx -> времена смен сервера (не больше 2 за 60 с)

    def _cfg(self) -> dict:
        return CONFIG

    def _buf_secs(self) -> float:
        """Целевой буфер: для живого TS с Энигмы хватает 5 сек, для HLS — buffer_seconds (12)."""
        if self._src_type == "ts":
            return CONFIG.get("buffer_seconds_ts", 5)
        return CONFIG["buffer_seconds"]

    def _sec2chunks(self, s: float) -> int:
        bps = CONFIG["bitrate_mbps"] * 125_000
        return max(1, int(bps * s / CONFIG["chunk_size"]))

    def _chunks2sec(self, n: int) -> float:
        bps = CONFIG["bitrate_mbps"] * 125_000
        return n * CONFIG["chunk_size"] / bps

    def _push(self, chunk: bytes):
        # клиентам отдаём только целые TS-пакеты (кратно 188): иначе keepalive-пакеты
        # вставились бы посреди пакета и сломали синхронизацию
        if self._tail:
            chunk = self._tail + chunk
        n = len(chunk) - len(chunk) % 188
        self._tail = chunk[n:]
        if n == 0:
            return
        if n != len(chunk):
            chunk = chunk[:n]
        self._last_data = time.monotonic()
        self._last_push = self._last_data
        if self._paced:
            # HLS приходит пачками по сегментам: выдаём клиентам ровно, по PCR самого потока
            self._pq.append((chunk, _chunk_pcr(chunk)))
            self._pq_bytes += len(chunk)
            if self._pq_task is None or self._pq_task.done():
                self._pq_task = asyncio.create_task(self._playout())
            return
        self._deliver(chunk)

    async def _wait_room(self):
        """Обратное давление вместо сброса: если очередь выравнивания полна, читатель/ffmpeg ждут, пока выдача её разгрузит.
        Раньше при переполнении срезались самые старые куски (те, что вот-вот пойдут клиенту) — дыры в потоке."""
        cap = CONFIG.get("pace_max_bytes", 200_000_000)
        while self._paced and self._pq_bytes > cap * 0.9 and self.running:
            await asyncio.sleep(0.05)

    def _cushion_s(self) -> float:
        """Сколько секунд потока лежит в очереди выравнивания (по PCR); это запас, который спасает клиента при просадке источника."""
        now = time.monotonic()
        c = getattr(self, "_cush_cache", None)
        if c and now - c[0] < 0.5:
            return c[1]
        pcrs = [pc for _, pc in self._pq if pc is not None]
        val = (pcrs[-1] - pcrs[0]) if len(pcrs) >= 2 and pcrs[-1] >= pcrs[0] else 0.0
        self._cush_cache = (now, val)
        return val

    def _deliver(self, chunk: bytes):
        self._buf.append(chunk)
        self.metrics.record(len(chunk))
        max_c = self._sec2chunks(self._buf_secs())
        while len(self._buf) > max_c:
            self._buf.popleft()
        for cip, q in self._clients:
            try: q.put_nowait(chunk)
            except asyncio.QueueFull:
                # клиент не успевает забирать: потерянный кусок = провал в потоке (скачок вперёд у плеера)
                self._drops += 1
                now = time.monotonic()
                if now - self._drop_log_at > 10:
                    self._drop_log_at = now
                    log.warning(f"{self._tag} клиент {cip}: очередь переполнена, потеряно кусков всего: {self._drops}")

    async def _playout(self):
        """Выдача из очереди по расписанию PCR: кусок уходит клиентам тогда, когда наступило его время.
        Старт — после накопления pace_start_s секунд по PCR; разрыв PCR (склейка сегментов) или отставание
        больше секунды — переякорение без ожидания; опустошение очереди — заново набираем запас."""
        start_s = CONFIG.get("pace_start_s", 8.0)
        max_bytes = CONFIG.get("pace_max_bytes", 200_000_000)   # ~2 мин потока 12 Мбит/с; 40 МБ срезали куски прямо перед выдачей (фризы)
        anchored = False
        first_anchor = True
        underruns: list = []      # моменты опустошения очереди за последние 10 мин: чем чаще, тем больше запас добираем
        rebuf_need = CONFIG.get("pace_rebuffer_s", 3.0)
        wall0 = pcr0 = last_pcr = None
        buffering_since = time.monotonic()
        while self.running:
            if not self._pq:
                if anchored and time.monotonic() - buffering_since > 1.0:
                    # очередь опустела в работе: источник/сеть не успевали — в следующий раз держим запас больше
                    _hls_back_grow(self.ch_id)
                    now_u = time.monotonic()
                    underruns = [t for t in underruns if now_u - t < 600] + [now_u]
                    # источник идёт ровно в реальном времени: запас после обрыва сам не восстановится, поэтому при повторах
                    # перед возобновлением выдачи копим больше (3 → 6 → 9 → 12 с)
                    rebuf_need = min(12.0, CONFIG.get("pace_rebuffer_s", 3.0) + 3.0 * (len(underruns) - 1))
                    log.info(f"{self._tag} очередь выравнивания опустела → запас канала {_hls_back(self.ch_id)} сегм.; "
                             f"добор перед выдачей {rebuf_need:.0f} с (обрывов за 10 мин: {len(underruns)})")
                anchored = False
                buffering_since = time.monotonic()
                await asyncio.sleep(0.05)
                continue
            if not anchored:
                pcrs = [pc for _, pc in self._pq if pc is not None]
                span = (pcrs[-1] - pcrs[0]) if len(pcrs) >= 2 and pcrs[-1] >= pcrs[0] else 0.0
                # PCR в очереди нет (или один) — выравнивать нечем, не задерживаем выдачу
                need = start_s if first_anchor else rebuf_need   # первый старт — полный запас, после обрыва — адаптивный добор
                if len(pcrs) >= 2 and span < need and time.monotonic() - buffering_since < need + 4:
                    await asyncio.sleep(0.05)
                    continue
                anchored = True
                first_anchor = False
                wall0, pcr0, last_pcr = time.monotonic(), (pcrs[0] if pcrs else None), None
            chunk, pcr = self._pq.popleft()
            self._pq_bytes -= len(chunk)
            if pcr is not None:
                now = time.monotonic()
                if pcr0 is None or (last_pcr is not None and (pcr < last_pcr - 0.05 or pcr > last_pcr + 2.0)):
                    wall0, pcr0 = now, pcr            # разрыв PCR: продолжаем от текущего момента
                else:
                    due = wall0 + (pcr - pcr0)
                    if due - now < -1.0:              # отстаём больше секунды: не догоняем пачкой
                        wall0, pcr0 = now, pcr
                    elif due > now:
                        await asyncio.sleep(due - now)
                last_pcr = pcr
            self._deliver(chunk)
            while self._pq_bytes > max_bytes * 1.5 and self._pq:      # последняя линия обороны (при обратном давлении не должна срабатывать)
                dropped, _ = self._pq.popleft()
                self._pq_bytes -= len(dropped)
                self._pq_dropped += len(dropped)
                if time.monotonic() - self._pq_drop_log > 5:
                    self._pq_drop_log = time.monotonic()
                    log.warning(f"{self._tag} очередь выравнивания переполнена ({max_bytes // 1000000} МБ): срезано {self._pq_dropped // 1024} КБ — дыра в потоке")

    async def _supervisor(self):
        """Сторож сессии: читатель умер (необработанное исключение) или молчит при подключённых клиентах —
        перезапускаем и пишем в журнал, где он стоял. Ловит «тихие» зависания, которые внутренние сторожа не видят."""
        idle_limit = CONFIG.get("supervisor_idle_secs", 45)
        while self.running:
            await asyncio.sleep(5)
            t = self._reader_task
            if t is None:
                continue
            if t.done():
                exc = None if t.cancelled() else t.exception()
                log.error(f"{self._tag} читатель источника завершился ({exc!r}) → перезапуск")
                self._reader_task = asyncio.create_task(self._reader())
                self._last_push = time.monotonic()
                continue
            silent = time.monotonic() - self._last_push
            if self._clients and silent > idle_limit:
                sio = io.StringIO()
                try: t.print_stack(limit=10, file=sio)
                except Exception: pass
                log.error(f"{self._tag} читатель завис: {silent:.0f} с без данных при клиентах → перезапуск; стек: "
                          + " | ".join(l.strip() for l in sio.getvalue().splitlines() if l.strip())[-600:])
                t.cancel()
                await asyncio.sleep(0.5)
                self._sink = None
                self._pq.clear(); self._pq_bytes = 0
                self._reader_task = asyncio.create_task(self._reader())
                self._last_push = time.monotonic()

    async def start(self):
        self._reader_task = asyncio.create_task(self._reader())
        asyncio.create_task(self._supervisor())
        if CONFIG.get("fallback_check_secs", 0) > 0 and len(self._sources) > 1:
            asyncio.create_task(self._primary_checker())
        if len(self._sources) > 1 and any("/stream/channel/" in s[1] for s in self._sources):
            asyncio.create_task(self._tvh_quality_checker())

    async def _tvh_quality_checker(self):
        """Доп. ранний сигнал деградации для TVHeadend-источников: счётчик `errors` из его же
        /api/status/subscriptions (демультиплексор/дескремблинг) — TVHeadend видит проблему раньше,
        чем она долетит до нас испорченными TS-байтами. Не вводит свой механизм переключения — только
        форсирует тот же self._fail/_switch_requested, которым уже пользуются TEI/CC и детектор просадки
        (см. _read_ts), и тот же хронический счётчик (2/5мин, 3/10мин, 4/15мин). Сопоставление по имени
        канала — реагирует только на СВОЮ подписку, не трогает соседей по тюнеру/транспондеру."""
        interval = CONFIG.get("tvh_quality_poll_secs", 10)
        hard_delta = CONFIG.get("tvh_quality_hard_delta", 30)
        baseline: dict = {}
        hist: dict = {}
        while self.running:
            await asyncio.sleep(interval)
            if not self.running:
                return
            idx = self._src_idx
            if idx >= len(self._sources):
                continue
            src_type, src_url = self._sources[idx][:2]
            if src_type != "ts" or "/stream/channel/" not in src_url:
                continue
            try:
                u = urlsplit(src_url)
                auth = aiohttp.BasicAuth(unquote(u.username), unquote(u.password)) if u.username else None
                tc = aiohttp.ClientTimeout(total=8)
                async with aiohttp.ClientSession(timeout=tc, auth=auth) as sess:
                    async with sess.get(f"http://{u.hostname}:{u.port or 9981}/api/status/subscriptions") as r:
                        if r.status != 200:
                            continue
                        data = await r.json(content_type=None)
            except Exception:
                continue
            entry = next((s for s in data.get("entries", []) if s.get("channel") == self.name), None)
            if entry is None:
                continue
            errors_now = entry.get("errors", 0)
            prev = baseline.get(idx)
            baseline[idx] = (errors_now, time.monotonic())
            if prev is None or errors_now < prev[0]:
                continue   # первый замер или подписка переоткрылась (счётчик сброшен) — пока без дельты
            delta = errors_now - prev[0]
            if delta <= 0:
                continue
            now = time.monotonic()
            h = hist.setdefault(idx, deque())
            h.append(now)
            while h and now - h[0] > 900:
                h.popleft()
            c5, c10, c15 = (sum(1 for t in h if now - t <= w) for w in (300, 600, 900))
            chronic = c5 >= 2 or c10 >= 3 or c15 >= 4
            hard = delta >= hard_delta
            log.warning(f"{self._tag} TVHeadend: +{delta} ошибок демультиплексора за {interval:.0f}с "
                        f"(эпизодов: {c5}/5мин {c10}/10мин {c15}/15мин)")
            if hard or chronic:
                log.warning(f"{self._tag} TVHeadend качество деградировало "
                            f"({'скачок' if hard else 'хронически'}) → переключаю источник [{idx}]")
                self._fail[idx] = (2, now)
                self._switch_requested = True

    def _kick_all_clients(self):
        for _, q in list(self._clients):
            try: q.put_nowait(None)
            except asyncio.QueueFull: pass

    def _with_host(self, idx: int, url: str) -> str:
        host = self._host_override.get(idx)
        return re.sub(r"^(https?://)[^/]+", lambda m: m.group(1) + host, url) if host else url

    def _rotate_host(self, idx: int, url: str):
        """У HLS-провайдера несколько серверов с одним токеном (ru3.hls.gd / 6.hls.gd): при сбое пробуем соседний."""
        hosts = CONFIG.get("hls_alt_hosts", [])   # 6.hls.gd отдаёт заглушку (stub_domain*.ts), автопереключение выключено
        m = re.match(r"^https?://([^/]+)", url)
        cur = self._host_override.get(idx) or (m.group(1) if m else "")
        if cur not in hosts or len(hosts) < 2:
            return None
        nxt = hosts[(hosts.index(cur) + 1) % len(hosts)]
        self._host_override[idx] = nxt
        return nxt

    async def _reader(self):
        prev_idx = -1
        while self.running:
            if not self._sources:
                await asyncio.sleep(5)
                continue
            idx = self._src_idx % len(self._sources)
            src_type, src_url, *_src_rest = self._sources[idx]
            src_name = _src_rest[0] if _src_rest else ""
            src_profile = _src_rest[1] if len(_src_rest) > 1 else ""
            self._src_type = src_type
            self._src_idx  = idx
            if prev_idx >= 0 and idx != prev_idx:
                log.info(f"{self._tag} Источник сменился [{prev_idx}→{idx}], сброс буфера")
                self._buf.clear()
                self._pq.clear(); self._pq_bytes = 0
                self._kick_all_clients()
                await asyncio.sleep(0.5)
            prev_idx = idx
            self._tail = b""
            self._paced = (src_type != "ts") and CONFIG.get("hls_pace", True)
            self._pq.clear(); self._pq_bytes = 0
            self._src_name = src_name
            src_url = self._with_host(idx, src_url)
            safe_url = re.sub(r"//[^/@]+@", "//", src_url)
            log.info(f"{self._tag} Источник [{idx}] {src_name or src_type.upper()}: {safe_url[:70]}...")
            try:
                prof = src_profile   # уже эффективный профиль (дорожка → источник), см. CmChannelDB.refresh()
                # поправка громкости/даунмикс 5.1 по таблице — только на обычной ссылке (не unify/gpu_h264).
                # Инцидент 2026-10-25: раньше форсировала _read_hls_ffmpeg (его собственный HLS-фетчер против
                # master_url) почти для всех tvteam-каналов → массовые «нет данных 15 сек» (ffmpeg заметно
                # менее устойчив к ru*.hls.gd, чем наш _fetch_seg). Исправлено: обычный звук теперь идёт через
                # _read_hls_audio_norm (сегменты докачивает наш читатель, ffmpeg только фильтрует звук на
                # выходе через stdin) — см. ветку ниже.
                af_fix, _ = (None, 0.0) if prof else _audio_fix_filter(self.ch_id)
                if src_type == "ts":
                    if prof:
                        await self._read_transcode(src_url, prof)
                    elif CONFIG.get("force_audio_aac", False):
                        need = await self._audio_needs_norm_open(src_url)
                        if need:
                            await self._read_ts_audio_norm(src_url)
                        else:
                            await self._read_ts(src_url)
                    elif af_fix:
                        await self._read_ts_audio_norm(src_url, af=af_fix)
                    else:
                        await self._read_ts(src_url)
                else:
                    if prof:
                        await self._read_transcode(src_url, prof, hls=True)
                    elif CONFIG.get("force_audio_aac", False):
                        # отдельные аудиодорожки (#EXT-X-MEDIA TYPE=AUDIO) всё равно требуют склейки через ffmpeg
                        # независимо от числа каналов — это уже определяет сам _read_hls() и уходит в _read_hls_ffmpeg
                        need = await self._audio_needs_norm_open(src_url)
                        if need:
                            await self._read_hls_ffmpeg(src_url, "")
                        else:
                            await self._read_hls(src_url)
                    elif af_fix:
                        # отдельные аудиодорожки (#EXT-X-MEDIA TYPE=AUDIO) наш _fetch_seg physически не видит
                        # (это другой URL, не сегменты видео-варианта) — там неизбежен ffmpeg-фетчер master_url;
                        # обычный (встроенный) звук — через собственный надёжный читатель, см. _read_hls_audio_norm.
                        master = ""
                        media_url = src_url
                        try:
                            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as _s:
                                async with _s.get(src_url, headers={"User-Agent": "VLC"}, allow_redirects=True) as _r:
                                    if _r.status == 200:
                                        master = await _r.text()
                                        media_url = str(_r.url)
                        except Exception:
                            master = ""
                        if re.search(r'#EXT-X-MEDIA:[^\n]*TYPE=AUDIO[^\n]*URI="', master):
                            await self._read_hls_ffmpeg(media_url, master, af=af_fix)
                        else:
                            await self._read_hls_audio_norm(src_url, af=af_fix)
                    elif CONFIG.get("hls_via_ffmpeg", False):
                        await self._read_hls_ffmpeg(src_url, "")
                    else:
                        await self._read_hls(src_url)
            except asyncio.CancelledError:
                return
            except _Restart:
                # не сбой: фоновая проба уточнила параметры канала — читаем тот же источник заново
                await asyncio.sleep(0.2)
                continue
            except Exception as e:
                if not self.running: return
                self._rc += 1
                now = time.monotonic()
                self._rc_times.append(now)
                while self._rc_times and self._rc_times[0] < now - 60:
                    self._rc_times.popleft()
                self._state = "warn"
                rot = [t for t in self._rot_hist.get(idx, []) if now - t < 60]
                self._rot_hist[idx] = rot
                if src_type != "ts" and len(rot) < 2:
                    alt = self._rotate_host(idx, src_url)
                    if alt:
                        rot.append(now)
                        log.warning(f"{self._tag} Ошибка: {e} → пробую другой сервер провайдера: {alt}")
                        await asyncio.sleep(0.5)
                        continue
                # первая ошибка источника за последние 90 сек — пробуем поднять его ещё раз, а не уходим сразу
                # на запасной (например, с HD tvteam на SD Энигмы из-за разовой задержки сегмента)
                cnt, t_last = self._fail.get(idx, (0, 0.0))
                cnt = cnt + 1 if now - t_last < 90 else 1
                self._fail[idx] = (cnt, now)
                if cnt == 1 and len(self._sources) > 1:
                    log.warning(f"{self._tag} Ошибка: {e} → повторная попытка того же источника [{idx}]")
                    await asyncio.sleep(1)
                    continue
                next_idx = (idx + 1) % len(self._sources)
                if next_idx != idx:
                    log.warning(f"{self._tag} Ошибка: {e} → переключаюсь на [{next_idx}]")
                    self._src_idx = next_idx
                    await asyncio.sleep(1)
                else:
                    log.warning(f"{self._tag} Ошибка: {e} → retry через 3 сек")
                    await asyncio.sleep(3)

    async def _read_hls(self, m3u8_url: str):
        pl_to  = aiohttp.ClientTimeout(connect=CONFIG["hls_connect_timeout"],
                                       sock_read=CONFIG["hls_read_timeout"])
        seg_to = aiohttp.ClientTimeout(connect=CONFIG["hls_connect_timeout"],
                                       sock_read=CONFIG["hls_seg_timeout"])
        fallback = CONFIG["hls_fallback_secs"]
        last_seq = -1
        seq_key = self._pkey(m3u8_url)
        mem = self._hls_seq.get(seq_key)
        if mem and time.monotonic() - mem[1] < 180:
            # читатель перезапущен (простой источника, сброс соединения): продолжаем с того места, где остановились,
            # а не качаем заново последние N сегментов, которые клиент уже видел (иначе повтор ~30 с эфира)
            last_seq = mem[0]
            log.info(f"{self._tag} перезапуск читателя: продолжаю после сегмента {last_seq} (без повторов)")
        last_seg_at = time.monotonic()
        prefer_cb_until = 0.0     # до какого момента берём плейлист в обход кеша (после успеха обхода)
        partial: dict = {}      # sq -> сколько байт недокачанного сегмента уже отдано
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as sess:
            self._last_data = time.monotonic()
            async with sess.get(m3u8_url, timeout=pl_to, allow_redirects=True) as r:
                if r.status != 200:
                    raise Exception(f"M3U8 HTTP {r.status}")
                content   = await r.text()
                media_url = str(r.url)
            if "#EXT-X-STREAM-INF" in content and re.search(r'#EXT-X-MEDIA:[^\n]*TYPE=AUDIO[^\n]*URI="', content):
                # звук в отдельных дорожках — свой сборщик сегментов видео+аудио не тянет, склеивает ffmpeg
                await self._read_hls_ffmpeg(media_url, content)
                return
            if "#EXT-X-STREAM-INF" in content:
                variants = _parse_master_m3u8(content, media_url)
                if not variants:
                    raise Exception("Master playlist без вариантов")
                _, media_url = variants[0]
            err_count = 0
            def _max_errors():
                target = self._sec2chunks(CONFIG["buffer_seconds"])
                ratio  = min(1.0, len(self._buf) / max(1, target))
                return max(1, int(1 + ratio * 4))
            while self.running:
                try:
                    fetch_url = media_url
                    if time.monotonic() < prefer_cb_until:      # обычный плейлист отставал — пока берём «в обход кеша»
                        fetch_url = media_url + ("&" if "?" in media_url else "?") + f"_cb={int(time.time() * 1000)}"
                    async with sess.get(fetch_url, timeout=pl_to, allow_redirects=True,
                                        headers={"Cache-Control": "no-cache"} if fetch_url != media_url else None) as r:
                        if r.status != 200: raise Exception(f"M3U8 HTTP {r.status}")
                        content = await r.text()
                        actual_base = str(r.url)
                    err_count = 0
                    self._last_data = time.monotonic()  # плейлист жив — сервер отвечает
                except asyncio.CancelledError: raise
                except _Restart: raise
                except Exception as e:
                    err_count += 1
                    me = _max_errors()
                    if err_count >= me:
                        raise Exception(f"M3U8: {err_count} ошибок подряд")
                    await asyncio.sleep(2); continue
                # фоновая проба числа аудиоканалов (кикнута в _reader() при первом подключении) досчиталась
                # и оказалось, что звук не стерео → перезапуск на путь с принудительным AAC, без ожидания
                # ручного переключения канала (self._sink не None = нас уже кормит _read_hls_ffmpeg/_read_transcode)
                if self._sink is None and CONFIG.get("force_audio_aac", False) \
                        and self._audio_needs_norm(m3u8_url) is True:
                    raise _Restart()
                target_dur, segs = _parse_m3u8(content, actual_base)
                if last_seq < 0 and segs:
                    last_seq = segs[max(0, len(segs) - _hls_back(self.ch_id))][0] - 1
                elif segs and segs[-1][0] < last_seq - max(15, 3 * len(segs)):
                    # источник перезапустил поток: нумерация сегментов началась заново (в URL меняется эпоха) —
                    # иначе ждали бы «sq > last_seq» вечно при живом плейлисте
                    log.info(f"{self._tag} нумерация сегментов сброшена ({segs[-1][0]} < {last_seq}) → перезахват потока")
                    last_seq = segs[max(0, len(segs) - _hls_back(self.ch_id))][0] - 1
                # простой: новых сегментов нет дольше 2 длительностей сегмента — пробуем плейлист «в обход кеша»
                # (лишний параметр, no-cache, новое соединение): обычный URL у провайдера бывает старше на сегмент и больше
                if segs and segs[-1][0] <= last_seq and time.monotonic() - last_seg_at > 2 * target_dur + 2:
                    try:
                        cb_url = media_url + ("&" if "?" in media_url else "?") + f"_cb={int(time.time() * 1000)}"
                        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True, family=socket.AF_INET),
                                                         timeout=pl_to) as s2:
                            async with s2.get(cb_url, headers={"Cache-Control": "no-cache", "Pragma": "no-cache",
                                                               "User-Agent": "VLC"}) as r2:
                                if r2.status == 200:
                                    td2, segs2 = _parse_m3u8(await r2.text(), str(r2.url))
                                    if segs2 and segs2[-1][0] > segs[-1][0]:
                                        log.info(f"{self._tag} плейлист в обход кеша свежее ({segs2[-1][0]} > {segs[-1][0]}) → беру его")
                                        target_dur, segs = td2, segs2
                                        prefer_cb_until = time.monotonic() + 120
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
                # недокачанные сегменты, которых уже нет в окне плейлиста, потеряны безвозвратно — но хотя бы видно в журнале
                in_window = {sq for sq, _ in segs}
                for sq0 in list(partial):
                    if sq0 not in in_window:
                        log.warning(f"{self._tag} сегмент {sq0} потерян: не докачан, а из окна плейлиста уже вышел (дыра в потоке)")
                        partial.pop(sq0)
                new_segs = [(sq, url) for sq, url in segs if sq > last_seq]
                for sq, seg_url in new_segs:
                    if not self.running: return
                    try:
                        if await self._fetch_seg(sess, seg_url, partial.get(sq, 0)) == 404:
                            last_seq = sq; err_count = 0; partial.pop(sq, None); continue
                        last_seq = sq; self._state = "normal"; err_count = 0
                        partial.pop(sq, None)
                        last_seg_at = time.monotonic()
                        self._hls_seq[seq_key] = (last_seq, last_seg_at)
                    except asyncio.CancelledError: raise
                    except _SegFail as e:
                        # НЕ пропускаем сегмент: докачаем с того же места в следующем проходе, пока он есть в плейлисте
                        partial[sq] = e.got
                        err_count += 1
                        if err_count >= _max_errors():
                            raise Exception(f"Сегмент: {err_count} ошибок подряд ({e})")
                        log.warning(f"{self._tag} сегмент {sq} не докачан ({e}), принято {e.got // 1024} КБ → докачаю")
                        await asyncio.sleep(1)
                        break
                    except Exception as e:
                        err_count += 1
                        if err_count >= _max_errors():
                            raise Exception(f"Сегмент: {err_count} ошибок подряд")
                        last_seq = sq
                        await asyncio.sleep(1)
                if fallback > 0 and time.monotonic() - self._last_data > fallback:
                    raise Exception(f"Нет данных {fallback} сек")
                # плейлист отвечает, а новых сегментов нет (источник встал/перезапущен) — «нет данных» выше это не ловит
                stale = max(10.0, 3 * target_dur)
                if time.monotonic() - last_seg_at > stale:
                    raise Exception(f"нет новых сегментов {stale:.0f} сек при живом плейлисте")
                await asyncio.sleep(max(1.0, target_dur / 2))
                if self._switch_requested:
                    self._switch_requested = False
                    raise Exception("Возврат на основной источник")

    async def _fetch_seg(self, sess: aiohttp.ClientSession, seg_url: str, resume_from: int = 0) -> int:
        """Качает сегмент, быстро «подёргивая» подвисший источник, пока буфер плеера не кончился.
        Попытка обрывается, если данных нет hls_stall_secs или скорость ниже hls_min_mbps
        (после 3 сек); докачка идёт с уже принятого места (Range, при 200 — пропуск принятого)
        на новом соединении, поэтому в поток не попадает дубль и обрыв TS."""
        stall    = CONFIG.get("hls_stall_secs", 3)
        min_mbps = CONFIG.get("hls_min_mbps", 4.0)
        retries  = CONFIG.get("hls_seg_retries", 4)
        got = resume_from                         # сколько байт сегмента уже отдано клиентам (при повторе — с прошлого раза)
        attempt = 0
        while True:
            # Профилактика просадок: пока в очереди выравнивания большой запас, медленный сегмент не рвём
            # (переподключение само стоит секунд); при малом запасе реагируем быстро.
            cush = self._cushion_s() if self._paced else 0.0
            healthy = cush >= CONFIG.get("hls_healthy_cushion_s", 8.0)
            stall_eff = stall            # ни одного байта дольше stall сек — рвём и докачиваем (Range) всегда: ожидание съедает запас
            min_mbps_eff = min(min_mbps, 1.0) if healthy else min_mbps
            to = aiohttp.ClientTimeout(connect=CONFIG["hls_connect_timeout"], sock_read=stall_eff)
            headers = {"Range": f"bytes={got}-"} if got else {}
            t0 = time.monotonic(); got_att = 0
            try:
                async with sess.get(seg_url, timeout=to, headers=headers, allow_redirects=True) as sr:
                    if sr.status == 404:
                        return 404
                    if sr.status == 416:          # запрошенное место за концом — сегмент уже получен целиком
                        return 200
                    if sr.status not in (200, 206):
                        raise Exception(f"HTTP {sr.status}")
                    skip = got if (got and sr.status == 200) else 0   # сервер проигнорировал Range
                    async for chunk in sr.content.iter_chunked(CONFIG["chunk_size"]):
                        if skip:
                            n = min(skip, len(chunk)); skip -= n; chunk = chunk[n:]
                            if not chunk:
                                continue
                        await self._wait_room()
                        if self._sink:
                            await self._sink(chunk)
                        else:
                            self._push(chunk)
                        got += len(chunk); got_att += len(chunk)
                        el = time.monotonic() - t0
                        if attempt < retries and el > 3.0 and got_att * 8 / el / 1e6 < min_mbps_eff:
                            raise asyncio.TimeoutError(f"медленно {got_att * 8 / el / 1e6:.1f} Мбит/с (запас {cush:.0f} с)")
                return 200
            except asyncio.CancelledError:
                raise
            except Exception as e:
                attempt += 1
                if attempt > retries or not self.running:
                    raise _SegFail(f"{type(e).__name__}: {e}", got) from e
                log.info(f"{self._tag} сегмент завис ({type(e).__name__} {e}), принято {got // 1024} КБ → "
                         f"переподключаюсь [{attempt}/{retries}]")
                await asyncio.sleep(0.2)

    async def _read_hls_ffmpeg(self, master_url: str, master: str, af: str | None = None):
        """HLS через ffmpeg без перекодирования (-c copy) с выравниванием скорости: сегменты иначе идут пачками
        по 20-45 Мбит/с с паузами до 12 с (микрофризы). Если master пустой — сначала читаем его сами:
        при отдельных аудиодорожках (#EXT-X-MEDIA TYPE=AUDIO) берём дорожку нужного языка, иначе все аудио."""
        lang = CONFIG.get("hls_audio_lang", "rus")
        if not master:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as sess:
                    async with sess.get(master_url, headers={"User-Agent": "VLC"}) as r:
                        master = await r.text() if r.status == 200 else ""
            except Exception:
                master = ""
        if re.search(r'#EXT-X-MEDIA:[^\n]*TYPE=AUDIO[^\n]*URI="', master):
            audio_maps = ["-map", f"0:m:language:{lang}" if f'LANGUAGE="{lang}"' in master else "0:a:0"]
            what = f"отдельное аудио, дорожка {audio_maps[1]}"
        else:
            audio_maps = ["-map", "0:a?"]      # все аудиодорожки как есть (звука может не быть)
            what = "обычный HLS"
        # видео тут никогда не перекодируется (это путь без транскода), но звук по настройке можно принудительно
        # свести в AAC стерео заданным битрейтом — полезно для дорожек в непредсказуемых форматах/каналах.
        # af задан (таблица поправок громкости/даунмикса 5.1, см. _audio_fix_filter) — приоритет над force_audio_aac.
        aac_br = CONFIG.get("force_audio_bitrate", 256)
        if af:
            audio_codec = ["-af", af, "-c:a", "aac", "-ac", "2", "-b:a", f"{aac_br}k"]
            what += f", звук: {af}"
        elif CONFIG.get("force_audio_aac", False):
            audio_codec = ["-c:a", "aac", "-ac", "2", "-b:a", f"{aac_br}k"]
            what += f", звук → AAC стерео {aac_br}k"
        else:
            audio_codec = ["-c:a", "copy"]
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-user_agent", "VLC",
               "-rw_timeout", "8000000",
               # без этих флагов ffmpeg при любом кратковременном сбое HTTP (сегмент/плейлист) просто молчит,
               # вместо повтора — наша собственная докачка через Range тут не участвует, т.к. сегменты качает
               # сам ffmpeg изнутри (-i на master_url), а не _fetch_seg
               "-reconnect", "1", "-reconnect_at_eof", "1", "-reconnect_streamed", "1",
               "-reconnect_delay_max", "5",
               # выравнивание: вход читаем строго в реальном времени (1.0 — остаёмся позади «живого края» на
               # запас в несколько сегментов; при >1 ffmpeg догоняет край и потом ждёт каждый новый сегмент —
               # это и давало микрофризы), стартовый запас hls_readrate_burst сек
               *(["-readrate", str(CONFIG["hls_readrate"]),
                  "-readrate_initial_burst", str(CONFIG.get("hls_readrate_burst", 4))]
                 if CONFIG.get("hls_readrate", 0) else []),
               "-live_start_index", "-3",   # фиксированно: при перезапуске ffmpeg запас «5 назад» откатывал бы картинку на 30 с
               "-i", master_url,
               "-map", "0:v:0", *audio_maps, "-c:v", "copy", *audio_codec, "-f", "mpegts", "pipe:1"]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        tail: deque = deque(maxlen=6)
        drain = asyncio.create_task(_drain_stderr(proc, tail))
        log.info(f"{self._tag} HLS → ffmpeg copy с выравниванием ({what})")
        self._last_data = time.monotonic()
        # сегменты приходят раз в ~6 сек, 12 сек буфера: дольше 15 сек молчания ждать нельзя — перезапуск
        idle_limit = CONFIG.get("hls_ffmpeg_idle_secs", 15)
        try:
            while self.running:
                try:
                    chunk = await asyncio.wait_for(proc.stdout.read(CONFIG["chunk_size"]), timeout=2)
                except asyncio.TimeoutError:
                    if time.monotonic() - self._last_data > idle_limit:
                        raise Exception(f"ffmpeg: нет данных {idle_limit} сек | " + " / ".join(tail)[-260:])
                    continue
                if not chunk:
                    raise Exception(f"ffmpeg завершился (код {proc.returncode}) | " + " / ".join(tail)[-260:])
                await self._wait_room()
                self._push(chunk)
                self._state = "normal"
                if self._switch_requested:
                    self._switch_requested = False
                    raise Exception("Возврат на основной источник")
        finally:
            drain.cancel()
            if proc.returncode is None:
                try: proc.kill()
                except ProcessLookupError: pass
            await proc.wait()

    async def _read_hls_audio_norm(self, m3u8_url: str, af: str):
        """HLS с обычным (не отдельным) звуком: сегменты докачивает наш же надёжный _read_hls/_fetch_seg
        (устойчив к обрывам конкретно у ru*.hls.gd — докачка с того же места, Range, адаптивные пороги), а не
        собственный HTTP-фетчер ffmpeg. Видео идёт копией, звук фильтруется (громкость/даунмикс 5.1, см.
        _audio_fix_filter) и кодируется в AAC через stdin ffmpeg — тот же приём, что _read_ts_audio_norm для TS.
        Инцидент 2026-10-25: когда коррекцию громкости форсировали через _read_hls_ffmpeg (его собственный
        HLS-фетчер против master_url), почти все tvteam-каналы массово ловили «нет данных 15 сек» — у ffmpeg
        HLS-демуксер оказался заметно менее устойчив к этому провайдеру, чем наш _fetch_seg."""
        aac_br = CONFIG.get("force_audio_bitrate", 256)
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-nostdin",
               "-fflags", "+genpts",
               "-f", "mpegts", "-analyzeduration", "1500000", "-probesize", "3000000", "-i", "pipe:0",
               "-map", "0:v:0", "-map", "0:a?", "-c:v", "copy",
               "-af", af, "-c:a", "aac", "-ac", "2", "-b:a", f"{aac_br}k",
               "-f", "mpegts", "pipe:1"]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        tail: deque = deque(maxlen=6)
        drain_task = asyncio.create_task(_drain_stderr(proc, tail))

        async def _sink(chunk: bytes):
            proc.stdin.write(chunk)
            await proc.stdin.drain()
        self._sink = _sink
        reader_task = asyncio.create_task(self._read_hls(m3u8_url))
        reader_task.add_done_callback(lambda t: t.cancelled() or t.exception())
        log.info(f"{self._tag} HLS → звук: {af} (видео копией, сегменты через собственный читатель)")
        self._last_data = time.monotonic()
        idle_limit = CONFIG.get("tc_idle_secs", 12)
        try:
            while self.running:
                if reader_task.done():
                    raise (reader_task.exception() or Exception("читатель HLS завершился"))
                try:
                    chunk = await asyncio.wait_for(proc.stdout.read(CONFIG["chunk_size"]), timeout=3)
                except asyncio.TimeoutError:
                    if time.monotonic() - self._last_data > idle_limit:
                        raise Exception(f"ffmpeg (аудио): нет данных {idle_limit} сек | " + " / ".join(tail)[-260:])
                    continue
                if not chunk:
                    raise Exception(f"ffmpeg (аудио) завершился (код {proc.returncode}) | " + " / ".join(tail)[-260:])
                self._last_data = time.monotonic()
                await self._wait_room()
                self._push(chunk)
                self._state = "normal"
                if self._switch_requested:
                    self._switch_requested = False
                    raise Exception("Возврат на основной источник")
        finally:
            self._sink = None
            reader_task.cancel()
            drain_task.cancel()
            try: proc.stdin.close()
            except Exception: pass
            if proc.returncode is None:
                try: proc.kill()
                except ProcessLookupError: pass
            await proc.wait()

    # ── Перекодирование на GPU (профиль источника или маркер канала: gpu_h264) ──────────
    # Приставке не подходят потоки с флагом чересстрочности (field_order=tt/bb, даже если контент
    # progressive: Энигма, часть каналов tvteam). Проверено на приставке: копия видео не играет,
    # перекодирование в progressive H.264 играет; при перекодированном видео звук тоже надо
    # перекодировать (ТВ 3 HD: видео перекод. + звук копия не играло, оба перекод. — играло).
    # Разрешение НЕ меняем и не увеличиваем.
    _tc_active = 0
    _cpu_deint = 0.0         # суммарный «вес» потоков, которые сейчас деинтерлейсит CPU (bwdif)
    _tc_bad: dict = {}       # ключ -> monotonic до которого перекодирование отключено (сбой GPU/ffmpeg)
    _probe_cache: dict = _probe_load()   # ключ -> (wall_time, info); в файле, переживает перезапуск
    _probing: set = set()
    _audio_ch_cache: dict = {}   # ключ -> число аудиоканалов (2=стерео, звук не трогаем); в памяти, дёшево перепробовать
    _ts_baseline_mbps: dict = {}   # ключ -> реальная здоровая скорость канала (SD/HD различаются в разы) для
                                    # _read_ts (просадка) и _probe_ts (проверка возврата) — не общий bitrate_mbps
    _audio_probing: set = set()

    @staticmethod
    def _pkey(url: str) -> str:
        return url.split("?")[0]   # у tvteam токен в query меняется, поток тот же

    @staticmethod
    def _default_info() -> dict:
        """Холодный старт без пробы: безопасные значения (проба идёт в фоне и кешируется)."""
        return {"default": True, "v": 0, "w": 1920, "h": 1080, "fps": 25.0,
                "flagged_interlaced": True, "interlaced": None, "audio": None}

    def _cached_probe(self, url: str) -> dict | None:
        hit = StreamSession._probe_cache.get(self._pkey(url))
        if hit and time.time() - hit[0] < _PROBE_TTL:
            return hit[1]
        return None

    async def _ffprobe_stream(self, url: str, idet_wait: float = 8.0) -> dict | None:
        cached = self._cached_probe(url)
        if cached:
            return cached
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-user_agent", "VLC", "-analyzeduration", "1500000",
            "-probesize", "2500000", "-print_format", "json", "-show_streams", "-i", url,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        # чересстрочность — по флагу кадров (interlaced_frame), параллельно с ffprobe потоков
        frames_task = asyncio.create_task(self._interlace_flag(url))
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=9)
            streams = json.loads(out).get("streams", [])
        except Exception:
            frames_task.cancel()
            try: proc.kill()
            except ProcessLookupError: pass
            return None
        try:
            il = await asyncio.wait_for(frames_task, timeout=idet_wait)
        except Exception:
            il = None    # не вышло — «неизвестно», НЕ progressive и не кешируем
        v = next((s for s in streams if s.get("codec_type") == "video" and s.get("width")), None)
        if not v:
            return None
        auds = [s for s in streams if s.get("codec_type") == "audio"]
        lang = CONFIG.get("hls_audio_lang", "rus")
        a = next((s for s in auds if (s.get("tags") or {}).get("language") == lang), auds[0] if auds else None)
        try:
            num, den = str(v.get("avg_frame_rate") or "0/1").split("/")
            fps = float(num) / float(den) if float(den) else 0.0
        except Exception:
            fps = 0.0
        info = {"v": v["index"], "w": int(v["width"]), "h": int(v["height"]), "fps": fps,
                "flagged_interlaced": v.get("field_order") in ("tt", "bb", "tb", "bt"),
                "interlaced": il,
                "audio": {"index": a["index"]} if a else None}
        if il is not None:      # без результата в кеш не пишем — иначе «неизвестно» закрепилось бы на сутки
            StreamSession._probe_cache[self._pkey(url)] = (time.time(), info)
            _probe_save(StreamSession._probe_cache)
        return info

    async def _probe_audio_channels(self, url: str) -> int | None:
        """Число каналов в аудиодорожке (2=стерео — звук не трогаем). Отдельный быстрый ffprobe без idet-пробы
        видео (она медленная и ненадёжна) — только для решения, нужен ли принудительный AAC-стерео."""
        key = self._pkey(url)
        if key in StreamSession._audio_ch_cache:
            return StreamSession._audio_ch_cache[key]
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "error", "-user_agent", "VLC", "-analyzeduration", "500000",
                "-probesize", "800000", "-select_streams", "a", "-show_entries", "stream=channels",
                "-print_format", "json", "-i", url,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=6)
            streams = json.loads(out).get("streams", [])
            ch = int(streams[0]["channels"]) if streams and streams[0].get("channels") else None
        except asyncio.CancelledError:
            if proc is not None and proc.returncode is None:
                try: proc.kill()
                except ProcessLookupError: pass
            raise
        except Exception:
            ch = None
        if ch is not None:
            StreamSession._audio_ch_cache[key] = ch
        return ch

    def _audio_needs_norm(self, url: str) -> bool | None:
        """True = звук не стерео (или несколько дорожек) → надо свести в AAC стерео. False = уже стерео, не трогаем.
        None = ещё не знаем (проба не завершилась) — читаем как есть, проба досчитается в фоне для следующего раза."""
        ch = StreamSession._audio_ch_cache.get(self._pkey(url))
        return None if ch is None else ch != 2

    async def _audio_needs_norm_open(self, url: str) -> bool:
        """Вызывается ПРИ ОТКРЫТИИ потока (до того как начали отдавать байты клиенту): ждём пробу числа
        аудиоканалов синхронно, но не дольше audio_probe_open_secs (по умолчанию 1.5с) — иначе канал открывался бы
        заметно медленнее. Если проба не уложилась (сеть/источник медленный) — отдаём как есть, добор в фоне
        (_kick_audio_probe) и периодическая проверка внутри _read_ts/_read_hls досчитают и перезапустят сами."""
        cached = StreamSession._audio_ch_cache.get(self._pkey(url))
        if cached is not None:
            return cached != 2
        timeout = CONFIG.get("audio_probe_open_secs", 1.5)
        try:
            ch = await asyncio.wait_for(self._probe_audio_channels(url), timeout=timeout)
        except asyncio.TimeoutError:
            self._kick_audio_probe(url)
            return False
        return ch is not None and ch != 2

    def _kick_audio_probe(self, url: str):
        key = self._pkey(url)
        if key in StreamSession._audio_probing:
            return
        StreamSession._audio_probing.add(key)

        async def _warm():
            try:
                await self._probe_audio_channels(url)
            finally:
                StreamSession._audio_probing.discard(key)
        asyncio.create_task(_warm())

    async def _interlace_flag(self, url: str) -> bool | None:
        """True = кадры закодированы как чересстрочные (interlaced_frame=1), False = progressive, None = не удалось.
        idet для этого не годится: он видит гребёнку только при движении, на статике чересстрочка = «progressive»."""
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-user_agent", "VLC", "-analyzeduration", "1500000",
            "-probesize", "2500000", "-select_streams", "v:0", "-read_intervals", "%+4",
            "-show_entries", "frame=interlaced_frame", "-print_format", "json", "-i", url,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=12)
            frames = json.loads(out).get("frames", [])
        except Exception:
            try: proc.kill()
            except ProcessLookupError: pass
            return None
        frames = frames[2:] if len(frames) > 6 else frames      # первые кадры после входа в середину GOP пропускаем
        if len(frames) < 3:
            return None
        il = sum(1 for f in frames if f.get("interlaced_frame") == 1)
        return il >= 0.6 * len(frames)

    def _tc_confirmed_fail(self) -> bool:
        """True, если перекодирование этого источника уже срывалось меньше 90 с назад (реальная вторая попытка
        провалилась) — тогда сбой подтверждён и стоит переключаться/банить GPU-попытки. False на первом сбое —
        _reader() даёт источнику ещё одну настоящую попытку (retry-same-source), не заводя долгий бан впустую:
        разовая заминка спутника из-за непогоды не должна сразу списывать источник."""
        cnt, t_last = self._fail.get(self._src_idx, (0, 0.0))
        return cnt >= 1 and time.monotonic() - t_last < 90

    async def _read_transcode(self, url: str, profile: str, hls: bool = False):
        """Поток url → GPU (декод + H.264 progressive) + AAC стерео 256 → буфер."""
        raw = self._read_hls if hls else self._read_ts
        key = self._pkey(url)
        has_backup = len(self._sources) > 1
        if profile != "gpu_h264":
            return await raw(url)
        # profile=gpu_h264 означает: для этого канала выяснили, что сырой поток НЕ смотрибелен на приставке
        # (флаг чересстрочности и т.п.) — поэтому если перекодирование сейчас недоступно, отдавать raw как
        # «работает» нельзя: клиент увидит зависшую картинку при живом звуке, а сессия при этом будет
        # выглядеть здоровой и никогда не переключится на резерв. Раз резерв есть — считаем источник сбойным.
        if not os.path.exists("/dev/dri/renderD128") or StreamSession._tc_bad.get(key, 0) > time.monotonic():
            if has_backup:
                raise Exception("перекодирование недоступно (raw для этого канала не смотрибелен) → переключаюсь")
            return await raw(url)
        if StreamSession._tc_active >= CONFIG.get("max_transcodes", 6):
            # это про загрузку GPU другими каналами, а не про здоровье ИСТОЧНИКА — не повод бросать спутник
            log.warning(f"{self._tag} перекодирований уже {StreamSession._tc_active} (лимит) → без перекодирования")
            return await raw(url)
        info = self._cached_probe(url)
        if info is None:
            if hls:
                # HLS и так стартует секунды — пробуем синхронно (нужны аудио-индекс и чересстрочность)
                info = await self._ffprobe_stream(url)
                if not info:
                    if not has_backup:
                        log.warning(f"{self._tag} ffprobe не разобрал поток → без перекодирования")
                        return await raw(url)
                    if self._tc_confirmed_fail():
                        StreamSession._tc_bad[key] = time.monotonic() + CONFIG.get("tc_cooldown_secs", 600)
                        log.warning(f"{self._tag} ffprobe второй раз подряд не разобрал поток → "
                                    f"{CONFIG.get('tc_cooldown_secs', 600) / 60:.0f} мин без перекодирования, переключаюсь")
                        raise Exception("ffprobe дважды не разобрал поток, raw не смотрибелен")
                    log.warning(f"{self._tag} ffprobe не разобрал поток → пробую источник ещё раз")
                    raise Exception("ffprobe не разобрал поток → повтор")
                if info["interlaced"] is None:
                    # idet не отработал: для HLS не повторяем медленную пробу на каждом старте — считаем progressive
                    info["interlaced"] = False
                    StreamSession._probe_cache[key] = (time.time(), info)
                    _probe_save(StreamSession._probe_cache)
            else:
                # Энигма: стартуем сразу с безопасными значениями, проба — в фоне для следующих запусков
                info = self._default_info()
                if key not in StreamSession._probing:
                    StreamSession._probing.add(key)

                    async def _warm():
                        try:
                            await self._ffprobe_stream(url)
                        finally:
                            StreamSession._probing.discard(key)
                    asyncio.create_task(_warm())
        # Приставка 4K, сеть локальная — битрейт не занижаем (источник Энигмы 6-11 Мбит/с)
        mbps = CONFIG.get("tc_mbps_1080", 12) if info["h"] >= 1000 else \
               CONFIG.get("tc_mbps_720", 8) if info["h"] >= 700 else CONFIG.get("tc_mbps_sd", 4)
        # Частоту кадров НЕ меняем: bwdif send_frame = 1 кадр на входной кадр (25→25, 30→30, 50→50, 60→60),
        # send_field удваивает (25i→50p, 30i→60p). Уровень H.264 и битрейт выбираем по РЕАЛЬНОЙ выходной частоте:
        # 1080p до 30 к/с — уровень 4.1, выше (50/60) — 4.2 (иначе приставка может отвергнуть поток).
        level = "4.2" if (info["h"] >= 1000 and (info["fps"] > 30.5 or info["fps"] <= 0)) else "4.1"
        # Деинтерлейс: VAAPI-фильтр драйвера radeonsi (motion_adaptive) оставляет остаточную гребёнку
        # (~32% кадров по idet) → мерцание в движении. CPU-bwdif чистый (0%).
        # field = 50p (настоящие 50 разных кадров из 50 полукадров, заметно лучше на 4K-приставке), frame = 25p.
        # Энигма — 1080i50 (50 полей/с): 50p заметно лучше. tvteam уже пережат в 25 к/с (MBAFF с меткой чересстрочности):
        # 50p там только синтезировал бы кадры и жёг CPU — для HLS достаточно 25p.
        # "~unify"-сессия: всегда стиль tvteam (frame/25p), независимо от типа активного источника —
        # иначе при переключении TVHeadend↔tvteam «единый поток» сам менял бы картинку между 50p и 25p.
        # frame безопаснее как общий стандарт: не удваивает кадры там, где их реально 25 (tvteam) и не
        # ломается на прогрессивных 24p-источниках, которые при field были бы досинтезированы вдвое
        # без реального выигрыша в чёткости. field (50p) оставлен только для обычных (не-unify) сессий
        # с TVHeadend/Энигма, где это настоящие 50 разных полукадров.
        if self.ch_id.endswith("~unify"):
            deint_rate = CONFIG.get("tc_deint_rate_hls", "frame")
        else:
            deint_rate = CONFIG.get("tc_deint_rate_hls", "frame") if hls else CONFIG.get("tc_deint_rate", "field")
        # «вес» потока для CPU: 1080 тяжёлый, SD лёгкий; лимит в единицах одного потока 1080i50.
        # Замер (4 ядра): 1 поток 1080i→50p = 2.6x реального времени, 2 = 1.4x каждый, 3 = 0.96x (не тянет)
        # → на каждый 1080i50 нужно ~1.5 ядра; лимит max_cpu_deint выставлен под число ядер контейнера.
        base = 1.0 if info["h"] >= 1000 else 0.4 if info["h"] >= 700 else 0.15
        limit = CONFIG.get("max_cpu_deint", 2.5)
        cost = base * (1.0 if deint_rate == "field" else 0.5)
        cpu_deint = (info["interlaced"] is True and StreamSession._cpu_deint + cost <= limit + 1e-6)
        if info["interlaced"] is True and not cpu_deint and deint_rate == "field" \
                and StreamSession._cpu_deint + base * 0.5 <= limit + 1e-6:
            # 50p не влезает — лучше CPU-деинтерлейс в 25p (вдвое дешевле), чем GPU с остаточной гребёнкой
            deint_rate, cost, cpu_deint = "frame", base * 0.5, True
            log.warning(f"{self._tag} CPU-деинтерлейс 50p не влезает ({StreamSession._cpu_deint:.2f} из {limit}) → 25p")
        elif info["interlaced"] is True and not cpu_deint:
            log.warning(f"{self._tag} CPU-деинтерлейс занят ({StreamSession._cpu_deint:.2f} из {limit}) → деинтерлейс на GPU (хуже)")
        if cpu_deint:
            # вес резервируем сразу, до запуска процесса: несколько каналов могут стартовать в одну секунду
            StreamSession._cpu_deint += cost
        if cpu_deint:
            vf = ["-vf", f"bwdif=mode=send_{deint_rate}:deint=all,format=nv12,hwupload"]
            hw_in = ["-vaapi_device", "/dev/dri/renderD128"]      # декод на CPU, кодирование на GPU
            if deint_rate == "field":
                level = "4.2"
        elif info["interlaced"] in (False, None):
            # Декод на CPU: VAAPI-декодер AMD на H.264 SD Энигмы даёт чёрные кадры.
            vf = ["-vf", "format=nv12,hwupload"]
            hw_in = ["-vaapi_device", "/dev/dri/renderD128"]
        else:
            vf = ["-vf", "format=nv12,hwupload,deinterlace_vaapi=mode=default:rate=frame"]
            hw_in = ["-vaapi_device", "/dev/dri/renderD128"]
        if info["h"] >= 1000 and level == "4.2":
            mbps = max(mbps, CONFIG.get("tc_mbps_1080_50p", 16))
        # HLS-перекодирование: сегменты читает наш надёжный читатель _read_hls (докачка, защита от тихих зависаний),
        # а ffmpeg получает готовый TS через stdin. Собственный HLS-демуксер ffmpeg на «живом краю» молчит
        # по 12 с раз в 1-2 минуты (перезапуски = зависания). Отдельные аудиодорожки — старый путь (ffmpeg по URL).
        pipe_mode = False
        if hls and CONFIG.get("hls_transcode_pipe", True):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as _s:
                    async with _s.get(url, headers={"User-Agent": "VLC"}) as _r:
                        _m = await _r.text() if _r.status == 200 else ""
                pipe_mode = bool(_m) and "TYPE=AUDIO" not in _m
            except Exception:
                pipe_mode = False
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning" if pipe_mode else "error", "-nostdin"]
        if not pipe_mode:        # -user_agent относится к http-входу; для pipe:0 ffmpeg падает «Option user_agent not found»
            cmd += ["-user_agent", "VLC"]
        # discardcorrupt выбрасывает кадры до следующего ключевого при любой порче (дыры по 2 с = длина GOP в потоке);
        # для входа через stdin наш читатель уже отдаёт целые сегменты — порча декодеру не нужна как повод резать поток
        cmd += ["-fflags", "+genpts" if pipe_mode else "+genpts+discardcorrupt"]
        if hls and not pipe_mode:
            if CONFIG.get("hls_readrate", 0):     # по умолчанию выключено: выравнивает _playout по PCR
                cmd += ["-readrate", str(CONFIG["hls_readrate"]),
                        "-readrate_initial_burst", str(CONFIG.get("hls_readrate_burst", 4))]
        elif not hls:
            cmd += ["-analyzeduration", "1000000", "-probesize", "1500000"]   # быстрый старт Энигмы
        vmap = "0:v:0" if info.get("default") else f"0:{info['v']}"
        if hls and not pipe_mode:
            cmd += ["-live_start_index", "-3"]
        if pipe_mode:
            cmd += [*hw_in, "-f", "mpegts", "-analyzeduration", "1500000", "-probesize", "3000000",
                    "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a:0?"]
        else:
            cmd += [*hw_in, "-i", url, "-map", vmap]
            if info.get("default"):
                cmd += ["-map", "0:a:0?"]
            elif info["audio"]:
                cmd += ["-map", f"0:{info['audio']['index']}"]
        cmd += [*vf, "-c:v", "h264_vaapi", "-profile:v", "high", "-level", level,
                "-b:v", f"{mbps}M", "-maxrate", f"{mbps * 1.25:g}M", "-bufsize", f"{mbps * 2}M",
                "-g", "50", "-c:a", "aac", "-ac", "2", "-b:a", f"{CONFIG.get('force_audio_bitrate', 256)}k",
                "-f", "mpegts", "pipe:1"]
        log.info(f"{self._tag} {'HLS' if hls else 'TS'} → GPU-перекодирование {info['w']}x{info['h']}"
                 f"{' (проба в фоне)' if info.get('default') else ''}"
                 f"{f' деинтерлейс CPU bwdif {deint_rate}' if cpu_deint else (' деинтерлейс GPU' if info['interlaced'] else '')}"
                 f" → H.264 {mbps}M, звук AAC 256{' (HLS читает наш читатель → ffmpeg через stdin)' if pipe_mode else ''}")
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdin=asyncio.subprocess.PIPE if pipe_mode else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE if pipe_mode else asyncio.subprocess.DEVNULL)
        except Exception:
            if cpu_deint:
                StreamSession._cpu_deint -= cost
            raise
        StreamSession._tc_active += 1
        self._last_data = time.monotonic()
        reader_task = None
        drain_task = None
        if pipe_mode:
            drain_task = asyncio.create_task(_drain_stderr(proc, deque(maxlen=6), self._tag))
            # отладка: если есть /tmp/dump_sink.flag — пишем в /tmp/sink_dump.ts всё, что читатель отдаёт ffmpeg
            dump = open("/tmp/sink_dump.ts", "wb") if os.path.exists("/tmp/dump_sink.flag") else None

            async def _sink(chunk: bytes):
                if dump:
                    dump.write(chunk)
                proc.stdin.write(chunk)
                await proc.stdin.drain()
            self._sink = _sink
            reader_task = asyncio.create_task(self._read_hls(url))
            reader_task.add_done_callback(lambda t: t.cancelled() or t.exception())
        t_start, got, fallback = time.monotonic(), 0, False
        idle_limit = CONFIG.get("tc_idle_secs", 12)
        first_limit = 15 if hls else 6     # нет данных с самого старта — декодер/GPU не справились
        try:
            while self.running:
                if reader_task is not None and reader_task.done():
                    raise (reader_task.exception() or Exception("читатель HLS завершился"))
                try:
                    chunk = await asyncio.wait_for(proc.stdout.read(CONFIG["chunk_size"]), timeout=3)
                except asyncio.TimeoutError:
                    if got == 0 and time.monotonic() - t_start > first_limit:
                        chunk = b""
                    elif time.monotonic() - self._last_data > idle_limit:
                        raise Exception(f"ffmpeg: нет данных {idle_limit} сек")
                    else:
                        continue
                if not chunk:
                    if got == 0 and time.monotonic() - t_start < first_limit + 10:
                        if has_backup and not self._tc_confirmed_fail():
                            # первая заминка (например, разовый сбой спутника из-за непогоды) — не баним источник
                            # и не переключаемся, даём _reader() честно попробовать его ещё раз
                            log.warning(f"{self._tag} ffmpeg не выдал данных → пробую источник ещё раз")
                            raise Exception("ffmpeg не выдал данных → повтор")
                        StreamSession._tc_bad[key] = time.monotonic() + CONFIG.get("tc_cooldown_secs", 600)
                        log.warning(f"{self._tag} ffmpeg {'второй раз подряд ' if has_backup else ''}"
                                    f"не выдал данных (GPU/декодер?) → "
                                    f"{CONFIG.get('tc_cooldown_secs', 600) / 60:.0f} мин без перекодирования")
                        fallback = True
                        break
                    raise Exception(f"ffmpeg завершился (код {proc.returncode})")
                got += len(chunk)
                await self._wait_room()
                self._push(chunk)
                self._state = "normal"
                if info.get("default") and time.monotonic() - t_start > 2:
                    c = self._cached_probe(url)     # фоновая проба готова: параметры канала отличаются?
                    if c and (c["interlaced"] or c["h"] != info["h"]):
                        log.info(f"{self._tag} проба: {c['w']}x{c['h']}"
                                 f"{' чересстрочный' if c['interlaced'] else ''} → перезапуск с точными параметрами")
                        raise _Restart()
                if self._switch_requested:
                    self._switch_requested = False
                    raise Exception("Возврат на основной источник")
        finally:
            StreamSession._tc_active -= 1
            if cpu_deint:
                StreamSession._cpu_deint -= cost
            if drain_task is not None:
                drain_task.cancel()
                try:
                    dump.close()
                except Exception:
                    pass
            if reader_task is not None:
                reader_task.cancel()
                self._sink = None
                try: proc.stdin.close()
                except Exception: pass
            if proc.returncode is None:
                try: proc.kill()
                except ProcessLookupError: pass
            await proc.wait()
        if fallback:
            if has_backup:
                raise Exception("перекодирование не пошло, raw не смотрибелен → переключаюсь")
            return await raw(url)

    async def _read_ts(self, url: str):
        tc = aiohttp.ClientTimeout(connect=CONFIG["ts_connect_timeout"],
                                   sock_read=CONFIG["ts_read_timeout"])
        has_backup    = len(self._sources) > 1
        tei_thr       = CONFIG.get("ts_tei_threshold", 0.02)
        cc_thr        = CONFIG.get("ts_cc_threshold",  0.01)
        q_window      = CONFIG.get("ts_quality_window", 5.0)
        quality_check = has_backup and (tei_thr > 0 or cc_thr > 0)
        tracker       = TsErrorTracker(q_window, tei_thr, cc_thr) if quality_check else None
        # Просадка непогоды на спутнике часто НЕ портит пакеты (TEI/CC в норме), тюнер просто резко режет
        # битрейт, оставаясь «живым» TCP-соединением — ни таймаут чтения, ни счётчик ошибок это не ловят,
        # поток просто подвисает без переключения. Ловим по факту: скорость ниже доли от bitrate_mbps подряд.
        # 2026-09-29: жёсткий 10с-порог на 30%-пороге дал ложное срабатывание (2.7-2.8М при норме 10.1М,
        # картинка на деле была целая — статмукс просто занизил битрейт на спокойной сцене, TEI/CC чистые).
        # Смягчено: порог ниже (20% вместо 30%), жёсткий таймер длиннее (20с вместо 10с).
        starve_ratio  = CONFIG.get("ts_starve_ratio", 0.2)
        starve_secs   = CONFIG.get("ts_starve_secs", 8.0)
        starve_hard_secs = CONFIG.get("ts_starve_hard_secs", 15.0)   # один эпизод дольше этого — переключаем
        # сразу, без «одной попытки того же источника»: слишком долго для случайной заминки статмукса
        # TVHeadend: реальный счётчик ошибок демультиплексора уже есть (_tvh_quality_checker, errors из
        # /api/status/subscriptions) — эвристика по ratchet-калиброванному "максимуму скорости" тут не нужна
        # и ложно срабатывает (TVHeadend-поток по-другому распределяет битрейт во времени, чем Энигма,
        # разовый всплеск статмукса попадает в калибровку как "норма канала" и потом бьёт по обычным колебаниям).
        is_tvheadend  = "/stream/channel/" in url
        starve_check  = has_backup and starve_ratio > 0 and not is_tvheadend
        starve_recorded = False   # этот эпизод уже учтён в хроническом счётчике (не дублировать, пока не отпустит)
        # Порог калибруется по РЕАЛЬНОЙ скорости этого канала, а не глобальным bitrate_mbps: SD-каналы
        # (напр. NTVSD «Че» ≈2.2 Мбит/с) идут заметно ниже HD-нормы 12М — фиксированный порог 12×0.3=3.6М
        # принимал их обычную скорость за просадку (ложное переключение, поймано 2026-09-29 на живом трафике).
        # Калибровка — это МАКСИМУМ скорости за сессию (растёт, никогда не падает сам по себе): разовая проба
        # сразу после прогрева тоже не годится — статмукс на спокойной сцене может занизить её в разы и тогда
        # порог не поймает настоящую просадку (тоже поймано в бою: ТНТ HD откалибровался по 6.7М вместо обычных
        # 10-12М). Стартуем с baseline из общего кэша (если канал уже когда-то читали), иначе 0 — тогда порог
        # сразу подхватится по ПЕРВОМУ же реальному замеру скорости после прогрева, без чужого допущения.
        starve_baseline = StreamSession._ts_baseline_mbps.get(self._pkey(url), 0.0)
        starve_floor  = CONFIG.get("ts_starve_floor_mbps", 0.5)
        starve_min    = max(starve_floor, starve_baseline * starve_ratio)
        low_since     = None
        last_q_check  = time.monotonic()
        last_q_log    = time.monotonic()
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as sess:
            async with sess.get(url, timeout=tc) as r:
                if r.status != 200:
                    raise Exception(f"TS HTTP {r.status}")
                self._last_data = time.monotonic()
                self._state = "normal"
                warmup_until = time.monotonic() + 10.0
                log.info(f"{self._tag} TS поток подключён"
                         + (" [мониторинг качества вкл]" if quality_check else "")
                         + (" [мониторинг просадки вкл]" if starve_check else ""))
                bytes_recv = 0
                async for chunk in r.content.iter_chunked(CONFIG["chunk_size"]):
                    if not self.running: return
                    if chunk:
                        if self._sink:
                            await self._sink(chunk)
                        else:
                            self._push(chunk)
                        bytes_recv += len(chunk)
                    if self._switch_requested:
                        self._switch_requested = False
                        raise Exception("Возврат на основной источник")
                    if tracker is not None and chunk:
                        tei_e, cc_e, total = tracker.scan(chunk)
                        tracker.push(tei_e, cc_e, total)
                    now = time.monotonic()
                    if now < warmup_until:
                        continue
                    if now - last_q_check >= 1.0:
                        last_q_check = now
                        # та же логика, что в _read_hls: фоновая проба каналов звука досчиталась → перезапуск
                        # на путь с принудительным AAC без ожидания ручного переключения канала
                        if self._sink is None and CONFIG.get("force_audio_aac", False) \
                                and self._audio_needs_norm(url) is True:
                            raise _Restart()
                        if tracker is not None:
                            degraded, tei_rate, cc_rate = tracker.is_degraded()
                            if now - last_q_log >= 60.0 and (tei_rate > 0 or cc_rate > 0):
                                log.info(f"{self._tag} TS качество: TEI={tei_rate:.2%} CC={cc_rate:.2%}")
                                last_q_log = now
                            if degraded:
                                log.warning(f"{self._tag} TS деградация: "
                                            f"TEI={tei_rate:.2%} (порог {tei_thr:.0%}), "
                                            f"CC={cc_rate:.2%} (порог {cc_thr:.0%}) → переключаю")
                                raise Exception(f"TS деградация: TEI={tei_rate:.2%} CC={cc_rate:.2%}")
                        if starve_check:
                            speed = self.metrics.speed() * 8 / 1e6
                            if speed > starve_baseline:
                                # высокий замер — канал явно может так быстро, поднимаем потолок (никогда не
                                # опускаем сам по себе: во время настоящей просадки speed ниже текущего max,
                                # порог остаётся прежним и продолжает ловить)
                                starve_baseline = speed
                                starve_min = max(starve_floor, starve_baseline * starve_ratio)
                                StreamSession._ts_baseline_mbps[self._pkey(url)] = starve_baseline
                            if 0 < speed < starve_min:
                                if low_since is None:
                                    low_since = now
                                dur = now - low_since
                                if dur >= starve_hard_secs:
                                    # один эпизод и так уже долгий — переключаем немедленно, без «попытки
                                    # того же источника»: это не разовая заминка статмукса
                                    log.warning(f"{self._tag} TS просел: {speed:.1f} Мбит/с < {starve_min:.1f} "
                                                f"(норма канала {starve_baseline:.1f}) дольше {dur:.0f}с (жёсткий порог "
                                                f"{starve_hard_secs:.0f}с) → переключаю немедленно")
                                    self._fail[self._src_idx] = (2, now)   # форсирует switch в _reader(), минуя retry-once
                                    raise Exception(f"TS просел: {speed:.1f} Мбит/с дольше {dur:.0f}с (жёсткий порог)")
                                if dur >= starve_secs and not starve_recorded:
                                    starve_recorded = True
                                    hist = self._starve_hist.setdefault(self._src_idx, deque())
                                    hist.append(now)
                                    while hist and now - hist[0] > 900:
                                        hist.popleft()
                                    c5  = sum(1 for t in hist if now - t <= 300)
                                    c10 = sum(1 for t in hist if now - t <= 600)
                                    c15 = len(hist)
                                    chronic = c5 >= 2 or c10 >= 3 or c15 >= 4
                                    log.warning(f"{self._tag} TS просел: {speed:.1f} Мбит/с < {starve_min:.1f} "
                                                f"(норма канала {starve_baseline:.1f}) дольше {starve_secs:.0f}с "
                                                f"(эпизодов: {c5}/5мин {c10}/10мин {c15}/15мин)")
                                    if chronic:
                                        log.warning(f"{self._tag} TS хронически нестабилен ({c5}/5мин {c10}/10мин "
                                                    f"{c15}/15мин) → переключаю без доп. попытки")
                                        self._fail[self._src_idx] = (2, now)
                                        raise Exception(f"TS хронически нестабилен: {c5}/5мин {c10}/10мин {c15}/15мин")
                            else:
                                low_since = None
                                starve_recorded = False
                # TS — живой поток, нормального завершения быть не должно
                raise Exception(f"TS поток завершён ({bytes_recv} байт)")

    async def _read_ts_audio_norm(self, url: str, af: str | None = None):
        """Спутник (Энигма) без видео-перекодирования: видео идёт копией, звук принудительно сводится в AAC
        стерео — либо по настройке force_audio_aac (многоканальный AC3 и т.п. → стерео без поправок), либо по
        таблице поправок громкости/даунмикса 5.1 (параметр af, см. _audio_fix_filter). Байты читает тот же
        _read_ts (мониторинг TEI/CC/просадки на входящих пакетах не теряется), но вместо буфера клиента кормит
        ими stdin ffmpeg; в буфер идёт уже перепакованный stdout."""
        aac_br = CONFIG.get("force_audio_bitrate", 256)
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-nostdin",
               "-fflags", "+genpts",
               "-f", "mpegts", "-analyzeduration", "1500000", "-probesize", "3000000", "-i", "pipe:0",
               "-map", "0:v:0", "-map", "0:a?", "-c:v", "copy",
               *(["-af", af] if af else []),
               "-c:a", "aac", "-ac", "2", "-b:a", f"{aac_br}k",
               "-f", "mpegts", "pipe:1"]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        tail: deque = deque(maxlen=6)
        # без tag: видео идёт copy (не декодируется), а TS-демуксер ffmpeg всё равно парсит NAL-заголовки H.264
        # для упаковки кадров и на битых пакетах со спутника печатает decode_slice_header/PPS-подобные ошибки —
        # они не означают, что что-то реально декодируется/портится в выходе. tail всё равно копится для
        # диагностики настоящего сбоя (ffmpeg завершился/не выдал данных).
        drain_task = asyncio.create_task(_drain_stderr(proc, tail))

        async def _sink(chunk: bytes):
            proc.stdin.write(chunk)
            await proc.stdin.drain()
        self._sink = _sink
        reader_task = asyncio.create_task(self._read_ts(url))
        reader_task.add_done_callback(lambda t: t.cancelled() or t.exception())
        log.info(f"{self._tag} TS → звук AAC стерео {aac_br}k{f' ({af})' if af else ''} (видео копией)")
        self._last_data = time.monotonic()
        idle_limit = CONFIG.get("tc_idle_secs", 12)
        try:
            while self.running:
                if reader_task.done():
                    raise (reader_task.exception() or Exception("читатель TS завершился"))
                try:
                    chunk = await asyncio.wait_for(proc.stdout.read(CONFIG["chunk_size"]), timeout=3)
                except asyncio.TimeoutError:
                    if time.monotonic() - self._last_data > idle_limit:
                        raise Exception(f"ffmpeg (аудио): нет данных {idle_limit} сек | " + " / ".join(tail)[-260:])
                    continue
                if not chunk:
                    raise Exception(f"ffmpeg (аудио) завершился (код {proc.returncode}) | " + " / ".join(tail)[-260:])
                self._last_data = time.monotonic()
                await self._wait_room()
                self._push(chunk)
                self._state = "normal"
                if self._switch_requested:
                    self._switch_requested = False
                    raise Exception("Возврат на основной источник")
        finally:
            self._sink = None
            reader_task.cancel()
            drain_task.cancel()
            try: proc.stdin.close()
            except Exception: pass
            if proc.returncode is None:
                try: proc.kill()
                except ProcessLookupError: pass
            await proc.wait()

    @staticmethod
    def _valid_ts(data: bytes, min_pkts: int = 5) -> bool:
        if len(data) < 188 * min_pkts:
            return False
        for start in range(188):
            if start >= len(data):
                break
            if data[start] != 0x47:
                continue
            count, pos = 0, start
            while pos + 188 <= len(data):
                if data[pos] == 0x47:
                    count += 1; pos += 188
                else:
                    break
            if count >= min_pkts:
                return True
        return False

    async def _probe_hls(self, m3u8_url: str) -> bool:
        pl_to  = aiohttp.ClientTimeout(connect=5, sock_read=10)
        seg_to = aiohttp.ClientTimeout(connect=5, sock_read=30)
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        try:
            async with aiohttp.ClientSession(connector=connector) as sess:
                async with sess.get(m3u8_url, timeout=pl_to, allow_redirects=True) as r:
                    if r.status != 200:
                        return False
                    content  = await r.text()
                    base_url = str(r.url)
                if "#EXT-X-STREAM-INF" in content:
                    variants = _parse_master_m3u8(content, base_url)
                    if not variants:
                        return False
                    _, media_url = variants[0]
                    async with sess.get(media_url, timeout=pl_to, allow_redirects=True) as r2:
                        if r2.status != 200:
                            return False
                        content  = await r2.text()
                        base_url = str(r2.url)
                target_dur, segs = _parse_m3u8(content, base_url)
                if not segs:
                    return False
                probe_segs = segs[-min(3, len(segs)):]
                total_bytes = 0; first_buf = b""
                t0 = time.monotonic()
                for i, (_, seg_url) in enumerate(probe_segs):
                    async with sess.get(seg_url, timeout=seg_to, allow_redirects=True) as sr:
                        if sr.status != 200:
                            log.warning(f"{self._tag} probe HLS: сегмент {i+1} HTTP {sr.status}")
                            return False
                        async for chunk in sr.content.iter_chunked(8192):
                            total_bytes += len(chunk)
                            if len(first_buf) < 188 * 20:
                                first_buf += chunk
                elapsed = time.monotonic() - t0
                if not self._valid_ts(first_buf):
                    log.warning(f"{self._tag} probe HLS: невалидный TS")
                    return False
                expected_dur = target_dur * len(probe_segs)
                ratio = elapsed / max(expected_dur, 0.1)
                speed_mbps = total_bytes * 8 / max(elapsed, 0.01) / 1_000_000
                log.info(f"{self._tag} probe HLS: {len(probe_segs)} сегм, {total_bytes//1024} КБ"
                         f" за {elapsed:.1f}с → {speed_mbps:.1f} Мбит/с, ratio={ratio:.2f}")
                if ratio > 1.5:
                    log.warning(f"{self._tag} probe HLS: слишком медленно (ratio={ratio:.2f})")
                    return False
                return True
        except Exception as e:
            log.debug(f"{self._tag} probe HLS exception: {e}")
            return False

    async def _probe_ts(self, url: str) -> bool:
        # Спутниковый мультиплекс статмуксует битрейт по сцене: короткая проба на статичной картинке
        # занижает результат даже на смотрибельном сигнале (проверено: 4.3 Мбит/с при пороге 6.0 — на деле
        # смотрибельно). Проба длиннее, порог мягче (доля от номинала настраивается ts_probe_min_ratio).
        probe_secs = CONFIG.get("ts_probe_secs", 8.0)
        # TVHeadend сюда не входит: его битрейт на спокойной SD-сцене штатно около 2 Мбит/с («Че»),
        # а без своей калибровки порог — 30% от номинала 12 Мбит/с = 3.6. Проба помечала живой поток
        # мёртвым (лог: 1.6 < 3.6 и 2.0 < 3.6) и не возвращала основной источник. Скорость решает
        # возврат у Энигмы и у TV Team (HLS, _probe_hls). Сохранённый замер TVHeadend для этого не нужен.
        check_speed = "/stream/channel/" not in url
        if not check_speed:
            StreamSession._ts_baseline_mbps.pop(self._pkey(url), None)
        baseline   = StreamSession._ts_baseline_mbps.get(self._pkey(url), CONFIG.get("bitrate_mbps", 12.0)) if check_speed else 0.0
        min_mbps   = baseline * CONFIG.get("ts_probe_min_ratio", 0.3) if check_speed else 0.0
        tc = aiohttp.ClientTimeout(connect=5, sock_connect=5, sock_read=probe_secs + 5)
        try:
            connector = aiohttp.TCPConnector(family=socket.AF_INET)
            async with aiohttp.ClientSession(connector=connector) as sess:
                async with sess.get(url, timeout=tc) as r:
                    if r.status != 200:
                        return False
                    buf = b""; t0 = time.monotonic()
                    async for chunk in r.content.iter_chunked(8192):
                        buf += chunk
                        if time.monotonic() - t0 >= probe_secs:
                            break
                    elapsed = max(time.monotonic() - t0, 0.01)
                    if not self._valid_ts(buf):
                        log.warning(f"{self._tag} probe TS: невалидный TS")
                        return False
                    speed_mbps = len(buf) * 8 / elapsed / 1_000_000
                    if not check_speed:
                        log.info(f"{self._tag} probe TS: {len(buf)//1024} КБ за {elapsed:.1f}с"
                                 f" → {speed_mbps:.1f} Мбит/с (TVHeadend, скорость не критерий)")
                        return True
                    log.info(f"{self._tag} probe TS: {len(buf)//1024} КБ за {elapsed:.1f}с"
                             f" → {speed_mbps:.1f} Мбит/с (мин {min_mbps:.1f})")
                    if speed_mbps < min_mbps:
                        log.warning(f"{self._tag} probe TS: слишком медленно"
                                    f" ({speed_mbps:.1f} < {min_mbps:.1f} Мбит/с)")
                        return False
                    return True
        except Exception as e:
            log.debug(f"{self._tag} probe TS exception: {e}")
            return False

    async def _primary_checker(self):
        interval = CONFIG.get("fallback_check_secs", 900)
        while self.running:
            self._next_check_at = time.monotonic() + interval
            await asyncio.sleep(interval)
            if not self.running:
                return
            if self._src_idx == 0:
                self._next_check_at = 0.0
                continue
            if not self._sources:
                continue
            self._next_check_at = 0.0
            primary_type, primary_url = self._sources[0][:2]
            log.info(f"{self._tag} Проверяю основной источник [{primary_type}]...")
            ok = False
            try:
                if primary_type == "hls":
                    ok = await self._probe_hls(primary_url)
                else:
                    ok = await self._probe_ts(primary_url)
            except Exception as e:
                log.debug(f"{self._tag} Проверка основного: {e}")
            if ok:
                log.info(f"{self._tag} Основной источник восстановлен → возвращаюсь")
                self._src_idx = 0
                self._switch_requested = True
            else:
                log.info(f"{self._tag} Основной источник недоступен → следующая проверка через {interval}с")

    def kill_ip(self, ip: str):
        for cip, q in self._clients:
            if cip == ip:
                try: q.put_nowait(None)
                except asyncio.QueueFull: pass

    async def connect_client(self, request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(headers={
            "Content-Type":  "video/mp2t",
            "Cache-Control": "no-cache",
            "Connection":    "keep-alive",
        })
        try:
            await resp.prepare(request)
        except Exception:
            return resp
        # очередь на ~30 сек при 12 Мбит/с: пачки сегментов не должны переполнять её у медленного клиента
        q   = asyncio.Queue(maxsize=CONFIG.get("client_queue_chunks", 600))
        ip  = request.remote
        self.kill_ip(ip)
        entry = (ip, q)
        self._clients.append(entry)
        log.info(f"{self._tag} Клиент {ip} подключён ({len(self._clients)} всего)")
        idle = 0.0
        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(q.get(), timeout=2)
                    idle = 0.0
                except asyncio.TimeoutError:
                    idle += 2
                    if idle >= 60:
                        log.warning(f"{self._tag} Клиент {ip}: нет данных 60 сек — закрываю")
                        break
                    # keepalive: null-пакеты (PID 0x1FFF) не дают плееру посчитать сессию мёртвой,
                    # пока источник «подёргивается» и буфер пополняется
                    try:
                        await asyncio.wait_for(resp.write(_TS_NULL * 4), timeout=10.0)
                    except asyncio.TimeoutError:
                        log.warning(f"{self._tag} Клиент {ip}: таймаут записи")
                        break
                    continue
                if chunk is None: break
                try:
                    tw = time.monotonic()
                    await asyncio.wait_for(resp.write(chunk), timeout=10.0)
                    dt = time.monotonic() - tw
                    if dt > 1.0:
                        # запись блокировалась: приёмник (приставка/сеть) не успевает читать — это не наш источник
                        log.warning(f"{self._tag} Клиент {ip}: запись заблокирована на {dt:.1f} с (очередь клиента {q.qsize()} кусков)")
                except asyncio.TimeoutError:
                    log.warning(f"{self._tag} Клиент {ip}: таймаут записи")
                    break
        except (ConnectionResetError, BrokenPipeError): pass
        except Exception as e: log.debug(f"{self._tag} Клиент {ip}: {e}")
        finally:
            if entry in self._clients: self._clients.remove(entry)
            log.info(f"{self._tag} Клиент {ip} ушёл ({len(self._clients)} осталось)")
        return resp

    def client_count(self) -> int:
        return len(self._clients)

    def stop(self):
        self.running = False
        self._kick_all_clients()   # немедленно уведомляем клиентов, не ждём 60 сек таймаута

    def status(self) -> dict:
        buf_sec = self._chunks2sec(len(self._buf))
        now     = time.monotonic()
        rc60    = sum(1 for t in self._rc_times if t > now - 60)
        return {
            "ch_id":          self.ch_id,
            "name":           self.name,
            "state":          self._state,
            "clients":        len(self._clients),
            "client_ips":     [ip for ip, _ in self._clients],
            "client_q":       [q.qsize() for _, q in self._clients],
            "drops":          self._drops,
            "pace_s":         round(self._cushion_s(), 1) if self._paced else None,
            "pace_mb":        round(self._pq_bytes / 1e6, 1) if self._paced else None,
            "pace_dropped_kb": self._pq_dropped // 1024,
            "buffer_sec":     round(buf_sec, 1),
            "target_sec":     self._buf_secs(),
            "speed_mbps":     round(self.metrics.speed() * 8 / 1_000_000, 2),
            "trend":          self.metrics.trend(),
            "reconnects":     self._rc,
            "reconnects_60s": rc60,
            "source_type":    self._src_type,
            "source_name":    self._src_name,
            "source_idx":     self._src_idx,
            "check_in_secs":  max(0, round(self._next_check_at - now))
                              if self._src_idx > 0 and self._next_check_at > 0 else None,
        }


class CmChannelDB:
    """Загружает URL треков из channel_manager's DB."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._map:   dict[str, list[tuple[str, str]]] = {}
        self._names: dict[str, str] = {}
        self.refresh()

    def refresh(self):
        new_map:   dict[str, list[tuple[str, str]]] = {}
        new_names: dict[str, str] = {}
        try:
            db = open_db(self._db_path)
            rows = db.execute(
                "SELECT c.id, c.name, t.url, s.name AS src_name, "
                # t.profile: '' = как у источника (по умолчанию), 'gpu_h264' = принудительно включено
                # для этой дорожки, 'off' = принудительно выключено (даже если у источника включено)
                "CASE t.profile WHEN 'off' THEN '' WHEN 'gpu_h264' THEN 'gpu_h264' "
                "ELSE COALESCE(s.profile,'') END AS eff_profile "
                "FROM channels c JOIN tracks t ON t.channel_id=c.id "
                "JOIN sources s ON s.id=t.source_id "
                "WHERE c.enabled=1 AND c.sort_order > 0 AND s.enabled=1 AND t.enabled=1 "
                "ORDER BY c.sort_order, c.name, t.priority"
            ).fetchall()
            db.close()
            unify = CONFIG.get("force_video_unify", False)
            for row in rows:
                ch_id = str(row["id"])
                # «Единый поток»: принудительно gpu_h264 АБСОЛЮТНО на всех дорожках, игнорируя даже
                # ручной per-track флажок OFF — по прямому запросу (единые кодек/развёртка важнее
                # точечных исключений). Безопасный клапан на перегрузку GPU остаётся прежним:
                # max_transcodes в _read_transcode просто откатит ЛИШНИЕ сверх лимита каналы на raw.
                eff_profile = "gpu_h264" if unify else (row["eff_profile"] or "")
                new_map.setdefault(ch_id, []).append((_detect_src_type(row["url"]), row["url"],
                                                      row["src_name"], eff_profile))
                new_names[ch_id] = row["name"]
        except Exception as e:
            log.warning(f"CmChannelDB.refresh: {e}")
        self._map   = new_map
        self._names = new_names
        log.info(f"CmChannelDB: {len(self._map)} каналов с треками")

    def sources(self, ch_id: str) -> list[tuple[str, str]]:
        return self._map.get(ch_id, [])

    def name(self, ch_id: str) -> str:
        return self._names.get(ch_id, ch_id)


class TsbDaemon:
    """Встроенный TS-буфер: управляет стриминговыми сессиями."""

    def __init__(self, db_path: str):
        self._db      = CmChannelDB(db_path)
        self._db_path = db_path
        self._sessions:   dict[str, StreamSession] = {}
        self._ip_history: dict[str, list[str]]     = {}
        self._ch_info:    dict[str, dict]          = {}
        self._ch_info_at: float                    = 0.0

    async def refresh_db(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._db.refresh)
        # живые сессии тоже получают новые адреса: текущее соединение не рвём,
        # но при следующей ошибке/переподключении _reader возьмёт уже свежий URL
        for ch_id, sess in self._sessions.items():
            fresh = self._db.sources(ch_id)
            if fresh:
                sess._sources = fresh

    async def _refresh_loop(self):
        while True:
            await asyncio.sleep(3600)
            await self.refresh_db()

    async def _get_session(self, ch_id: str) -> StreamSession:
        # ch_id вида "<канал>~<N>" — принудительный источник N (по порядку приоритета), без автопереключения
        # "<канал>~unify" — отдельная сессия с принудительным gpu_h264 на всех дорожках (единый кодек/
        # деинтерлейс/звук AAC стерео), независимо от глобального force_video_unify и ручных per-track OFF —
        # своя ссылка (/stream/{id}?unify=1), не трогает обычную сессию того же канала
        base, _, forced = ch_id.partition("~")
        if ch_id not in self._sessions:
            sources = self._db.sources(base)
            name    = self._db.name(base)
            if forced.isdigit() and int(forced) < len(sources):
                sources = [sources[int(forced)]]
                name = f"{name} [источник {int(forced) + 1}]"
            elif forced == "unify":
                sources = [(s[0], s[1], s[2] if len(s) > 2 else "", "gpu_h264") for s in sources]
                name = f"{name} [единый поток]"
            sess = StreamSession(ch_id, sources, name)
            await sess.start()
            self._sessions[ch_id] = sess
            asyncio.create_task(self._idle_watch(ch_id))
        elif not forced:
            fresh = self._db.sources(base)
            if fresh:
                self._sessions[ch_id]._sources = fresh
        elif forced == "unify":
            fresh = self._db.sources(base)
            if fresh:
                self._sessions[ch_id]._sources = [(s[0], s[1], s[2] if len(s) > 2 else "", "gpu_h264") for s in fresh]
        return self._sessions[ch_id]

    async def _idle_watch(self, ch_id: str):
        idle = CONFIG["session_idle_timeout"]
        while ch_id in self._sessions:
            await asyncio.sleep(idle)
            sess = self._sessions.get(ch_id)
            if sess and sess.client_count() == 0:
                sess.stop()
                del self._sessions[ch_id]
                log.info(f"[ch:{ch_id}] Сессия удалена (нет клиентов)")
                # Убираем из истории IP чтобы не блокировать слот
                for hist in self._ip_history.values():
                    if ch_id in hist:
                        hist.remove(ch_id)
                return

    # ── Handlers ─────────────────────────────────────────────────────────────

    async def h_sources(self, req: web.Request) -> web.Response:
        """Источники канала в порядке приоритета (индекс = параметр ?src= у /stream/{id})."""
        ch_id = req.match_info["id"]
        srcs = self._db.sources(ch_id)
        return web.json_response({"sources": [
            {"idx": i, "type": s[0], "name": (s[2] if len(s) > 2 else "")} for i, s in enumerate(srcs)
        ]})

    async def h_stream(self, req: web.Request) -> web.StreamResponse:
        ch_id = req.match_info["id"]
        src   = req.query.get("src", "")
        if src.isdigit():
            ch_id = f"{ch_id}~{int(src)}"
        elif req.query.get("unify", "") == "1":
            ch_id = f"{ch_id}~unify"
        ip    = req.remote
        # Убиваем этот IP из всех других сессий
        for h, s in list(self._sessions.items()):
            if h != ch_id:
                s.kill_ip(ip)
        hist = self._ip_history.setdefault(ip, [])
        if ch_id in hist: hist.remove(ch_id)
        hist.append(ch_id)
        while len(hist) > 1:
            old = hist.pop(0)
            if old in self._sessions:
                old_sess = self._sessions[old]
                # Проверяем клиентов с ДРУГИХ IP (наш ещё числится в _clients до asyncio)
                other_clients = [c for c in old_sess._clients if c[0] != ip]
                if not other_clients:
                    # Только наш клиент был — останавливаем сессию немедленно
                    old_sess.stop()
                    del self._sessions[old]
        sess = await self._get_session(ch_id)
        return await sess.connect_client(req)

    async def h_tsb_events(self, req: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(headers={
            "Content-Type":      "text/event-stream",
            "Cache-Control":     "no-cache",
            "Connection":        "keep-alive",
            "X-Accel-Buffering": "no",
        })
        await resp.prepare(req)
        sent = len(_memlog.records)
        base = f"http://{req.host}"
        try:
            while True:
                new_logs = _memlog.records[sent:]
                sent = len(_memlog.records)
                sessions = self._enrich_sessions([s.status() for s in self._sessions.values()], base)
                data = {
                    "sessions":      sessions,
                    "total_clients": sum(s.client_count() for s in self._sessions.values()),
                    "logs":          new_logs,
                }
                await resp.write(f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode())
                await asyncio.sleep(1)
        except (ConnectionResetError, asyncio.CancelledError, BrokenPipeError):
            pass
        return resp

    def _enrich_sessions(self, sessions: list[dict], base_url: str) -> list[dict]:
        """Добавляет icon_url, epg (текущая передача), stream_url к каждой сессии."""
        now = time.monotonic()
        if now - self._ch_info_at > 60.0:
            try:
                db = open_db(CONFIG["db_path"])
                self._ch_info = {
                    str(r["id"]): {"icon_url": r["icon_url"], "epg_id": r["epg_id"]}
                    for r in db.execute("SELECT id, icon_url, epg_id FROM channels")
                }
                db.close()
                self._ch_info_at = now
            except Exception:
                pass
        ch_info = self._ch_info

        for s in sessions:
            ch_id = str(s.get("ch_id", ""))
            info = ch_info.get(ch_id, {})
            epg_id = info.get("epg_id") or ""
            s["icon_url"]   = info.get("icon_url") or ""
            s["epg"]        = _epg_cache.get_current_program(epg_id)
            s["stream_url"] = f"{base_url}/stream/{ch_id}" if ch_id else ""
        return sessions

    async def h_tsb_status(self, req: web.Request) -> web.Response:
        base = f"http://{req.host}"
        sessions = self._enrich_sessions([s.status() for s in self._sessions.values()], base)
        return web.json_response({
            "sessions":      sessions,
            "total_clients": sum(s.client_count() for s in self._sessions.values()),
        })

    async def h_tsb_refresh_db(self, req: web.Request) -> web.Response:
        await self.refresh_db()
        return web.json_response({"ok": True, "channels": len(self._db._map)})

    async def h_tsb_session_reset(self, req: web.Request) -> web.Response:
        ch_id = req.match_info["id"].upper()
        sess = self._sessions.pop(ch_id, None)
        if sess is None:
            return web.json_response({"ok": False, "error": "no session"}, status=404)
        sess.stop()
        return web.json_response({"ok": True, "channel": ch_id})

    async def h_tsb_config_get(self, req: web.Request) -> web.Response:
        return web.json_response({k: CONFIG.get(k, d) for k, d in [
            ("buffer_seconds", 12), ("buffer_seconds_ts", 3), ("bitrate_mbps", 12.0), ("hls_fallback_secs", 30), ("fallback_check_secs", 900),
            ("session_idle_timeout", 30), ("hls_connect_timeout", 5), ("hls_read_timeout", 12), ("hls_seg_timeout", 30),
            ("ts_connect_timeout", 5), ("ts_read_timeout", 15), ("ts_tei_threshold", 0.02), ("ts_cc_threshold", 0.01),
            ("ts_quality_window", 5.0),
            ("audio_gain_fix_enabled", False),
            ("force_audio_aac", False), ("force_audio_bitrate", 256), ("hls_audio_lang", "rus"),
            ("tvh_quality_poll_secs", 10), ("tvh_quality_hard_delta", 30),
            ("force_video_unify", False), ("max_transcodes", 3),
        ]})

    async def h_tsb_config_post(self, req: web.Request) -> web.Response:
        try:
            data = await req.json()
            allowed = {
                "buffer_seconds", "buffer_seconds_ts", "bitrate_mbps", "hls_fallback_secs", "fallback_check_secs",
                "session_idle_timeout", "hls_connect_timeout", "hls_read_timeout", "hls_seg_timeout",
                "ts_connect_timeout", "ts_read_timeout", "ts_tei_threshold", "ts_cc_threshold",
                "ts_quality_window",
                "audio_gain_fix_enabled",
                "force_audio_aac", "force_audio_bitrate", "hls_audio_lang",
                "tvh_quality_poll_secs", "tvh_quality_hard_delta",
                "force_video_unify", "max_transcodes",
            }
            for k, v in data.items():
                if k in allowed:
                    CONFIG[k] = v
            _save_cfg()
            return web.json_response({"ok": True})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

    async def h_tsb_restart(self, req: web.Request) -> web.Response:
        svc = "channel-manager"
        asyncio.create_task(asyncio.to_thread(subprocess.Popen, ["systemctl", "restart", svc]))
        return web.json_response({"ok": True})


# ─── APP ─────────────────────────────────────────────────────────────────────

class App:
    def __init__(self, tsb: TsbDaemon):
        self._db  = CONFIG["db_path"]
        self._tsb = tsb

    @contextmanager
    def _con(self):
        con = open_db(self._db)
        try:
            with con:
                yield con
        finally:
            con.close()

    async def h_index(self, req: web.Request) -> web.Response:
        return web.Response(text=_HTML, content_type="text/html")

    async def h_playlist(self, req: web.Request) -> web.Response:
        host = req.host.split(":")[0]
        base = f"http://{host}:{CONFIG['port']}"
        m3u  = generate_m3u(self._db, base)
        return web.Response(text=m3u, content_type="application/x-mpegurl",
                            headers={"Content-Disposition": "attachment; filename=playlist.m3u8"})

    async def h_playlist_unify(self, req: web.Request) -> web.Response:
        host = req.host.split(":")[0]
        base = f"http://{host}:{CONFIG['port']}"
        m3u  = generate_m3u(self._db, base, unify=True)
        return web.Response(text=m3u, content_type="application/x-mpegurl",
                            headers={"Content-Disposition": "attachment; filename=playlist-unify.m3u8"})

    # ── Тестовые варианты обработки потока Энигмы (для подбора того, что понимает приставка) ──

    _T_AAC = ["-c:a", "aac", "-ac", "2", "-b:a", "256k"]
    _T_HW  = ["-hwaccel", "vaapi", "-hwaccel_device", "/dev/dri/renderD128",
              "-hwaccel_output_format", "vaapi"]
    _T_ENC = ["-c:v", "h264_vaapi", "-profile:v", "high", "-level", "4.1",
              "-b:v", "8M", "-maxrate", "10M", "-bufsize", "16M"]
    # variant: (опции входа, опции видео, опции звука, описание)
    _TEST_VARIANTS = {
        "aac":           ([], ["-c:v", "copy"], _T_AAC, "видео копия, звук AAC (проверка MP2)"),
        "aac_sps":       ([], ["-c:v", "copy", "-bsf:v", "dump_extra=freq=keyframe"], _T_AAC,
                          "видео копия + SPS/PPS на каждом ключевом кадре, звук AAC"),
        "enc":           (_T_HW, _T_ENC, ["-c:a", "copy"], "видео перекодировано (progressive), звук MP2 копия"),
        "enc_aac":       (_T_HW, _T_ENC, _T_AAC, "видео перекодировано (progressive) + звук AAC"),
        "enc_deint_aac": (_T_HW, ["-vf", "deinterlace_vaapi=mode=default:rate=frame"] + _T_ENC, _T_AAC,
                          "видео с деинтерлейсом 25p + звук AAC"),
        # сравнение качества для настоящей чересстрочки: CPU-bwdif, кодирование на GPU
        "bwdif25_12m":   (["-vaapi_device", "/dev/dri/renderD128"],
                          ["-vf", "bwdif=mode=send_frame:deint=all,format=nv12,hwupload", "-c:v", "h264_vaapi",
                           "-profile:v", "high", "-level", "4.1", "-b:v", "12M", "-maxrate", "15M",
                           "-bufsize", "24M", "-g", "50"], _T_AAC, "CPU-деинтерлейс 25p, 12 Мбит/с"),
        "bwdif50_12m":   (["-vaapi_device", "/dev/dri/renderD128"],
                          ["-vf", "bwdif=mode=send_field:deint=all,format=nv12,hwupload", "-c:v", "h264_vaapi",
                           "-profile:v", "high", "-level", "4.2", "-b:v", "12M", "-maxrate", "15M",
                           "-bufsize", "24M", "-g", "50"], _T_AAC, "CPU-деинтерлейс 50p (50 к/с), 12 Мбит/с"),
    }
    _test_active = 0

    def _enigma_url(self, ch_id: str, src: str | None = None) -> str | None:
        """URL трека для теста: по умолчанию Энигма, либо трек указанного источника (?src=<id>)."""
        with self._con() as con:
            if src and src.isdigit():
                row = con.execute("SELECT url FROM tracks WHERE channel_id=? AND source_id=? LIMIT 1",
                                  (ch_id, int(src))).fetchone()
            else:
                row = con.execute(
                    "SELECT t.url FROM tracks t JOIN sources s ON s.id=t.source_id "
                    "WHERE t.channel_id=? AND s.src_type='enigma2' ORDER BY t.enabled DESC, t.priority LIMIT 1",
                    (ch_id,)).fetchone()
        return row["url"] if row else None

    async def h_test_stream(self, req: web.Request) -> web.StreamResponse:
        variant = req.match_info["variant"]
        if variant.isdigit():   # номер из плейлиста (№2..№6) вместо имени варианта
            names = list(self._TEST_VARIANTS)
            variant = names[int(variant) - 2] if 2 <= int(variant) < 2 + len(names) else variant
        v = self._TEST_VARIANTS.get(variant)
        url = self._enigma_url(req.match_info["id"], req.query.get("src"))
        if not v or not url:
            return web.Response(status=404, text="нет такого варианта или у канала нет трека Энигмы")
        if App._test_active >= 3:
            return web.Response(status=503, text="слишком много тестовых потоков")
        pre, vid, aud, _ = v
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-fflags", "+genpts",
               *pre, "-i", url, "-map", "0:v:0", "-map", "0:a:0", *vid, *aud, "-f", "mpegts", "pipe:1"]
        resp = web.StreamResponse(headers={"Content-Type": "video/mp2t", "Cache-Control": "no-cache"})
        await resp.prepare(req)
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        App._test_active += 1
        log.info(f"test-stream {variant} {req.match_info['id']} → {req.remote}")
        try:
            while True:
                chunk = await proc.stdout.read(65424)
                if not chunk:
                    break
                await resp.write(chunk)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            App._test_active -= 1
            if proc.returncode is None:
                try: proc.kill()
                except ProcessLookupError: pass
            await proc.wait()
        return resp

    # ── Сканирование потоков Энигмы: реальная чересстрочность (idet) → кеш проб ──────────
    _scan = {"running": False, "done": 0, "total": 0, "results": []}

    async def h_probe_scan(self, req: web.Request) -> web.Response:
        if req.method == "GET":
            s = App._scan
            return web.json_response({"running": s["running"], "done": s["done"], "total": s["total"],
                                      "results": s["results"]})
        if App._scan["running"]:
            return web.json_response({"ok": False, "error": "уже идёт"})
        with self._con() as con:
            rows = con.execute(
                "SELECT c.name, t.url, s.name AS src FROM channels c JOIN tracks t ON t.channel_id=c.id "
                "JOIN sources s ON s.id=t.source_id WHERE s.src_type='enigma2' AND t.enabled=1 AND s.enabled=1 "
                "AND c.enabled=1 AND c.sort_order>0 ORDER BY c.sort_order, c.name").fetchall()
        rows = [(r["name"], r["url"], r["src"]) for r in rows]
        App._scan = {"running": True, "done": 0, "total": len(rows), "results": []}
        asyncio.create_task(self._probe_scan_task(rows))
        return web.json_response({"ok": True, "total": len(rows)})

    async def _probe_scan_task(self, rows):
        sess = StreamSession("scan", [], "scan")
        try:
            for name, url, src in rows:
                # свежая проба (кеш не используем), по одному каналу — не перегружаем тюнеры Энигмы
                StreamSession._probe_cache.pop(sess._pkey(url), None)
                try:
                    info = await sess._ffprobe_stream(url)
                except Exception as e:
                    info = None
                App._scan["results"].append({
                    "name": name, "src": src,
                    "ok": bool(info),
                    "size": f"{info['w']}x{info['h']}" if info else None,
                    "flag": info["flagged_interlaced"] if info else None,
                    "interlaced": info["interlaced"] if info else None,
                })
                App._scan["done"] += 1
                await asyncio.sleep(0.5)
        finally:
            App._scan["running"] = False
            log.info(f"probe-scan завершён: {App._scan['done']}/{App._scan['total']}")

    async def h_test_playlist(self, req: web.Request) -> web.Response:
        host = req.host.split(":")[0]
        base = f"http://{host}:{CONFIG['port']}"
        lines = ["#EXTM3U"]
        with self._con() as con:
            chans = con.execute(
                "SELECT DISTINCT c.id, c.name FROM channels c JOIN tracks t ON t.channel_id=c.id "
                "JOIN sources s ON s.id=t.source_id WHERE s.src_type='enigma2' AND t.enabled=1 "
                "AND c.name IN ('Первый канал','НТВ HD','СТС','ТНТ HD','Че') ORDER BY c.name").fetchall()
        for ch in chans:
            lines.append(f'#EXTINF:-1,{ch["name"]} №1 оригинал (как с Энигмы)')
            lines.append(f"{base}/stream/{ch['id']}")
            for i, (name, (_, _, _, desc)) in enumerate(self._TEST_VARIANTS.items(), 2):
                lines.append(f'#EXTINF:-1,{ch["name"]} №{i} {desc}')
                lines.append(f"{base}/test/{name}/{ch['id']}")
        # те же варианты для потока tvteam (src=2): ТВ 3 HD и «Fan» для сравнения
        with self._con() as con:
            tv = con.execute("SELECT c.id, c.name FROM channels c JOIN tracks t ON t.channel_id=c.id "
                             "WHERE t.source_id=2 AND c.name IN ('ТВ 3 HD','Fan') ORDER BY c.name").fetchall()
        for ch in tv:
            lines.append(f'#EXTINF:-1,{ch["name"]} (tvteam) №1 оригинал (как с tvteam)')
            lines.append(f"{base}/stream/{ch['id']}")
            for i, (name, (_, _, _, desc)) in enumerate(self._TEST_VARIANTS.items(), 2):
                lines.append(f'#EXTINF:-1,{ch["name"]} (tvteam) №{i} {desc}')
                lines.append(f"{base}/test/{name}/{ch['id']}?src=2")
        return web.Response(text="\n".join(lines) + "\n", content_type="application/x-mpegurl")

    async def h_epg(self, req: web.Request) -> web.Response:
        xml = await _epg_cache.get_filtered(self._db)
        return web.Response(body=xml, content_type="text/xml")

    # Тех-канал: null-TS + sync-all в фоне
    async def h_sync_all_stream(self, req: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(headers={
            "Content-Type": "video/mp2t",
            "Cache-Control": "no-cache",
        })
        await resp.prepare(req)
        done = asyncio.Event()
        async def _run():
            try:
                await sync_all_sources(self._db)
            finally:
                done.set()
        asyncio.create_task(_run())
        await _stream_null_ts(resp, done)
        return resp

    # ── Channels ─────────────────────────────────────────────────────────

    async def h_channels(self, req: web.Request) -> web.Response:
        src_id = req.query.get("source_id")
        base_q = """
            SELECT c.id, c.name, c.group_name, c.icon_url, c.epg_id,
                   c.enabled, c.sort_order,
                   COUNT(DISTINCT t.id) AS track_count,
                   GROUP_CONCAT(DISTINCT s.name) AS sources
            FROM channels c
            LEFT JOIN tracks t ON t.channel_id=c.id
            LEFT JOIN sources s ON s.id=t.source_id
        """
        with self._con() as con:
            if src_id:
                rows = con.execute(
                    base_q + " WHERE c.id IN (SELECT channel_id FROM tracks WHERE source_id=?)"
                    " GROUP BY c.id ORDER BY c.sort_order, c.group_name, c.name",
                    (int(src_id),),
                ).fetchall()
            else:
                rows = con.execute(
                    base_q + " GROUP BY c.id ORDER BY c.sort_order, c.group_name, c.name"
                ).fetchall()
        return web.json_response({"channels": [dict(r) for r in rows]})

    async def h_channel_add(self, req: web.Request) -> web.Response:
        d = await req.json()
        name = (d.get("name") or "").strip()
        if not name:
            return web.json_response({"ok": False, "error": "name обязателен"})
        ch_id = _stable_id(f"manual:{name}")
        try:
            with self._con() as con:
                con.execute(
                    "INSERT OR IGNORE INTO channels (id,name,name_norm,group_name,icon_url,epg_id) "
                    "VALUES (?,?,?,?,?,?)",
                    (ch_id, name, normalize_name(name),
                     d.get("group_name",""), d.get("icon_url",""), d.get("epg_id","")),
                )
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)})
        return web.json_response({"ok": True, "id": ch_id})

    async def h_channel_get(self, req: web.Request) -> web.Response:
        ch_id = req.match_info["id"]
        with self._con() as con:
            ch = con.execute("SELECT * FROM channels WHERE id=?", (ch_id,)).fetchone()
            if not ch:
                return web.json_response({"error": "not found"}, status=404)
            tracks = con.execute("""
                SELECT t.id, t.priority, t.stream_type, t.url, t.profile, t.source_id,
                       s.name AS source_name, s.profile AS source_profile
                FROM tracks t JOIN sources s ON s.id=t.source_id
                WHERE t.channel_id=? ORDER BY t.priority
            """, (ch_id,)).fetchall()
        return web.json_response({"channel": dict(ch), "tracks": [dict(r) for r in tracks]})

    async def h_channel_patch(self, req: web.Request) -> web.Response:
        ch_id = req.match_info["id"]
        d = await req.json()
        # profile больше не поддерживается на уровне канала — см. profile у дорожки (PATCH /api/tracks/{id})
        keys = {"enabled","name","group_name","icon_url","epg_id","sort_order"}
        upd = {k: v for k, v in d.items() if k in keys}
        if not upd:
            return web.json_response({"ok": False})
        sql = ", ".join(f"{k}=?" for k in upd)
        with self._con() as con:
            con.execute(f"UPDATE channels SET {sql} WHERE id=?", (*upd.values(), ch_id))
        # кеш источников TS-буфера (CmChannelDB) зависит от enabled/sort_order/name —
        # без refresh только что включённый канал отдаёт пустой поток до часового обновления
        if upd.keys() & {"enabled", "sort_order", "name"}:
            await self._tsb.refresh_db()
        return web.json_response({"ok": True})

    async def h_channel_delete(self, req: web.Request) -> web.Response:
        with self._con() as con:
            con.execute("DELETE FROM channels WHERE id=?", (req.match_info["id"],))
        await self._tsb.refresh_db()
        return web.json_response({"ok": True})

    async def h_channel_renumber(self, req: web.Request) -> web.Response:
        """Переназначает sort_order последовательно (1,2,3…) сохраняя порядок, убирая дубли."""
        with self._con() as con:
            rows = con.execute(
                "SELECT id FROM channels WHERE sort_order > 0 AND sort_order < 9999 "
                "ORDER BY sort_order, name"
            ).fetchall()
            for i, row in enumerate(rows, 1):
                con.execute("UPDATE channels SET sort_order=? WHERE id=?", (i, row["id"]))
            # обнуляем sort_order=9999 (беспорядковые из старой миграции) → 0
            con.execute("UPDATE channels SET sort_order=0 WHERE sort_order=9999")
        await self._tsb.refresh_db()
        return web.json_response({"ok": True, "count": len(rows)})

    # ── Sources ───────────────────────────────────────────────────────────

    async def h_source_groups(self, req: web.Request) -> web.Response:
        """GET /api/sources/{id}/groups — группы/букеты источника из tracks.group_name."""
        sid = int(req.match_info["id"])
        with self._con() as con:
            grp_rows = con.execute("""
                SELECT t.group_name,
                    COUNT(t.channel_id) AS total,
                    SUM(t.enabled) AS enabled_cnt
                FROM tracks t
                WHERE t.source_id=? AND t.group_name != ''
                GROUP BY t.group_name ORDER BY t.group_name
            """, (sid,)).fetchall()
            flt = {r["group_name"]: r["enabled"] for r in con.execute(
                "SELECT group_name, enabled FROM source_group_filters WHERE source_id=?", (sid,)
            ).fetchall()}
        groups = []
        for r in grp_rows:
            gn = r["group_name"]
            fe = flt.get(gn, 1)  # по умолчанию включено если записи нет
            groups.append({"group": gn, "total": r["total"],
                           "enabled_cnt": r["enabled_cnt"], "filter_enabled": fe})
        return web.json_response({"groups": groups})

    async def h_source_groups_channels(self, req: web.Request) -> web.Response:
        """GET /api/sources/{id}/groups/{grp}/channels"""
        sid = int(req.match_info["id"])
        grp = req.match_info["grp"]
        with self._con() as con:
            rows = con.execute("""
                SELECT c.id, c.name, t.enabled
                FROM channels c JOIN tracks t ON t.channel_id=c.id
                WHERE t.source_id=? AND t.group_name=?
                ORDER BY c.sort_order, c.name
            """, (sid, grp)).fetchall()
        return web.json_response({"channels": [dict(r) for r in rows]})

    async def h_source_groups_patch(self, req: web.Request) -> web.Response:
        """POST /api/sources/{id}/groups/filters — batch enable/disable групп и каналов."""
        sid = int(req.match_info["id"])
        d = await req.json()
        # groups: [{group, enabled}, ...]
        # channels: [{id, enabled}, ...]
        affected: set[str] = set()
        with self._con() as con:
            for g in d.get("groups", []):
                gn, ge = g["group"], 1 if g["enabled"] else 0
                con.execute(
                    "INSERT OR REPLACE INTO source_group_filters (source_id,group_name,enabled) "
                    "VALUES (?,?,?)", (sid, gn, ge)
                )
                con.execute("""
                    UPDATE tracks SET enabled=?
                    WHERE source_id=? AND group_name=?
                """, (ge, sid, gn))
                rows = con.execute(
                    "SELECT channel_id FROM tracks WHERE source_id=? AND group_name=?",
                    (sid, gn)
                ).fetchall()
                affected.update(r["channel_id"] for r in rows)
            for ch in d.get("channels", []):
                con.execute("""
                    UPDATE tracks SET enabled=?
                    WHERE source_id=? AND channel_id=?
                """, (1 if ch["enabled"] else 0, sid, ch["id"]))
                affected.add(ch["id"])
        await self._tsb.refresh_db()
        reset_count = 0
        for ch_id in affected:
            sess = self._tsb._sessions.pop(ch_id.upper(), None)
            if sess:
                sess.stop()
                reset_count += 1
        return web.json_response({"ok": True, "reset_sessions": reset_count})

    async def h_sources(self, req: web.Request) -> web.Response:
        with self._con() as con:
            rows = con.execute("""
                SELECT s.*, COUNT(t.id) AS track_count
                FROM sources s LEFT JOIN tracks t ON t.source_id=s.id
                GROUP BY s.id ORDER BY s.priority, s.name
            """).fetchall()
        return web.json_response({"sources": [dict(r) for r in rows]})

    async def h_source_add(self, req: web.Request) -> web.Response:
        d = await req.json()
        name = (d.get("name") or "").strip()
        if not name:
            return web.json_response({"ok": False, "error": "name обязателен"})
        try:
            with self._con() as con:
                con.execute(
                    "INSERT INTO sources (name,src_type,stream_type,url,priority) VALUES (?,?,?,?,?)",
                    (name, d.get("src_type","m3u"), d.get("stream_type","hls"),
                     d.get("url"), int(d.get("priority",50))),
                )
        except sqlite3.IntegrityError:
            return web.json_response({"ok": False, "error": "имя уже занято"})
        return web.json_response({"ok": True})

    async def h_source_patch(self, req: web.Request) -> web.Response:
        sid = int(req.match_info["id"])
        d = await req.json()
        keys = {"enabled","priority","url","name","refresh_secs","stream_type","auth_user","auth_pass","profile"}
        upd = {k: v for k, v in d.items() if k in keys}
        if not upd:
            return web.json_response({"ok": False})
        if upd.get("profile") not in (None, "", "gpu_h264"):
            return web.json_response({"ok": False, "error": "profile: '' или 'gpu_h264'"})
        sql = ", ".join(f"{k}=?" for k in upd)
        with self._con() as con:
            con.execute(f"UPDATE sources SET {sql} WHERE id=?", (*upd.values(), sid))
        if "profile" in upd:
            await self._tsb.refresh_db()   # новые подключения сразу берут профиль
            for key in list(self._tsb._sessions):   # живые сессии перезапускаем, плееры переподключатся
                sess = self._tsb._sessions.pop(key, None)
                if sess:
                    sess.stop()
        return web.json_response({"ok": True})

    async def h_source_delete(self, req: web.Request) -> web.Response:
        sid = int(req.match_info["id"])
        with self._con() as con:
            con.execute("DELETE FROM tracks WHERE source_id=?", (sid,))
            con.execute("DELETE FROM sources WHERE id=?", (sid,))
        return web.json_response({"ok": True})

    async def h_source_sync(self, req: web.Request) -> web.Response:
        sid = int(req.match_info["id"])
        with self._con() as con:
            row = con.execute("SELECT * FROM sources WHERE id=?", (sid,)).fetchone()
        if not row:
            return web.json_response({"ok": False, "error": "не найден"}, status=404)
        src = dict(row)
        try:
            if src["src_type"] == "enigma2":
                ch, tr = await sync_enigma2(sid, src["url"] or "", src["priority"], self._db,
                                            src.get("auth_user",""), src.get("auth_pass",""))
            else:
                ch, tr = await sync_m3u(sid, src["url"] or "", src["stream_type"],
                                        src["priority"], self._db)
        except Exception as e:
            log.exception(f"sync #{sid}")
            return web.json_response({"ok": False, "error": str(e)})
        await _notify_sync_done()
        return web.json_response({"ok": True, "added_channels": ch, "updated_tracks": tr})

    # ── Tracks ────────────────────────────────────────────────────────────

    async def h_track_add(self, req: web.Request) -> web.Response:
        d = await req.json()
        try:
            with self._con() as con:
                con.execute(
                    "INSERT INTO tracks (channel_id,source_id,priority,stream_type,url) "
                    "VALUES (?,?,?,?,?)",
                    (d["channel_id"], int(d["source_id"]),
                     int(d.get("priority",50)), d.get("stream_type","hls"), d["url"]),
                )
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)})
        return web.json_response({"ok": True})

    async def h_track_patch(self, req: web.Request) -> web.Response:
        tid = int(req.match_info["id"])
        d = await req.json()
        keys = {"priority","url","stream_type","profile"}
        upd = {k: v for k, v in d.items() if k in keys}
        if not upd:
            return web.json_response({"ok": False})
        if upd.get("profile") not in (None, "", "gpu_h264", "off"):
            return web.json_response({"ok": False, "error": "profile: '' (как у источника), 'gpu_h264' или 'off'"})
        if "url" in upd:
            upd["url_manual"] = 1
        with self._con() as con:
            ch_id = con.execute("SELECT channel_id FROM tracks WHERE id=?", (tid,)).fetchone()
            sql = ", ".join(f"{k}=?" for k in upd)
            con.execute(f"UPDATE tracks SET {sql} WHERE id=?", (*upd.values(), tid))
        if "profile" in upd and ch_id:
            # профиль читается при выборе источника — живую сессию перезапускаем, плеер переподключится
            await self._tsb.refresh_db()
            for key in {ch_id["channel_id"], ch_id["channel_id"].upper()}:
                sess = self._tsb._sessions.pop(key, None)
                if sess:
                    sess.stop()
        return web.json_response({"ok": True})

    async def h_track_delete(self, req: web.Request) -> web.Response:
        with self._con() as con:
            con.execute("DELETE FROM tracks WHERE id=?", (int(req.match_info["id"]),))
        return web.json_response({"ok": True})

    # ── Sync-all / Migrate / Config / Logs ───────────────────────────────

    async def h_sync_all(self, req: web.Request) -> web.Response:
        try:
            result = await sync_all_sources(self._db)
        except Exception as e:
            log.exception("sync_all")
            return web.json_response({"ok": False, "error": str(e)})
        return web.json_response({"ok": True, **result})

    async def h_migrate(self, req: web.Request) -> web.Response:
        d = await req.json()
        path = d.get("tvlink_path", "/opt/tvlink/data")
        try:
            loop = asyncio.get_event_loop()
            ch, tr = await loop.run_in_executor(
                None, lambda: migrate_from_tvlink(path, self._db))
        except Exception as e:
            log.exception("migrate")
            return web.json_response({"ok": False, "error": str(e)})
        return web.json_response({"ok": True, "channels": ch, "tracks": tr})

    # ── EPG ──────────────────────────────────────────────────────────────

    async def h_epg_channels(self, req: web.Request) -> web.Response:
        q = (req.query.get("q") or "").strip().lower()
        with self._con() as con:
            if q:
                rows = con.execute(
                    "SELECT epg_id, display_name, icon_url FROM epg_channels "
                    "WHERE lower(display_name) LIKE ? OR lower(epg_id) LIKE ? "
                    "ORDER BY display_name LIMIT 50",
                    (f"%{q}%", f"%{q}%"),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT epg_id, display_name, icon_url FROM epg_channels "
                    "ORDER BY display_name LIMIT 200"
                ).fetchall()
        return web.json_response({"channels": [dict(r) for r in rows]})

    async def h_epg_refresh(self, req: web.Request) -> web.Response:
        try:
            n = await _epg_cache.refresh_epg_channels(self._db)
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)})
        if _epg_cache.last_error:
            return web.json_response({"ok": False, "error": _epg_cache.last_error, "count": n})
        return web.json_response({"ok": True, "count": n})

    async def h_epg_automap(self, req: web.Request) -> web.Response:
        """Авто-маппинг: матчим наши каналы с epg_channels по нормализованному имени."""
        with self._con() as con:
            epg_rows = con.execute(
                "SELECT epg_id, display_name FROM epg_channels"
            ).fetchall()
            ch_rows = con.execute("SELECT id, name, epg_id FROM channels").fetchall()

        # Индекс EPG по свежей норм
        epg_by_norm: dict[str, list] = {}
        for r in epg_rows:
            n = normalize_name(r["display_name"])
            if n:
                epg_by_norm.setdefault(n, []).append(r["epg_id"])

        mapped = 0
        with self._con() as con:
            for ch in ch_rows:
                norm = normalize_name(ch["name"])
                ids = epg_by_norm.get(norm) or []
                epg_id = ids[0] if len(ids) == 1 else None
                # обновляем name_norm заодно
                con.execute("UPDATE channels SET name_norm=? WHERE id=?", (norm, ch["id"]))
                if epg_id and not ch["epg_id"]:
                    con.execute("UPDATE channels SET epg_id=? WHERE id=? AND (epg_id IS NULL OR epg_id='')", (epg_id, ch["id"]))
                    mapped += 1
        log.info(f"EPG auto-map: {mapped}/{len(ch_rows)} каналов")
        _epg_cache.invalidate_filtered()  # сбросить кэш отфильтрованного XMLTV
        return web.json_response({"ok": True, "mapped": mapped, "total": len(ch_rows)})

    async def h_config_patch(self, req: web.Request) -> web.Response:
        d = await req.json()
        allowed = {"epg_url", "auto_sync_secs"}
        for k, v in d.items():
            if k in allowed:
                CONFIG[k] = v
        _save_cfg()
        return web.json_response({"ok": True})

    async def h_config(self, req: web.Request) -> web.Response:
        return web.json_response({k: CONFIG.get(k) for k in
            ("port", "db_path", "epg_url", "auto_sync_secs")})

    async def h_logs(self, req: web.Request) -> web.Response:
        return web.json_response({"logs": list(_memlog.records[-300:])})


def make_app(tsb: TsbDaemon) -> web.Application:
    a   = App(tsb)
    app = web.Application()
    # ── Стриминг ─────────────────────────────────────────────────────────────
    app.router.add_get ("/stream/{id:[0-9A-Fa-f]+}", tsb.h_stream)
    app.router.add_get ("/tsb/events",            tsb.h_tsb_events)
    app.router.add_get ("/tsb/status",            tsb.h_tsb_status)
    app.router.add_get ("/tsb/sources/{id}",      tsb.h_sources)
    app.router.add_get ("/tsb/config",            tsb.h_tsb_config_get)
    app.router.add_post("/tsb/config",            tsb.h_tsb_config_post)
    app.router.add_post("/tsb/refresh-db",        tsb.h_tsb_refresh_db)
    app.router.add_post("/tsb/restart",           tsb.h_tsb_restart)
    app.router.add_post("/tsb/session/{id}/reset", tsb.h_tsb_session_reset)
    # ── Channel Manager ───────────────────────────────────────────────────────
    app.router.add_get ("/",                      a.h_index)
    app.router.add_get ("/playlist.m3u8",         a.h_playlist)
    app.router.add_get ("/playlist-unify.m3u8",   a.h_playlist_unify)
    app.router.add_get ("/xmltv",                 a.h_epg)
    app.router.add_get ("/sync-all",              a.h_sync_all_stream)
    app.router.add_get ("/test/playlist.m3u8",    a.h_test_playlist)
    app.router.add_get ("/api/probe-scan",        a.h_probe_scan)
    app.router.add_post("/api/probe-scan",        a.h_probe_scan)
    app.router.add_get ("/test/{variant}/{id}",   a.h_test_stream)
    app.router.add_get ("/api/config",            a.h_config)
    app.router.add_get ("/api/channels",          a.h_channels)
    app.router.add_post("/api/channels",            a.h_channel_add)
    app.router.add_post("/api/channels/renumber",  a.h_channel_renumber)
    app.router.add_get ("/api/channels/{id}",      a.h_channel_get)
    app.router.add_patch("/api/channels/{id}",     a.h_channel_patch)
    app.router.add_delete("/api/channels/{id}",    a.h_channel_delete)
    app.router.add_get ("/api/sources",                          a.h_sources)
    app.router.add_post("/api/sources",                          a.h_source_add)
    app.router.add_get ("/api/sources/{id}/groups",              a.h_source_groups)
    app.router.add_get ("/api/sources/{id}/groups/{grp}/channels", a.h_source_groups_channels)
    app.router.add_post("/api/sources/{id}/groups/filters",      a.h_source_groups_patch)
    app.router.add_patch("/api/sources/{id}",                    a.h_source_patch)
    app.router.add_delete("/api/sources/{id}",                   a.h_source_delete)
    app.router.add_post("/api/sources/{id}/sync",                a.h_source_sync)
    app.router.add_post("/api/tracks",            a.h_track_add)
    app.router.add_patch("/api/tracks/{id}",      a.h_track_patch)
    app.router.add_delete("/api/tracks/{id}",     a.h_track_delete)
    app.router.add_post("/api/sync-all",          a.h_sync_all)
    app.router.add_post("/api/migrate",           a.h_migrate)
    app.router.add_get ("/api/logs",              a.h_logs)
    app.router.add_get ("/api/epg/channels",      a.h_epg_channels)
    app.router.add_post("/api/epg/refresh",       a.h_epg_refresh)
    app.router.add_post("/api/epg/auto-map",      a.h_epg_automap)
    app.router.add_patch("/api/config",           a.h_config_patch)
    return app


async def _epg_keepalive(db_path: str | None = None):
    """Раз в 10 минут проверяет возраст программы. Старше часа — качает заново.
    После сбоя повтор через 5 минут, удачный кэш при этом не затирается."""
    await asyncio.sleep(90)
    while True:
        stamp = _epg_cache._fetched_at
        age = asyncio.get_event_loop().time() - stamp
        if stamp == 0 or age >= 3600:
            try:
                await _epg_cache.refresh_epg_channels(db_path)
            except Exception:
                log.exception("epg keepalive")
            if _epg_cache._fetched_at == stamp:
                await asyncio.sleep(300)
                continue
        await asyncio.sleep(600)


async def _main_async():
    _first_run_setup(CONFIG["db_path"])
    tsb = TsbDaemon(CONFIG["db_path"])
    global _on_sync_done
    _on_sync_done = tsb.refresh_db
    app = make_app(tsb)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", CONFIG["port"]).start()
    log.info(f"Channel Manager + TS Buffer: http://0.0.0.0:{CONFIG['port']}")
    log.info(f"DB:  {CONFIG['db_path']}")
    log.info(f"EPG: {CONFIG['epg_url']}")
    asyncio.create_task(_auto_sync_loop(CONFIG["db_path"]))
    asyncio.create_task(_url_watch_loop(CONFIG["db_path"]))
    asyncio.create_task(_epg_cache.refresh_epg_channels(CONFIG["db_path"]))
    asyncio.create_task(_epg_keepalive(CONFIG["db_path"]))
    asyncio.create_task(tsb._refresh_loop())
    await asyncio.Event().wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--migrate", metavar="PATH")
    parser.add_argument("--port",    type=int)
    parser.add_argument("--db",      metavar="PATH")
    args = parser.parse_args()

    if args.port: CONFIG["port"]    = args.port
    if args.db:   CONFIG["db_path"] = args.db

    if args.migrate:
        ch, tr = migrate_from_tvlink(args.migrate, CONFIG["db_path"])
        print(f"Готово: {ch} каналов, {tr} треков → {CONFIG['db_path']}")
        return

    log.setLevel(CONFIG.get("log_level", "INFO"))
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()

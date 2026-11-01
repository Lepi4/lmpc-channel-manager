#!/usr/bin/env python3
"""
Одноразовая миграция из TVLINK SQLite → channel_manager DB.

Запуск:
  python migrate_from_tvlink.py \
    --tvlink  "D:\...\channel-manager\tvlink_data" \
    --db      "D:\...\channel-manager\channels.db"

После выполнения channel_manager.py уже будет знать все каналы,
треки (Enigma2 + HLS) и EPG-маппинги.
"""

import sqlite3
import os
import re
import sys
import argparse
import hashlib
import datetime

# ─── helpers ─────────────────────────────────────────────────────────────────

_VARIANT_RE = re.compile(
    r'\b(4k|uhd|hdr|fhd|hd\s*50\s*fps|hd50fps|hd50|hd|sd|double)\b',
    re.IGNORECASE,
)

def _s(v) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace").strip()
    return str(v).strip() if v is not None else ""

def _norm(name: str) -> str:
    n = name.lower().strip()
    n = _VARIANT_RE.sub("", n)
    n = re.sub(r"[^\w\s+]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

def _track_prio(src_prio: int, stream_url: str) -> int:
    return src_prio * 100   # variant priority = 50 (нет суффикса в URL)

# ─── SOURCES ─────────────────────────────────────────────────────────────────

_SRC_DEF = {
    # name → (src_type, stream_type, base_priority)
    "NTVHD":        ("enigma2", "ts",  1),
    "tvteamonline": ("m3u",     "hls", 5),
    "NTVSD":        ("enigma2", "ts",  10),
}

# ─── SCHEMA (минимальный, создаём если нет) ───────────────────────────────────

_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT UNIQUE NOT NULL,
    src_type     TEXT NOT NULL DEFAULT 'm3u',
    stream_type  TEXT NOT NULL DEFAULT 'hls',
    url          TEXT,
    priority     INTEGER NOT NULL DEFAULT 50,
    enabled      INTEGER NOT NULL DEFAULT 1,
    refresh_secs INTEGER NOT NULL DEFAULT 21600,
    last_sync    TEXT
);

CREATE TABLE IF NOT EXISTS channels (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    name_norm  TEXT NOT NULL DEFAULT '',
    group_name TEXT NOT NULL DEFAULT '',
    icon_url   TEXT NOT NULL DEFAULT '',
    epg_id     TEXT NOT NULL DEFAULT '',
    enabled    INTEGER NOT NULL DEFAULT 1,
    sort_order INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tracks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    source_id   INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    priority    INTEGER NOT NULL DEFAULT 500,
    stream_type TEXT NOT NULL DEFAULT 'hls',
    url         TEXT NOT NULL,
    UNIQUE(channel_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_tracks_ch ON tracks(channel_id);
CREATE INDEX IF NOT EXISTS idx_ch_norm   ON channels(name_norm);
"""

# ─── MAIN ────────────────────────────────────────────────────────────────────

def migrate(tvlink_path: str, cm_db_path: str, dry_run: bool = False) -> None:
    ch_db_path  = os.path.join(tvlink_path, "channels.db")
    set_db_path = os.path.join(tvlink_path, "setup.db")

    if not os.path.exists(ch_db_path):
        print(f"❌  channels.db не найден: {ch_db_path}")
        sys.exit(1)
    if not os.path.exists(set_db_path):
        print(f"❌  setup.db не найден: {set_db_path}")
        sys.exit(1)

    os.makedirs(os.path.dirname(os.path.abspath(cm_db_path)), exist_ok=True)
    cm = sqlite3.connect(cm_db_path)
    cm.row_factory = sqlite3.Row
    cm.executescript(_SCHEMA)

    ch_db  = sqlite3.connect(ch_db_path)
    set_db = sqlite3.connect(set_db_path)

    # ── 1. Создаём / обновляем источники ─────────────────────────────────────
    print("── Источники...")

    # Читаем URL из input_sources
    src_urls: dict[str, str] = {}
    for row in set_db.execute("SELECT srcName, srcUrl FROM input_sources"):
        n, u = _s(row[0]), _s(row[1])
        if n and u:
            src_urls[n] = u

    # Enigma2 хост из первой записи NTVHD
    enigma2_host = ""
    try:
        row = set_db.execute("SELECT chUrl FROM NTVHD LIMIT 1").fetchone()
        if row:
            m = re.match(r"https?://([^:/]+)", _s(row[0]))
            if m:
                enigma2_host = m.group(1)
    except Exception:
        pass

    src_ids: dict[str, int] = {}
    for name, (src_type, stream_type, prio) in _SRC_DEF.items():
        url = src_urls.get(name, "")
        if src_type == "enigma2":
            url = enigma2_host   # хост Enigma2

        if not dry_run:
            cm.execute(
                "INSERT OR IGNORE INTO sources (name,src_type,stream_type,url,priority) "
                "VALUES (?,?,?,?,?)",
                (name, src_type, stream_type, url, prio),
            )
            cm.execute(
                "UPDATE sources SET url=?, src_type=?, stream_type=?, priority=? WHERE name=?",
                (url, src_type, stream_type, prio, name),
            )
            row = cm.execute("SELECT id FROM sources WHERE name=?", (name,)).fetchone()
            if row:
                src_ids[name] = row["id"]

        print(f"   {name}: {src_type}/{stream_type} prio={prio}  url={url[:60] if url else '—'}")

    # ── 2. bind_id → list[(src_name, stream_type, prio, url)] ────────────────
    print("── Индекс треков...")
    bind_map: dict[str, list] = {}
    for src_name, (_, stream_type, prio) in _SRC_DEF.items():
        try:
            for row in set_db.execute(f"SELECT chID, chUrl FROM {src_name}"):
                bid, url = _s(row[0]), _s(row[1])
                if bid and url:
                    bind_map.setdefault(bid, []).append(
                        (src_name, stream_type, prio, url)
                    )
        except Exception as e:
            print(f"   {src_name}: {e}")

    print(f"   Уникальных bind-ID: {len(bind_map)}")

    # ── 3. Каналы ─────────────────────────────────────────────────────────────
    print("── Каналы...")
    ch_count = tr_count = skip_count = 0

    rows = ch_db.execute(
        "SELECT chID, chTitle, chGroup, chLogo, chNum, "
        "chEPG_auto, chEPG_hand, chBinds, chExist "
        "FROM channels"
    ).fetchall()

    for row in rows:
        ch_id    = _s(row[0])
        name     = _s(row[1])
        group    = _s(row[2])
        logo     = _s(row[3])
        num_str  = _s(row[4])
        epg_auto = _s(row[5])
        epg_hand = _s(row[6])
        binds    = _s(row[7])
        exist    = row[8]

        if not ch_id or not name:
            skip_count += 1
            continue

        # EPG id — используем hex chID (TVLINK XMLTV использует его как channel id)
        # Дополнительно сохраняем raw EPG строку из TVLINK для справки в поле epg_id
        # Приоритет: chEPG_hand > chEPG_auto
        epg_id = ch_id   # default = совпадает с id канала в TVLINK xmltv

        # sort_order из chNum ("009" → 9)
        try:
            sort_order = int(num_str)
        except (ValueError, TypeError):
            sort_order = 9999

        enabled = 1 if (exist == 1 or exist == True or exist == b'\x01') else 0

        if not dry_run:
            cm.execute(
                "INSERT OR REPLACE INTO channels "
                "(id, name, name_norm, group_name, icon_url, epg_id, enabled, sort_order) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (ch_id, name, _norm(name), group, logo, epg_id, enabled, sort_order),
            )
        ch_count += 1

        # Треки
        if binds:
            for bid in [b.strip() for b in binds.split(",") if b.strip()]:
                for (src_name, stream_type, prio, url) in bind_map.get(bid, []):
                    sid = src_ids.get(src_name)
                    if not sid:
                        continue
                    track_prio = prio * 100   # нет суффикса качества в URL
                    if not dry_run:
                        cm.execute(
                            "INSERT OR REPLACE INTO tracks "
                            "(channel_id, source_id, priority, stream_type, url) "
                            "VALUES (?,?,?,?,?)",
                            (ch_id, sid, track_prio, stream_type, url),
                        )
                    tr_count += 1

    if not dry_run:
        now = datetime.datetime.now().isoformat(timespec="seconds")
        for sid in src_ids.values():
            cm.execute("UPDATE sources SET last_sync=? WHERE id=?", (now, sid))
        cm.commit()

    cm.close()
    ch_db.close()
    set_db.close()

    print()
    print(f"✓  Каналов  : {ch_count}  (пропущено: {skip_count})")
    print(f"✓  Треков   : {tr_count}")
    if enigma2_host:
        print(f"✓  Enigma2  : {enigma2_host}:8001")
    print(f"✓  DB        : {cm_db_path}")
    if dry_run:
        print("   (dry-run — в DB ничего не записано)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Миграция TVLINK → channel_manager")
    ap.add_argument("--tvlink", default="tvlink_data",
                    help="Папка с channels.db и setup.db (default: tvlink_data)")
    ap.add_argument("--db",     default="channels.db",
                    help="Путь к channel_manager DB (default: channels.db)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Только показать что будет сделано, не писать в DB")
    args = ap.parse_args()

    migrate(
        tvlink_path=args.tvlink,
        cm_db_path=args.db,
        dry_run=args.dry_run,
    )

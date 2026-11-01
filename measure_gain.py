#!/usr/bin/env python3
"""Еженедельный автозамер громкости каналов для Channel Manager (см. channel_gain.json/README.md рядом).
Меряет через ТОТ ЖЕ даунмикс, что реально используется в проде на обычной ссылке (для 5.1 — center+3дБ,
LFE подмешан слегка), поэтому посчитанная поправка остаётся верной, даже если даунмикс потом подкрутят.
Обновляет запись канала только если новая поправка отличается от текущей больше чем на 20% (и не меньше
1 дБ абсолютно, чтобы не дёргаться от шума замера) — запускается по таймеру systemd раз в неделю, в 2:00.
"""
import sqlite3, subprocess, json, re, time, os, csv, sys

DB_PATH    = "/opt/channel-manager/channels.db"
GAIN_FILE  = "/opt/channel-manager/channel_gain.json"
CSV_FILE   = "/opt/channel-manager/channel_gain.csv"
LOG_FILE   = "/opt/channel-manager/measure_gain.log"

TARGET_LUFS  = -24.0
DEAD_ZONE    = 1.5
GAIN_MIN, GAIN_MAX = -12.0, 9.0
PEAK_CEILING = -1.0
MEASURE_SECONDS = 20
UPDATE_REL_THRESHOLD = 0.20   # 20% относительного расхождения -> обновляем запись
MIN_DELTA_DB = 1.0            # и не меньше 1 дБ абсолютно (иначе шум замера дёргает таблицу каждую неделю)

PAN_SIDE = "pan=stereo|FL=1.0*FL+1.0*FC+0.707*SL+0.25*LFE|FR=1.0*FR+1.0*FC+0.707*SR+0.25*LFE"
PAN_BACK = "pan=stereo|FL=1.0*FL+1.0*FC+0.707*BL+0.25*LFE|FR=1.0*FR+1.0*FC+0.707*BR+0.25*LFE"


def log(msg: str):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def top_sources() -> dict:
    """channel_id -> (url, name) по топ-приоритету трека — та же логика выбора, что у живого плеера."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("""
        SELECT c.id AS cid, c.name AS cname, t.url AS url
        FROM channels c
        JOIN tracks t ON t.channel_id = c.id
        JOIN sources s ON s.id = t.source_id
        WHERE c.enabled=1 AND c.sort_order>0 AND t.enabled=1 AND s.enabled=1
        ORDER BY c.id, t.priority
    """).fetchall()
    con.close()
    out = {}
    for r in rows:
        out.setdefault(r["cid"], (r["url"], r["cname"]))
    return out


def probe_layout(url: str):
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-user_agent", "VLC", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name,channels,channel_layout",
             "-of", "json", url],
            capture_output=True, text=True, timeout=15)
        st = (json.loads(r.stdout).get("streams") or [None])[0]
        if not st:
            return None, None
        layout = (st.get("channel_layout") or "").strip() or f"{st.get('channels', '?')}ch"
        return st.get("codec_name"), layout
    except Exception:
        return None, None


def measure_one(url: str, layout: str):
    """20 сек через даунмикс, идентичный продовому (_audio_fix_filter в channel_manager.py)."""
    if layout.startswith("5.1"):
        pan = PAN_BACK if ("(back)" in layout or layout.strip() == "5.1") else PAN_SIDE
        af = f"{pan},ebur128"
    else:
        af = "aresample=out_channel_layout=stereo,ebur128"
    cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-user_agent", "VLC",
           "-t", str(MEASURE_SECONDS), "-i", url, "-af", af, "-f", "null", "-"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=MEASURE_SECONDS + 20)
    except Exception as e:
        return None, None, str(e)
    err = r.stderr
    # ebur128 печатает промежуточные замеры КАЖДУЮ секунду в реальном времени (там I часто служебное -70.0,
    # пока интеграция не накопилась), а финальный Summary-блок — только в самом конце вывода. Первое
    # совпадение regex ловит именно промежуточное значение (баг 2026-10-25: все 247 каналов "замерились"
    # в -70.0 LUFS → поправка клипилась в максимум +9дБ) — нужно ПОСЛЕДНЕЕ совпадение, не первое.
    m_i = re.findall(r"I:\s*(-?\d+\.?\d*)\s*LUFS", err)
    m_p = re.findall(r"Peak:\s*(-?\d+\.?\d*)\s*dBFS", err)
    if not m_i:
        return None, None, "ebur128: нет Integrated loudness в stderr"
    lufs = float(m_i[-1])
    if lufs <= -60.0:
        # -70 LUFS — служебное значение ebur128 «недостаточно данных», а не реальная тишина 20 секунд эфира;
        # лучше не измерено, чем ошибочная поправка в максимум
        return None, None, f"подозрительно тихо ({lufs} LUFS) — похоже на сбой замера, а не реальный эфир"
    return lufs, (float(m_p[-1]) if m_p else None), None


def compute_gain(lufs: float, peak: float | None) -> float:
    diff = TARGET_LUFS - lufs
    if abs(diff) < DEAD_ZONE:
        return 0.0
    g = max(GAIN_MIN, min(GAIN_MAX, diff))
    if peak is not None and g > 0 and peak + g > PEAK_CEILING:
        g = max(0.0, PEAK_CEILING - peak)
    return round(g, 1)


def filt(c: dict) -> str:
    g = c.get("gain_db", 0.0) or 0.0
    layout = c.get("layout", "")
    if layout.startswith("5.1"):
        back = "(back)" in layout or layout.strip() == "5.1"
        pan = PAN_BACK if back else PAN_SIDE
        return f"{pan},volume={g:+.1f}dB" if g else pan
    return f"volume={g:+.1f}dB" if g else ""


def write_csv(data: dict):
    rows = []
    for cid, c in data["channels"].items():
        g = c.get("gain_db", 0.0) or 0.0
        rows.append([cid, c.get("name", ""), c.get("codec", ""), c.get("layout", ""),
                     str(c.get("lufs", "")).replace(".", ","), str(c.get("peak_dbfs", "")).replace(".", ","),
                     str(g).replace(".", ","), filt(c),
                     "ok · в допуске" if g == 0 else "ok", data["meta"].get("generated", "")])
    with open(CSV_FILE, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["id_потока", "канал", "кодек", "раскладка", "громкость_LUFS", "пик_dBFS",
                     "поправка_дБ", "ffmpeg_фильтр", "статус", "замер"])
        w.writerows(rows)


def main():
    with open(GAIN_FILE, encoding="utf-8") as f:
        data = json.load(f)
    channels = data.setdefault("channels", {})
    sources = top_sources()
    checked = updated = failed = 0
    for cid, (url, name) in sources.items():
        checked += 1
        codec, layout = probe_layout(url)
        if not layout:
            failed += 1
            log(f"{cid} {name}: не удалось определить раскладку звука — пропуск")
            continue
        lufs, peak, err = measure_one(url, layout)
        if lufs is None:
            failed += 1
            log(f"{cid} {name}: замер не удался ({err})")
            continue
        new_gain = compute_gain(lufs, peak)
        old = channels.get(cid, {})
        old_gain = old.get("gain_db", 0.0) or 0.0
        rel_base = max(1.0, abs(old_gain))
        if abs(new_gain - old_gain) / rel_base > UPDATE_REL_THRESHOLD and abs(new_gain - old_gain) >= MIN_DELTA_DB:
            channels[cid] = {
                "name": name, "gain_db": new_gain, "lufs": round(lufs, 1),
                "peak_dbfs": round(peak, 1) if peak is not None else None,
                "codec": codec or old.get("codec", "?"), "layout": layout,
            }
            updated += 1
            log(f"{cid} {name}: {old_gain:+.1f} -> {new_gain:+.1f} дБ (LUFS {lufs:.1f})")
    data["meta"]["generated"] = time.strftime("%Y-%m-%d %H:%M")
    data["meta"]["measured"]  = checked - failed
    data["meta"]["total"]     = checked
    tmp = GAIN_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, GAIN_FILE)
    write_csv(data)
    log(f"Готово: проверено {checked}, обновлено {updated}, не замерено {failed}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"ОШИБКА: {e}")
        sys.exit(1)

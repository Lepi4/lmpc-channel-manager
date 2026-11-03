# LMPC Channel Manager

Лицензирование: [PolyForm Noncommercial 1.0.0](LICENSE.md) для самостоятельных материалов Lepi4. Разрешены некоммерческое использование, копирование, изменение и распространение. [Область действия и лицензии стороннего кода](LICENSE-SCOPE.md).

Local IPTV channel management and an HLS/MPEG-TS buffer for Lampa Home Media Center.
Supports Tvheadend, Enigma2 and M3U sources, XMLTV EPG, per-source priorities,
fallback streams and optional FFmpeg transcoding. Intended for a trusted home LAN.

## Run

Requires Python 3.11 or later, aiohttp 3.x, and FFmpeg for transcoding.
Validated on Debian 13 with Python 3.13 and aiohttp 3.11.16.

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
sudo mkdir -p /opt/channel-manager
cp config.example.json /opt/channel-manager/config.json
.venv/bin/python channel_manager.py --port 3002 --db /opt/channel-manager/channels.db
```

Open `http://localhost:3002` and add your own sources. The server listens on all
interfaces. This repository contains no preconfigured accounts or channels.
`CHANNEL_MANAGER_CONFIG` overrides the configuration file location.
`--version` reports the source version without starting the service.

The output playlist is `/playlist.m3u8`; runtime status is `/tsb/status`.
Channel/EPG synchronization runs every six hours by default. That synchronization
does not update the program.

## State and updates

Keep `config.json`, `channels.db` and its SQLite WAL files, `channel_gain.json`,
`hls_back.json`, and `probe_cache.json` outside Git. Source URLs and credentials
are stored in the local database and must never be published.

Each release supplies `channel-manager.tar.gz` for the LMPC panel. It contains
only program files and a version manifest. The panel verifies GitHub's SHA-256,
compiles the Python source, and retains the existing database/configuration.
It does not install or update Python dependencies automatically.

The panel can check or prepare a release on a daily schedule. Applying an update
is manual; it restarts the selected service and can briefly interrupt television.
The previous program is backed up before replacement. Program rollback keeps
newer channels and preferences. Database schema migrations must remain backward
compatible with the preceding release.

Build and validate a release locally:

```sh
python3 tests.py
python3 build_release.py --output dist
```

Create releases through the repository's **Release** workflow. The workflow
does not replace assets of an existing release.

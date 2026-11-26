#!/bin/bash
# Master GPU installer — Ubuntu 20.04+
# usage: bash install.sh [port]
set -e
PORT=${1:-8080}
DIR=/root/mastergpu
BASE=${MG_BASE:-http://212.32.253.10:8080/files}
[ "$(id -u)" = 0 ] || { echo "run as root"; exit 1; }
export DEBIAN_FRONTEND=noninteractive
need=""; for c in ffmpeg unzip curl; do command -v $c >/dev/null 2>&1 || need="$need $c"; done
[ -n "$need" ] && { apt-get update -qq; apt-get install -y -qq $need; }
SRC=$(cd "$(dirname "$0")" 2>/dev/null && pwd)
if [ ! -f "$SRC/mastergpu" ]; then
  TMP=$(mktemp -d); curl -fsSL "$BASE/mastergpu.zip" -o "$TMP/mg.zip"; unzip -qo "$TMP/mg.zip" -d "$TMP"; SRC="$TMP/mastergpu"
fi
systemctl stop mastergpu 2>/dev/null || true
mkdir -p "$DIR/data"
if [ "$SRC" != "$DIR" ]; then
  install -m 755 "$SRC/mastergpu" "$DIR/mastergpu"
  install -m 755 "$SRC/mediamtx" "$DIR/mediamtx"
  install -m 755 "$SRC/install.sh" "$DIR/install.sh"
fi
cat > /etc/systemd/system/mastergpu.service <<UNIT
[Unit]
Description=Master GPU streaming panel
After=network-online.target
Wants=network-online.target

[Service]
Environment=HOME=/root
Environment=CUDA_CACHE_PATH=$DIR/data/cuda-cache
ExecStart=$DIR/mastergpu -dir $DIR/data -listen :$PORT
Restart=always
RestartSec=2
LimitNOFILE=1048576
KillMode=process
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now mastergpu >/dev/null 2>&1
for i in $(seq 1 20); do curl -fs "http://127.0.0.1:$PORT/api/version" >/dev/null && break; sleep 1; done
CJ=$(mktemp); PASS=admin
if curl -fs -c "$CJ" -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' "http://127.0.0.1:$PORT/api/login" >/dev/null; then
  NP=$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 12)
  if curl -fs -b "$CJ" -H 'Content-Type: application/json' -d "{\"current\":\"admin\",\"username\":\"admin\",\"password\":\"$NP\"}" "http://127.0.0.1:$PORT/api/me/password" >/dev/null; then
    PASS=$NP
    curl -fs -c "$CJ" -H 'Content-Type: application/json' -d "{\"username\":\"admin\",\"password\":\"$PASS\"}" "http://127.0.0.1:$PORT/api/login" >/dev/null
  fi
  curl -fs -b "$CJ" -X PUT -H 'Content-Type: application/json' -d "{\"update_url\":\"$BASE/latest.json\"}" "http://127.0.0.1:$PORT/api/settings" >/dev/null && echo "Update API: $BASE/latest.json"
else
  PASS="(unchanged)"
fi
rm -f "$CJ"
IP=$(curl -fs4 --max-time 5 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')
echo "Master GPU: http://$IP:$PORT    user: admin  pass: $PASS"

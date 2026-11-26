#!/bin/bash
# Publish a new Master GPU version to the update server.
# usage: RP=<root password> scripts/release.sh <version> "<notes>"
set -e
VER=$1; NOTES=${2:-"Master GPU $1"}
HOST=${MG_HOST:-212.32.253.10}; BASE=${MG_BASE:-http://$HOST:8080/files}; DIR=/opt/mastergpu/data/files
[ -n "$VER" ] || { echo "usage: $0 <version> [notes]"; exit 1; }
cd "$(dirname "$0")/.."
sed -i "s/Version = \".*\"/Version = \"$VER\"/" internal/version/version.go
OUT=dist/pkg/mastergpu; mkdir -p $OUT
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X gpupanel/internal/version.Build=$(git rev-parse --short HEAD)" -o $OUT/mastergpu ./cmd/mastergpu
cp scripts/install.sh $OUT/install.sh
[ -f $OUT/mediamtx ] || curl -fsSL "$BASE/mastergpu.zip" -o dist/old.zip && unzip -qo -j dist/old.zip mastergpu/mediamtx -d $OUT
(cd dist/pkg && rm -f ../mastergpu.zip && zip -qr ../mastergpu.zip mastergpu)
python3 - "$VER" "$NOTES" "$BASE" <<'PY'
import hashlib,json,sys
b=open('dist/pkg/mastergpu/mastergpu','rb').read()
json.dump({"version":sys.argv[1],"url":sys.argv[3]+"/mastergpu","sha256":hashlib.sha256(b).hexdigest(),"size":len(b),"notes":sys.argv[2]},open('dist/latest.json','w'),indent=1)
PY
python3 - "$HOST" "$DIR" <<'PY'
import os,sys,paramiko
t=paramiko.Transport((sys.argv[1],22));t.connect(username='root',password=os.environ['RP'])
s=paramiko.SFTPClient.from_transport(t);d=sys.argv[2]+'/'
for l,r in [('dist/pkg/mastergpu/mastergpu','mastergpu'),('dist/mastergpu.zip','mastergpu.zip'),('scripts/install.sh','install.sh'),('dist/latest.json','latest.json')]:
    s.put(l,d+r+'.tmp'); s.posix_rename(d+r+'.tmp',d+r)
PY
echo "published $VER -> $BASE/latest.json"

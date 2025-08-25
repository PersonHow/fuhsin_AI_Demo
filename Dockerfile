# --- Elasticsearch + IK + UTF-8 字典（全部鎖定 8.15.1）---
FROM docker.elastic.co/elasticsearch/elasticsearch:8.15.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl python3 python3-pip ca-certificates \
    && rm -rf /var/lib/apt/lists/* && apt-get clean

# 安裝 IK（先官方 get.infini.cloud，失敗再走 legacy zip；都固定 8.15.1）
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install --batch \
      https://get.infini.cloud/elasticsearch/analysis-ik/8.15.1 \
  || ( echo "primary failed, trying legacy stable zip..." && \
       curl -fsSL -o /tmp/analysis-ik.zip \
         https://release.infinilabs.com/analysis-ik/stable/elasticsearch-analysis-ik-8.15.1.zip && \
       /usr/share/elasticsearch/bin/elasticsearch-plugin install --batch file:///tmp/analysis-ik.zip && \
       rm -f /tmp/analysis-ik.zip )

# 確保 plugins/config 存在
RUN mkdir -p /usr/share/elasticsearch/plugins/analysis-ik/config

# 若缺核心 .dic，從官方包「種入」到 plugins/config（抓 8.15.1）
RUN python3 - <<'PY'
import os, io, zipfile, urllib.request, pathlib
url = "https://release.infinilabs.com/analysis-ik/stable/elasticsearch-analysis-ik-8.15.1.zip"
data = urllib.request.urlopen(url).read()
z = zipfile.ZipFile(io.BytesIO(data))
cfg_dirs = [n for n in z.namelist() if n.endswith("config/")]
cfg = sorted(cfg_dirs, key=len)[0] if cfg_dirs else "config/"
out = pathlib.Path("/usr/share/elasticsearch/plugins/analysis-ik/config")
out.mkdir(parents=True, exist_ok=True)
need = {"main.dic","surname.dic","stopwords.dic","quantifier.dic","suffix.dic","preposition.dic"}
have = set(p.name for p in out.glob("*.dic"))
for n in z.namelist():
    base = n.split("/")[-1]
    if n.startswith(cfg) and base.endswith(".dic") and (base in need and base not in have):
        (out/base).write_bytes(z.read(n))
print("seeded:", sorted(p.name for p in out.glob("*.dic")))
PY

# 複製你的 cfg / 自訂字典到「plugins/config」
COPY config/analysis-ik/IKAnalyzer.cfg.xml            /usr/share/elasticsearch/plugins/analysis-ik/config/IKAnalyzer.cfg.xml
COPY config/analysis-ik/stopwords.txt                 /usr/share/elasticsearch/plugins/analysis-ik/config/stopwords.txt
COPY config/analysis-ik/traditional_chinese_dict.txt  /usr/share/elasticsearch/plugins/analysis-ik/config/traditional_chinese_dict.txt

# 將字典與停用詞正規化成 UTF-8（無 BOM）+ LF，清理零寬字元
RUN python3 - <<'PY'
from pathlib import Path
import re
root = Path("/usr/share/elasticsearch/plugins/analysis-ik/config")
for name in ("traditional_chinese_dict.txt","stopwords.txt"):
    p = root/name
    b = p.read_bytes()
    enc = None
    if b.startswith(b"\xff\xfe"): enc="utf-16le"
    elif b.startswith(b"\xfe\xff"): enc="utf-16be"
    elif b.startswith(b"\xef\xbb\xbf"): enc="utf-8-sig"
    tried = [e for e in (enc,"utf-8","cp950","big5","utf-16le","utf-16be") if e]
    for e in tried:
        try:
            s = b.decode(e); break
        except: pass
    else:
        s = b.decode("utf-8","ignore")
    s = s.replace("\r","")
    s = re.sub("[\u200B-\u200D\uFEFF]", "", s)
    s = "".join(ch for ch in s if ch.isprintable() or ch in "\n\t")
    p.write_text(s, encoding="utf-8")
print("normalized to utf-8")
PY

# 讓 ES 的 config/analysis-ik 指向 plugins/config（避免路徑打架）
RUN rm -rf /usr/share/elasticsearch/config/analysis-ik \
 && ln -s /usr/share/elasticsearch/plugins/analysis-ik/config /usr/share/elasticsearch/config/analysis-ik

# 你要的 Python 套件（選配）
RUN pip3 install --no-cache-dir opencc-python-reimplemented requests pandas

# 權限
RUN chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/plugins/analysis-ik /usr/share/elasticsearch/config/analysis-ik

USER elasticsearch
EXPOSE 9200 9300

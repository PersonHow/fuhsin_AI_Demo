#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, time, json, hashlib
from datetime import datetime, timezone
from pathlib import Path
import requests
from opencc import OpenCC

ES_URL   = os.environ.get("ES_URL",  "http://es01:9200")
ES_USER  = os.environ.get("ES_USER", "elastic")
ES_PASS  = os.environ.get("ES_PASS", "admin@12345")
IMPORT_DIR = Path(os.environ.get("IMPORT_DIR", "/data/import"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
SLEEP_SEC  = int(os.environ.get("SLEEP", "5"))

session = requests.Session()
session.auth = (ES_USER, ES_PASS)
session.headers.update({"Content-Type": "application/json"})

cc_s2t = OpenCC("s2t")
cc_t2s = OpenCC("t2s")

def log(msg): print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

def ensure_index_template():
    tpl = {
      "index_patterns": ["sql-*"],
      "template": {
        "settings": {
          "analysis": {}
        },
        "mappings": {
          "properties": {
            "@timestamp":            {"type": "date"},
            "filename":              {"type": "keyword"},
            "statement_idx":         {"type": "integer"},
            "content":               {"type": "text", "analyzer":"ik_max_word", "search_analyzer":"ik_smart"},
            "content_traditional":   {"type": "text", "analyzer":"ik_max_word", "search_analyzer":"ik_smart"},
            "content_simplified":    {"type": "text", "analyzer":"ik_max_word", "search_analyzer":"ik_smart"}
          }
        }
      }
    }
    r = session.put(f"{ES_URL}/_index_template/sql-template", data=json.dumps(tpl), timeout=30)
    r.raise_for_status()
    log("index template ensured: sql-template")

def today_index():
    # 依日期滾動：sql-YYYY.MM.DD
    return "sql-" + datetime.now(timezone.utc).strftime("%Y.%m.%d")

def robust_read_text(path: Path) -> str:
    b = path.read_bytes()
    encs = ["utf-8", "utf-8-sig", "cp950", "big5", "utf-16le", "utf-16be"]
    for e in encs:
        try:
            s = b.decode(e)
            break
        except Exception:
            continue
    else:
        s = b.decode("utf-8", "ignore")
    return s.replace("\r", "")

_SQL_INLINE_COMMENT = re.compile(r"--[^\n]*")
_SQL_BLOCK_COMMENT  = re.compile(r"/\*.*?\*/", re.S)

def split_sql_statements(text: str):
    # 去註解、trim，再用分號切；保留非空句子
    no_comment = _SQL_BLOCK_COMMENT.sub("", _SQL_INLINE_COMMENT.sub("", text))
    parts = [p.strip() for p in re.split(r";\s*(?:\n|$)", no_comment)]
    return [p for p in parts if p]

def to_docs(filename: str, statements: list[str]):
    idx_docs = []
    for i, stmt in enumerate(statements):
        raw = stmt
        trad = cc_s2t.convert(raw)      # 不論原文繁簡，一律產出繁體
        simp = cc_t2s.convert(raw)      # 以及簡體
        _id  = hashlib.sha1(f"{filename}::{hashlib.sha1(raw.encode('utf-8')).hexdigest()}".encode("utf-8")).hexdigest()
        doc = {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "filename": filename,
            "statement_idx": i,
            "content": raw,
            "content_traditional": trad,
            "content_simplified":  simp
        }
        idx_docs.append((_id, doc))
    return idx_docs

def bulk_send(index_name: str, items: list[tuple[str, dict]]):
    if not items: return
    lines = []
    for _id, src in items:
        lines.append(json.dumps({"index": {"_index": index_name, "_id": _id}}, ensure_ascii=False))
        lines.append(json.dumps(src, ensure_ascii=False))
    payload = "\n".join(lines) + "\n"
    r = session.post(f"{ES_URL}/_bulk", data=payload.encode("utf-8"), headers={"Content-Type": "application/x-ndjson"}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if data.get("errors"):
        # 印出前幾個錯誤，方便排查
        errs = [it for it in data.get("items", []) if it.get("index", {}).get("error")]
        log(f"[WARN] bulk has errors, sample: {errs[:3]}")
    log(f"bulk indexed: {len(items)} docs -> {index_name}")

def process_file(path: Path):
    log(f"processing {path.name} ...")
    text = robust_read_text(path)
    statements = split_sql_statements(text)
    docs = to_docs(path.name, statements)
    # 批次送
    index_name = today_index()
    for i in range(0, len(docs), BATCH_SIZE):
        bulk_send(index_name, docs[i:i+BATCH_SIZE])
    # rename 成 .done
    done = path.with_suffix(path.suffix + ".done")
    try:
        path.rename(done)
        log(f"done -> {done.name}")
    except Exception as e:
        log(f"[WARN] cannot rename to .done: {e}")

def main():
    IMPORT_DIR.mkdir(parents=True, exist_ok=True)
    ensure_index_template()
    log(f"watching dir: {IMPORT_DIR}")
    while True:
        # 只處理 .sql（忽略 *.done）
        files = sorted([p for p in IMPORT_DIR.glob("*.sql") if not p.name.endswith(".done")])
        for p in files:
            try:
                process_file(p)
            except Exception as e:
                log(f"[ERROR] {p.name}: {e}")
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()

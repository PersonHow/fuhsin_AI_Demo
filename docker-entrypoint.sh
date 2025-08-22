#!/bin/bash
set -e

# 如果是第一次啟動，複製詞典
if [ ! -f "/usr/share/elasticsearch/config/analysis-ik/.initialized" ]; then
    echo "初始化 IK 詞典..."
    cp -r /usr/share/elasticsearch/config/analysis-ik/* /usr/share/elasticsearch/plugins/analysis-ik/config/ || true
    touch /usr/share/elasticsearch/config/analysis-ik/.initialized
fi

# 啟動 Elasticsearch
exec /usr/local/bin/docker-entrypoint.sh elasticsearch

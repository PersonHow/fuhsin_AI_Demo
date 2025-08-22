# Elasticsearch 8.15.1 + IK plugin 8.15.1
FROM docker.elastic.co/elasticsearch/elasticsearch:8.15.1

ENV ES_VERSION=8.15.1
ENV IK_VERSION=8.15.1

# 官方推薦：使用 plugin 管理器安裝 IK
USER elasticsearch
WORKDIR /usr/share/elasticsearch

# 直接透過 plugin CLI 從官方短鏈安裝（省去 ZIP 檔）
RUN bin/elasticsearch-plugin install --batch \
    https://get.infini.cloud/elasticsearch/analysis-ik/${IK_VERSION}

# 不需要在 image 內生成 IKAnalyzer.cfg.xml
# 你已在 compose 把它以 bind mount 掛到 /usr/share/elasticsearch/config/analysis-ik
# （IK 會從 $ES_HOME/config/analysis-ik 讀設定與詞典）

# entrypoint / cmd 照官方即可

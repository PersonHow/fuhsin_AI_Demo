FROM docker.elastic.co/elasticsearch/elasticsearch:8.15.1

# 切換到 root 用戶進行安裝
USER root

# 安裝必要工具（保持簡潔）
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 下載並安裝 IK 分詞器（保持原有）
RUN curl -L -o /tmp/elasticsearch-analysis-ik-8.15.1.zip \
    "https://release.infinilabs.com/analysis-ik/stable/elasticsearch-analysis-ik-8.15.1.zip" && \
    /usr/share/elasticsearch/bin/elasticsearch-plugin install --batch file:///tmp/elasticsearch-analysis-ik-8.15.1.zip && \
    rm /tmp/elasticsearch-analysis-ik-8.15.1.zip

# 安裝 Python 套件（用於資料處理）
# 注意：sqlite3 是 Python 內建模組，不需要安裝
RUN pip3 install --no-cache-dir \
    opencc-python-reimplemented \
    requests \
    pandas

# ✅ 把你的 cfg / 自訂字典放到「外掛的 config」資料夾
COPY config/analysis-ik/IKAnalyzer.cfg.xml /usr/share/elasticsearch/plugins/analysis-ik/config/IKAnalyzer.cfg.xml
COPY config/analysis-ik/stopwords.txt /usr/share/elasticsearch/plugins/analysis-ik/config/stopwords.txt
COPY config/analysis-ik/traditional_chinese_dict.txt /usr/share/elasticsearch/plugins/analysis-ik/config/traditional_chinese_dict.txt

# 權限
RUN chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/plugins/analysis-ik

# （如果你有其它腳本…保留）
RUN mkdir -p /usr/local/bin/data-processor
COPY scripts/auto_importer.py /usr/local/bin/data-processor/
RUN chmod +x /usr/local/bin/data-processor/auto_importer.py

USER elasticsearch
EXPOSE 9200 9300

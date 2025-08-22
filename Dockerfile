FROM docker.elastic.co/elasticsearch/elasticsearch:8.15.1

# 設置版本變數
ENV ES_VERSION=8.15.1
ENV IK_VERSION=8.15.1

# 切換到 root 用戶進行安裝
USER root

# 安裝系統依賴
RUN yum install -y wget unzip && \
    yum clean all

# 下載並安裝 IK 分詞器
RUN curl -L -o /tmp/elasticsearch-analysis-ik-${IK_VERSION}.zip \
    "https://release.infinilabs.com/analysis-ik/stable/elasticsearch-analysis-ik-${IK_VERSION}.zip" && \
    /usr/share/elasticsearch/bin/elasticsearch-plugin install --batch file:///tmp/elasticsearch-analysis-ik-${IK_VERSION}.zip && \
    rm /tmp/elasticsearch-analysis-ik-${IK_VERSION}.zip

# 創建必要的目錄
RUN mkdir -p /usr/share/elasticsearch/config/analysis-ik && \
    mkdir -p /usr/share/elasticsearch/dictionaries/converted && \
    mkdir -p /usr/share/elasticsearch/dictionaries/custom && \
    chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/config/analysis-ik && \
    chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/dictionaries

# 設置繁體中文環境
ENV LANG=zh_TW.UTF-8
ENV LC_ALL=zh_TW.UTF-8

# 切換回 elasticsearch 用戶
USER elasticsearch

# 設置工作目錄
WORKDIR /usr/share/elasticsearch

# 暴露端口
EXPOSE 9200 9300

# 啟動腳本
COPY --chown=elasticsearch:elasticsearch docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
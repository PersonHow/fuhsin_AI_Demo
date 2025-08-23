FROM docker.elastic.co/elasticsearch/elasticsearch:8.15.1

# 切換到 root 用戶進行安裝
USER root

# 下載並安裝 IK 分詞器
RUN curl -L -o /tmp/elasticsearch-analysis-ik-8.15.1.zip \
    "https://release.infinilabs.com/analysis-ik/stable/elasticsearch-analysis-ik-8.15.1.zip" && \
    /usr/share/elasticsearch/bin/elasticsearch-plugin install --batch file:///tmp/elasticsearch-analysis-ik-8.15.1.zip && \
    rm /tmp/elasticsearch-analysis-ik-8.15.1.zip

# 切換回 elasticsearch 用戶
USER elasticsearch

# 暴露端口
EXPOSE 9200 9300

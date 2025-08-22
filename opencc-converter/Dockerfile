# opencc-converter/Dockerfile
FROM python:3.9-slim

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 安裝 OpenCC
RUN git clone https://github.com/BYVoid/OpenCC.git /tmp/opencc && \
    cd /tmp/opencc && \
    make && \
    make install && \
    ldconfig && \
    rm -rf /tmp/opencc

# 安裝 Python 依賴
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 創建工作目錄
WORKDIR /app

# 複製腳本
COPY convert_dict.py /app/
COPY process_ik_dicts.sh /app/

# 設置執行權限
RUN chmod +x /app/process_ik_dicts.sh

# 創建必要的目錄
RUN mkdir -p /dictionaries/original && \
    mkdir -p /dictionaries/converted && \
    mkdir -p /dictionaries/custom && \
    mkdir -p /config/custom

# 設置入口點
ENTRYPOINT ["/app/process_ik_dicts.sh"]
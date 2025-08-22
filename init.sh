#!/bin/bash
# init.sh - Elasticsearch IK 繁體中文環境初始化腳本

set -e

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 顯示標題
echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}   Elasticsearch IK 繁體中文環境初始化工具    ${NC}"
echo -e "${BLUE}================================================${NC}"

# 檢查必要的命令
echo -e "${YELLOW}[1/8] 檢查系統環境...${NC}"
for cmd in docker docker-compose curl wget; do
    if ! command -v $cmd &> /dev/null; then
        echo -e "${RED}錯誤: $cmd 未安裝${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✓ 系統環境檢查通過${NC}"

# 創建目錄結構
echo -e "${YELLOW}[2/8] 創建目錄結構...${NC}"
mkdir -p {data,logs,plugins}/elasticsearch
mkdir -p data/kibana
mkdir -p logs/converter
mkdir -p dictionaries/{original,converted,custom}
mkdir -p config/{elasticsearch,ik/custom,nginx}
mkdir -p scripts
mkdir -p backup/dictionaries
mkdir -p docs

echo -e "${GREEN}✓ 目錄結構創建完成${NC}"

# 創建環境變數文件
echo -e "${YELLOW}[3/8] 創建環境配置...${NC}"
cat > .env << 'EOF'
# Elasticsearch 配置
ELASTIC_PASSWORD=admin@12345
ES_HEAP_SIZE=1g

# Kibana 配置
KIBANA_PASSWORD=kibana123

# 詞典服務配置
DICT_SERVER_PORT=8080

# 版本配置
ES_VERSION=8.15.1
IK_VERSION=8.15.1
EOF

# 創建 Elasticsearch 配置
cat > config/elasticsearch/elasticsearch.yml << 'EOF'
# Elasticsearch 配置文件
cluster.name: elasticsearch-cluster
node.name: es01
network.host: 0.0.0.0

# 路徑配置
path.data: /usr/share/elasticsearch/data
path.logs: /usr/share/elasticsearch/logs

# 安全配置
xpack.security.enabled: true
xpack.security.enrollment.enabled: true
xpack.security.http.ssl.enabled: false
xpack.security.transport.ssl.enabled: false

# 監控配置
xpack.monitoring.collection.enabled: true

# IK 分詞器預設配置
index.analysis.analyzer.default.type: ik_max_word
EOF

# 創建 IK 配置文件
cat > config/ik/IKAnalyzer.cfg.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
    <comment>IK Analyzer 繁體中文配置</comment>
    
    <!-- 使用轉換後的繁體詞典 -->
    <entry key="ext_dict">
        tc_main.dic;
        tc_quantifier.dic;
        tc_suffix.dic;
        tc_surname.dic;
        custom/taiwan_words.dic;
        custom/user_dict.dic
    </entry>
    
    <!-- 使用轉換後的停用詞詞典 -->
    <entry key="ext_stopwords">
        tc_stopword.dic;
        custom/user_stopword.dic
    </entry>
    
    <!-- 遠程詞典配置（熱更新） -->
    <entry key="remote_ext_dict">http://dict-server/hot_words.dic</entry>
    
    <!-- 遠程停用詞詞典 -->
    <!-- <entry key="remote_ext_stopwords">http://dict-server/remote_stopwords.dic</entry> -->
</properties>
EOF

# 創建自定義簡繁對照表
cat > config/ik/custom/custom_convert_map.txt << 'EOF'
# 自定義簡繁轉換對照表
# 格式：簡體 => 繁體
# 用於修正 OpenCC 預設轉換的特殊詞彙

# 台灣特有詞彙
软件 => 軟體
硬件 => 硬體
网络 => 網路
程序 => 程式
服务器 => 伺服器
数据库 => 資料庫
内存 => 記憶體
缓存 => 快取
默认 => 預設
视窗 => 視窗
文件夹 => 資料夾
鼠标 => 滑鼠
硬盘 => 硬碟
优盘 => 隨身碟
宽带 => 寬頻
带宽 => 頻寬
字节 => 位元組
信息 => 資訊
数字 => 數位
音频 => 音訊
视频 => 視訊
搜索 => 搜尋
在线 => 線上
离线 => 離線
邮政编码 => 郵遞區號
身份证 => 身分證
出租车 => 計程車
地铁 => 捷運
公交车 => 公車
自行车 => 腳踏車
摩托车 => 機車
土豆 => 馬鈴薯
西红柿 => 番茄
菠萝 => 鳳梨
酸奶 => 優格
奶酪 => 起司
EOF

# 創建台灣特色詞彙
cat > dictionaries/custom/taiwan_words.dic << 'EOF'
# 台灣特色詞彙
軟體開發
硬體設備
網路連線
程式設計
伺服器維護
資料庫管理
記憶體不足
快取清理
預設值
視窗系統
資料夾
滑鼠游標
硬碟空間
隨身碟
寬頻網路
頻寬限制
位元組
資訊安全
數位轉型
音訊設備
視訊會議
搜尋引擎
線上學習
離線模式
郵遞區號
身分證字號
計程車
捷運站
公車站牌
腳踏車道
機車停車格
台積電
聯發科
鴻海精密
華碩電腦
宏碁
中華電信
台灣大哥大
遠傳電信
統一超商
全家便利商店
悠遊卡
一卡通
電子支付
行動支付
第三方支付
雲端運算
大數據分析
人工智慧
機器學習
深度學習
物聯網
區塊鏈
5G網路
智慧城市
智慧製造
EOF

# 創建用戶自定義詞典模板
cat > dictionaries/custom/user_dict.dic << 'EOF'
# 用戶自定義詞典
# 請在此添加您的自定義詞彙
# 格式：詞彙（每行一個）
EOF

cat > dictionaries/custom/user_stopword.dic << 'EOF'
# 用戶自定義停用詞
# 請在此添加您的停用詞
EOF

cat > dictionaries/custom/hot_words.dic << 'EOF'
# 熱詞詞典（可遠程更新）
# 用於動態添加新詞彙
EOF

# 創建 Nginx 配置
cat > config/nginx/default.conf << 'EOF'
server {
    listen 80;
    server_name localhost;
    
    location / {
        root /usr/share/nginx/html;
        add_header Cache-Control "no-cache";
        add_header Access-Control-Allow-Origin "*";
    }
    
    location ~ \.dic$ {
        root /usr/share/nginx/html;
        add_header Content-Type "text/plain; charset=utf-8";
        add_header Cache-Control "no-cache";
        add_header Access-Control-Allow-Origin "*";
    }
}
EOF

echo -e "${GREEN}✓ 配置文件創建完成${NC}"

# 下載 IK 原始詞典（從 infinilabs）
echo -e "${YELLOW}[4/8] 下載 IK 原始詞典...${NC}"
IK_VERSION="8.15.1"
DOWNLOAD_URL="https://release.infinilabs.com/analysis-ik/stable/elasticsearch-analysis-ik-${IK_VERSION}.zip"
TEMP_DIR="/tmp/ik_download_$$"

# 創建臨時目錄
mkdir -p $TEMP_DIR

# 下載
echo -e "下載 IK 分詞器 v${IK_VERSION}..."
if command -v wget &> /dev/null; then
    wget -q -O "$TEMP_DIR/ik.zip" "$DOWNLOAD_URL" || {
        echo -e "${RED}下載失敗！${NC}"
        echo -e "${YELLOW}請手動執行: ./download_ik_dicts.sh${NC}"
        rm -rf $TEMP_DIR
    }
elif command -v curl &> /dev/null; then
    curl -sL -o "$TEMP_DIR/ik.zip" "$DOWNLOAD_URL" || {
        echo -e "${RED}下載失敗！${NC}"
        echo -e "${YELLOW}請手動執行: ./download_ik_dicts.sh${NC}"
        rm -rf $TEMP_DIR
    }
fi

# 如果下載成功，解壓縮
if [ -f "$TEMP_DIR/ik.zip" ]; then
    echo -e "解壓縮詞典文件..."
    cd $TEMP_DIR
    unzip -q ik.zip
    
    # 尋找並複製詞典
    CONFIG_DIR=$(find . -name "config" -type d | head -1)
    if [ -n "$CONFIG_DIR" ] && [ -d "$CONFIG_DIR" ]; then
        cp $CONFIG_DIR/*.dic "$OLDPWD/dictionaries/original/" 2>/dev/null || true
        echo -e "${GREEN}✓ 詞典下載完成${NC}"
    fi
    
    cd $OLDPWD
    rm -rf $TEMP_DIR
else
    echo -e "${YELLOW}提示：稍後請執行 ./download_ik_dicts.sh 下載詞典${NC}"
fi

# 創建 docker-entrypoint.sh
echo -e "${YELLOW}[5/8] 創建啟動腳本...${NC}"
cat > docker-entrypoint.sh << 'EOF'
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
EOF

chmod +x docker-entrypoint.sh

echo -e "${GREEN}✓ 啟動腳本創建完成${NC}"

# 創建維護腳本
echo -e "${YELLOW}[6/8] 創建維護腳本...${NC}"

# 更新詞典腳本
cat > scripts/update_dicts.sh << 'EOF'
#!/bin/bash
# 更新詞典腳本

echo "正在更新詞典..."

# 重新運行轉換
docker-compose run --rm dict-converter

# 重新載入詞典
curl -X POST "http://elastic:admin@12345@localhost:9200/_analyze" \
  -H 'Content-Type: application/json' \
  -d '{
    "analyzer": "ik_max_word",
    "text": "測試詞典更新"
  }'

echo "詞典更新完成！"
EOF

# 測試分詞腳本
cat > scripts/test_analyzer.sh << 'EOF'
#!/bin/bash
# 測試分詞效果

if [ $# -eq 0 ]; then
    echo "使用方法: $0 \"要測試的文本\""
    exit 1
fi

TEXT="$1"

echo "測試文本: $TEXT"
echo "===================="

echo -e "\n使用 ik_max_word 分詞:"
curl -s -X POST "http://elastic:admin@12345@localhost:9200/_analyze" \
  -H 'Content-Type: application/json' \
  -d "{
    \"analyzer\": \"ik_max_word\",
    \"text\": \"$TEXT\"
  }" | jq -r '.tokens[].token' | tr '\n' ' '

echo -e "\n\n使用 ik_smart 分詞:"
curl -s -X POST "http://elastic:admin@12345@localhost:9200/_analyze" \
  -H 'Content-Type: application/json' \
  -d "{
    \"analyzer\": \"ik_smart\",
    \"text\": \"$TEXT\"
  }" | jq -r '.tokens[].token' | tr '\n' ' '

echo -e "\n"
EOF

# 備份腳本
cat > scripts/backup.sh << 'EOF'
#!/bin/bash
# 備份詞典和配置

BACKUP_DIR="backup/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "備份詞典和配置到 $BACKUP_DIR..."

# 備份詞典
cp -r dictionaries "$BACKUP_DIR/"

# 備份配置
cp -r config "$BACKUP_DIR/"

# 壓縮備份
tar -czf "$BACKUP_DIR.tar.gz" "$BACKUP_DIR"
rm -rf "$BACKUP_DIR"

echo "備份完成: $BACKUP_DIR.tar.gz"
EOF

chmod +x scripts/*.sh

echo -e "${GREEN}✓ 維護腳本創建完成${NC}"

# 創建文檔
echo -e "${YELLOW}[7/8] 創建文檔...${NC}"

cat > docs/README.md << 'EOF'
# Elasticsearch IK 繁體中文分詞環境

本專案提供了一個完整的 Elasticsearch + IK 分詞器繁體中文環境，使用 OpenCC 進行詞典轉換。

## 快速開始

1. 初始化環境：
   ```bash
   ./init.sh
   ```

2. 啟動服務：
   ```bash
   docker-compose up -d
   ```

3. 測試分詞：
   ```bash
   ./scripts/test_analyzer.sh "測試繁體中文分詞效果"
   ```

## 服務訪問

- Elasticsearch: http://localhost:9200 (elastic/admin@12345)
- Kibana: http://localhost:5601 (elastic/admin@12345)
- 詞典服務: http://localhost:8080

## 目錄結構

請參考專案根目錄的結構說明。

## 維護指南

請參考 `docs/MAINTENANCE.md`
EOF

cat > docs/MAINTENANCE.md << 'EOF'
# 維護指南

## 日常維護

### 1. 更新詞典

當需要更新詞典時：

```bash
# 1. 編輯自定義詞典
vim dictionaries/custom/user_dict.dic

# 2. 重新運行轉換
./scripts/update_dicts.sh

# 3. 重啟 Elasticsearch（如需要）
docker-compose restart elasticsearch
```

### 2. 添加熱詞

編輯 `dictionaries/custom/hot_words.dic`，詞典會自動更新。

### 3. 備份

定期執行備份：

```bash
./scripts/backup.sh
```

### 4. 監控

查看 Elasticsearch 健康狀態：

```bash
curl -u elastic:admin@12345 http://localhost:9200/_cluster/health?pretty
```

## 詞典更新流程

### 自動更新（推薦）

1. 修改 `dictionaries/custom/` 下的詞典文件
2. 執行 `./scripts/update_dicts.sh`
3. 系統會自動重新載入詞典

### 手動更新

1. 修改詞典文件
2. 重新運行詞典轉換：
   ```bash
   docker-compose run --rm dict-converter
   ```
3. 重啟 Elasticsearch：
   ```bash
   docker-compose restart elasticsearch
   ```

## 故障排除

請參考 `docs/TROUBLESHOOTING.md`
EOF

cat > docs/TROUBLESHOOTING.md << 'EOF'
# 故障排除指南

## 常見問題

### 1. Elasticsearch 無法啟動

**症狀**：容器反覆重啟

**解決方案**：
```bash
# 檢查日誌
docker-compose logs elasticsearch

# 檢查權限
sudo chown -R 1000:1000 data/elasticsearch
sudo chmod -R 755 data/elasticsearch
```

### 2. 分詞效果不佳

**症狀**：某些詞彙無法正確分詞

**解決方案**：
1. 檢查詞典是否正確載入：
   ```bash
   docker exec elasticsearch ls -la /usr/share/elasticsearch/config/analysis-ik/
   ```

2. 添加到自定義詞典：
   ```bash
   echo "新詞彙" >> dictionaries/custom/user_dict.dic
   ./scripts/update_dicts.sh
   ```

### 3. 記憶體不足

**症狀**：OutOfMemoryError

**解決方案**：
編輯 `.env` 文件，增加堆記憶體：
```
ES_HEAP_SIZE=2g
```

### 4. 詞典轉換失敗

**症狀**：dict-converter 容器錯誤

**解決方案**：
```bash
# 查看轉換日誌
docker-compose logs dict-converter

# 手動運行轉換
docker-compose run --rm dict-converter bash
# 在容器內手動執行轉換
```

## 性能優化

### 1. 調整分詞模式

- `ik_max_word`：細粒度分詞，召回率高
- `ik_smart`：智能分詞，準確率高

### 2. 索引優化

```json
{
  "settings": {
    "index": {
      "refresh_interval": "30s",
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  }
}
```

### 3. JVM 調優

編輯 `.env`：
```
ES_JAVA_OPTS=-Xms2g -Xmx2g -XX:+UseG1GC
```
EOF
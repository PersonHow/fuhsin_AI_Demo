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

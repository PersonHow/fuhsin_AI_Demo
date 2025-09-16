# Build stage：用 Debian 基底避開 musl/optional deps 的坑
FROM node:20 AS builder
WORKDIR /app

# 先只複製依賴檔案，善用快取
COPY package.json package-lock.json ./
RUN npm ci

# 再複製程式碼並建置
COPY . .
RUN npm run build

# Runtime stage：用 nginx 服務靜態檔案
FROM nginx:alpine

# 把前端產物放到預設根目錄
COPY --from=builder /app/dist /usr/share/nginx/html

# 你的 nginx 設定（沿用原本檔名）
COPY nginx.conf /etc/nginx/conf.d/default.conf

# 健康檢查
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:80 || exit 1

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

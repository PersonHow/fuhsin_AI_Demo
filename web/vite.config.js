import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const proxyTarget = process.env.VITE_PROXY_TARGET || 'http://localhost:8000'
console.log('[vite] proxy target =', proxyTarget)
export default defineConfig({
  plugins: [react()],
  css:{
    preprocessorOptions:{
      scss:{
        api: 'modern-compiler'
      }
    }
  },
  server: {
    host: '0.0.0.0',
    port: 5174,
    proxy: {
      // 讓前端打 /api 走 Docker DNS 到 rag-api:8000
      '/api': {
        target: proxyTarget,
        changeOrigin: true,
        // 若後端路徑不含 /api 前綴，可加：
        rewrite: path => path.replace(/^\/api/, '')
      }
      
    }
  }

})

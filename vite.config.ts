import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist', // Esto especifica la carpeta de salida
    rollupOptions: {
      output: {
        manualChunks: undefined,
      },
    },
  },
  server: {
    proxy: {
      '/*': {
        target: 'index.html',
        changeOrigin: true,
        rewrite: () => '/index.html'  // Eliminado el parámetro 'path' que no se usa
      }
    }
  },
})
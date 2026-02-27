FROM node:20-slim

# Evitar prompts interativos durante instalação
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependências do sistema: Chromium, Python3, pip, ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    python3 \
    python3-pip \
    python3-venv \
    ffmpeg \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libgtk-3-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instalar yt-dlp e instaloader via pip
RUN pip3 install --break-system-packages yt-dlp instaloader

# Configurar variáveis para Chromium e yt-dlp
ENV CHROME_PATH=/usr/bin/chromium
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV YT_DLP_PATH=/usr/local/bin/yt-dlp

# Diretório de trabalho
WORKDIR /app

# Copiar package.json primeiro para cache de camadas do Docker
COPY package*.json ./
RUN npm install --omit=dev

# Copiar o restante dos arquivos
COPY . .

# Criar diretório de dados persistente
RUN mkdir -p /app/data

# Porta (será sobrescrita pela variável PORT do EasyPanel)
EXPOSE 3000

# Healthcheck para o EasyPanel monitorar
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD node -e "fetch('http://localhost:' + (process.env.PORT || 3000) + '/api/accounts').then(r => r.ok ? process.exit(0) : process.exit(1)).catch(() => process.exit(1))"

CMD ["node", "server.js"]

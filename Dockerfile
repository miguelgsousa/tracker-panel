FROM node:22-slim

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
RUN install -d -m 0700 /run/secrets

# Copiar package.json primeiro para cache de camadas do Docker
COPY package*.json ./
RUN npm install --omit=dev

# Copiar o restante dos arquivos
COPY . .

# Criar diretório de dados persistente
RUN mkdir -p /app/data

# Porta (será sobrescrita pela variável PORT do EasyPanel)
EXPOSE 3000

# Probe the protected local panel using server env only (never log credentials).
# This checks process/auth readiness, not Meta consent or provider availability.
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD node -e "const e=process.env,headers={};if(e.METRICS_USERNAME&&e.METRICS_PASSWORD)headers.Authorization='Basic '+Buffer.from(e.METRICS_USERNAME+':'+e.METRICS_PASSWORD).toString('base64');fetch('http://127.0.0.1:'+(e.PORT||3000)+'/api/accounts',{headers}).then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "server.js"]

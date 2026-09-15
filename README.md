# Tracker Panel

Painel de acompanhamento de redes sociais, com métricas oficiais de Instagram e Facebook integradas ao visual escuro/ciano existente.

## Métricas oficiais

Nas seções **Instagram** e **Facebook**:

- Conexão OAuth e seleção de contas/Páginas autorizadas.
- Hoje, Ontem, 7, 30 e 90 dias; datas e limitações da API explícitas.
- Indicadores, série diária disponível e conteúdos com pesquisa, filtros e ordenação.
- Detalhes de conteúdo com tempo assistido e abandono inicial quando suportados. Países por vídeo e curvas de retenção não são inventados quando a API não os oferece.
- Cache por conta/provedor/período, requisições concorrentes compartilhadas e proteção contra respostas fora de ordem.
- Última leitura válida preservada em falhas, com indicação de desatualização e timestamp original; sem trocar dados entre períodos.
- Exportação CSV de leituras válidas, com proteção contra fórmulas; exportação de leituras desatualizadas desabilitada.
- Tokens e dados de métricas cifrados em armazenamento privado do servidor; sem tokens no navegador.

As listas legadas do Tracker e as métricas oficiais têm finalidades distintas: adicionar um perfil público não autoriza automaticamente acesso às suas estatísticas privadas.

## Executar

Requer **Node >=22.13**. O Dockerfile usa Node 22.

```sh
npm ci
node --env-file=/caminho/privado/metrics.env server.js
```

Antes de habilitar métricas, siga [METRICS_BACKEND.md](METRICS_BACKEND.md) e configure o arquivo privado a partir de [.env.metrics.example](.env.metrics.example). Cadastre os callbacks no domínio HTTPS do **Tracker**, não apenas no domínio do Lume. Conclua o consentimento oficial nas duas plataformas.

Esta integração inclui o mecanismo do Lume no próprio processo do Tracker. Não conecta automaticamente ao banco, às contas ou à implantação do Lume existente. Não versione arquivos de ambiente, cookies, tokens, bancos ou chaves de cifragem.

Contrato de endpoints: [METRICS_CONTRACT.md](METRICS_CONTRACT.md).

## Verificação

```sh
npm test
# Verificações locais de navegador, com Chromium instalado:
CHROMIUM_PATH=/usr/bin/chromium node scripts/verify-metrics-local.mjs
CHROMIUM_PATH=/usr/bin/chromium node scripts/verify-metrics-browser.mjs
```

A primeira verificação usa o servidor real local com armazenamento temporário vazio. A segunda usa respostas de teste explicitamente identificadas para exercitar períodos, falhas, detalhes e layout. Nenhuma delas comprova consentimento ou métricas reais da Meta. Screenshots são gravados fora do repositório, em `/tmp/tracker-metrics-evidence`.

# Tracker Panel

Painel de acompanhamento de redes sociais, com métricas oficiais de Instagram e Facebook integradas ao visual escuro/ciano existente.

## Métricas oficiais

Nas seções **Instagram** e **Facebook**:

- **Adicionar Perfil** abre um modal compacto, no mesmo ponto das outras abas; **Continuar com Instagram/Facebook** leva à autorização oficial. Não há um segundo cabeçalho de Insights ou filtros sem contas.
- O modal verifica a configuração antes de liberar a conexão e explica falhas de acesso/configuração, com opção de verificar novamente.
- Após a autorização, seleção de contas/Páginas e filtros de métricas aparecem na própria aba. No Facebook, monitorar uma URL pública permanece uma opção legada separada, sem Insights.
- Hoje, Ontem, 7, 30 e 90 dias; datas e limitações da API explícitas.
- Indicadores, série diária disponível e conteúdos com pesquisa, filtros e ordenação.
- Detalhes de conteúdo com tempo assistido e abandono inicial quando suportados. Países por vídeo e curvas de retenção não são inventados quando a API não os oferece.
- Cache por conta/provedor/período, requisições concorrentes compartilhadas e proteção contra respostas fora de ordem.
- Última leitura válida preservada em falhas, com indicação de desatualização e timestamp original; sem trocar dados entre períodos.
- Exportação CSV de leituras válidas, com proteção contra fórmulas; exportação de leituras desatualizadas desabilitada.
- Tokens e dados de métricas cifrados em armazenamento privado do servidor; sem tokens no navegador.

As listas legadas do Tracker e as métricas oficiais têm finalidades distintas: adicionar um perfil público não autoriza automaticamente acesso às suas estatísticas privadas.

## Conectar Instagram/Facebook

### Preparar uma instalação nova

Requer **Node >=22.13**. O Dockerfile usa Node 22.

```sh
npm ci
# Execute no servidor real, como o usuário do serviço; substitua domínio e caminho:
npm run setup:metrics -- --origin https://SEU-DOMINIO-TRACKER --secrets-file /caminho/privado/tracker-meta.json
# Depois execute o comando node --env-file=... server.js impresso pelo setup.
```

O JSON privado deve existir, conter somente os pares de ID/segredo dos apps Meta e ter permissões corretas; veja [METRICS_BACKEND.md](METRICS_BACKEND.md). O setup valida esse arquivo sem revelar valores, gera credenciais/chave novas e armazenamento separado fora do Git (diretórios 0700 e env 0600). Por padrão cria uma pasta nova no home; `--output-dir /caminho/privado/NOVA-pasta` permite escolher outra. Nunca sobrescreve uma pasta existente, importa contas ou inicia/publica o serviço.

**Instalação existente:** não substitua o ambiente atual pelo gerado sem revisar os caminhos. O setup cria também um `DB_PATH` vazio: seus perfis de YouTube/TikTok/outras abas não aparecerão nele. Preserve o `DB_PATH` privado existente e, se já houver métricas conectadas, preserve também seu `DATA_DIR` e `TOKEN_ENCRYPTION_KEY`. Faça backup antes; o setup não migra nem apaga contas.

O endereço `https://github.com/miguelgsousa/tracker-panel` é o repositório, **não o domínio da implantação**. É necessário informar a origem HTTPS real, sem barra final; HTTP é rejeitado inclusive em localhost pelo backend OAuth. Cadastre `<PUBLIC_BASE_URL>/auth/instagram/callback` e/ou `<PUBLIC_BASE_URL>/auth/facebook/callback` nos apps correspondentes, configure o proxy HTTPS e conclua o consentimento oficial. Arquivos criados nesta máquina não configuram automaticamente outro servidor.

Se aparecer `metrics_access_not_configured` com `METRICS_USERNAME` / `METRICS_PASSWORD`, falta configuração do processo no servidor; adicionar perfis ou atualizar o Git não corrige isso. O setup só gera configuração local — não comprova implantação, validade dos apps ou acesso a métricas reais. A alternativa manual está em [.env.metrics.example](.env.metrics.example); não execute com os campos obrigatórios em branco.

Esta integração inclui o mecanismo do Lume no próprio processo do Tracker. Não conecta automaticamente ao banco, às contas ou à implantação do Lume existente. Não versione arquivos de ambiente, cookies, tokens, bancos ou chaves de cifragem.

Contrato de endpoints: [METRICS_CONTRACT.md](METRICS_CONTRACT.md).

## Verificação

```sh
npm test
# Verificações locais de navegador, com Chromium instalado:
CHROMIUM_PATH=/usr/bin/chromium node scripts/verify-metrics-local.mjs
CHROMIUM_PATH=/usr/bin/chromium node scripts/verify-metrics-browser.mjs
CHROMIUM_PATH=/usr/bin/chromium node scripts/verify-account-addition.mjs
```

A primeira verificação usa o servidor real local com armazenamento temporário vazio. A segunda usa respostas de teste explicitamente identificadas para exercitar períodos, falhas, detalhes e layout. Nenhuma delas comprova consentimento ou métricas reais da Meta. Screenshots são gravados fora do repositório, em `/tmp/tracker-metrics-evidence`.

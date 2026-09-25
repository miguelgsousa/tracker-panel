# Métricas das contas conectadas ao Zernio

O modo Zernio substitui o mecanismo Meta direto apenas para as rotas de métricas. Os demais trackers e seus dados locais são preservados. As contas são descobertas no servidor pela API Zernio; não é necessário autorizá-las novamente no Tracker.

## Configuração privada

Defina no ambiente do processo/container, nunca em arquivos públicos ou no JavaScript:

```dotenv
METRICS_ENABLED=true
METRICS_PROVIDER=zernio
PUBLIC_BASE_URL=https://SEU-DOMINIO
METRICS_USERNAME=SEU_USUARIO
METRICS_PASSWORD=SUA_SENHA_FORTE
# Escolha uma das alternativas, com valores reais somente no servidor:
# ZERNIO_API_KEY=CHAVE_UNICA
# ZERNIO_API_KEYS={"principal":"CHAVE_1","outra":"CHAVE_2"}
```

`ZERNIO_API_KEYS` é um objeto JSON de rótulo para chave. Use rótulos estáveis. Proteja o arquivo de ambiente com permissão 0600 e diretório privado; não o inclua no Git nem no contexto de build. Um arquivo de ambiente Docker não usa aspas externas em torno do objeto JSON. Configure as credenciais de acesso do Tracker antes de expor o serviço: toda a origem, incluindo os trackers legados, permanece protegida por HTTP Basic. Escritas exigem `Origin` exatamente igual ao `PUBLIC_BASE_URL`.

Não são necessários novos tokens Meta, app secrets, consentimentos OAuth ou um banco Lume para o adaptador Zernio. Dados anteriores do modo Meta não são apagados. Para voltar ao modo anterior, restaure a configuração de provedor anterior e reinicie o Tracker.

## Contas e métricas

- As abas Instagram e Facebook importam suas contas já conectadas ao Zernio. Conexões são administradas no Zernio; o Tracker não desconecta contas remotamente.
- `GET /v1/accounts` descobre as contas e sua situação de conexão. `GET /v1/analytics` fornece os posts, inclusive externos, com paginação.
- Insights da conta Instagram e da Página Facebook são consultados nos respectivos endpoints Zernio. Views/interações da conta não são substituídas por somas lifetime dos posts.
- Curtidas, comentários, compartilhamentos e salvos da tabela são valores acumulados desde a publicação. O período seleciona a data de publicação dos posts; não transforma esses valores em eventos ocorridos no período.
- Alcance único de contas diferentes não é somado. Campos não disponibilizados permanecem `null`/`—`, nunca zero inventado.
- Hoje não apresenta uma soma de posts como contador intradiário. Indisponibilidade, limites de intervalo, falhas e truncamento devem permanecer explícitos.
- Os endpoints Zernio têm cache/atraso próprios. Atualizar o Tracker relê a API, não garante uma atualização imediata na plataforma social. Analytics depende das permissões e do plano/add-on Zernio.
- Chaves, tokens, objetos brutos de conta e erros privados do provedor não são enviados ao navegador. A listagem contém somente os campos necessários à interface.

## Verificação

Execute `npm test`. Os testes de provedor usam respostas simuladas identificadas como fixtures, não comprovam a validade de uma chave real. Depois de configurar o ambiente, confira no endereço público autenticado `/api/metrics/config`, `/api/metrics/accounts`, os dashboards Instagram/Facebook e os controles de período no navegador. Confirme que o usuário sem autenticação recebe 401 e que arquivos de configuração não são servidos.

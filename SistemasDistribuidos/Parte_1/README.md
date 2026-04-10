# BBS/IRC Messaging System — Sistemas Distribuídos

## Introdução

Este projeto implementa um sistema simplificado de troca de mensagens inspirado no **BBS (Bulletin Board System)** e no **IRC (Internet Relay Chat)**. O objetivo é demonstrar conceitos de sistemas distribuídos — comunicação entre processos, persistência de dados e orquestração de serviços — na prática.

Nesta **Parte 1**, o sistema suporta:

- Login de bots (clientes) nos servidores
- Listagem de canais disponíveis
- Criação de novos canais
- Persistência em disco: log de logins e lista de canais por servidor

---

## Grupo

Projeto em dupla — **2 integrantes** → 2 linguagens de programação.

| Integrante | Linguagem | Componentes |
|---|---|---|
| Gustavo Bertoluzzi Cardoso Ra:22.123.016-2 | **Python 3.12** | `python-server-1`, `python-server-2`, `python-bot-1`, `python-bot-2` |
| Isabella Vieira Silva Rosseto Ra:22.222.036-0 | **Java 21** | `java-server-1`, `java-server-2`, `java-bot-1`, `java-bot-2` |

Além disso, há **2 bots cruzados** para demonstrar interoperabilidade:

- `python-bot-cross-1` → conecta nos servidores Java
- `java-bot-cross-1` → conecta nos servidores Python

---

## Arquitetura

```
┌────────────────────────────────────────────────────────────────┐
│                       Docker Network: bbs-net                  │
│                                                                │
│  python-server-1 ◄── python-bot-1                             │
│  python-server-1 ◄── java-bot-cross-1   (cross-language)      │
│                                                                │
│  python-server-2 ◄── python-bot-2                             │
│  python-server-2 ◄── java-bot-cross-1   (cross-language)      │
│                                                                │
│  java-server-1   ◄── java-bot-1                               │
│  java-server-1   ◄── python-bot-cross-1 (cross-language)      │
│                                                                │
│  java-server-2   ◄── java-bot-2                               │
│  java-server-2   ◄── python-bot-cross-1 (cross-language)      │
└────────────────────────────────────────────────────────────────┘
```

Fluxo de mensagens por bot:

```
Bot → LOGIN       → Servidor responde LOGIN_OK / LOGIN_ERROR
Bot → LIST_CHANNELS → Servidor responde CHANNELS_LIST
Bot → CREATE_CHANNEL → Servidor responde CHANNEL_CREATED / CHANNEL_EXISTS / CHANNEL_ERROR
```

---

## Escolhas Técnicas

### Linguagens

- **Python 3.12** — linguagem do Gustavo. Excelente suporte a ZeroMQ (`pyzmq`) e MessagePack (`msgpack`).
- **Java 21** — linguagem da Isabella. Suporte a ZeroMQ via `jeromq` (pure Java) e MessagePack via `msgpack-core`.

### Serialização: MessagePack

Todos os dados trafegam em **binário usando [MessagePack](https://msgpack.org/)** — zero JSON, XML ou texto simples no wire.

| Critério | Escolha |
|---|---|
| Formato | MessagePack (binário, compacto) |
| Lib Python | `msgpack 1.1.0` |
| Lib Java | `msgpack-core 0.9.8` |

Todas as mensagens contêm obrigatoriamente:
- `type` — tipo da mensagem (ex.: `"LOGIN"`, `"LOGIN_OK"`)
- `timestamp` — Unix timestamp em ponto flutuante (`double`)

### Transporte: ZeroMQ REQ/REP

O padrão **REQ/REP** do ZeroMQ foi utilizado conforme os diagramas de sequência do enunciado:
- Servidor faz `bind` na porta `5555`
- Cliente faz `connect` e envia uma requisição por vez, aguardando resposta

### Persistência: JSON em disco

Cada servidor mantém **seu próprio conjunto de arquivos** no diretório `/data` (montado como volume Docker):

| Arquivo | Conteúdo |
|---|---|
| `logins.json` | Lista de todos os logins com `username`, `timestamp` e `server` |
| `channels.json` | Lista com os nomes dos canais criados |

Os arquivos são escritos atomicamente (write-to-temp + rename) para evitar corrupção.

**Por que JSON?** Simples, sem dependência extra de banco de dados, legível para debug durante o desenvolvimento. O formato de troca de mensagens (wire) permanece binário (MessagePack).

---

## Como Executar

### Pré-requisitos

- Docker Desktop (ou Docker Engine + Compose v2)

### Subir todos os serviços

```bash
docker compose up --build
```

Isso irá:
1. Fazer build das imagens Python e Java
2. Subir 4 servidores + 6 bots (4 nativos + 2 cruzados)
3. Exibir no terminal todos os envios e recebimentos de mensagem

### Rodar novamente (sem rebuild)

```bash
docker compose up
```

Os dados persistidos em `./data/` serão carregados, e os canais criados anteriormente aparecerão na listagem.

### Encerrar

```bash
docker compose down
```

Para apagar os dados persistidos também:

```bash
docker compose down -v
rm -rf ./data
```

---

## Estrutura do Projeto

```
SistemasDistribuidos/
├── README.md
├── docker-compose.yaml
├── data/                        ← criado automaticamente pelo Docker
│   ├── python-server-1/
│   │   ├── logins.json
│   │   └── channels.json
│   ├── python-server-2/
│   ├── java-server-1/
│   └── java-server-2/
├── python/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── server.py
│   └── client.py
└── java/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/bbs/
        ├── Launcher.java
        ├── Protocol.java
        ├── Server.java
        └── Client.java
```

---

## Protocolo de Mensagens

### Tipos de Requisição (cliente → servidor)

| `type` | Campos extras | Descrição |
|---|---|---|
| `LOGIN` | `username: str` | Login do bot |
| `LIST_CHANNELS` | — | Listar canais |
| `CREATE_CHANNEL` | `channel_name: str` | Criar canal |

### Tipos de Resposta (servidor → cliente)

| `type` | Campos extras | Descrição |
|---|---|---|
| `LOGIN_OK` | `username: str` | Login aprovado |
| `LOGIN_ERROR` | `error: str` | Login rejeitado |
| `CHANNELS_LIST` | `channels: [str]` | Lista de canais |
| `CHANNEL_CREATED` | `channel_name: str` | Canal criado |
| `CHANNEL_EXISTS` | `channel_name: str` | Canal já existia |
| `CHANNEL_ERROR` | `error: str` | Nome inválido |

### Regras de Validação

- **Username / Channel name** devem seguir o padrão `[a-zA-Z0-9_-]` com no máximo 64 caracteres
- Username vazio → `LOGIN_ERROR`
- Channel name vazio ou com caracteres inválidos → `CHANNEL_ERROR`
- Canal duplicado → `CHANNEL_EXISTS` (não é erro fatal; o bot continua)

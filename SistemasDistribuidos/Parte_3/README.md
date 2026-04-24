# BBS/IRC Messaging System — Parte 3: Relógios e Heartbeat

## Grupo

| Integrante | RA | Linguagem |
|---|---|---|
| Gustavo Bertoluzzi Cardoso | 22.123.016-2 | **Python 3.12** |
| Isabella Vieira Silva Rosseto | 22.222.036-0 | **Java 21** |

---

## O que foi implementado nesta parte

Esta parte adiciona dois grandes recursos ao sistema:

### 1. Relógio Lógico de Lamport

Implementado em **todos** os processos (servidores e bots, Python e Java).

**Regras seguidas (conforme enunciado):**
- O contador é incrementado **antes do envio** de cada mensagem e enviado junto com ela no campo `logical_clock`
- Ao **receber** uma mensagem, o processo atualiza seu contador: `clock = max(clock, clock_recebido)`

Toda mensagem trocada no sistema agora possui o campo `logical_clock` além do `timestamp` físico. Nos logs você verá o campo `lc=` em cada envio e recebimento, por exemplo:

```
[python-bot-1] → SEND  type=PUBLISH  lc=42  ts=2026-04-24 11:57:01.090
[python-bot-1] ← RECV  from=...      type=PUBLISH_OK  lc=45  ts=2026-04-24 11:57:01.095
```

---

### 2. Serviço de Referência (`reference`)

Novo container Python que centraliza:

#### a) Atribuição de rank (GET_RANK)
Cada servidor, ao iniciar, envia seu nome e recebe um rank único sequencial:

```
Servidor → REQ: {type: "GET_RANK", name: "python-server-1", logical_clock: N}
Referência → REP: {type: "RANK_REPLY", rank: 1, logical_clock: M, ref_time: T}
```

O rank é atribuído na ordem de registro (primeiro a chegar → rank 1).

#### b) Lista de servidores disponíveis (LIST)
Logo após o GET_RANK, cada servidor consulta quem mais está disponível:

```
Servidor → REQ: {type: "LIST", logical_clock: N}
Referência → REP: {type: "LIST_REPLY", servers: [{name, rank}, ...], logical_clock: M}
```

#### c) Heartbeat (a cada 10 mensagens de clientes)
Cada servidor conta as mensagens recebidas de clientes. A cada 10 mensagens, envia um heartbeat à referência:

```
Servidor → REQ: {type: "HEARTBEAT", name: "python-server-1", logical_clock: N}
Referência → REP: {type: "HEARTBEAT_OK", logical_clock: M, ref_time: T}
```

Se um servidor parar de enviar heartbeats por mais de **60 segundos**, a referência o remove da lista de servidores disponíveis.

#### d) Sincronização do relógio físico
Aproveitando cada resposta da referência (GET_RANK e HEARTBEAT), o servidor calcula e corrige seu offset de relógio físico usando a fórmula NTP simplificada:

```
RTT = t_recv - t_send
offset = (ref_time + RTT/2) - t_recv
corrected_time = time.now() + offset
```

Nos logs dos servidores você verá:
```
[python-server-1] ⏱ CLOCK SYNC  ref_time=2026-04-24 11:57:01.090  offset=+0.002s
```

---

## Arquitetura completa (Parte 3)

```
┌──────────────────────────────────────────────────────────────────────┐
│                      Docker Network: bbs-net                         │
│                                                                      │
│  ┌─────────────┐   GET_RANK / HEARTBEAT    ┌───────────────────┐    │
│  │ python-srv-1│ ─────────────────────────►│                   │    │
│  │ python-srv-2│ ◄─────────────────────────│   reference       │    │
│  │ java-srv-1  │   rank / ref_time / list  │   (porta 5559)    │    │
│  │ java-srv-2  │                           └───────────────────┘    │
│  └──────┬──────┘                                                     │
│         │ REP (porta 5555)          ┌────────────────────────┐       │
│         │◄── bots fazem REQ ────────│ python-bot-1/2         │       │
│         │                           │ java-bot-1/2           │       │
│         │ PUB (→ proxy XSUB 5557)  │ python-bot-cross-1     │       │
│         ▼                           │ java-bot-cross-1       │       │
│  ┌─────────────┐                   └────────┬───────────────-┘       │
│  │ pubsub-proxy│◄────────────────────────────┘                       │
│  │  XSUB:5557  │  SUB ← proxy XPUB:5558                             │
│  │  XPUB:5558  │──────────────────────────► bots (subscribed)       │
│  └─────────────┘                                                     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Estrutura do projeto

```
Parte_3/
├── README.md                        ← este arquivo
├── docker-compose.yaml
├── data/                            ← criado automaticamente pelo Docker
│   ├── python-server-1/
│   │   ├── logins.json
│   │   ├── channels.json
│   │   └── messages.json
│   ├── python-server-2/
│   ├── java-server-1/
│   └── java-server-2/
├── python/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── reference.py                 ← NOVO: serviço de referência
│   ├── server.py                    ← atualizado: clock lógico + heartbeat + sync físico
│   ├── client.py                    ← atualizado: clock lógico
│   └── proxy.py                     ← sem alteração (Parte 2)
└── java/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/bbs/
        ├── Launcher.java            ← sem alteração
        ├── Protocol.java            ← sem alteração
        ├── Server.java              ← atualizado: clock lógico + heartbeat + sync físico
        └── Client.java              ← atualizado: clock lógico
```

---

## Protocolo de mensagens — Parte 3

### Campos adicionados em TODAS as mensagens (clientes ↔ servidores)

| Campo | Tipo | Descrição |
|---|---|---|
| `logical_clock` | `int` | Valor do relógio de Lamport do remetente **após** incremento |

### Novos tipos — Servidor ↔ Referência

| `type` | Direção | Campos extras | Descrição |
|---|---|---|---|
| `GET_RANK` | servidor → referência | `name: str` | Registrar e obter rank |
| `RANK_REPLY` | referência → servidor | `rank: int`, `ref_time: float` | Rank atribuído + timestamp da referência |
| `LIST` | servidor → referência | — | Listar servidores disponíveis |
| `LIST_REPLY` | referência → servidor | `servers: [{name, rank}]`, `ref_time: float` | Lista atual |
| `HEARTBEAT` | servidor → referência | `name: str` | Atualizar disponibilidade |
| `HEARTBEAT_OK` | referência → servidor | `ref_time: float` | Confirmação + timestamp para sync |

---

## Como executar

### Pré-requisitos

- **Docker Desktop** instalado e rodando

### Subir tudo do zero (1ª vez)

```bash
cd Parte_3
docker compose up --build
```

O sistema irá:
1. Subir o **`reference`** (porta 5559) e o **`pubsub-proxy`** (portas 5557/5558)
2. Subir os 4 servidores — cada um registra com a referência e recebe seu rank
3. Subir os 6 bots — entram em loop infinito publicando mensagens

### Executar sem rebuild (dados persistidos mantidos)

```bash
docker compose up
```

### Parar

```bash
# Ctrl+C no terminal com os logs, depois:
docker compose down

# Para apagar dados persistidos também:
docker compose down && rm -rf data
```

---

## Comandos úteis durante a execução

Abra um **segundo terminal** na pasta `Parte_3/`:

```bash
# Ver status de todos os containers (deve mostrar 12 containers Up)
docker compose ps

# Acompanhar o serviço de referência ao vivo
docker compose logs -f reference

# Ver rank e clock sync de um servidor Python
docker compose logs -f python-server-1 | grep -E "lc=|CLOCK SYNC|Rank|Known"

# Ver rank e clock sync de um servidor Java
docker compose logs -f java-server-1 | grep -E "lc=|CLOCK SYNC|Rank|Known"

# Ver relógio lógico crescendo em um bot Python
docker compose logs -f python-bot-1 | grep "lc="

# Ver relógio lógico crescendo em um bot Java
docker compose logs -f java-bot-1 | grep "lc="

# Ver mensagens recebidas por pub/sub
docker compose logs -f python-bot-1 | grep -E "SUB-RECV|SUBSCRIBED"

# Ver dados persistidos de um servidor
cat data/python-server-1/messages.json
cat data/java-server-1/channels.json
```

---

## O que esperar nos logs

### Serviço de referência
```
[reference] GET_RANK (new)  name=python-server-1  rank=1
[reference] GET_RANK (new)  name=java-server-1    rank=4
[reference] HEARTBEAT  name=python-server-1  status=updated
[reference] → SEND  type=HEARTBEAT_OK  lc=42
```

### Servidores (Python e Java)
```
[python-server-1] ⏱ CLOCK SYNC  ref_time=2026-04-24 11:56:32.584  offset=+0.002s
[python-server-1] ✓ Registered  rank=1
[python-server-1] ✓ Known servers: [{'name': 'python-server-1', 'rank': 1}]
  Rank         : 1

[python-server-1] ← RECV  type=PUBLISH  lc=42  ts=2026-04-24 11:57:01.090
[python-server-1] → SEND  type=PUBLISH_OK  lc=43  ts=2026-04-24 11:57:01.095
[python-server-1] ⇒ PUB   channel=canal-python-bot-1-1234  lc=43
```

### Bots (Python e Java)
```
[python-bot-1] → SEND  type=LOGIN  lc=1   ts=2026-04-24 11:56:42.819
[python-bot-1] ← RECV  type=LOGIN_OK  lc=5  ts=2026-04-24 11:56:42.829

[python-bot-1] ☆ SUBSCRIBED to channel 'canal-python-bot-1-1234' (total=1)
[python-bot-1] → publishing 10 messages to 'canal-python-bot-1-1234'  lc=24

[python-bot-1] ★ SUB-RECV  channel=canal-python-bot-1-1234  lc=47
               message  = xk3mq9ab #1
               from     = python-bot-1  via python-server-1
               sent_ts  = 2026-04-24 11:57:01.090
               recv_ts  = 2026-04-24 11:57:01.095
```

---

## Escolhas técnicas

### Relógio de Lamport
- **Python**: classe `LamportClock` com `threading.Lock` para thread-safety (necessário pois o bot usa threads)
- **Java**: `AtomicLong` com CAS loop para operação `max(clock, received)` sem locks explícitos

### Sincronização de relógio físico
Algoritmo NTP simplificado (single-sided):
```
RTT = t_recv - t_send
offset = (ref_time + RTT/2) - t_recv
corrected_time = time.now() + offset
```

### Heartbeat
- Disparado a cada **10 mensagens de clientes** recebidas (contadas no loop principal do servidor)
- Timeout de remoção: **60 segundos** sem heartbeat
- Verificação de expirados: a cada **10 segundos** (thread de limpeza na referência)

### Comunicação Servidor ↔ Referência
- Socket **REQ** separado no servidor (não interfere com o socket REP dos clientes)
- Timeout de **5 segundos** — se a referência não responder, o servidor loga um aviso e continua normalmente

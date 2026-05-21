# Kafka JSON Loader

CLI-утилита для автоматической генерации и отправки сообщений в Kafka. Подходит для Windows 11 и не требует `librdkafka`: проект написан на Go и использует `github.com/segmentio/kafka-go`.

## Что умеет

- автоматически генерировать `key`, `value`, `headers`;
- отправлять одно или несколько сообщений за запуск;
- работать в непрерывном режиме с заданной скоростью;
- подключаться к Kafka по SSL/TLS;
- запускаться через `go run`, `go build` или PowerShell-скрипт;
- при необходимости все еще работать через `-config` в legacy-режиме.

## Быстрый запуск без файла

### Через Go

```powershell
go run .\cmd\kafka-json-loader `
  -brokers localhost:9092 `
  -topic demo.events `
  -count 10 `
  -key-prefix order `
  -event-type order.created `
  -source windows-loader
```

### Через PowerShell-обертку

```powershell
.\scripts\send-kafka.ps1 `
  -Brokers "localhost:9092" `
  -Topic "demo.events" `
  -Count 10 `
  -KeyPrefix "order" `
  -EventType "order.created" `
  -Source "windows-loader"
```

Что генерируется автоматически на каждый message:

- `key`: строка вида `order-<unix>-000001`
- `headers`: `content-type`, `generator`, `source`, `host`, `sequence`, `event-type`
- `value`: JSON-объект с `id`, `sequence`, `eventType`, `generatedAt`, `customer`, `order`, `flags`

## Непрерывный режим

### По скорости

```powershell
go run .\cmd\kafka-json-loader `
  -brokers localhost:9092 `
  -topic demo.events `
  -continuous `
  -rate 20 `
  -key-prefix load `
  -event-type load.test
```

### По интервалу

```powershell
go run .\cmd\kafka-json-loader `
  -brokers localhost:9092 `
  -topic demo.events `
  -continuous `
  -interval 500ms `
  -duration 2m
```

### Через PowerShell-обертку

```powershell
.\scripts\send-kafka.ps1 `
  -Brokers "localhost:9092" `
  -Topic "demo.events" `
  -Continuous `
  -Rate 20 `
  -Duration "5m"
```

В непрерывном режиме:

- `-continuous` включает бесконечную генерацию до остановки процесса.
- `-rate` задает сообщений в секунду.
- `-interval` задает паузу между сообщениями.
- `-duration` задает ограничение по времени, после которого утилита завершится сама.
- нужно указывать либо `-rate`, либо `-interval`, но не оба сразу.

## Основные параметры CLI

- `-brokers` - список брокеров через запятую, обязательно.
- `-topic` - имя топика, обязательно.
- `-count` - сколько сообщений сгенерировать.
- `-continuous` - непрерывная генерация сообщений до остановки процесса.
- `-rate` - сообщений в секунду для непрерывного режима.
- `-interval` - интервал между сообщениями для непрерывного режима.
- `-duration` - максимальная длительность непрерывного режима.
- `-key-prefix` - префикс для Kafka key.
- `-event-type` - тип события внутри JSON payload.
- `-source` - источник в payload и headers.
- `-client-id` - Kafka client id.
- `-acks` - `none`, `one`, `all`.
- `-compression` - `none`, `gzip`, `snappy`, `lz4`, `zstd`.

### Настройки `ssl`

```powershell
go run .\cmd\kafka-json-loader `
  -brokers kafka01.example.local:9093,kafka02.example.local:9093 `
  -topic secure.events `
  -count 5 `
  -ssl `
  -ssl-server-name kafka01.example.local `
  -ssl-ca-file .\certs\ca.pem `
  -ssl-cert-file .\certs\client.pem `
  -ssl-key-file .\certs\client-key.pem
```

- `-ssl` - включает TLS.
- `-ssl-server-name` - имя хоста для TLS handshake и проверки сертификата.
- `-ssl-ca-file` - PEM-файл корневого или промежуточного CA.
- `-ssl-cert-file` - PEM-файл клиентского сертификата для mutual TLS.
- `-ssl-key-file` - приватный ключ клиентского сертификата.
- `-ssl-insecure-skip-verify` - отключает проверку сертификата сервера. Лучше использовать только для диагностики.

Поддерживаются два типовых режима:

- Server TLS: `-ssl` и `-ssl-ca-file`, если нужно доверять кастомному CA.
- Mutual TLS: дополнительно указываются `-ssl-cert-file` и `-ssl-key-file`.

## Legacy-режим через JSON-файл

Если понадобится полностью ручное управление payload, старый режим все еще работает:

```powershell
go run .\cmd\kafka-json-loader -config .\payload.json
```

## Запуск на Windows 11

### Сборка `.exe`

```powershell
go build -o .\bin\kafka-json-loader.exe .\cmd\kafka-json-loader
```

После сборки:

```powershell
.\bin\kafka-json-loader.exe -brokers localhost:9092 -topic demo.events -count 100
```

Пример непрерывной отправки:

```powershell
.\bin\kafka-json-loader.exe -brokers localhost:9092 -topic demo.events -continuous -rate 50 -duration 10m
```

## Вспомогательные утилиты для Windows 11

Рекомендую такой набор:

1. `Go 1.22+` - для сборки и запуска утилиты.
2. `PowerShell 7` - удобный стандартный способ запускать скрипты и автоматизацию.
3. `Windows Terminal` - удобнее для работы с несколькими профилями и логами.
4. `Docker Desktop` - если нужен локальный Kafka/Redpanda для тестов.
5. `kcat` через `WSL` - полезен для быстрой проверки того, что сообщения действительно попали в топик.

Если нужен максимально простой локальный стенд на Windows, практичнее всего использовать не "чистый" Kafka, а `Redpanda` в Docker.

Если Kafka работает с SSL-сертификатами, удобно хранить их в отдельной папке проекта, например `.\certs\`, и передавать пути через CLI-флаги.

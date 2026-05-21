# Kafka JSON Loader

CLI-утилита для отправки сообщений в Kafka из JSON-файла. Подходит для Windows 11 и не требует `librdkafka`: проект написан на Go и использует `github.com/segmentio/kafka-go`.

## Что умеет

- отправлять одно или несколько сообщений за запуск;
- задавать `key`, `value`, `headers`;
- подключаться к Kafka по SSL/TLS;
- принимать значения как обычный JSON или как явно типизированный payload;
- запускаться через `go run`, `go build` или PowerShell-скрипт.

## Формат входного JSON

Корневой объект:

```json
{
  "brokers": ["localhost:9092"],
  "topic": "demo.events",
  "clientId": "windows-loader",
  "requiredAcks": "all",
  "compression": "snappy",
  "ssl": {
    "enabled": true,
    "serverName": "kafka01.example.local",
    "caFile": ".\\certs\\ca.pem",
    "certFile": ".\\certs\\client.pem",
    "keyFile": ".\\certs\\client-key.pem",
    "insecureSkipVerify": false
  },
  "batchTimeoutMs": 1000,
  "writeTimeoutMs": 10000,
  "messages": [
    {
      "key": "text-key",
      "value": {"hello": "world"},
      "headers": {
        "source": "loader"
      }
    }
  ]
}
```

Поля:

- `brokers` - список Kafka broker-ов, обязателен.
- `topic` - имя топика, обязательно.
- `clientId` - опциональный Kafka client id.
- `requiredAcks` - `none`, `one`, `all`.
- `compression` - `none`, `gzip`, `snappy`, `lz4`, `zstd`.
- `ssl` - настройки SSL/TLS-подключения.
- `batchTimeoutMs`, `writeTimeoutMs`, `readTimeoutMs` - таймауты в миллисекундах.
- `messages` - массив сообщений.

### Настройки `ssl`

```json
{
  "ssl": {
    "enabled": true,
    "serverName": "kafka01.example.local",
    "caFile": ".\\certs\\ca.pem",
    "certFile": ".\\certs\\client.pem",
    "keyFile": ".\\certs\\client-key.pem",
    "insecureSkipVerify": false
  }
}
```

- `enabled` - включает TLS.
- `serverName` - имя хоста для TLS handshake и проверки сертификата.
- `caFile` - PEM-файл корневого или промежуточного CA, которым подписан сертификат брокера.
- `certFile` - PEM-файл клиентского сертификата, если Kafka требует mutual TLS.
- `keyFile` - приватный ключ клиентского сертификата.
- `insecureSkipVerify` - отключает проверку сертификата сервера. Лучше использовать только для временной диагностики.

Поддерживаются два типовых режима:

- Server TLS: `enabled=true` и `caFile`, если нужно доверять кастомному CA.
- Mutual TLS: дополнительно указываются `certFile` и `keyFile`.

## Формат `key`, `value`, `headers`

`key` и `value` могут быть:

- `null`
- строкой
- числом
- `true` / `false`
- объектом или массивом, которые будут сериализованы в JSON
- типизированным объектом:

```json
{
  "type": "string | json | base64 | hex | null",
  "data": "..."
}
```

Примеры:

- `{ "type": "string", "data": "abc" }` - UTF-8 строка;
- `{ "type": "json", "data": { "id": 1 } }` - JSON-сериализация поля `data`;
- `{ "type": "base64", "data": "SGVsbG8=" }` - бинарные данные из base64;
- `{ "type": "hex", "data": "0a0b0c" }` - бинарные данные из hex;
- `{ "type": "null" }` - пустое значение.

`headers` можно передавать в двух формах:

```json
{
  "content-type": "application/json",
  "trace-id": "abc"
}
```

или

```json
[
  { "key": "content-type", "value": "application/json" },
  { "key": "trace-id", "value": { "type": "hex", "data": "0a0b0c" } }
]
```

## Запуск на Windows 11

### Вариант 1. Через Go

```powershell
go run .\cmd\kafka-json-loader -config .\examples\message-object.json
```

### Вариант 2. Через PowerShell-обертку

```powershell
.\scripts\send-kafka.ps1 -Config .\examples\message-object.json
```

### Сборка `.exe`

```powershell
go build -o .\bin\kafka-json-loader.exe .\cmd\kafka-json-loader
```

После сборки:

```powershell
.\bin\kafka-json-loader.exe -config .\examples\message-object.json
```

Для SSL-примера:

```powershell
.\bin\kafka-json-loader.exe -config .\examples\message-ssl.json
```

Важно: в JSON для Windows путь нужно писать с двойным обратным слешем, например `".\\certs\\ca.pem"`.

## Вспомогательные утилиты для Windows 11

Рекомендую такой набор:

1. `Go 1.22+` - для сборки и запуска утилиты.
2. `PowerShell 7` - удобный стандартный способ запускать скрипты и автоматизацию.
3. `Windows Terminal` - удобнее для работы с несколькими профилями и логами.
4. `Docker Desktop` - если нужен локальный Kafka/Redpanda для тестов.
5. `kcat` через `WSL` - полезен для быстрой проверки того, что сообщения действительно попали в топик.

Если нужен максимально простой локальный стенд на Windows, практичнее всего использовать не "чистый" Kafka, а `Redpanda` в Docker.

Если Kafka работает с SSL-сертификатами, удобно хранить их в отдельной папке проекта, например `.\certs\`, и указывать пути до PEM-файлов прямо в JSON-конфиге.

## Примеры

- [examples/message-object.json](/Users/victor/Documents/New%20project%202/examples/message-object.json)
- [examples/message-mixed.json](/Users/victor/Documents/New%20project%202/examples/message-mixed.json)
- [examples/message-ssl.json](/Users/victor/Documents/New%20project%202/examples/message-ssl.json)

#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
# Go возвращает успех и при пустой выборке: проверяем наличие теста до запуска fixture.
test_package=./internal/kafka
test_name=TestAuthenticatedKafka
go test -tags=component "$test_package" -list "^${test_name}$" | grep -Fx "$test_name" >/dev/null
trap 'echo "Kafka fixture failed at line $LINENO" >&2' ERR
work=$(mktemp -d)
name="ovm184-kafka-$$"
cleanup() { result=$?; if [ "$result" -ne 0 ]; then docker logs --tail 30 "$name" 2>/dev/null || true; fi; docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT
echo "Preparing Kafka certificates"
chmod 755 "$work"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$work/server.key" -out "$work/ca.crt" -days 1 -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' >/dev/null 2>&1
chmod 644 "$work/server.key"
cat > "$work/redpanda.yaml" <<'YAML'
redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: 0
  developer_mode: true
  seed_servers: []
  rpc_server: {address: 0.0.0.0, port: 33145}
  advertised_rpc_api: {address: 127.0.0.1, port: 33145}
  kafka_api:
    - {address: 0.0.0.0, port: 19093, authentication_method: sasl}
  advertised_kafka_api:
    - {address: 127.0.0.1, port: 19093}
  kafka_api_tls:
    - enabled: true
      cert_file: /certs/ca.crt
      key_file: /certs/server.key
      require_client_auth: false
  admin:
    - {address: 0.0.0.0, port: 9644}
  enable_sasl: true
  superusers: [bootstrap]
YAML
chmod 644 "$work/redpanda.yaml"
echo "Starting isolated Kafka"
docker create --name "$name" -p 127.0.0.1:19093:19093 --entrypoint /bin/sh redpandadata/redpanda:v24.3.18 -c 'cp /certs/redpanda.yaml /tmp/redpanda.yaml; exec rpk redpanda start --config /tmp/redpanda.yaml --overprovisioned --smp 1 --memory 512M --reserve-memory 0M --check=false'  >/dev/null
# docker cp работает и с удалённым daemon/Colima, где host TMPDIR не смонтирован.
docker cp "$work" "$name:/certs"
docker start "$name" >/dev/null
for attempt in $(seq 1 40); do
 if [ "$(docker inspect --format '{{.State.Running}}' "$name")" != true ]; then
 echo "Kafka exited before readiness" >&2
 docker logs --tail 20 "$name" >&2
 exit 1
 fi
 if docker exec "$name" rpk cluster health --api-urls 127.0.0.1:9644 >/dev/null 2>&1; then break; fi
 sleep 1
done
echo "Configuring restricted Kafka credentials"
password=$(openssl rand -hex 24)
admin_password=$(openssl rand -hex 24)
docker exec "$name" rpk security user create bootstrap -p "$admin_password" --mechanism SCRAM-SHA-256 --api-urls 127.0.0.1:9644 >/dev/null
docker exec "$name" rpk security user create client -p "$password" --mechanism SCRAM-SHA-256 --api-urls 127.0.0.1:9644 >/dev/null
rpk() { docker exec "$name" rpk "$@" -X brokers=127.0.0.1:19093 -X tls.enabled=true -X tls.ca=/certs/ca.crt -X user=bootstrap -X pass="$admin_password" -X sasl.mechanism=SCRAM-SHA-256; }
rpk topic create ovm184-secure ovm184-denied >/dev/null
rpk security acl create --allow-principal User:client --operation read,write,describe --topic ovm184-secure >/dev/null
rpk security acl create --allow-principal User:client --operation read --group ovm184-group >/dev/null
PARKER_SECURE_KAFKA=127.0.0.1:19093 PARKER_SECURE_CA="$work/ca.crt" PARKER_SECURE_PASSWORD="$password" go test -race -tags=component "$test_package" -run "^${test_name}$" -count=1 -v

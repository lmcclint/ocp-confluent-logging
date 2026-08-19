#!/usr/bin/env bash
#
# Proves the pipeline end to end: runs a throwaway console consumer inside the
# cluster, over TLS, against the topic the collector writes to. If OpenShift log
# records come back, every link works - collector RBAC, broker TLS, CA trust,
# topic.
set -euo pipefail

CONFLUENT_NS="${CONFLUENT_NS:-confluent}"
TOPIC="${TOPIC:-ocp-logs.log-demo-a}"
MESSAGES="${MESSAGES:-5}"
TIMEOUT_MS="${TIMEOUT_MS:-60000}"
POD="verify-consumer-$$"

cleanup() { oc delete pod "$POD" -n "$CONFLUENT_NS" --ignore-not-found --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

IMAGE="$(oc get sts kafka -n "$CONFLUENT_NS" -o jsonpath='{.spec.template.spec.containers[0].image}')"

# Show the fan-out first: one topic per namespace, created by the broker on
# demand as the forwarder discovers each namespace.
echo "==> Log topics currently in the cluster:"
oc exec kafka-0 -n "$CONFLUENT_NS" -c kafka -- \
  kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \
  | grep '^ocp-' | sed 's/^/    /' || true

echo
echo "==> Consuming up to $MESSAGES messages from '$TOPIC' (image: $IMAGE)"

# The brokers present a certificate signed by our private CA. Kafka clients read
# a PEM trust store directly, so the CA the operator holds in ca-pair-sslcerts
# can be mounted as-is - no JKS conversion.
oc run "$POD" \
  --namespace "$CONFLUENT_NS" \
  --image="$IMAGE" \
  --restart=Never \
  --quiet \
  --overrides='{
    "spec": {
      "securityContext": {
        "runAsNonRoot": true,
        "seccompProfile": {"type": "RuntimeDefault"}
      },
      "containers": [{
        "name": "consumer",
        "image": "'"$IMAGE"'",
        "command": ["bash","-c","printf \"security.protocol=SSL\\nssl.truststore.type=PEM\\nssl.truststore.location=/mnt/ca/tls.crt\\n\" > /tmp/client.properties && kafka-console-consumer --bootstrap-server kafka.'"$CONFLUENT_NS"'.svc.cluster.local:9071 --topic '"$TOPIC"' --from-beginning --max-messages '"$MESSAGES"' --timeout-ms '"$TIMEOUT_MS"' --consumer.config /tmp/client.properties"],
        "securityContext": {
          "allowPrivilegeEscalation": false,
          "capabilities": {"drop": ["ALL"]}
        },
        "volumeMounts": [{"name":"ca","mountPath":"/mnt/ca","readOnly":true}]
      }],
      "volumes": [{"name":"ca","secret":{"secretName":"ca-pair-sslcerts","items":[{"key":"tls.crt","path":"tls.crt"}]}}]
    }
  }' >/dev/null

oc wait --for=condition=Ready --timeout=120s pod/"$POD" -n "$CONFLUENT_NS" >/dev/null 2>&1 || true

OUT="$(mktemp)"
trap 'cleanup; rm -f "$OUT"' EXIT
oc logs -f "$POD" -n "$CONFLUENT_NS" 2>&1 | tee "$OUT"

# Assert on the message count, not on the pod's exit status.
#
# kafka-console-consumer exits 0 even when it times out having read nothing, so
# a phase check reports success on an empty topic - which is precisely the
# failure this script exists to catch.
COUNT="$(grep -oE 'Processed a total of [0-9]+ messages' "$OUT" | grep -oE '[0-9]+' | tail -1)"
COUNT="${COUNT:-0}"

echo
if [[ "$COUNT" -gt 0 ]]; then
  echo "==> Pipeline verified: consumed $COUNT records. OpenShift logs are reaching Kafka over TLS."
else
  {
    echo "==> FAILED: consumed 0 records from '$TOPIC'."
    echo "    Demo app:   oc logs -n log-demo-a deploy/log-demo --tail=5"
    echo "    Collector:  oc logs -n openshift-logging ds/kafka-forwarder | tail"
    echo "    Forwarder:  oc get clusterlogforwarder -n openshift-logging -o yaml"
    echo "    Broker:     oc logs -n $CONFLUENT_NS kafka-0 --tail=50"
    echo
    echo "    If the topic list above is empty, no namespace matched the"
    echo "    forwarder's input glob (log-demo-*) - check 40-logging."
  } >&2
  exit 1
fi

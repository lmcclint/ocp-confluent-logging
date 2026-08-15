#!/usr/bin/env bash
#
# Brings up the whole stack in dependency order on an empty OpenShift cluster.
#
# Each stage waits for the specific condition the next stage depends on, rather
# than sleeping: OLM has to establish the ClusterLogForwarder CRD before that CR
# can be applied, CFK has to establish its CRDs before the Confluent CRs can be,
# and the log collector cannot verify a broker certificate that does not exist
# yet.
#
# Idempotent. Re-running against a partially built cluster resumes rather than
# duplicating; the two imperative steps (helm, openssl) are both no-ops when
# their output is already in place.
#
# Usage:
#   ./bootstrap.sh            # full bootstrap
#   ./bootstrap.sh --verify   # bootstrap, then consume from the topic to prove it
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFLUENT_NS="${CONFLUENT_NS:-confluent}"
LOGGING_NS="${LOGGING_NS:-openshift-logging}"
CFK_CHART_VERSION="${CFK_CHART_VERSION:-0.1718.10}"
VERIFY=0

[[ "${1:-}" == "--verify" ]] && VERIFY=1

log()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m    %s\033[0m\n' "$*"; }

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "required command not found: $1" >&2; exit 1; }
}
need oc
need helm
need openssl

oc whoami >/dev/null || { echo "not logged in to a cluster" >&2; exit 1; }
log "Target: $(oc whoami --show-server) as $(oc whoami)"

# ---------------------------------------------------------------- 00 namespaces
log "00: namespaces"
oc apply -k "$HERE/00-namespaces"

# ---------------------------------------------------------------- 10 operators
log "10a: Cluster Logging operator (OLM)"
oc apply -k "$HERE/10-operators/cluster-logging"

log "     waiting for the ClusterLogForwarder CRD to be established"
# The Subscription resolves an InstallPlan and installs a CSV before the CRD
# exists, so `oc wait` on the CRD would fail on a not-found object. Poll first.
for _ in $(seq 1 60); do
  oc get crd clusterlogforwarders.observability.openshift.io >/dev/null 2>&1 && break
  sleep 5
done
oc wait --for=condition=Established --timeout=300s \
  crd/clusterlogforwarders.observability.openshift.io

log "10b: Confluent for Kubernetes (Helm)"
# Deliberately not the OperatorHub bundle: confluent-for-kubernetes.v3.3.0 ships
# a ClusterRole with no batch/jobs rule and the operator CrashLoopBackOffs on a
# cache-sync timeout. The chart at the same app version grants it. See
# 10-operators/cfk/values.yaml.
helm repo add confluentinc https://packages.confluent.io/helm >/dev/null 2>&1 || true
helm repo update confluentinc >/dev/null
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes \
  --version "$CFK_CHART_VERSION" \
  --namespace "$CONFLUENT_NS" \
  -f "$HERE/10-operators/cfk/values.yaml" \
  --wait --timeout 5m

log "     waiting for CFK CRDs"
oc wait --for=condition=Established --timeout=120s \
  crd/kafkas.platform.confluent.io \
  crd/kraftcontrollers.platform.confluent.io \
  crd/controlcenters.platform.confluent.io

# ---------------------------------------------------------------- 20 certs
log "20: certificate authority"
"$HERE/20-certs/gen-ca.sh"

# ---------------------------------------------------------------- 30 confluent
log "30: Confluent Platform"
oc apply -k "$HERE/30-confluent"

# Point Control Center's Route at this cluster's ingress domain. The manifest
# carries a placeholder so the repo stays portable and free of any one
# cluster's hostnames; this is the only value patched at deploy time.
APPS_DOMAIN="$(oc get ingresses.config/cluster -o jsonpath='{.spec.domain}' 2>/dev/null || true)"
if [[ -n "$APPS_DOMAIN" ]]; then
  log "     routing Control Center at $APPS_DOMAIN"
  oc patch controlcenter controlcenter -n "$CONFLUENT_NS" --type=merge \
    -p "{\"spec\":{\"externalAccess\":{\"route\":{\"domain\":\"$APPS_DOMAIN\"}}}}"
else
  warn "could not read the cluster ingress domain; Control Center's Route will use the placeholder"
fi

log "     waiting for KRaft controller (this takes a few minutes)"
oc wait --for=jsonpath='{.status.phase}'=RUNNING --timeout=600s \
  -n "$CONFLUENT_NS" kraftcontroller/kraftcontroller

log "     waiting for Kafka"
oc wait --for=jsonpath='{.status.phase}'=RUNNING --timeout=600s \
  -n "$CONFLUENT_NS" kafka/kafka

# ---------------------------------------------------------------- 40 logging
log "40: log forwarding"
# The collector verifies the broker certificate against the CA generated in
# stage 20. Publish its public half into the logging namespace; the private key
# stays on disk and out of the cluster's logging side entirely.
oc create configmap confluent-kafka-ca \
  --from-file=ca.crt="$HERE/20-certs/generated/ca.pem" \
  --namespace "$LOGGING_NS" \
  --dry-run=client -o yaml | oc apply -f -

oc apply -k "$HERE/40-logging"

log "     waiting for the collector DaemonSet"
for _ in $(seq 1 60); do
  oc get daemonset -n "$LOGGING_NS" kafka-forwarder >/dev/null 2>&1 && break
  sleep 5
done
oc rollout status daemonset/kafka-forwarder -n "$LOGGING_NS" --timeout=300s || \
  warn "collector rollout did not complete; check: oc describe clusterlogforwarder/kafka-forwarder -n $LOGGING_NS"

# ---------------------------------------------------------------- 50 demo
log "50: demo workloads"
# Two copies of the same app in log-demo-a and log-demo-b. The forwarder's
# input glob only matches `log-demo-*`, so these are the only namespaces
# collected - and each lands in its own topic purely by namespace name.
oc apply -k "$HERE/50-demo"
for ns in log-demo-a log-demo-b; do
  oc rollout status deployment/log-demo -n "$ns" --timeout=300s || \
    warn "demo app in $ns did not become ready"
done

# ---------------------------------------------------------------- done
log "Bootstrap complete"
C3_HOST="$(oc get route -n "$CONFLUENT_NS" -o jsonpath='{.items[?(@.metadata.name=="controlcenter-bootstrap")].spec.host}' 2>/dev/null || true)"
[[ -n "$C3_HOST" ]] && echo "    Control Center: https://$C3_HOST"
echo "    Topics:         ocp-logs.<namespace>, one per collected namespace"
for ns in log-demo-a log-demo-b; do
  H="$(oc get route log-demo -n "$ns" -o jsonpath='{.spec.host}' 2>/dev/null || true)"
  [[ -n "$H" ]] && echo "    Demo app:       http://$H  (each request is logged)"
done
echo
echo "    Verify with: $HERE/verify.sh"

if [[ $VERIFY -eq 1 ]]; then
  "$HERE/verify.sh"
fi

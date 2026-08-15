#!/usr/bin/env bash
#
# Removes everything bootstrap.sh created, in reverse order.
#
# CRDs are deleted last and deliberately: leaving orphaned
# platform.confluent.io CRDs behind is what makes a "clean" cluster behave
# differently from a genuinely fresh one on the next bootstrap.
#
# The generated CA in 20-certs/generated is left on disk. Delete it by hand if
# you want the next bootstrap to mint a new one.
set -euo pipefail

CONFLUENT_NS="${CONFLUENT_NS:-confluent}"
LOGGING_NS="${LOGGING_NS:-openshift-logging}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

log "50: demo workloads"
oc delete -k "$HERE/50-demo" --ignore-not-found

log "40: log forwarding"
oc delete -k "$HERE/40-logging" --ignore-not-found
oc delete configmap confluent-kafka-ca -n "$LOGGING_NS" --ignore-not-found

log "30: Confluent Platform"
oc delete -k "$HERE/30-confluent" --ignore-not-found

log "     waiting for component pods to go away"
oc wait --for=delete pod -l app.kubernetes.io/part-of=confluent-platform \
  -n "$CONFLUENT_NS" --timeout=300s 2>/dev/null || true

log "20: certificate secret"
oc delete secret ca-pair-sslcerts -n "$CONFLUENT_NS" --ignore-not-found

log "10: operators"
helm uninstall confluent-operator --namespace "$CONFLUENT_NS" 2>/dev/null || true
oc delete -k "$HERE/10-operators/cluster-logging" --ignore-not-found
oc delete csv -n "$LOGGING_NS" -l operators.coreos.com/cluster-logging."$LOGGING_NS"= --ignore-not-found 2>/dev/null || true

log "     Confluent CRDs"
CRDS="$(oc get crd -o name 2>/dev/null | grep platform.confluent.io || true)"
[[ -n "$CRDS" ]] && oc delete $CRDS --ignore-not-found

log "00: namespaces"
oc delete namespace "$CONFLUENT_NS" --ignore-not-found
# openshift-logging is left in place: it is a standard OpenShift namespace and
# other logging components may depend on it.

log "Teardown complete"

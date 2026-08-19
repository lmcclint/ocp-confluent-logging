# ocp-confluent-logging

A minimal, repeatable Confluent Platform deployment on OpenShift, wired to a
`ClusterLogForwarder` so OpenShift ships its own logs into Kafka over TLS.

Built as a test lab: single broker, single KRaft controller, no SASL, no RBAC.
It is deliberately the smallest thing that proves the pipeline end to end.

```
  namespaces                                       confluent namespace
 ┌──────────┐                                  ┌──────────────────────────┐
 │log-demo-a│─┐                                │ Kafka ── KRaft ctrl      │
 │log-demo-b│─┤  ┌──────────────────┐ tls:9071 │  ├─ ocp-logs.log-demo-a  │
 └──────────┘ └─►│ collector (DS)   ├────────► │  ├─ ocp-logs.log-demo-b  │
  audit logs────►│ openshift-logging│          │  ├─ ocp-audit            │
                 └──────────────────┘          │  ├─ Schema Registry      │
                         ▲                     │  └─ Control Center       │
               ClusterLogForwarder             └──────────────────────────┘
               ├─ application (log-demo-*)           ▲
               └─ audit (all nodes)                  │ certs signed by
                                               one self-signed CA (20-certs)
```

Application logs are routed to **one topic per namespace** —
`ocp-logs.<namespace>` — with no per-application configuration. Two identical
demo apps in two namespaces land in two separate topics purely by where they
run. Audit logs (kube-apiserver, openshift-apiserver, host auditd, OVN) go to
a single `ocp-audit` topic.
## Quick start

```bash
oc login …            # cluster-admin
./bootstrap.sh --verify
```

Roughly 10–15 minutes on a cold cluster, most of it pulling ~1GB Confluent
images. `--verify` finishes by running a TLS console consumer against the topic
and printing real log records.

Requires `oc`, `helm`, and `openssl` on the client, and a default StorageClass
with `ReadWriteOnce` on the cluster.

## Layout

| Path | What it does |
|---|---|
| `00-namespaces/` | `confluent` and `openshift-logging` |
| `10-operators/cluster-logging/` | OperatorGroup + Subscription, Red Hat logging `stable-6.4` |
| `10-operators/cfk/values.yaml` | Helm values for Confluent for Kubernetes |
| `20-certs/gen-ca.sh` | Mints the CA, loads it as `ca-pair-sslcerts` |
| `30-confluent/` | KRaftController, Kafka, Schema Registry, Control Center + Route, topic overrides |
| `40-logging/` | ServiceAccount, collector RBAC, ClusterLogForwarder |
| `50-demo/` | Two demo apps in `log-demo-a` / `log-demo-b` |
| `bootstrap.sh` / `teardown.sh` / `verify.sh` | Ordered apply, removal, proof |

Every numbered directory is `oc apply -k`-able on its own. `bootstrap.sh` only
adds ordering and the waits between stages, so promoting this to Argo CD later
is mostly a matter of replacing those waits with sync waves.

This repo contains **no cluster-specific hostnames**. Control Center and the
demo apps each get a plain OpenShift `Route` with no `host` field, so OpenShift
generates one from the resource and namespace names plus the cluster's own
ingress domain — `controlcenter-confluent.apps.<your-cluster>`. Nothing needs
patching per environment.

That is why Control Center's Route lives in its own
`30-confluent/controlcenter-route.yaml` rather than using CFK's
`spec.externalAccess`: CFK's route support makes `domain` a required field,
which would force one cluster's hostname into the manifest.

Only two steps are imperative: the Helm install and the `openssl` CA
generation. Both are idempotent, so re-running `bootstrap.sh` against a
half-built cluster resumes instead of duplicating.

## Four things that will bite you

These are not hypotheticals. Each one was hit while building this.

### 1. Do not install CFK from OperatorHub

The certified bundle `confluent-for-kubernetes.v3.3.0` ships a ClusterRole with
no `batch/jobs` rule, but the 3.3.0 operator binary starts a cluster-scoped Job
informer. The cache never syncs and the operator CrashLoopBackOffs after two
minutes:

```
failed to wait for kraftmigrationjob caches to sync
jobs.batch is forbidden: User "system:serviceaccount:openshift-operators:confluent-for-kubernetes"
  cannot list resource "jobs" in API group "batch" at the cluster scope
```

The Helm chart at the *identical* app version (chart `0.1718.10`, app `3.3.0`)
does grant `batch/jobs`. This repo installs from the chart, scoped to the
`confluent` namespace — which additionally renders the operator's RBAC as a
namespaced Role, keeping the Job informer namespace-scoped.

If you previously installed from OperatorHub, remove the Subscription **and**
the orphaned CRDs before bootstrapping; OLM leaves `platform.confluent.io` CRDs
behind, and a cluster carrying them does not behave like a fresh one.

### 2. Bound the topic by size, or the firehose kills the broker

`application` + `infrastructure` across every node is a lot of data — on a
7-node cluster it wrote **20Gi in about eight minutes**. Kafka's default
`log.retention.bytes` is `-1`, unbounded, so time-based retention alone cannot
save you.

Because `__cluster_metadata` shares the broker's data volume, filling the disk
does not merely drop logs. It kills the broker outright, with no recovery:

```
Error while appending records to __cluster_metadata-0 in dir /mnt/data/data0/logs
java.io.IOException: No space left on device
```

This is why `kafka.yaml` sets a cluster-wide `log.retention.bytes` of 1Gi per
partition. It matters more now than when it was written: topics are created on
demand per namespace, so no topic carries its own cap and the broker-wide
default is the only bound on any of them.

The current configuration keeps this in check two ways — `application` logs
only, and only from namespaces matching `log-demo-*`. Re-enabling the
infrastructure pipeline removes both protections at once.

### 3. Control Center pins the whole stack to 7.9.x

There is no `cp-enterprise-control-center:8.x` on Docker Hub — CP 8 replaced
classic Control Center with Control Center next-gen, which needs its own
Prometheus and Alertmanager pods. Rather than run a mixed-version stack, every
component here is pinned to **7.9.9** with the **3.3.0** init container (which
tracks the CFK version, not the CP version).

Kafka 8.1.0 works fine if you drop Control Center. Note that KRaft metadata
versions cannot be downgraded, so switching 8.x → 7.9.x means deleting the CRs
*and* their PVCs, not just editing the image tag.

### 4. Control Center renders a blank page if you size it by request alone

CFK derives the JVM heap from the pod's memory **request**, and it hands the
JVM the entire request as `-Xmx`. With `memory: 2Gi` that produced `-Xmx2G` and
no headroom whatsoever for RocksDB state stores (off-heap), direct buffers or
metaspace. The result was not a crash — it was worse, because it looks healthy:

```
pod 1/1 Running, no restarts, HTTP 200 on every request
but: 3812Mi RSS against a 2Gi request, CPU pinned at its 500m request
```

The UI serves `index.html` and the JS bundle fine, then the SPA's API calls
never get answered because the backend is permanently collecting garbage. In
the browser that reads as a blank or barely-loading page, and in C3's access
log you see repeated `GET /` with no API requests following them.

`controlcenter.yaml` therefore sets `-Xmx` explicitly via `configOverrides.jvm`
rather than letting the request imply it, gives the pod real limits, and cuts
`streams.num.stream.threads` to 2 (C3 otherwise runs a thread per core across
several topologies, each with its own RocksDB state):

| | before | after |
|---|---|---|
| memory | 3812Mi | ~1050Mi |
| CPU | 461m | ~100m |
| `GET /` | slow / blank UI | 48ms |

If you resize Control Center, set the heap explicitly — do not just raise the
memory request and assume the JVM will leave itself room.

## TLS

`gen-ca.sh` mints one self-signed CA. CFK's `tls.autoGeneratedCerts: true`
uses it to issue per-component server certificates:

```
subject = CN=kafka
issuer  = CN=ocp-confluent-logging-ca
SAN     = kafka, kafka.confluent, kafka.confluent.svc,
          kafka.confluent.svc.cluster.local, *.kafka.confluent, …
```

`bootstrap.sh` copies the public half into `openshift-logging` as the ConfigMap
`confluent-kafka-ca`, which the forwarder's `tls.ca` points at. One CA, trusted
at both ends.

The private key lives only in `20-certs/generated/` and is gitignored — the one
piece of state this repo cannot be fully declarative about. Regenerating it
invalidates every certificate CFK has already issued, so `gen-ca.sh` refuses to
overwrite without `--force`.

Two TLS details CFK will not infer for you:

- `dependencies.kRaftController.controllerListener.tls.enabled` on the **Kafka**
  CR must mirror `listeners.controller.tls.enabled` on the **KRaftController**
  CR. Without it the broker dials the SSL controller listener in plaintext and
  never registers: `SSL handshake failed caused by Unrecognized SSL message,
  plaintext connection?`
- `metricReporter.tls.enabled` must be set once the internal listener is TLS,
  or CFK rejects the Kafka CR outright.

## Topic per namespace

The forwarder's output topic is a template, not a fixed name:

```yaml
topic: 'ocp-logs.{.kubernetes.namespace_name||"unknown"}'
```

The `||` fallback is **mandatory** — the API rejects a dynamic value without
one. It catches records with no namespace (anything not from a pod), which
would otherwise have nowhere to go. With an application-only pipeline nothing
should reach `ocp-logs.unknown`; records showing up there mean something is
being collected that you did not expect.

Because the names are not known in advance, they cannot be pre-created as
`KafkaTopic` CRs — the broker creates them on demand, which is why
`kafka.yaml` sets `auto.create.topics.enable=true` and
`default.replication.factor=1` (auto-creation otherwise tries for RF 3 and
fails on a single broker).

The trade-off: auto-created topics get **no per-topic retention cap**, so the
cluster-wide `log.retention.bytes` is the only thing bounding them — unless you
override it per topic.

### Overriding settings for one namespace

`30-confluent/topic-overrides.yaml` pins `ocp-logs.log-demo-b` tighter than the
cluster default, while `log-demo-a` is deliberately left alone so the two can
be compared:

| topic | KafkaTopic CR | effective `retention.bytes` |
|---|---|---|
| `ocp-logs.log-demo-b` | yes | 100Mi (`DYNAMIC_TOPIC_CONFIG`) |
| `ocp-logs.log-demo-a` | no | 1Gi (inherited `STATIC_BROKER_CONFIG`) |

```bash
oc exec kafka-0 -n confluent -c kafka -- \
  kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name ocp-logs.log-demo-b --describe
```

```
retention.bytes=104857600 synonyms={
    DYNAMIC_TOPIC_CONFIG:retention.bytes=104857600,       <- the CR
    STATIC_BROKER_CONFIG:log.retention.bytes=1073741824,  <- kafka.yaml
    DEFAULT_CONFIG:log.retention.bytes=-1 }               <- Kafka default
```

CFK **adopts** a topic that already exists rather than failing or recreating
it. Applying this CR against a topic already holding 1572 records altered it in
place with every record intact, so these can safely be added after the fact,
once you discover a namespace needs different retention.

Deleting one of these CRs is untested here — do not assume the underlying topic
survives it. To put a topic back on cluster defaults, change the config
explicitly rather than deleting the CR.

Note also that CFK's `KafkaTopic` status does not self-heal against changes
made outside it: a CR whose topic was deleted directly on the broker still
reported `CREATED`. Trust `kafka-configs`/`kafka-topics` over CR status when
they disagree.

## Scope: which namespaces are collected

A named input, not the built-in `application`, so only the demo namespaces are
picked up:

```yaml
inputs:
  - name: demo-apps
    type: application
    application:
      includes:
        - namespace: "log-demo-*"
```

This filters on **namespace name**, not namespace labels — the API has no
namespace-label selector. Your options are:

| Want | Use |
|---|---|
| Namespaces by name/pattern | `includes` / `excludes` with globs |
| Opt-in per workload | `selector:` — matches **pod** labels, so every pod must carry one |
| Everything | Drop the custom input, use the built-in `application` |

**Audit logs** are on by default, forwarded to a fixed `ocp-audit` topic. This
covers kube-apiserver, openshift-apiserver, host auditd, and OVN audit — the
"who did what" records useful for compliance and forensics.

**Infrastructure logs** are deliberately off. A commented-out pipeline in
`40-logging/clusterlogforwarder.yaml` turns them back on — the
`collect-infrastructure-logs` binding is already in place, so it validates as
soon as you uncomment it. Mind the volume: cluster-wide infrastructure logs
wrote 20Gi in about eight minutes on a 7-node cluster. Infrastructure records
carry no namespace, so they need their own output with a fixed topic (e.g.
`ocp-infra`) rather than the per-namespace template.

## Demo apps

`50-demo/` deploys the same tiny Python app into `log-demo-a` and
`log-demo-b`. Each one emits a JSON line every 10 seconds, and one more per
HTTP request:

```bash
curl http://$(oc get route log-demo -n log-demo-a -o jsonpath='{.spec.host}')/hello
```

```json
{"ts":"…","namespace":"log-demo-a","pod":"log-demo-…","event":"request",
 "hit":1,"path":"/hello","message":"handled request 1 in log-demo-a"}
```

That record appears in `ocp-logs.log-demo-a` and **not** in
`ocp-logs.log-demo-b`, which is the point of the exercise — two identical
deployments, separated by namespace alone.

The namespaces must keep matching the `log-demo-*` glob, or their logs are
silently never collected.

## Verifying

```bash
./verify.sh                              # lists ocp-* topics then reads log-demo-a
TOPIC=ocp-logs.log-demo-b ./verify.sh    # the other namespace
TOPIC=ocp-audit ./verify.sh              # audit logs
MESSAGES=50 ./verify.sh                  # more records
```

It runs a throwaway `kafka-console-consumer` in-cluster with the CA mounted,
and asserts on the **record count** — `kafka-console-consumer` exits 0 even
when it times out having read nothing, so checking the pod's exit status would
report success against an empty topic.

Control Center is at the Route printed by `bootstrap.sh`
(`oc get route -n confluent`). It is passthrough TLS with a private CA, so
expect a browser warning.

## Teardown

```bash
./teardown.sh
```

Removes everything in reverse, including the Confluent CRDs. It leaves the
`openshift-logging` namespace (standard OpenShift) and the generated CA on
disk; delete `20-certs/generated/` by hand to force a new one.

## Adapting it

- **Add SASL**: add an `authentication` block to `listeners.internal` in
  `kafka.yaml`, and a matching one under `kafka:` in the forwarder output.
- **Scale out**: raise `replicas` on the Kafka and KRaftController CRs and drop
  the single-node `configOverrides` in `kafka.yaml`.
- **More namespaces**: widen the `includes` glob on the `demo-apps` input, or
  drop the custom input entirely to collect every application namespace. New
  topics appear on their own as logs arrive.
- **Different log sources**: edit `inputRefs`. Each source needs the matching
  `collect-*-logs` ClusterRoleBinding in `40-logging/rbac.yaml`, or the
  forwarder fails validation. All three bindings are already in place.

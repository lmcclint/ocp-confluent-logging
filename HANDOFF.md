# Handoff

State of the work as of 2026-08-19, for picking up on another machine or in a
new session. `README.md` documents how the thing *works*; this file records
what is **proven**, what is **not**, and what to do next.

## Where it stands

Everything in this repo ran successfully on a 7-node OpenShift 4.18 cluster
(`ocp1`, vSphere, `thin-csi` default StorageClass). Verified end to end:

- CFK 3.3.0 operator healthy (Helm, namespace-scoped to `confluent`)
- Cluster Logging 6.4.6 healthy, `ClusterLogForwarder` `Ready=True`
- Single-broker KRaft Kafka on CP 7.9.9, TLS on the internal listener
- Application logs from `log-demo-a` / `log-demo-b` landing in
  `ocp-logs.log-demo-a` / `ocp-logs.log-demo-b`
- Per-namespace routing genuinely isolating: a marker sent to app A appeared
  in A's topic and was absent from B's
- Per-topic override via `KafkaTopic` CR taking precedence over the broker
  default, adopting an existing topic without data loss (1572 records intact)
- Control Center reachable and responsive after the JVM heap fix

## What is NOT proven

Read this before claiming anything works.

1. **`bootstrap.sh` has never run start to finish.** The cluster was built
   incrementally, fixing problems between stages. Every stage has run and every
   manifest has been applied, but never as one uninterrupted script. Ordering,
   waits and timeouts are written but untested as a unit. **This is the single
   most important next step.**
2. **`teardown.sh` has never been run at all.** Not once, in any form.
3. **The Control Center Route change is untested.** Commit `44cab69` used CFK's
   `spec.externalAccess` with a hardcoded domain. That was replaced with a
   standalone `30-confluent/controlcenter-route.yaml` with no `host`, so
   OpenShift generates it. The service name (`controlcenter`), port name
   (`external`) and `passthrough` termination were taken from the Route CFK
   itself created earlier, so they should be right — but the `oc` session had
   expired by then and it was never applied. **Verify this first.**
4. **Deleting a `KafkaTopic` CR** — unknown whether it deletes the underlying
   topic. Do not test on a topic holding data you want.

## Next steps, in order

1. **Verify the new Control Center Route.**
   ```bash
   oc login …
   oc apply -k 30-confluent/
   oc get route controlcenter -n confluent      # expect an auto-generated host
   curl -sk -o /dev/null -w '%{http_code}\n' "https://$(oc get route controlcenter -n confluent -o jsonpath='{.spec.host}')/"
   ```
   Expect `200`. If the Route has no endpoints, check the port name:
   `oc get svc controlcenter -n confluent -o jsonpath='{.spec.ports[*].name}'`.
   Note the old CFK-managed Route was named `controlcenter-bootstrap`; removing
   `externalAccess` should delete it, and the new one is just `controlcenter`.

2. **Do a clean-room run.** This is the whole point of the repo and the main
   outstanding gap. On a *fresh* cluster:
   ```bash
   ./bootstrap.sh --verify
   ```
   Watch for: stage waits that are too short on slower storage, and the
   Docker Hub pull of ~1GB Confluent images. Fix whatever ordering assumptions
   turn out to be wrong.

3. **Then test `teardown.sh`**, and confirm a second `bootstrap.sh` after it
   still works. Leftover `platform.confluent.io` CRDs are the likely failure
   mode — they make a "clean" cluster behave unlike a fresh one.

4. **Optional, if the demo needs it:** uncomment the infrastructure pipeline in
   `40-logging/clusterlogforwarder.yaml`. Read gotcha #2 in the README first —
   infrastructure logs are what filled a 20Gi volume in eight minutes.

## Environment notes

- **Do not install CFK from OperatorHub.** README gotcha #1. This wasted real
  time; the operator CrashLoopBackOffs and the cause is not obvious.
- The CA private key lives in `20-certs/generated/` and is gitignored. It does
  **not** travel with the repo. On a new machine `gen-ca.sh` mints a fresh one,
  which is fine for a new cluster. Pointing a new client at an *existing*
  deployment would need the original key copied across out of band.
- `oc` sessions on this cluster expire after a day or so; re-login before
  assuming something is broken.

## Repo / account context

- Origin: `https://github.com/leemmcc/ocp-confluent-logging` (**private**),
  owned by the personal account `leemmcc`.
- `lmcclint` (work account) was invited as a collaborator with write access.
  The invitation may still be pending.
- Intent is to relocate this to a work repo. Cleaner than forking:
  ```bash
  git clone https://github.com/leemmcc/ocp-confluent-logging
  cd ocp-confluent-logging
  git remote set-url origin https://github.com/<work-org>/ocp-confluent-logging
  git push -u origin master
  ```
- The repo deliberately contains **no cluster hostnames, IPs or credentials**.
  Keep it that way — Routes omit `host` so OpenShift generates one.

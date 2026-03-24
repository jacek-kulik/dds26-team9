#!/usr/bin/env bash
set -u

TS="$(date +%F-%H%M%S)"
OUT="pod-forensics-$TS"
mkdir -p "$OUT"/{describe,logs,previous,inspect,files,meta}

echo "[*] Output: $OUT"

kubectl get pods -A -o wide > "$OUT/meta/pods.txt" 2>&1 || true
kubectl get events -A --sort-by=.lastTimestamp > "$OUT/meta/events.txt" 2>&1 || true

while read -r ns pod; do
  [ -z "$ns" ] && continue
  echo "[*] Pod: $ns/$pod"

  kubectl describe pod -n "$ns" "$pod" > "$OUT/describe/${ns}__${pod}.txt" 2>&1 || true

  containers="$(kubectl get pod -n "$ns" "$pod" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null || true)"

  for c in $containers; do
    echo "    [-] Container: $c"

    kubectl logs -n "$ns" "$pod" -c "$c" --timestamps \
      > "$OUT/logs/${ns}__${pod}__${c}.log" 2>&1 || true

    kubectl logs -n "$ns" "$pod" -c "$c" --previous --timestamps \
      > "$OUT/previous/${ns}__${pod}__${c}__previous.log" 2>&1 || true

    kubectl exec -n "$ns" "$pod" -c "$c" -- sh -lc '
      echo "=== date ==="
      date || true
      echo

      echo "=== hostname ==="
      hostname || true
      echo

      echo "=== ps ==="
      ps aux || ps -ef || true
      echo

      echo "=== pid1 cmdline ==="
      tr "\0" " " < /proc/1/cmdline 2>/dev/null || true
      echo
      echo

      echo "=== pid1 stdout/stderr fds ==="
      ls -l /proc/1/fd/1 /proc/1/fd/2 2>/dev/null || true
      echo

      echo "=== env ==="
      env | sort || true
      echo

      echo "=== common log candidates ==="
      find /var/log /tmp /app /home /usr/src /workspace / -maxdepth 4 \
        \( -name "*.log" -o -name "*.out" -o -name "*.txt" \) 2>/dev/null | sort | head -200 || true
      echo

      echo "=== recent files (possible logs) ==="
      find /var/log /tmp /app /home /usr/src /workspace / -maxdepth 4 -type f 2>/dev/null \
        | xargs -r ls -lt 2>/dev/null | head -200 || true
      echo
    ' > "$OUT/inspect/${ns}__${pod}__${c}.txt" 2>&1 || true

    kubectl exec -n "$ns" "$pod" -c "$c" -- sh -lc '
      find /var/log /tmp /app /home /usr/src /workspace / -maxdepth 4 \
        \( -name "*.log" -o -name "*.out" -o -name "*.txt" \) 2>/dev/null | sort | head -50
    ' > "$OUT/inspect/${ns}__${pod}__${c}__filelist.txt" 2>&1 || true

    mkdir -p "$OUT/files/${ns}__${pod}__${c}"

    while read -r f; do
      [ -z "$f" ] && continue
      safe="$(echo "$f" | sed 's#/#__#g')"
      echo "        [copy] $f"
      kubectl exec -n "$ns" "$pod" -c "$c" -- sh -lc "cat \"$f\" 2>/dev/null" \
        > "$OUT/files/${ns}__${pod}__${c}/${safe}" 2>/dev/null || true
    done < "$OUT/inspect/${ns}__${pod}__${c}__filelist.txt"

  done
done < <(kubectl get pods -A --no-headers -o custom-columns='NS:.metadata.namespace,POD:.metadata.name')

echo
echo "[*] Done. Look at:"
echo "    $OUT/inspect/"
echo "    $OUT/files/"
echo "    $OUT/describe/"
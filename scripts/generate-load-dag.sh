#!/usr/bin/env bash
set -euo pipefail

task_count="${1:-1000}"
output="${2:-}"

emit_json() {
  printf '{\n'
  printf '  "dagId": "load-test-%s",\n' "$task_count"
  printf '  "name": "Load Test %s Tasks",\n' "$task_count"
  printf '  "tasks": [\n'
  for i in $(seq 1 "$task_count"); do
    task_id="task-$i"
    if [ "$i" -eq 1 ]; then
      depends='[]'
    else
      depends='["task-1"]'
    fi
    comma=","
    if [ "$i" -eq "$task_count" ]; then
      comma=""
    fi
    printf '    {"taskId": "%s", "type": "http_call", "dependsOn": %s, "config": {}, "timeoutSecs": 30, "retries": 3}%s\n' "$task_id" "$depends" "$comma"
  done
  printf '  ]\n'
  printf '}\n'
}

if [ -n "$output" ]; then
  mkdir -p "$(dirname "$output")"
  emit_json > "$output"
  echo "$output"
else
  emit_json
fi
